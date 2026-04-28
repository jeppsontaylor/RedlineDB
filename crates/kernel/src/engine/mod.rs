pub mod concurrent_heap;
pub mod lock;
pub mod page_heap;
pub mod tx;

use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::catalog::{
    CatalogManager, CatalogStore, SchemaEpoch, SqliteSchemaRow, apply_create_index,
    apply_create_table, apply_drop_index, apply_drop_table, bootstrap_schema, lookup_table,
};
use crate::engine::lock::RowLockManager;
use crate::engine::page_heap::{PageBackedHeap, VacuumStats};
use crate::engine::tx::{ConcurrentTxStatus, TxStatusStats};
use crate::format::{Csn, DEFAULT_PAGE_SIZE, Lsn, Page, RelId, RowId, TxId};
use crate::storage::{
    BufferPool, BufferPoolStats, ControlFile, ControlStore, DEFAULT_CHECKPOINT_BATCH_PAGES,
    PageFile, TxStatusCheckpoint, TxStatusStore,
};
use crate::txn::Isolation;
use crate::wal::{
    WalConfig, WalCoordinator, WalPayload, WalReader, WalRecord, WalRecordKind, WalScanReport,
};
use crate::{Error, Result};

pub use tx::Txn;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckpointStats {
    pub control: ControlFile,
    pub flushed_pages: usize,
    pub flush_batches: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageStatsSnapshot {
    pub buffer: BufferPoolStats,
    pub tx: TxStatusStats,
    pub checkpoint: Option<ControlFile>,
    pub resident_heap_pages: usize,
    pub wal_written_lsn: Lsn,
    pub wal_durable_lsn: Lsn,
    pub vacuum_horizon_csn: Csn,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryReport {
    pub scanned_records: usize,
    pub valid_end_lsn: Lsn,
    pub torn_tail: bool,
    pub page_images_redone: usize,
    pub legacy_mutations_redone: usize,
    pub commits_recovered: usize,
    pub replay_from_lsn: Lsn,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct RecoveryMetrics {
    page_images_redone: usize,
    legacy_mutations_redone: usize,
    commits_recovered: usize,
}

impl RecoveryReport {
    fn from_scan(scan: WalScanReport, metrics: RecoveryMetrics, replay_from_lsn: Lsn) -> Self {
        Self {
            scanned_records: scan.records.len(),
            valid_end_lsn: scan.valid_end_lsn,
            torn_tail: scan.torn_tail,
            page_images_redone: metrics.page_images_redone,
            legacy_mutations_redone: metrics.legacy_mutations_redone,
            commits_recovered: metrics.commits_recovered,
            replay_from_lsn,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineConfig {
    pub rel_id: RelId,
    pub wal: WalConfig,
    pub lock_shards: usize,
    pub heap_lanes: usize,
    pub page_size: usize,
    pub buffer_pool_pages: usize,
    pub data_file_name: String,
}

impl Default for EngineConfig {
    fn default() -> Self {
        let parallelism = std::thread::available_parallelism()
            .map(|value| value.get())
            .unwrap_or(4);
        Self {
            rel_id: RelId(1),
            wal: WalConfig::default(),
            lock_shards: (parallelism * 4).max(16),
            heap_lanes: parallelism.max(4),
            page_size: DEFAULT_PAGE_SIZE,
            buffer_pool_pages: 1024,
            data_file_name: "data.redline".to_owned(),
        }
    }
}

#[derive(Debug)]
pub struct Engine {
    rel_id: RelId,
    txs: ConcurrentTxStatus,
    #[allow(dead_code)]
    buffer: Arc<BufferPool>,
    heap: PageBackedHeap,
    catalog: CatalogManager,
    catalog_store: CatalogStore,
    locks: RowLockManager,
    wal: Arc<WalCoordinator>,
    control: ControlStore,
    tx_status_store: TxStatusStore,
    checkpoint: Mutex<Option<ControlFile>>,
}

impl Engine {
    pub fn create(path: impl AsRef<Path>, config: EngineConfig) -> Result<Arc<Self>> {
        std::fs::create_dir_all(path.as_ref())?;
        let page_file = Arc::new(PageFile::create(
            path.as_ref().join(&config.data_file_name),
            config.page_size,
        )?);
        let buffer = Arc::new(BufferPool::new(page_file, config.buffer_pool_pages)?);
        let wal = Arc::new(WalCoordinator::create(
            path.as_ref().join("wal"),
            config.wal,
        )?);
        let control = ControlStore::new(path.as_ref())?;
        let tx_status_store = TxStatusStore::new(path.as_ref())?;
        let checkpoint = control.load_latest()?;
        let catalog_store = CatalogStore::new(path.as_ref());
        let loaded_catalog = catalog_store.load()?;
        let initial_catalog = loaded_catalog
            .clone()
            .unwrap_or_else(|| bootstrap_schema(RelId(10_000)));
        if loaded_catalog.is_none() {
            catalog_store.save(&initial_catalog)?;
        }
        let buffer = Arc::clone(&buffer);
        Ok(Arc::new(Self {
            rel_id: config.rel_id,
            txs: ConcurrentTxStatus::new(),
            buffer: Arc::clone(&buffer),
            heap: PageBackedHeap::new_with_wal(
                config.rel_id,
                config.heap_lanes,
                buffer,
                Some(Arc::clone(&wal)),
            )?,
            catalog: CatalogManager::new(initial_catalog),
            catalog_store,
            locks: RowLockManager::new(config.lock_shards),
            wal,
            control,
            tx_status_store,
            checkpoint: Mutex::new(checkpoint),
        }))
    }

    pub fn open(path: impl AsRef<Path>, config: EngineConfig) -> Result<Arc<Self>> {
        Self::open_with_recovery_report(path, config).map(|(engine, _report)| engine)
    }

    pub fn open_with_recovery_report(
        path: impl AsRef<Path>,
        config: EngineConfig,
    ) -> Result<(Arc<Self>, RecoveryReport)> {
        let wal_dir = path.as_ref().join("wal");
        let mut reader = WalReader::new(&wal_dir, config.wal.clone());
        let scan_report = reader.scan_report()?;
        let txs = ConcurrentTxStatus::new();
        std::fs::create_dir_all(path.as_ref())?;
        let control = ControlStore::new(path.as_ref())?;
        let tx_status_store = TxStatusStore::new(path.as_ref())?;
        let checkpoint = control.load_latest()?;
        let page_path = path.as_ref().join(&config.data_file_name);
        let page_file = if checkpoint.is_some() {
            Arc::new(PageFile::open(page_path, config.page_size)?)
        } else {
            Arc::new(PageFile::create(page_path, config.page_size)?)
        };
        let buffer = Arc::new(BufferPool::new(page_file, config.buffer_pool_pages)?);
        let wal = Arc::new(WalCoordinator::open(wal_dir, config.wal)?);
        let heap = PageBackedHeap::new_with_wal(
            config.rel_id,
            config.heap_lanes,
            Arc::clone(&buffer),
            Some(Arc::clone(&wal)),
        )?;
        let catalog_store = CatalogStore::new(path.as_ref());
        let initial_catalog = catalog_store
            .load()?
            .unwrap_or_else(|| bootstrap_schema(RelId(10_000)));
        let replay_from_lsn = if let Some(checkpoint) = checkpoint {
            let tx_status = tx_status_store.load(checkpoint.generation)?;
            if tx_status.generation != checkpoint.generation {
                return Err(Error::CorruptPage(
                    "tx status checkpoint generation mismatch",
                ));
            }
            for (tx_id, csn) in tx_status.entries {
                txs.publish_recovered_commit(tx_id, csn);
            }
            txs.restore_frontier(
                tx_status.next_tx,
                tx_status.next_csn,
                tx_status.published_csn,
            );
            checkpoint.checkpoint_lsn
        } else {
            Lsn::ZERO
        };
        let metrics = recover_heap(&scan_report.records, replay_from_lsn, &txs, &heap)?;
        let page_count = heap.page_count()?;
        heap.load_row_directory_from_pages(page_count)?;
        heap.load_reusable_pages_from_pages(page_count)?;
        let catalog = CatalogManager::new(initial_catalog);
        let engine = Arc::new(Self {
            rel_id: config.rel_id,
            txs,
            buffer: Arc::clone(&buffer),
            heap,
            catalog,
            catalog_store,
            locks: RowLockManager::new(config.lock_shards),
            wal,
            control,
            tx_status_store,
            checkpoint: Mutex::new(checkpoint),
        });
        Ok((
            engine,
            RecoveryReport::from_scan(scan_report, metrics, replay_from_lsn),
        ))
    }

    pub fn begin(&self, isolation: Isolation) -> Result<Txn> {
        if isolation == Isolation::Serializable {
            return Err(Error::UnsupportedIsolation);
        }
        Ok(self.txs.begin_txn(isolation))
    }

    pub fn get(&self, tx: &mut Txn, row_id: RowId) -> Result<Option<Vec<u8>>> {
        tx.ensure_open()?;
        self.refresh_read_committed(tx);
        let snapshot = tx.snapshot().clone();
        self.heap.get(&self.txs, &snapshot, Some(tx.id()), row_id)
    }

    pub fn insert(&self, tx: &mut Txn, payload: Vec<u8>) -> Result<RowId> {
        tx.ensure_open()?;
        self.refresh_read_committed(tx);
        let row_id = self.heap.reserve_row_id();
        self.heap
            .insert_with_row_id(tx.id(), row_id, payload, Lsn(1))?;
        Ok(row_id)
    }

    pub fn insert_for_relation(
        &self,
        tx: &mut Txn,
        rel_id: RelId,
        row_id: RowId,
        payload: Vec<u8>,
    ) -> Result<()> {
        tx.ensure_open()?;
        self.refresh_read_committed(tx);
        self.heap
            .insert_for_relation(tx.id(), rel_id, row_id, payload, Lsn(1))
    }

    pub fn reserve_row_id(&self) -> RowId {
        self.heap.reserve_row_id()
    }

    pub fn insert_with_row_id(&self, tx: &mut Txn, row_id: RowId, payload: Vec<u8>) -> Result<()> {
        tx.ensure_open()?;
        self.refresh_read_committed(tx);
        self.heap
            .insert_with_row_id(tx.id(), row_id, payload, Lsn(1))
    }

    pub fn update(&self, tx: &mut Txn, row_id: RowId, payload: Vec<u8>) -> Result<()> {
        tx.ensure_open()?;
        self.refresh_read_committed(tx);
        self.lock_row(tx, row_id)?;
        self.refresh_read_committed(tx);
        self.heap
            .update(tx.id(), tx.snapshot(), &self.txs, row_id, payload, Lsn(1))
    }

    pub fn update_for_relation(
        &self,
        tx: &mut Txn,
        rel_id: RelId,
        row_id: RowId,
        payload: Vec<u8>,
    ) -> Result<()> {
        tx.ensure_open()?;
        self.refresh_read_committed(tx);
        self.lock_row_in_rel(tx, rel_id, row_id)?;
        self.refresh_read_committed(tx);
        self.heap.update_for_relation(
            tx.id(),
            tx.snapshot(),
            &self.txs,
            rel_id,
            row_id,
            payload,
            Lsn(1),
        )
    }

    pub fn delete(&self, tx: &mut Txn, row_id: RowId) -> Result<()> {
        tx.ensure_open()?;
        self.refresh_read_committed(tx);
        self.lock_row(tx, row_id)?;
        self.refresh_read_committed(tx);
        self.heap
            .delete(tx.id(), tx.snapshot(), &self.txs, row_id, Lsn(1))
    }

    pub fn delete_for_relation(&self, tx: &mut Txn, rel_id: RelId, row_id: RowId) -> Result<()> {
        tx.ensure_open()?;
        self.refresh_read_committed(tx);
        self.lock_row_in_rel(tx, rel_id, row_id)?;
        self.refresh_read_committed(tx);
        self.heap
            .delete_for_relation(tx.id(), tx.snapshot(), &self.txs, rel_id, row_id, Lsn(1))
    }

    pub fn commit(&self, mut tx: Txn) -> Result<Csn> {
        tx.ensure_open()?;
        let pending_schema = tx.pending_schema_snapshot();

        let commit_result = match self
            .wal
            .append_commit(tx.id(), || self.txs.reserve_commit_csn())
        {
            Ok((csn, append)) => match self.wal.flush_until(append.end_lsn) {
                Ok(_) => {
                    self.txs.publish_commit(tx.id(), csn);
                    Ok(csn)
                }
                Err(err) => {
                    self.txs.cancel_reserved_csn(csn);
                    Err(err)
                }
            },
            Err(err) => Err(err),
        };

        match commit_result {
            Ok(csn) => {
                let schema_result = if let Some(snapshot) = pending_schema {
                    match self.catalog_store.save(&snapshot) {
                        Ok(()) => {
                            self.catalog.publish(snapshot);
                            Ok(())
                        }
                        Err(err) => Err(err),
                    }
                } else {
                    Ok(())
                };
                self.release_locks(&mut tx);
                tx.close();
                schema_result.map(|_| csn)
            }
            Err(err) => {
                self.txs.abort(tx.id());
                self.release_locks(&mut tx);
                tx.close();
                Err(err)
            }
        }
    }

    pub fn rollback(&self, mut tx: Txn) -> Result<()> {
        tx.ensure_open()?;
        self.txs.abort(tx.id());
        self.release_locks(&mut tx);
        tx.close();
        Ok(())
    }

    pub fn tx_state(&self, tx_id: TxId) -> crate::txn::TxState {
        self.txs.state(tx_id)
    }

    pub fn tx_status_stats(&self) -> TxStatusStats {
        self.txs.stats()
    }

    pub fn schema_epoch(&self) -> SchemaEpoch {
        self.catalog.version()
    }

    pub fn schema_snapshot(&self) -> Arc<crate::catalog::SchemaSnapshot> {
        self.catalog.current()
    }

    pub fn validate_schema_epoch(&self, epoch: SchemaEpoch) -> Result<()> {
        if self.schema_epoch() == epoch {
            Ok(())
        } else {
            Err(Error::SchemaChanged)
        }
    }

    pub fn sqlite_schema(&self) -> Vec<SqliteSchemaRow> {
        self.catalog.current().sqlite_schema_rows()
    }

    pub fn lookup_table(
        &self,
        tx: &Txn,
        name: crate::catalog::QualifiedName,
    ) -> Result<Arc<crate::catalog::TableDef>> {
        let snapshot = self.catalog_snapshot_for_tx(tx);
        lookup_table(&snapshot, &name)
    }

    pub fn create_table(
        &self,
        tx: &mut Txn,
        spec: crate::catalog::CreateTableSpec,
    ) -> Result<Arc<crate::catalog::TableDef>> {
        tx.ensure_open()?;
        let _ddl = self.catalog.lock_ddl();
        let next = apply_create_table((*self.catalog_snapshot_for_tx(tx)).clone(), spec)?;
        let next = Arc::new(next);
        let table = next
            .tables
            .last()
            .cloned()
            .ok_or(Error::CatalogCorrupt("created table missing from snapshot"))?;
        tx.set_pending_schema_snapshot(Arc::clone(&next));
        Ok(table)
    }

    pub fn drop_table(&self, tx: &mut Txn, spec: crate::catalog::DropTableSpec) -> Result<()> {
        tx.ensure_open()?;
        let _ddl = self.catalog.lock_ddl();
        let next = apply_drop_table((*self.catalog_snapshot_for_tx(tx)).clone(), spec)?;
        tx.set_pending_schema_snapshot(Arc::new(next));
        Ok(())
    }

    pub fn create_index(
        &self,
        tx: &mut Txn,
        spec: crate::catalog::CreateIndexSpec,
    ) -> Result<Arc<crate::catalog::IndexDef>> {
        tx.ensure_open()?;
        let _ddl = self.catalog.lock_ddl();
        let next = apply_create_index((*self.catalog_snapshot_for_tx(tx)).clone(), spec)?;
        let next = Arc::new(next);
        let index = next
            .indexes
            .last()
            .cloned()
            .ok_or(Error::CatalogCorrupt("created index missing from snapshot"))?;
        tx.set_pending_schema_snapshot(Arc::clone(&next));
        Ok(index)
    }

    pub fn drop_index(&self, tx: &mut Txn, spec: crate::catalog::DropIndexSpec) -> Result<()> {
        tx.ensure_open()?;
        let _ddl = self.catalog.lock_ddl();
        let next = apply_drop_index((*self.catalog_snapshot_for_tx(tx)).clone(), spec)?;
        tx.set_pending_schema_snapshot(Arc::new(next));
        Ok(())
    }

    pub fn oldest_active_snapshot_csn(&self) -> Csn {
        self.txs.oldest_active_snapshot_csn()
    }

    pub fn vacuum(&self) -> Result<VacuumStats> {
        self.vacuum_with_horizon(self.oldest_active_snapshot_csn())
    }

    pub fn vacuum_with_horizon(&self, horizon: Csn) -> Result<VacuumStats> {
        self.heap.vacuum(horizon, &self.txs)
    }

    pub fn flush_heap_pages(&self) -> Result<()> {
        let durable_lsn = self.wal.durable_lsn()?;
        self.heap.flush_all(durable_lsn)
    }

    pub fn resident_heap_pages(&self) -> usize {
        self.heap.resident_pages()
    }

    pub fn row_directory_entries(&self) -> Result<Vec<(RowId, crate::format::TuplePtr)>> {
        self.heap.row_directory_entries()
    }

    pub fn relation_rowids(&self, rel_id: RelId) -> Result<Vec<RowId>> {
        self.heap.relation_rowids(rel_id)
    }

    pub fn buffer_pool_stats(&self) -> BufferPoolStats {
        self.heap.buffer_stats()
    }

    pub fn storage_stats(&self) -> Result<StorageStatsSnapshot> {
        Ok(StorageStatsSnapshot {
            buffer: self.heap.buffer_stats(),
            tx: self.txs.stats(),
            checkpoint: self.checkpoint_info()?,
            resident_heap_pages: self.heap.resident_pages(),
            wal_written_lsn: self.wal.written_lsn()?,
            wal_durable_lsn: self.wal.durable_lsn()?,
            vacuum_horizon_csn: self.oldest_active_snapshot_csn(),
        })
    }

    pub fn checkpoint(&self) -> Result<ControlFile> {
        self.checkpoint_with_stats().map(|stats| stats.control)
    }

    pub fn checkpoint_with_stats(&self) -> Result<CheckpointStats> {
        let durable_lsn = self.wal.flush_all()?;
        let flush = self
            .heap
            .flush_dirty_batches(durable_lsn, DEFAULT_CHECKPOINT_BATCH_PAGES)?;
        let page_count = self.heap.page_count()?;
        let mut checkpoint = self
            .checkpoint
            .lock()
            .map_err(|_| Error::CorruptPage("checkpoint mutex poisoned"))?;
        let generation = checkpoint
            .map(|control| control.generation + 1)
            .unwrap_or(1);
        self.tx_status_store.write(&TxStatusCheckpoint {
            generation,
            next_tx: self.txs.next_tx(),
            next_csn: self.txs.next_csn(),
            published_csn: self.txs.published_csn(),
            entries: self.txs.committed_states(),
        })?;
        let next = self
            .control
            .write_next(*checkpoint, durable_lsn, page_count)?;
        self.wal
            .prune_segments_below_checkpoint_lsn(next.checkpoint_lsn)?;
        *checkpoint = Some(next);
        Ok(CheckpointStats {
            control: next,
            flushed_pages: flush.flushed_pages,
            flush_batches: flush.batches,
        })
    }

    pub fn checkpoint_info(&self) -> Result<Option<ControlFile>> {
        self.checkpoint
            .lock()
            .map(|checkpoint| *checkpoint)
            .map_err(|_| Error::CorruptPage("checkpoint mutex poisoned"))
    }

    fn refresh_read_committed(&self, tx: &mut Txn) {
        if tx.isolation() == Isolation::ReadCommitted {
            tx.replace_snapshot(self.txs.snapshot());
        }
    }

    fn catalog_snapshot_for_tx(&self, tx: &Txn) -> Arc<crate::catalog::SchemaSnapshot> {
        tx.pending_schema_snapshot()
            .unwrap_or_else(|| self.catalog.current())
    }

    fn lock_row(&self, tx: &mut Txn, row_id: RowId) -> Result<()> {
        if tx.has_row_lock(row_id) {
            return Ok(());
        }
        self.locks.lock(self.rel_id, row_id, tx.id())?;
        tx.push_row_lock(row_id);
        Ok(())
    }

    fn lock_row_in_rel(&self, tx: &mut Txn, rel_id: RelId, row_id: RowId) -> Result<()> {
        if tx.has_row_lock(row_id) {
            return Ok(());
        }
        self.locks.lock(rel_id, row_id, tx.id())?;
        tx.push_row_lock(row_id);
        Ok(())
    }

    fn release_locks(&self, tx: &mut Txn) {
        let tx_id = tx.id();
        for row_id in tx.drain_row_locks() {
            self.locks.unlock(self.rel_id, row_id, tx_id);
        }
    }
}

fn recover_heap(
    records: &[WalRecord],
    replay_from_lsn: Lsn,
    txs: &ConcurrentTxStatus,
    heap: &PageBackedHeap,
) -> Result<RecoveryMetrics> {
    let mut committed = std::collections::HashMap::new();
    let mut metrics = RecoveryMetrics::default();
    for record in records {
        if record.kind == WalRecordKind::Commit {
            match WalPayload::decode(&record.payload)? {
                WalPayload::Commit { tx_id, csn } => {
                    committed.insert(tx_id, csn);
                    txs.publish_recovered_commit(tx_id, csn);
                    metrics.commits_recovered += 1;
                }
                _ => return Err(Error::CorruptWal("commit record has non-commit payload")),
            }
        }
    }

    for record in records {
        if record.lsn < replay_from_lsn {
            continue;
        }
        if record.kind == WalRecordKind::Commit {
            continue;
        }
        match WalPayload::decode(&record.payload)? {
            WalPayload::PageImage {
                page_id: _,
                page_lsn: _,
                page_bytes,
            } if committed.contains_key(&record.tx_id) => {
                let page = Page::from_bytes(page_bytes)?;
                heap.redo_page_image(page, record_end_lsn(record))?;
                metrics.page_images_redone += 1;
            }
            WalPayload::PageImage { .. } => {}
            WalPayload::HeapInsert {
                tx_id,
                rel_id,
                row_id,
                payload,
            } if record.kind == WalRecordKind::PageDelta && committed.contains_key(&tx_id) => {
                heap.insert_recovered_for_relation(tx_id, rel_id, row_id, payload)?;
                metrics.legacy_mutations_redone += 1;
            }
            WalPayload::HeapUpdate {
                tx_id,
                rel_id,
                row_id,
                payload,
            } if record.kind == WalRecordKind::PageDelta && committed.contains_key(&tx_id) => {
                heap.update_recovered_for_relation(tx_id, rel_id, row_id, payload)?;
                metrics.legacy_mutations_redone += 1;
            }
            WalPayload::HeapDelete {
                tx_id,
                rel_id,
                row_id,
            } if record.kind == WalRecordKind::PageDelta && committed.contains_key(&tx_id) => {
                heap.delete_recovered_for_relation(tx_id, rel_id, row_id)?;
                metrics.legacy_mutations_redone += 1;
            }
            WalPayload::HeapInsert { .. }
            | WalPayload::HeapUpdate { .. }
            | WalPayload::HeapDelete { .. } => {}
            WalPayload::Commit { .. } => unreachable!("commit records are skipped above"),
        }
    }

    Ok(metrics)
}

fn record_end_lsn(record: &WalRecord) -> Lsn {
    Lsn(record.lsn.0 + record.encoded_len() as u64)
}
