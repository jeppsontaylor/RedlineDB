use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Condvar, Mutex};

use smallvec::SmallVec;

use crate::engine::ConcurrentTxStatus;
use crate::format::bytes::{read_u16, read_u32, read_u64, write_u16, write_u32, write_u64};
use crate::format::{
    Csn, PAGE_HEADER_LEN, Page, PageGeneration, PageId, PageKind, RelId, SLOT_LEN, TuplePtr, TxId,
};
use crate::storage::BufferPool;
use crate::txn::{Snapshot, TxState};
use crate::wal::{WalCoordinator, WalPayload, WalRecordKind};
use crate::{Error, Result};

pub const INDEX_SPECIAL_LEN: usize = 256;
const INDEX_MAGIC: u32 = 0x5244_4958; // "RDIX"
pub const INDEX_VERSION: u16 = 2;
const PAGE_META_KIND: u8 = 1;
const PAGE_LEAF_KIND: u8 = 2;
const PAGE_INTERNAL_KIND: u8 = 3;
const META_ROOT_PAGE_OFF: usize = 16;
const META_ROOT_LEVEL_OFF: usize = 24;
const META_UNIQUENESS_OFF: usize = 26;
const META_HIGH_KEY_LEN_OFF: usize = 32;
const META_HIGH_KEY_OFF: usize = 34;
const PAGE_LEVEL_OFF: usize = 8;
const PAGE_INDEX_ID_OFF: usize = 10;
const PAGE_LEFT_OFF: usize = 18;
const PAGE_RIGHT_OFF: usize = 26;
const PAGE_HIGH_KEY_LEN_OFF: usize = 34;
const PAGE_HIGH_KEY_OFF: usize = 36;
const NON_TRANSACTIONAL_DELETE_TX: TxId = TxId(u64::MAX);

#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IndexId(pub u64);

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexUniqueness {
    NonUnique = 0,
    Unique = 1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexDescriptor {
    pub index_id: IndexId,
    pub rel_id: RelId,
    pub uniqueness: IndexUniqueness,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct IndexRowRef {
    pub row_id: crate::format::RowId,
    pub tuple: TuplePtr,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyBuf {
    bytes: SmallVec<[u8; 96]>,
    logical_len: usize,
}

impl KeyBuf {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn clear(&mut self) {
        self.bytes.clear();
        self.logical_len = 0;
    }

    pub fn extend_logical(&mut self, key: &[u8]) {
        self.logical_len = key.len();
        self.bytes.extend_from_slice(key);
    }

    pub fn append_row_ref_suffix(&mut self, row: IndexRowRef) {
        self.bytes.extend_from_slice(&row.row_id.0.to_be_bytes());
        self.bytes
            .extend_from_slice(&row.tuple.page_id.0.to_be_bytes());
        self.bytes.extend_from_slice(&row.tuple.slot.to_be_bytes());
        self.bytes
            .extend_from_slice(&row.tuple.generation.0.to_be_bytes());
    }

    pub fn logical_len(&self) -> usize {
        self.logical_len
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.bytes
    }
}

impl Default for KeyBuf {
    fn default() -> Self {
        Self {
            bytes: SmallVec::new(),
            logical_len: 0,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexValidationReport {
    pub pages_seen: usize,
    pub leaf_pages: usize,
    pub internal_pages: usize,
    pub errors: Vec<&'static str>,
}

/// Live (non-dead) leaf entry surfaced by [`BtreeIndex::iter_all_entries`].
/// Lane INT consumes this to cross-check every index entry against the heap
/// row directory; the integrity checker has no business inspecting tombstones
/// (those represent rolled-back or vacuumed deletions, not durable state).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexEntry {
    pub logical_key: Vec<u8>,
    pub row: IndexRowRef,
    pub leaf_page_id: PageId,
}

#[derive(Debug, Default)]
pub struct UniqueKeyLockTable {
    shards: Vec<Mutex<HashMap<Vec<u8>, UniqueKeyLockState>>>,
    cvars: Vec<Condvar>,
}

#[derive(Clone, Copy, Debug, Default)]
struct UniqueKeyLockState {
    owner: Option<u64>,
    depth: usize,
}

/// Owns its lock table via `Arc` so the guard can be stored across SQL-side
/// transaction state (e.g. inside `SessionState`) without lifetime gymnastics.
/// The guard's `Drop` releases the per-key reservation for `owner`.
#[derive(Debug)]
pub struct UniqueKeyGuard {
    table: Arc<UniqueKeyLockTable>,
    shard: usize,
    key: Vec<u8>,
    owner: u64,
}

impl UniqueKeyLockTable {
    pub fn new(shards: usize) -> Self {
        let shards = shards.max(1);
        let mut locks = Vec::with_capacity(shards);
        let mut cvars = Vec::with_capacity(shards);
        for _ in 0..shards {
            locks.push(Mutex::new(HashMap::new()));
            cvars.push(Condvar::new());
        }
        Self {
            shards: locks,
            cvars,
        }
    }

    pub fn lock(self: &Arc<Self>, key: &[u8], owner: u64) -> Result<UniqueKeyGuard> {
        let shard = self.shard(key);
        let mut map = self.shards[shard]
            .lock()
            .map_err(|_| Error::CorruptPage("unique lock shard poisoned"))?;
        let key = key.to_vec();
        loop {
            let state = map.entry(key.clone()).or_default();
            match state.owner {
                None => {
                    state.owner = Some(owner);
                    state.depth = 1;
                    return Ok(UniqueKeyGuard {
                        table: Arc::clone(self),
                        shard,
                        key,
                        owner,
                    });
                }
                Some(current) if current == owner => {
                    state.depth += 1;
                    return Ok(UniqueKeyGuard {
                        table: Arc::clone(self),
                        shard,
                        key,
                        owner,
                    });
                }
                Some(_) => {
                    map = self.cvars[shard]
                        .wait(map)
                        .map_err(|_| Error::CorruptPage("unique lock wait poisoned"))?;
                }
            }
        }
    }

    fn unlock(&self, shard: usize, key: Vec<u8>, owner: u64) {
        if let Ok(mut map) = self.shards[shard].lock() {
            if let Some(state) = map.get_mut(&key)
                && state.owner == Some(owner)
            {
                state.depth = state.depth.saturating_sub(1);
                if state.depth == 0 {
                    map.remove(&key);
                }
            }
            self.cvars[shard].notify_all();
        }
    }

    fn shard(&self, key: &[u8]) -> usize {
        let mut hash = 0_u64;
        for byte in key {
            hash = hash.wrapping_mul(131).wrapping_add(*byte as u64);
        }
        hash as usize % self.shards.len().max(1)
    }
}

impl Drop for UniqueKeyGuard {
    fn drop(&mut self) {
        self.table
            .unlock(self.shard, std::mem::take(&mut self.key), self.owner);
    }
}

#[derive(Clone, Debug)]
struct MetaHeader {
    index_id: IndexId,
    root_page_id: PageId,
    root_level: u16,
    uniqueness: IndexUniqueness,
}

#[derive(Clone, Debug)]
struct PageHeader {
    kind: u8,
    level: u16,
    index_id: IndexId,
    left: Option<PageId>,
    right: Option<PageId>,
    high_key: Vec<u8>,
}

#[derive(Debug)]
struct IndexInner {
    buffer: Arc<BufferPool>,
    meta_page_id: PageId,
    desc: Mutex<IndexDescriptor>,
    unique_locks: Arc<UniqueKeyLockTable>,
    wal: Option<Arc<WalCoordinator>>,
    structure_lock: Mutex<()>,
    /// Lifetime counter of leaf pages pinned by `range_scan`. Updated even
    /// when the feature gate is off so `BtreeIndex::stats()` is callable
    /// in any build, then consumed by Lane KH P1 #6 tests to assert the
    /// scan terminates as soon as the next leaf's first key falls outside
    /// the requested upper bound.
    range_scan_leaves_visited: AtomicU64,
}

/// Per-index runtime counters surfaced for tests and observability.
/// Currently only `range_scan_leaves_visited` is wired; future waves can
/// extend this with point-lookup chain length, split counts, etc.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IndexStats {
    pub range_scan_leaves_visited: u64,
}

#[derive(Clone, Debug)]
pub struct BtreeIndex {
    inner: Arc<IndexInner>,
}

impl BtreeIndex {
    pub fn create(buffer: Arc<BufferPool>, desc: IndexDescriptor) -> Result<Self> {
        Self::create_with_wal(buffer, desc, None)
    }

    pub fn create_with_wal(
        buffer: Arc<BufferPool>,
        desc: IndexDescriptor,
        wal: Option<Arc<WalCoordinator>>,
    ) -> Result<Self> {
        let meta_guard = buffer.allocate(PageKind::BtreeMeta, desc.rel_id)?;
        let root_guard = buffer.allocate(PageKind::BtreeLeaf, desc.rel_id)?;

        meta_guard.with_page_mut(|page| {
            page.reinitialize_with_special(
                PageKind::BtreeMeta,
                meta_guard.page_id(),
                desc.rel_id,
                PageGeneration::ONE,
                INDEX_SPECIAL_LEN,
            )?;
            Self::write_meta(
                page,
                &MetaHeader {
                    index_id: desc.index_id,
                    root_page_id: root_guard.page_id(),
                    root_level: 0,
                    uniqueness: desc.uniqueness,
                },
            )
        })?;
        // Keep newly-created WAL-backed index pages unflushable until the
        // create path records real page images for the DDL transaction.
        let create_lsn = if wal.is_some() {
            crate::format::Lsn(u64::MAX)
        } else {
            crate::format::Lsn(1)
        };
        meta_guard.mark_dirty(create_lsn)?;
        root_guard.with_page_mut(|page| {
            page.reinitialize_with_special(
                PageKind::BtreeLeaf,
                root_guard.page_id(),
                desc.rel_id,
                PageGeneration::ONE,
                INDEX_SPECIAL_LEN,
            )?;
            Self::write_page_header(
                page,
                &PageHeader {
                    kind: PAGE_LEAF_KIND,
                    level: 0,
                    index_id: desc.index_id,
                    left: None,
                    right: None,
                    high_key: Vec::new(),
                },
            )
        })?;
        root_guard.mark_dirty(create_lsn)?;

        Ok(Self {
            inner: Arc::new(IndexInner {
                buffer,
                meta_page_id: meta_guard.page_id(),
                desc: Mutex::new(desc),
                unique_locks: Arc::new(UniqueKeyLockTable::new(128)),
                wal,
                structure_lock: Mutex::new(()),
                range_scan_leaves_visited: AtomicU64::new(0),
            }),
        })
    }

    pub fn open(
        buffer: Arc<BufferPool>,
        meta_page_id: PageId,
        desc: IndexDescriptor,
    ) -> Result<Self> {
        Self::open_with_wal(buffer, meta_page_id, desc, None)
    }

    pub fn open_with_wal(
        buffer: Arc<BufferPool>,
        meta_page_id: PageId,
        desc: IndexDescriptor,
        wal: Option<Arc<WalCoordinator>>,
    ) -> Result<Self> {
        let index = Self {
            inner: Arc::new(IndexInner {
                buffer,
                meta_page_id,
                desc: Mutex::new(desc),
                unique_locks: Arc::new(UniqueKeyLockTable::new(128)),
                wal,
                structure_lock: Mutex::new(()),
                range_scan_leaves_visited: AtomicU64::new(0),
            }),
        };
        index.validate()?;
        Ok(index)
    }

    pub fn format_version(buffer: &BufferPool, meta_page_id: PageId) -> Result<u16> {
        let guard = buffer.pin(meta_page_id)?;
        guard.with_page(|page| {
            let special = page.special_bytes()?;
            if read_u32(special, 0)? != INDEX_MAGIC {
                return Err(Error::CorruptPage("index magic mismatch"));
            }
            read_u16(special, 4)
        })
    }

    pub fn insert(&self, logical_key: &[u8], row: IndexRowRef) -> Result<()> {
        self.insert_tx(crate::format::TxId::ZERO, logical_key, row)
    }

    pub fn insert_tx(
        &self,
        tx_id: crate::format::TxId,
        logical_key: &[u8],
        row: IndexRowRef,
    ) -> Result<()> {
        let _structure = self
            .inner
            .structure_lock
            .lock()
            .map_err(|_| Error::CorruptPage("index structure mutex poisoned"))?;
        let mut physical = KeyBuf::new();
        physical.extend_logical(logical_key);
        physical.append_row_ref_suffix(row);
        // Navigate by the candidate's *physical* bytes: separators in the tree
        // are normally logical keys, but a duplicate-key split (where every
        // entry on a leaf shared one logical key) installs a physical-key
        // separator instead. Comparing with physical bytes works for both
        // cases — physical = logical || row_ref_suffix, so it sorts identically
        // to logical for non-duplicate keys and resolves duplicates by row_id.
        loop {
            let root = self.meta()?.root_page_id;
            let path = self.find_leaf_path(root, physical.as_slice())?;
            let leaf_id = *path.last().ok_or(Error::CorruptPage("empty search path"))?;
            let guard = self.inner.buffer.pin(leaf_id)?;
            let mut page = guard.mutable_frame()?;
            let page_ref = page
                .page
                .as_mut()
                .ok_or(Error::CorruptPage("resident frame missing page"))?;
            let header = Self::read_page_header(page_ref)?;
            if header.kind != PAGE_LEAF_KIND {
                return Err(Error::CorruptPage("expected leaf page"));
            }
            // Defensive: if the leaf still claims our key belongs to a sibling
            // (concurrent split window), retry the descent.
            if !header.high_key.is_empty() && physical.as_slice() >= header.high_key.as_slice() {
                continue;
            }
            let mut entries = self.read_entries(page_ref)?;
            let candidate = Entry::Leaf {
                logical_key: logical_key.to_vec(),
                row,
                physical: physical.as_slice().to_vec(),
                create_tx: tx_id,
                delete_tx: TxId::ZERO,
            };
            match entries.binary_search_by(|entry| entry.compare(&candidate)) {
                Ok(pos) => {
                    if self.meta()?.uniqueness == IndexUniqueness::Unique {
                        return Err(Error::WriteConflict);
                    }
                    entries.insert(pos, candidate);
                }
                Err(pos) => entries.insert(pos, candidate),
            }
            let body_capacity = page_ref
                .as_bytes()
                .len()
                .saturating_sub(PAGE_HEADER_LEN + INDEX_SPECIAL_LEN);
            let required = Self::encoded_entries_len(&entries) + entries.len() * SLOT_LEN;
            if required > body_capacity {
                drop(page);
                let path = self.find_leaf_path(self.meta()?.root_page_id, physical.as_slice())?;
                let leaf_id = *path.last().ok_or(Error::CorruptPage("empty search path"))?;
                self.split_leaf_and_insert(
                    &path[..path.len().saturating_sub(1)],
                    leaf_id,
                    logical_key,
                    row,
                    physical.as_slice().to_vec(),
                    tx_id,
                )?;
                return Ok(());
            }
            Self::rewrite_leaf(
                page_ref,
                self.descriptor().index_id,
                &entries,
                header.left,
                header.right,
                header.high_key,
            )?;
            drop(page);
            // Lane E failpoint: armed before the leaf-write becomes visible
            // (mark_dirty + record_page_image). A crash here proves the index
            // entry is either fully reflected post-recovery or absent.
            crate::fail_point!("index::insert");
            self.record_page_image(leaf_id, tx_id)?;
            return Ok(());
        }
    }

    pub fn insert_unique(&self, owner: u64, logical_key: &[u8], row: IndexRowRef) -> Result<()> {
        let _guard = self.lock_unique_key(owner, logical_key)?;
        if !self.point_lookup(logical_key)?.is_empty() {
            return Err(Error::WriteConflict);
        }
        self.insert(logical_key, row)
    }

    pub fn delete_mark(&self, logical_key: &[u8], row: IndexRowRef) -> Result<()> {
        self.delete_mark_tx(NON_TRANSACTIONAL_DELETE_TX, logical_key, row)
    }

    pub fn delete_mark_tx(
        &self,
        tx_id: crate::format::TxId,
        logical_key: &[u8],
        row: IndexRowRef,
    ) -> Result<()> {
        self.delete_mark_tx_inner(tx_id, logical_key, row, None)
    }

    pub fn delete_mark_tx_visible(
        &self,
        tx_status: &ConcurrentTxStatus,
        snapshot: &Snapshot,
        owner: Option<TxId>,
        tx_id: TxId,
        logical_key: &[u8],
        row: IndexRowRef,
    ) -> Result<()> {
        self.delete_mark_tx_inner(tx_id, logical_key, row, Some((tx_status, snapshot, owner)))
    }

    fn delete_mark_tx_inner(
        &self,
        tx_id: TxId,
        logical_key: &[u8],
        row: IndexRowRef,
        visibility: Option<(&ConcurrentTxStatus, &Snapshot, Option<TxId>)>,
    ) -> Result<()> {
        let _structure = self
            .inner
            .structure_lock
            .lock()
            .map_err(|_| Error::CorruptPage("index structure mutex poisoned"))?;
        let mut probe = KeyBuf::new();
        probe.extend_logical(logical_key);
        probe.append_row_ref_suffix(row);
        let path = self.find_leaf_path(self.meta()?.root_page_id, probe.as_slice())?;
        let mut leaf_id = *path.last().ok_or(Error::CorruptPage("empty search path"))?;
        loop {
            let guard = self.inner.buffer.pin(leaf_id)?;
            let mut page = guard.mutable_frame()?;
            let page_ref = page
                .page
                .as_mut()
                .ok_or(Error::CorruptPage("resident frame missing page"))?;
            let header = Self::read_page_header(page_ref)?;
            if header.kind != PAGE_LEAF_KIND {
                return Err(Error::CorruptPage("expected leaf page"));
            }
            let mut entries = self.read_entries(page_ref)?;
            let mut changed = false;
            let mut last_key_matches_search = false;
            for entry in &mut entries {
                if let Entry::Leaf {
                    logical_key: key,
                    row: entry_row,
                    delete_tx,
                    ..
                } = entry
                {
                    if key.as_slice() == logical_key {
                        last_key_matches_search = true;
                    }
                    if key.as_slice() == logical_key && *entry_row == row {
                        if delete_marker_visible(*delete_tx, visibility) {
                            return Ok(());
                        }
                        *delete_tx = tx_id;
                        changed = true;
                        break;
                    }
                }
            }
            if changed {
                Self::rewrite_leaf(
                    page_ref,
                    self.descriptor().index_id,
                    &entries,
                    header.left,
                    header.right,
                    header.high_key,
                )?;
                drop(page);
                // Lane E failpoint: armed before the delete-mark becomes durable;
                // verifies that recovery either restores the entry (if pre-fsync)
                // or surfaces the tombstone (if post-fsync) but never both.
                crate::fail_point!("index::delete");
                self.record_page_image(leaf_id, tx_id)?;
                return Ok(());
            }
            // No match here. If duplicates of this logical_key may continue on
            // the right sibling, walk right and keep looking.
            drop(page);
            if last_key_matches_search && let Some(right) = header.right {
                leaf_id = right;
                continue;
            }
            return Ok(());
        }
    }

    pub fn compact_leaf_page(&self, page_id: PageId) -> Result<()> {
        let guard = self.inner.buffer.pin(page_id)?;
        let mut page = guard.mutable_frame()?;
        let page_ref = page
            .page
            .as_mut()
            .ok_or(Error::CorruptPage("resident frame missing page"))?;
        let header = Self::read_page_header(page_ref)?;
        if header.kind != PAGE_LEAF_KIND {
            return Err(Error::CorruptPage("expected leaf page"));
        }
        let entries = self.read_entries(page_ref)?;
        let live: Vec<_> = entries
            .into_iter()
            .filter(|entry| entry.physically_live())
            .collect();
        if live.len() == self.read_entries(page_ref)?.len() {
            return Ok(());
        }
        Self::rewrite_leaf(
            page_ref,
            self.descriptor().index_id,
            &live,
            header.left,
            header.right,
            header.high_key,
        )?;
        drop(page);
        // LSN sentinel: mutation. Leaf compaction rewrites the page in place.
        guard.mark_dirty(crate::format::Lsn(1))?;
        Ok(())
    }

    pub fn prune_committed_deletes_before(
        &self,
        page_id: PageId,
        tx_status: &ConcurrentTxStatus,
        horizon: Csn,
    ) -> Result<()> {
        let guard = self.inner.buffer.pin(page_id)?;
        let mut page = guard.mutable_frame()?;
        let page_ref = page
            .page
            .as_mut()
            .ok_or(Error::CorruptPage("resident frame missing page"))?;
        let header = Self::read_page_header(page_ref)?;
        if header.kind != PAGE_LEAF_KIND {
            return Err(Error::CorruptPage("expected leaf page"));
        }
        let entries = self.read_entries(page_ref)?;
        let live: Vec<_> = entries
            .iter()
            .filter(|entry| !entry.is_committed_deleted_before(tx_status, horizon))
            .cloned()
            .collect();
        if live.len() == entries.len() {
            return Ok(());
        }
        Self::rewrite_leaf(
            page_ref,
            self.descriptor().index_id,
            &live,
            header.left,
            header.right,
            header.high_key,
        )?;
        drop(page);
        guard.mark_dirty(crate::format::Lsn(1))?;
        Ok(())
    }

    pub fn lock_unique_key(&self, owner: u64, logical_key: &[u8]) -> Result<UniqueKeyGuard> {
        self.inner.unique_locks.lock(logical_key, owner)
    }

    pub fn redo_page_image(&self, page: Page) -> Result<()> {
        self.inner.buffer.write_page_direct(&page)?;
        let page_id = page.header()?.page_id;
        if let Ok(guard) = self.inner.buffer.pin(page_id) {
            guard.with_page_mut(|resident| {
                *resident = page.clone();
                Ok(())
            })?;
            guard.mark_dirty(page.header()?.page_lsn)?;
        }
        Ok(())
    }

    fn record_page_image(&self, page_id: PageId, tx_id: crate::format::TxId) -> Result<()> {
        let guard = self.inner.buffer.pin(page_id)?;
        if tx_id == crate::format::TxId::ZERO {
            return guard.mark_dirty(crate::format::Lsn(1));
        }
        let Some(wal) = &self.inner.wal else {
            return guard.mark_dirty(crate::format::Lsn(1));
        };
        let mut page = guard.with_page(|page| Ok(page.clone()))?;
        page.set_page_lsn(crate::format::Lsn::ZERO)?;
        let payload = WalPayload::PageImage {
            page_id,
            page_lsn: crate::format::Lsn::ZERO,
            page_bytes: page.as_bytes().to_vec(),
        };
        let append = wal.append(WalRecordKind::PageImage, tx_id, payload.encode()?)?;
        guard.mark_dirty(append.end_lsn)
    }

    pub fn point_lookup(&self, logical_key: &[u8]) -> Result<Vec<IndexRowRef>> {
        self.point_lookup_filter(logical_key, |entry| entry.physically_live())
    }

    pub fn point_lookup_visible(
        &self,
        tx_status: &ConcurrentTxStatus,
        snapshot: &Snapshot,
        owner: Option<TxId>,
        logical_key: &[u8],
    ) -> Result<Vec<IndexRowRef>> {
        self.point_lookup_filter(logical_key, |entry| {
            entry_visible(entry, tx_status, snapshot, owner)
        })
    }

    fn point_lookup_filter(
        &self,
        logical_key: &[u8],
        mut visible: impl FnMut(&Entry) -> bool,
    ) -> Result<Vec<IndexRowRef>> {
        // Descend to the leaf whose physical-key range *starts* at or below
        // the search key. With physical separators throughout, navigation
        // keyed on `logical_key` always lands on a leaf whose first entry's
        // logical key is <= search_key OR is the smallest logical key strictly
        // greater than the search key (the latter happens when duplicates
        // straddled an internal split). From there we walk right through
        // sibling leaves until a leaf whose entries are entirely past the
        // search key — at that point all matches have been collected.
        let mut page_id = self.find_leaf(self.meta()?.root_page_id, logical_key)?;
        let mut out: Vec<IndexRowRef> = Vec::new();
        loop {
            let guard = self.inner.buffer.pin(page_id)?;
            let (first_past_key, right_link) = guard.with_page(|page| {
                let header = Self::read_page_header(page)?;
                let entries = self.read_entries(page)?;
                let mut matched_here = false;
                let mut first_entry_key: Option<Vec<u8>> = None;
                for entry in &entries {
                    if let Entry::Leaf {
                        logical_key: key,
                        row,
                        ..
                    } = entry
                    {
                        if first_entry_key.is_none() {
                            first_entry_key = Some(key.clone());
                        }
                        if !visible(entry) {
                            continue;
                        }
                        if key.as_slice() == logical_key {
                            out.push(*row);
                            matched_here = true;
                        } else if matched_here {
                            // Within one leaf, matches are contiguous because
                            // entries are sorted by physical (= logical || row
                            // suffix). Once we pass them we can stop scanning.
                            break;
                        }
                    }
                }
                let first_past = first_entry_key
                    .as_deref()
                    .map(|k| k > logical_key)
                    .unwrap_or(false);
                Ok((first_past, header.right))
            })?;
            // Stop once we land on a leaf whose first entry's logical key is
            // already strictly past the search key — the leaf chain is sorted,
            // so nothing further could match.
            if first_past_key {
                break;
            }
            match right_link {
                Some(next) => page_id = next,
                None => break,
            }
        }
        Ok(out)
    }

    pub fn range_scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<IndexRowRef>> {
        self.range_scan_filter(start, end, |entry| entry.physically_live())
    }

    pub fn range_scan_visible(
        &self,
        tx_status: &ConcurrentTxStatus,
        snapshot: &Snapshot,
        owner: Option<TxId>,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<IndexRowRef>> {
        self.range_scan_filter(start, end, |entry| {
            entry_visible(entry, tx_status, snapshot, owner)
        })
    }

    fn range_scan_filter(
        &self,
        start: &[u8],
        end: &[u8],
        mut visible: impl FnMut(&Entry) -> bool,
    ) -> Result<Vec<IndexRowRef>> {
        let mut out = Vec::new();
        let mut leaf_id = self.find_leaf(self.meta()?.root_page_id, start)?;
        loop {
            let guard = self.inner.buffer.pin(leaf_id)?;
            self.inner
                .range_scan_leaves_visited
                .fetch_add(1, AtomicOrdering::Relaxed);
            let (next, last_logical_key) = guard.with_page(|page| {
                let header = Self::read_page_header(page)?;
                let mut last_key: Option<Vec<u8>> = None;
                for entry in self.read_entries(page)? {
                    if !visible(&entry) {
                        continue;
                    }
                    if let Entry::Leaf {
                        logical_key, row, ..
                    } = entry
                    {
                        if logical_key.as_slice() >= start && logical_key.as_slice() < end {
                            out.push(row);
                        }
                        last_key = Some(logical_key);
                    }
                }
                Ok((header.right, last_key))
            })?;
            // Wave 7 P1 #6: terminate the leaf walk as soon as the leaf
            // we just scanned has a logical key already at or past the
            // upper bound. Without this, a `WHERE k BETWEEN 5 AND 10`
            // over a 100K-entry index loaded every leaf to the end of
            // the chain — O(N) instead of O(log N + result_size). The
            // entry-level `< end` filter still trims partial overlap on
            // the boundary leaf; this short-circuit only skips
            // *subsequent* leaves whose every key would fail the
            // filter. `Bound::Unbounded` callers pass a sentinel `end`
            // (e.g. `[0xff; 32]`) that can never be reached, so they
            // keep the legacy walk-to-end behavior.
            if let Some(last) = last_logical_key.as_deref()
                && last >= end
            {
                break;
            }
            match next {
                Some(next_id) => {
                    leaf_id = next_id;
                }
                None => break,
            }
        }
        Ok(out)
    }

    /// Returns a snapshot of per-index runtime counters. Lane KH P1 #6
    /// tests use `range_scan_leaves_visited` to assert the early-exit
    /// path is hit; recovery and benches can sample this for
    /// observability.
    pub fn stats(&self) -> IndexStats {
        IndexStats {
            range_scan_leaves_visited: self
                .inner
                .range_scan_leaves_visited
                .load(AtomicOrdering::Relaxed),
        }
    }

    pub fn validate(&self) -> Result<IndexValidationReport> {
        let meta = self.meta()?;
        let mut report = IndexValidationReport {
            pages_seen: 0,
            leaf_pages: 0,
            internal_pages: 0,
            errors: Vec::new(),
        };
        self.validate_page(meta.root_page_id, meta.root_level, &mut report)?;
        Ok(report)
    }

    /// Walk every live leaf entry in physical-key order. Lane INT uses this
    /// to dump the index for a heap/index equivalence check. The walk:
    ///
    /// 1. Descends from the meta root to the leftmost leaf (following
    ///    `header.left` on each internal page).
    /// 2. Reads each leaf, emits its non-`dead` `Entry::Leaf` rows, then
    ///    follows the `header.right` sibling pointer.
    ///
    /// The result is materialised into a `Vec<IndexEntry>` rather than a
    /// streaming iterator because `BufferPool::pin` returns RAII guards;
    /// keeping a guard live across an iterator boundary would tangle
    /// lifetimes for callers. For paper-grade integrity checks the index
    /// fits in O(N) memory anyway — this is a one-shot diagnostic.
    pub fn iter_all_entries(&self) -> Result<Vec<IndexEntry>> {
        // Lane E failpoint: armed before the integrity checker dumps the
        // tree so the failpoint matrix can later exercise crash-during-check
        // semantics without taking a structural lock or recording any new
        // page images.
        crate::fail_point!("integrity::index::dump");
        let meta = self.meta()?;
        let mut leaf_id = self.leftmost_leaf(meta.root_page_id)?;
        let mut out = Vec::new();
        loop {
            let guard = self.inner.buffer.pin(leaf_id)?;
            let (next, entries) = guard.with_page(|page| {
                let header = Self::read_page_header(page)?;
                if header.kind != PAGE_LEAF_KIND {
                    return Err(Error::CorruptPage(
                        "iter_all_entries: expected leaf in chain",
                    ));
                }
                let entries = self.read_entries(page)?;
                Ok((header.right, entries))
            })?;
            for entry in entries {
                if let Entry::Leaf {
                    logical_key,
                    row,
                    delete_tx,
                    ..
                } = entry
                {
                    if delete_tx != TxId::ZERO {
                        continue;
                    }
                    out.push(IndexEntry {
                        logical_key,
                        row,
                        leaf_page_id: leaf_id,
                    });
                }
            }
            match next {
                Some(next_id) if next_id != leaf_id => leaf_id = next_id,
                _ => return Ok(out),
            }
        }
    }

    fn leftmost_leaf(&self, page_id: PageId) -> Result<PageId> {
        let guard = self.inner.buffer.pin(page_id)?;
        let next = guard.with_page(|page| {
            let header = Self::read_page_header(page)?;
            if header.kind == PAGE_LEAF_KIND {
                return Ok(None);
            }
            if header.kind == PAGE_INTERNAL_KIND {
                let child = header
                    .left
                    .ok_or(Error::CorruptPage("internal page missing leftmost child"))?;
                return Ok(Some(child));
            }
            Err(Error::CorruptPage(
                "leftmost_leaf: unexpected page kind in tree",
            ))
        })?;
        match next {
            Some(child) => self.leftmost_leaf(child),
            None => Ok(page_id),
        }
    }

    pub fn descriptor(&self) -> IndexDescriptor {
        self.inner
            .desc
            .lock()
            .expect("descriptor mutex poisoned")
            .clone()
    }

    /// Returns the meta-page id for this B-tree. Lane A persists this in the
    /// catalog snapshot so recovery can reopen the index handle.
    pub fn meta_page_id(&self) -> PageId {
        self.inner.meta_page_id
    }

    /// Log WAL `PageImage` records for the meta and root pages of a freshly
    /// created index so recovery can reconstruct the B-tree even if the
    /// engine drops before a checkpoint flushes the buffer pool. Caller must
    /// supply a real (non-zero) `TxId` whose commit LSN covers these images.
    pub fn record_initial_page_images(&self, tx_id: crate::format::TxId) -> Result<()> {
        let meta_id = self.inner.meta_page_id;
        let root_id = self.meta()?.root_page_id;
        self.record_page_image(meta_id, tx_id)?;
        self.record_page_image(root_id, tx_id)?;
        Ok(())
    }

    fn split_leaf_and_insert(
        &self,
        ancestors: &[PageId],
        leaf_id: PageId,
        logical_key: &[u8],
        row: IndexRowRef,
        physical: Vec<u8>,
        tx_id: crate::format::TxId,
    ) -> Result<()> {
        // Lane E failpoint: armed at the start of leaf split, before any
        // structural change is applied. Crashing here exercises recovery from
        // a half-applied split (no new pages allocated yet on disk).
        crate::fail_point!("index::split");
        let rel_id = self.descriptor().rel_id;
        let index_id = self.descriptor().index_id;
        let guard = self.inner.buffer.pin(leaf_id)?;
        let (left_entries, right_entries, left_high, header) = guard.with_page(|page| {
            let mut entries = self.read_entries(page)?;
            entries.push(Entry::Leaf {
                logical_key: logical_key.to_vec(),
                row,
                physical,
                create_tx: tx_id,
                delete_tx: TxId::ZERO,
            });
            entries.sort_by(|a, b| a.compare(b));
            // Pick a split position that does not cleave a duplicate-key run
            // unless every entry on the page shares one logical key. This keeps
            // `point_lookup` correct under the existing right-walk traversal:
            // every duplicate of a given logical_key sits on a single leaf,
            // and the parent separator is the right-half's first key (strictly
            // greater than every key on the left half).
            let split = Self::choose_leaf_split(&entries);
            let right_entries = entries.split_off(split);
            // Most splits land on a clean key boundary (chosen by
            // `choose_leaf_split`) and can use the right half's first logical
            // key as a compact separator. When duplicates of one logical key
            // span both halves (only possible when the page held one logical
            // key throughout), fall back to the right half's first *physical*
            // key (logical_key || row_ref suffix) so the separator is strictly
            // greater than every left-side entry.
            let left_high = match (entries.last(), right_entries.first()) {
                (Some(last_left), Some(first_right))
                    if last_left.logical_key() == first_right.logical_key() =>
                {
                    first_right
                        .physical()
                        .map(|p| p.to_vec())
                        .unwrap_or_default()
                }
                _ => right_entries
                    .first()
                    .and_then(|entry| entry.logical_key())
                    .unwrap_or_default()
                    .to_vec(),
            };
            let header = Self::read_page_header(page)?;
            Ok((entries, right_entries, left_high, header))
        })?;
        let right_guard = self.inner.buffer.allocate(PageKind::BtreeLeaf, rel_id)?;
        guard.with_page_mut(|page| {
            Self::rewrite_leaf(
                page,
                index_id,
                &left_entries,
                header.left,
                Some(right_guard.page_id()),
                left_high.clone(),
            )
        })?;
        right_guard.with_page_mut(|right_page| {
            right_page.reinitialize_with_special(
                PageKind::BtreeLeaf,
                right_guard.page_id(),
                rel_id,
                PageGeneration::ONE,
                INDEX_SPECIAL_LEN,
            )?;
            Self::rewrite_leaf(
                right_page,
                index_id,
                &right_entries,
                Some(leaf_id),
                header.right,
                header.high_key.clone(),
            )
        })?;
        self.record_page_image(leaf_id, tx_id)?;
        self.record_page_image(right_guard.page_id(), tx_id)?;

        self.propagate_split(
            ancestors,
            leaf_id,
            right_guard.page_id(),
            left_high,
            1,
            tx_id,
        )?;
        Ok(())
    }

    fn propagate_split(
        &self,
        ancestors: &[PageId],
        left_page: PageId,
        right_page: PageId,
        separator: Vec<u8>,
        mut left_level: u16,
        tx_id: crate::format::TxId,
    ) -> Result<()> {
        let meta = self.meta()?;
        let mut ancestors = ancestors.to_vec();
        let mut current_left = left_page;
        let mut current_right = right_page;
        let mut current_separator = separator;

        loop {
            if let Some(parent_id) = ancestors.pop() {
                let guard = self.inner.buffer.pin(parent_id)?;
                let (header, mut entries) = guard.with_page(|page| {
                    let header = Self::read_page_header(page)?;
                    let entries = self.read_entries(page)?;
                    Ok((header, entries))
                })?;
                if header.kind != PAGE_INTERNAL_KIND {
                    return Err(Error::CorruptPage("expected internal page"));
                }
                let parent_level = header.level;
                entries.push(Entry::Internal {
                    separator: current_separator.clone(),
                    child: current_right,
                });
                entries.sort_by(|a, b| a.compare(b));
                let body_capacity = self.page_body_capacity(parent_id)?;
                let required = Self::encoded_entries_len(&entries) + entries.len() * SLOT_LEN;
                if required <= body_capacity {
                    guard.with_page_mut(|page| {
                        Self::rewrite_internal(
                            page,
                            meta.index_id,
                            header.level,
                            &entries,
                            header.left,
                            header.right,
                            header.high_key.clone(),
                        )
                    })?;
                    self.record_page_image(parent_id, tx_id)?;
                    return Ok(());
                }

                let split = entries.len() / 2;
                let right_entries = entries.split_off(split);
                let left_entries = entries;
                let right_guard = self
                    .inner
                    .buffer
                    .allocate(PageKind::BtreeInternal, self.descriptor().rel_id)?;
                let right_left_child = match right_entries.first() {
                    Some(Entry::Internal { child, .. }) => *child,
                    _ => {
                        return Err(Error::CorruptPage(
                            "internal split produced empty right side",
                        ));
                    }
                };
                let right_separator = self.min_key_for_page(right_left_child)?;
                guard.with_page_mut(|page| {
                    Self::rewrite_internal(
                        page,
                        meta.index_id,
                        parent_level,
                        &left_entries,
                        header.left,
                        Some(right_guard.page_id()),
                        right_separator.clone(),
                    )
                })?;
                right_guard.with_page_mut(|page| {
                    page.reinitialize_with_special(
                        PageKind::BtreeInternal,
                        right_guard.page_id(),
                        self.descriptor().rel_id,
                        PageGeneration::ONE,
                        INDEX_SPECIAL_LEN,
                    )?;
                    Self::rewrite_internal(
                        page,
                        meta.index_id,
                        parent_level,
                        &right_entries,
                        Some(right_left_child),
                        header.right,
                        header.high_key.clone(),
                    )
                })?;
                self.record_page_image(parent_id, tx_id)?;
                self.record_page_image(right_guard.page_id(), tx_id)?;
                current_left = parent_id;
                current_right = right_guard.page_id();
                current_separator = right_separator;
                left_level = parent_level.saturating_add(1);
                continue;
            }

            let rel_id = self.descriptor().rel_id;
            let root_guard = self
                .inner
                .buffer
                .allocate(PageKind::BtreeInternal, rel_id)?;
            root_guard.with_page_mut(|page| {
                page.reinitialize_with_special(
                    PageKind::BtreeInternal,
                    root_guard.page_id(),
                    rel_id,
                    PageGeneration::ONE,
                    INDEX_SPECIAL_LEN,
                )?;
                Self::write_page_header(
                    page,
                    &PageHeader {
                        kind: PAGE_INTERNAL_KIND,
                        level: left_level,
                        index_id: meta.index_id,
                        left: Some(current_left),
                        right: None,
                        high_key: Vec::new(),
                    },
                )?;
                let entries = vec![Entry::Internal {
                    separator: current_separator.clone(),
                    child: current_right,
                }];
                Self::rewrite_internal(
                    page,
                    meta.index_id,
                    left_level,
                    &entries,
                    Some(current_left),
                    None,
                    Vec::new(),
                )
            })?;
            self.record_page_image(root_guard.page_id(), tx_id)?;
            self.set_meta_root(root_guard.page_id(), left_level, tx_id)?;
            return Ok(());
        }
    }

    fn validate_page(
        &self,
        page_id: PageId,
        expected_level: u16,
        report: &mut IndexValidationReport,
    ) -> Result<()> {
        let guard = self.inner.buffer.pin(page_id)?;
        let (header, entries) = guard.with_page(|page| {
            let header = Self::read_page_header(page)?;
            let entries = self.read_entries(page)?;
            Ok((header, entries))
        })?;
        report.pages_seen += 1;
        match header.kind {
            PAGE_LEAF_KIND => report.leaf_pages += 1,
            PAGE_INTERNAL_KIND => report.internal_pages += 1,
            _ => report.errors.push("unexpected page kind"),
        }
        if header.level != expected_level {
            report.errors.push("level mismatch");
        }
        if header.index_id != self.descriptor().index_id {
            report.errors.push("index id mismatch");
        }
        if header.kind == PAGE_INTERNAL_KIND && header.left.is_none() {
            report.errors.push("internal page missing leftmost child");
        }
        let mut last: Option<Vec<u8>> = None;
        let mut children = Vec::new();
        let entry_count = entries.len();
        for entry in entries {
            match entry {
                Entry::Leaf { physical, .. } => {
                    if let Some(prev) = &last
                        && prev.as_slice() > physical.as_slice()
                    {
                        report.errors.push("leaf entries out of order");
                    }
                    last = Some(physical);
                }
                Entry::Internal { child, .. } => children.push(child),
            }
        }
        if header.kind == PAGE_INTERNAL_KIND {
            if let Some(left) = header.left {
                children.insert(0, left);
            }
            if children.len() != entry_count + 1 {
                report.errors.push("internal child count mismatch");
            }
        }
        drop(guard);
        for child in children {
            self.validate_page(child, expected_level.saturating_sub(1), report)?;
        }
        Ok(())
    }

    fn find_leaf(&self, page_id: PageId, key: &[u8]) -> Result<PageId> {
        Ok(*self
            .find_leaf_path(page_id, key)?
            .last()
            .ok_or(Error::CorruptPage("empty search path"))?)
    }

    fn find_leaf_path(&self, mut page_id: PageId, key: &[u8]) -> Result<Vec<PageId>> {
        let mut path = Vec::new();
        loop {
            path.push(page_id);
            let guard = self.inner.buffer.pin(page_id)?;
            let next = guard.with_page(|page| {
                let header = Self::read_page_header(page)?;
                if !header.high_key.is_empty() && key >= header.high_key.as_slice() {
                    return Ok(header.right);
                }
                if header.kind == PAGE_LEAF_KIND {
                    return Ok(None);
                }
                let mut chosen = header.left;
                if chosen.is_none() {
                    return Err(Error::CorruptPage("internal page missing leftmost child"));
                }
                for entry in self.read_entries(page)? {
                    if let Entry::Internal { separator, child } = entry {
                        if key < separator.as_slice() {
                            break;
                        }
                        chosen = Some(child);
                    }
                }
                Ok(chosen)
            })?;
            match next {
                Some(next_id) if next_id != page_id => page_id = next_id,
                _ => return Ok(path),
            }
        }
    }

    fn min_key_for_page(&self, page_id: PageId) -> Result<Vec<u8>> {
        let guard = self.inner.buffer.pin(page_id)?;
        let (header, entries) = guard.with_page(|page| {
            let header = Self::read_page_header(page)?;
            let entries = self.read_entries(page)?;
            Ok((header, entries))
        })?;
        if header.kind == PAGE_INTERNAL_KIND {
            let child = header
                .left
                .ok_or(Error::CorruptPage("internal page missing leftmost child"))?;
            return self.min_key_for_page(child);
        }
        // Propagate the LEAF's first key. Use the logical bytes when the
        // entire subtree shares a single logical key — that's the
        // duplicate-run case where we need an unambiguous separator that
        // survives propagation; the physical bytes (logical || row_ref)
        // resolve ties strictly. Otherwise the logical key suffices and is
        // shorter, keeping internal pages compact.
        let first_logical = entries.iter().find_map(|e| e.logical_key());
        let last_logical = entries.iter().rev().find_map(|e| e.logical_key());
        match (first_logical, last_logical) {
            (Some(first), Some(last)) if first == last => {
                // All entries on this leaf share one logical key. Use the
                // first entry's physical bytes so the separator is strictly
                // greater than every key on the left sibling but still sorts
                // before any key with a greater logical value.
                let first_physical = entries.iter().find_map(|e| e.physical());
                Ok(first_physical
                    .map(|p| p.to_vec())
                    .unwrap_or_else(|| first.to_vec()))
            }
            (Some(first), _) => Ok(first.to_vec()),
            _ => Ok(Vec::new()),
        }
    }

    fn page_body_capacity(&self, page_id: PageId) -> Result<usize> {
        let guard = self.inner.buffer.pin(page_id)?;
        guard.with_page(|page| {
            Ok(page
                .as_bytes()
                .len()
                .saturating_sub(PAGE_HEADER_LEN + INDEX_SPECIAL_LEN))
        })
    }

    fn meta(&self) -> Result<MetaHeader> {
        let guard = self.inner.buffer.pin(self.inner.meta_page_id)?;
        guard.with_page(Self::read_meta)
    }

    fn set_meta_root(
        &self,
        root_page_id: PageId,
        root_level: u16,
        tx_id: crate::format::TxId,
    ) -> Result<()> {
        let guard = self.inner.buffer.pin(self.inner.meta_page_id)?;
        guard.with_page_mut(|page| {
            let mut meta = Self::read_meta(page)?;
            meta.root_page_id = root_page_id;
            meta.root_level = root_level;
            Self::write_meta(page, &meta)
        })?;
        self.record_page_image(self.inner.meta_page_id, tx_id)
    }

    fn read_entries(&self, page: &Page) -> Result<Vec<Entry>> {
        let header = page.header()?;
        let mut entries = Vec::new();
        for slot in 0..page.slot_count()? {
            let cell = page.cell(slot)?;
            match header.kind {
                PageKind::BtreeLeaf => entries.push(LeafCell::decode(cell)?),
                PageKind::BtreeInternal => entries.push(InternalCell::decode(cell)?),
                _ => return Err(Error::CorruptPage("unsupported index page kind")),
            }
        }
        Ok(entries)
    }

    fn rewrite_leaf(
        page: &mut Page,
        index_id: IndexId,
        entries: &[Entry],
        left: Option<PageId>,
        right: Option<PageId>,
        high_key: Vec<u8>,
    ) -> Result<()> {
        let page_id = page.header()?.page_id;
        let rel_id = page.header()?.rel_id;
        page.reinitialize_with_special(
            PageKind::BtreeLeaf,
            page_id,
            rel_id,
            PageGeneration::ONE,
            INDEX_SPECIAL_LEN,
        )?;
        Self::write_page_header(
            page,
            &PageHeader {
                kind: PAGE_LEAF_KIND,
                level: 0,
                index_id,
                left,
                right,
                high_key,
            },
        )?;
        for entry in entries {
            if let Entry::Leaf {
                logical_key,
                row,
                physical,
                create_tx,
                delete_tx,
            } = entry
            {
                page.insert_cell(&LeafCell::encode(
                    logical_key,
                    *row,
                    physical,
                    *create_tx,
                    *delete_tx,
                ))?;
            }
        }
        Ok(())
    }

    fn rewrite_internal(
        page: &mut Page,
        index_id: IndexId,
        level: u16,
        entries: &[Entry],
        left: Option<PageId>,
        right: Option<PageId>,
        high_key: Vec<u8>,
    ) -> Result<()> {
        let page_id = page.header()?.page_id;
        let rel_id = page.header()?.rel_id;
        page.reinitialize_with_special(
            PageKind::BtreeInternal,
            page_id,
            rel_id,
            PageGeneration::ONE,
            INDEX_SPECIAL_LEN,
        )?;
        Self::write_page_header(
            page,
            &PageHeader {
                kind: PAGE_INTERNAL_KIND,
                level,
                index_id,
                left,
                right,
                high_key,
            },
        )?;
        for entry in entries {
            if let Entry::Internal { separator, child } = entry {
                page.insert_cell(&InternalCell::encode(separator, *child))?;
            }
        }
        Ok(())
    }

    fn read_page_header(page: &Page) -> Result<PageHeader> {
        let special = page.special_bytes()?;
        if special.len() < META_HIGH_KEY_OFF {
            return Err(Error::CorruptPage("index special header too small"));
        }
        let kind = special[6];
        let level = read_u16(special, PAGE_LEVEL_OFF)?;
        let index_id = IndexId(read_u64(special, PAGE_INDEX_ID_OFF)?);
        let left = decode_opt_page_id(read_u64(special, PAGE_LEFT_OFF)?);
        let right = decode_opt_page_id(read_u64(special, PAGE_RIGHT_OFF)?);
        let high_key_len = read_u16(special, PAGE_HIGH_KEY_LEN_OFF)? as usize;
        let high_key = if high_key_len == 0 {
            Vec::new()
        } else {
            special[PAGE_HIGH_KEY_OFF..PAGE_HIGH_KEY_OFF + high_key_len].to_vec()
        };
        Ok(PageHeader {
            kind,
            level,
            index_id,
            left,
            right,
            high_key,
        })
    }

    fn write_page_header(page: &mut Page, header: &PageHeader) -> Result<()> {
        let special = page.special_bytes_mut()?;
        special.fill(0);
        crate::format::bytes::write_u32(special, 0, INDEX_MAGIC)?;
        crate::format::bytes::write_u16(special, 4, INDEX_VERSION)?;
        special[6] = header.kind;
        special[7] = 0;
        write_u16(special, PAGE_LEVEL_OFF, header.level)?;
        write_u64(special, PAGE_INDEX_ID_OFF, header.index_id.0)?;
        write_u64(
            special,
            PAGE_LEFT_OFF,
            header.left.map(|p| p.0).unwrap_or(u64::MAX),
        )?;
        write_u64(
            special,
            PAGE_RIGHT_OFF,
            header.right.map(|p| p.0).unwrap_or(u64::MAX),
        )?;
        write_u16(special, PAGE_HIGH_KEY_LEN_OFF, header.high_key.len() as u16)?;
        if !header.high_key.is_empty() {
            crate::format::bytes::write_bytes(special, PAGE_HIGH_KEY_OFF, &header.high_key)?;
        }
        Ok(())
    }

    fn read_meta(page: &Page) -> Result<MetaHeader> {
        let special = page.special_bytes()?;
        if read_u16(special, 4)? != INDEX_VERSION {
            return Err(Error::UnsupportedVersion(read_u16(special, 4)?));
        }
        let index_id = IndexId(read_u64(special, 8)?);
        let root_page_id = PageId(read_u64(special, META_ROOT_PAGE_OFF)?);
        let root_level = read_u16(special, META_ROOT_LEVEL_OFF)?;
        let uniqueness = match special[META_UNIQUENESS_OFF] {
            0 => IndexUniqueness::NonUnique,
            1 => IndexUniqueness::Unique,
            _ => return Err(Error::CorruptPage("unknown uniqueness")),
        };
        Ok(MetaHeader {
            index_id,
            root_page_id,
            root_level,
            uniqueness,
        })
    }

    fn write_meta(page: &mut Page, meta: &MetaHeader) -> Result<()> {
        let special = page.special_bytes_mut()?;
        special.fill(0);
        write_u32(special, 0, INDEX_MAGIC)?;
        write_u16(special, 4, INDEX_VERSION)?;
        special[6] = PAGE_META_KIND;
        special[7] = 0;
        write_u64(special, 8, meta.index_id.0)?;
        write_u64(special, META_ROOT_PAGE_OFF, meta.root_page_id.0)?;
        write_u16(special, META_ROOT_LEVEL_OFF, meta.root_level)?;
        special[META_UNIQUENESS_OFF] = meta.uniqueness as u8;
        write_u16(special, META_HIGH_KEY_LEN_OFF, 0)?;
        Ok(())
    }
}

fn decode_opt_page_id(raw: u64) -> Option<PageId> {
    if raw == u64::MAX {
        None
    } else {
        Some(PageId(raw))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Entry {
    Leaf {
        logical_key: Vec<u8>,
        row: IndexRowRef,
        physical: Vec<u8>,
        create_tx: TxId,
        delete_tx: TxId,
    },
    Internal {
        separator: Vec<u8>,
        child: PageId,
    },
}

impl Entry {
    fn compare(&self, other: &Self) -> Ordering {
        match (self, other) {
            (
                Entry::Leaf { physical: left, .. },
                Entry::Leaf {
                    physical: right, ..
                },
            ) => left.cmp(right),
            (
                Entry::Internal {
                    separator: left, ..
                },
                Entry::Internal {
                    separator: right, ..
                },
            ) => left.cmp(right),
            _ => Ordering::Equal,
        }
    }

    fn logical_key(&self) -> Option<&[u8]> {
        match self {
            Entry::Leaf { logical_key, .. } => Some(logical_key),
            Entry::Internal { separator, .. } => Some(separator),
        }
    }

    fn physical(&self) -> Option<&[u8]> {
        match self {
            Entry::Leaf { physical, .. } => Some(physical),
            Entry::Internal { .. } => None,
        }
    }

    fn physically_live(&self) -> bool {
        matches!(self, Entry::Leaf { delete_tx, .. } if *delete_tx == TxId::ZERO)
    }

    fn is_committed_deleted_before(&self, tx_status: &ConcurrentTxStatus, horizon: Csn) -> bool {
        match self {
            Entry::Leaf { delete_tx, .. } if *delete_tx != TxId::ZERO => {
                matches!(tx_status.state(*delete_tx), TxState::Committed(csn) if csn < horizon)
            }
            _ => false,
        }
    }
}

struct LeafCell;
struct InternalCell;

impl LeafCell {
    fn encode(
        logical_key: &[u8],
        row: IndexRowRef,
        physical: &[u8],
        create_tx: TxId,
        delete_tx: TxId,
    ) -> Vec<u8> {
        let mut out = Vec::with_capacity(42 + logical_key.len() + physical.len());
        out.extend_from_slice(&(logical_key.len() as u16).to_le_bytes());
        out.extend_from_slice(&(physical.len() as u16).to_le_bytes());
        out.extend_from_slice(&row.row_id.0.to_le_bytes());
        out.extend_from_slice(&row.tuple.page_id.0.to_le_bytes());
        out.extend_from_slice(&row.tuple.slot.to_le_bytes());
        out.extend_from_slice(&row.tuple.generation.0.to_le_bytes());
        out.extend_from_slice(&create_tx.0.to_le_bytes());
        out.extend_from_slice(&delete_tx.0.to_le_bytes());
        out.extend_from_slice(logical_key);
        out.extend_from_slice(physical);
        out
    }

    fn decode(bytes: &[u8]) -> Result<Entry> {
        if bytes.len() < 42 {
            return Err(Error::BufferTooSmall {
                needed: 42,
                actual: bytes.len(),
            });
        }
        let logical_len = read_u16(bytes, 0)? as usize;
        let physical_len = read_u16(bytes, 2)? as usize;
        let row = IndexRowRef {
            row_id: crate::format::RowId(read_u64(bytes, 4)?),
            tuple: TuplePtr::new_with_generation(
                PageId(read_u64(bytes, 12)?),
                read_u16(bytes, 20)?,
                PageGeneration(read_u32(bytes, 22)?),
            ),
        };
        let create_tx = TxId(read_u64(bytes, 26)?);
        let delete_tx = TxId(read_u64(bytes, 34)?);
        let logical_start = 42;
        let physical_start = logical_start + logical_len;
        let physical_end = physical_start + physical_len;
        if physical_end > bytes.len() {
            return Err(Error::CorruptPage("leaf cell overflow"));
        }
        Ok(Entry::Leaf {
            logical_key: bytes[logical_start..physical_start].to_vec(),
            row,
            physical: bytes[physical_start..physical_end].to_vec(),
            create_tx,
            delete_tx,
        })
    }
}

impl InternalCell {
    fn encode(separator: &[u8], child: PageId) -> Vec<u8> {
        let mut out = Vec::with_capacity(10 + separator.len());
        out.extend_from_slice(&(separator.len() as u16).to_le_bytes());
        out.extend_from_slice(&child.0.to_le_bytes());
        out.extend_from_slice(separator);
        out
    }

    fn decode(bytes: &[u8]) -> Result<Entry> {
        if bytes.len() < 10 {
            return Err(Error::BufferTooSmall {
                needed: 10,
                actual: bytes.len(),
            });
        }
        let len = read_u16(bytes, 0)? as usize;
        let child = PageId(read_u64(bytes, 2)?);
        let end = 10 + len;
        if end > bytes.len() {
            return Err(Error::CorruptPage("internal cell overflow"));
        }
        Ok(Entry::Internal {
            separator: bytes[10..end].to_vec(),
            child,
        })
    }
}

impl IndexDescriptor {
    pub fn new(index_id: IndexId, rel_id: RelId, uniqueness: IndexUniqueness) -> Self {
        Self {
            index_id,
            rel_id,
            uniqueness,
        }
    }
}

impl IndexRowRef {
    pub fn new(tuple: TuplePtr) -> Self {
        Self {
            row_id: crate::format::RowId::ZERO,
            tuple,
        }
    }

    pub fn with_row_id(row_id: crate::format::RowId, tuple: TuplePtr) -> Self {
        Self { row_id, tuple }
    }
}

pub fn compare_physical_key(left: &[u8], right: &[u8]) -> Ordering {
    left.cmp(right)
}

pub fn encode_physical_key(logical_key: &[u8], row: IndexRowRef) -> KeyBuf {
    let mut key = KeyBuf::new();
    key.extend_logical(logical_key);
    key.append_row_ref_suffix(row);
    key
}

impl BtreeIndex {
    /// Choose a split position for a leaf whose entries are sorted by
    /// `physical` (logical_key + row_ref suffix). Prefers a position near the
    /// midpoint where the boundary lies between two distinct logical keys —
    /// that keeps every duplicate of a given key on a single leaf, which is
    /// the invariant `point_lookup`'s right-walk relies on. When the entire
    /// page is one duplicate run the midpoint is returned and the caller
    /// adopts a physical-key separator instead (see `split_leaf_and_insert`).
    fn choose_leaf_split(entries: &[Entry]) -> usize {
        let n = entries.len();
        if n <= 1 {
            return n;
        }
        let mid = n / 2;
        // Search outward from the midpoint for a position `i` where
        // entries[i-1].logical_key != entries[i].logical_key.
        for offset in 0..n {
            for &candidate in &[mid.saturating_add(offset), mid.saturating_sub(offset)] {
                if candidate == 0 || candidate >= n {
                    continue;
                }
                let prev = entries[candidate - 1].logical_key();
                let next = entries[candidate].logical_key();
                if prev != next {
                    return candidate;
                }
            }
        }
        // Whole page is one duplicate run; fall back to the midpoint and rely
        // on the physical-key separator path in the caller.
        mid
    }

    fn encoded_entries_len(entries: &[Entry]) -> usize {
        // Leaf cells are encoded as 2+2+8+8+2+4 = 26 bytes of header plus the
        // logical_key + physical payload. Internal cells are 2+8 = 10 bytes of
        // header plus the separator. The pre-split size estimator used to say
        // `18` for leaves which underestimated by 8 bytes per entry; with many
        // entries that meant the post-split halves no longer fit, surfacing
        // as `Error::PageFull` ("no free slot space on page").
        entries
            .iter()
            .map(|entry| match entry {
                Entry::Leaf {
                    logical_key,
                    physical,
                    ..
                } => 42 + logical_key.len() + physical.len(),
                Entry::Internal { separator, .. } => 10 + separator.len(),
            })
            .sum()
    }
}

fn entry_visible(
    entry: &Entry,
    tx_status: &ConcurrentTxStatus,
    snapshot: &Snapshot,
    owner: Option<TxId>,
) -> bool {
    let Entry::Leaf {
        create_tx,
        delete_tx,
        ..
    } = entry
    else {
        return false;
    };
    if *create_tx != TxId::ZERO && !tx_status.is_tx_visible(*create_tx, snapshot, owner) {
        return false;
    }
    if *delete_tx == NON_TRANSACTIONAL_DELETE_TX {
        return false;
    }
    if *delete_tx != TxId::ZERO && tx_status.is_tx_visible(*delete_tx, snapshot, owner) {
        return false;
    }
    true
}

fn delete_marker_visible(
    delete_tx: TxId,
    visibility: Option<(&ConcurrentTxStatus, &Snapshot, Option<TxId>)>,
) -> bool {
    if delete_tx == TxId::ZERO {
        return false;
    }
    if delete_tx == NON_TRANSACTIONAL_DELETE_TX {
        return true;
    }
    let Some((tx_status, snapshot, owner)) = visibility else {
        return true;
    };
    tx_status.is_tx_visible(delete_tx, snapshot, owner)
}
