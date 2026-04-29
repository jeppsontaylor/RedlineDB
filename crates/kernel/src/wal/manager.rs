use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::format::bytes::read_u32;
use crate::format::{Csn, Lsn, TxId};
use crate::io::{FileHandle, FileSystem, StdFileSystem};
use crate::wal::{WAL_HEADER_LEN, WalPayload, WalRecord, WalRecordKind};
use crate::{Error, Result};

pub const DEFAULT_WAL_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;
pub const DEFAULT_WAL_BUFFER_BYTES: usize = 16 * 1024 * 1024;
pub const DEFAULT_WAL_WRITE_BATCH_BYTES: usize = 1024 * 1024;
pub const DEFAULT_GROUP_COMMIT_DELAY_US: u64 = 200;
pub const DEFAULT_GROUP_COMMIT_MAX_BATCH_BYTES: u64 = 4 * 1024 * 1024;
const WAL_SEGMENT_EXT: &str = ".wal";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalConfig {
    pub segment_bytes: u64,
    pub wal_buffer_bytes: usize,
    pub wal_write_batch_bytes: usize,
    pub group_commit_delay_us: u64,
    pub group_commit_max_batch_bytes: u64,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            segment_bytes: DEFAULT_WAL_SEGMENT_BYTES,
            wal_buffer_bytes: DEFAULT_WAL_BUFFER_BYTES,
            wal_write_batch_bytes: DEFAULT_WAL_WRITE_BATCH_BYTES,
            group_commit_delay_us: DEFAULT_GROUP_COMMIT_DELAY_US,
            group_commit_max_batch_bytes: DEFAULT_GROUP_COMMIT_MAX_BATCH_BYTES,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WalAppend {
    pub start_lsn: Lsn,
    pub end_lsn: Lsn,
}

#[derive(Debug)]
pub struct WalManager<Fs: FileSystem = StdFileSystem> {
    dir: PathBuf,
    fs: Fs,
    config: WalConfig,
    active_segment: u64,
    active_offset: u64,
    active_file: Fs::File,
    written_lsn: Lsn,
    durable_lsn: Lsn,
    prev_lsn: Lsn,
}

impl WalManager<StdFileSystem> {
    pub fn create(path: impl AsRef<Path>, config: WalConfig) -> Result<Self> {
        Self::create_with_fs(path, config, StdFileSystem)
    }

    pub fn open(path: impl AsRef<Path>, config: WalConfig) -> Result<Self> {
        Self::open_with_fs(path, config, StdFileSystem)
    }

    pub fn prune_segments_below_checkpoint_lsn(&mut self, checkpoint_lsn: Lsn) -> Result<usize> {
        let keep_segment = segment_for_lsn(checkpoint_lsn, self.config.segment_bytes);
        self.prune_segments_below(keep_segment)
    }

    pub fn prune_segments_below(&mut self, segment: u64) -> Result<usize> {
        if segment == 0 {
            return Ok(0);
        }

        // Lane E failpoint: armed before any WAL segment removal so harnesses
        // can crash between checkpoint completion and prune, observing whether
        // recovery still succeeds with stale segments on disk.
        crate::fail_point!("wal::prune");
        let mut removed = 0_usize;
        for candidate in self.segment_numbers()? {
            if candidate < segment && candidate < self.active_segment {
                let path = segment_path(&self.dir, candidate);
                if let Err(err) = std::fs::remove_file(path)
                    && err.kind() != std::io::ErrorKind::NotFound
                {
                    return Err(err.into());
                }
                removed += 1;
            }
        }
        Ok(removed)
    }
}

#[derive(Debug)]
pub struct WalCoordinator {
    shared: Arc<WalCoordinatorShared>,
    writer: Mutex<Option<JoinHandle<()>>>,
    config: WalConfig,
    dir: PathBuf,
}

#[derive(Debug)]
struct WalCoordinatorShared {
    state: Mutex<WalCoordinatorState>,
    cvar: Condvar,
}

#[derive(Debug)]
struct WalCoordinatorState {
    reserved_lsn: Lsn,
    prev_lsn: Lsn,
    durable_lsn: Lsn,
    pending: VecDeque<QueuedWalRecord>,
    pending_bytes: usize,
    flush_requested_lsn: Lsn,
    shutdown: bool,
    failure: Option<&'static str>,
}

#[derive(Debug)]
struct QueuedWalRecord {
    append: WalAppend,
    encoded: Vec<u8>,
}

impl WalCoordinator {
    pub fn create(path: impl AsRef<Path>, config: WalConfig) -> Result<Self> {
        let dir = path.as_ref().to_path_buf();
        let wal = WalManager::create(&dir, config.clone())?;
        Self::new(dir, wal, config)
    }

    pub fn open(path: impl AsRef<Path>, config: WalConfig) -> Result<Self> {
        let dir = path.as_ref().to_path_buf();
        let wal = WalManager::open(&dir, config.clone())?;
        Self::new(dir, wal, config)
    }

    fn new(dir: PathBuf, wal: WalManager, config: WalConfig) -> Result<Self> {
        let reserved_lsn = wal.written_lsn();
        let prev_lsn = wal.prev_lsn;
        let durable_lsn = wal.durable_lsn();
        let shared = Arc::new(WalCoordinatorShared {
            state: Mutex::new(WalCoordinatorState {
                reserved_lsn,
                prev_lsn,
                durable_lsn,
                pending: VecDeque::new(),
                pending_bytes: 0,
                flush_requested_lsn: Lsn::ZERO,
                shutdown: false,
                failure: None,
            }),
            cvar: Condvar::new(),
        });
        let writer_shared = Arc::clone(&shared);
        let writer_config = config.clone();
        let writer = thread::Builder::new()
            .name("redlinedb-wal-writer".to_owned())
            .spawn(move || wal_writer_loop(wal, writer_config, writer_shared))?;
        Ok(Self {
            shared,
            writer: Mutex::new(Some(writer)),
            config,
            dir,
        })
    }

    pub fn append(&self, kind: WalRecordKind, tx_id: TxId, payload: Vec<u8>) -> Result<WalAppend> {
        self.append_with_payload(kind, tx_id, payload)
    }

    pub fn append_commit(
        &self,
        tx_id: TxId,
        reserve_csn: impl FnOnce() -> Csn,
    ) -> Result<(Csn, WalAppend)> {
        let encoded_len = WAL_HEADER_LEN
            .checked_add(17)
            .ok_or(Error::CorruptWal("record length overflow"))?;
        let mut state = self.wait_for_wal_buffer(encoded_len)?;
        let csn = reserve_csn();
        let payload = WalPayload::Commit { tx_id, csn }.encode()?;
        let append = enqueue_reserved_record(
            &mut state,
            self.config.segment_bytes,
            WalRecordKind::Commit,
            tx_id,
            payload,
            encoded_len as u64,
        )?;
        self.shared.cvar.notify_all();
        Ok((csn, append))
    }

    pub fn append_commit_with_csn(&self, tx_id: TxId, csn: Csn) -> Result<WalAppend> {
        let encoded_len = WAL_HEADER_LEN
            .checked_add(17)
            .ok_or(Error::CorruptWal("record length overflow"))?;
        let mut state = self.wait_for_wal_buffer(encoded_len)?;
        let payload = WalPayload::Commit { tx_id, csn }.encode()?;
        let append = enqueue_reserved_record(
            &mut state,
            self.config.segment_bytes,
            WalRecordKind::Commit,
            tx_id,
            payload,
            encoded_len as u64,
        )?;
        self.shared.cvar.notify_all();
        Ok(append)
    }

    pub fn flush_until(&self, target_lsn: Lsn) -> Result<Lsn> {
        // Lane E failpoint: coordinator entry to the durability barrier; any
        // injection here aborts before the writer thread is signalled.
        crate::fail_point!("wal::flush_until");
        let mut state = self
            .shared
            .state
            .lock()
            .map_err(|_| Error::CorruptWal("wal coordinator mutex poisoned"))?;

        loop {
            check_wal_failure(&state)?;
            if state.durable_lsn >= target_lsn {
                return Ok(state.durable_lsn);
            }

            if target_lsn > state.flush_requested_lsn {
                state.flush_requested_lsn = target_lsn;
                self.shared.cvar.notify_all();
            }

            state = self
                .shared
                .cvar
                .wait(state)
                .map_err(|_| Error::CorruptWal("wal coordinator wait poisoned"))?;
        }
    }

    pub fn flush_all(&self) -> Result<Lsn> {
        // Lane E failpoint: full-WAL flush is the checkpoint barrier; injection
        // here lets harnesses crash between commit-fsync and checkpoint-fsync.
        crate::fail_point!("wal::flush_all");
        let written_lsn = self
            .shared
            .state
            .lock()
            .map_err(|_| Error::CorruptWal("wal coordinator mutex poisoned"))?
            .reserved_lsn;
        self.flush_until(written_lsn)
    }

    pub fn written_lsn(&self) -> Result<Lsn> {
        self.shared
            .state
            .lock()
            .map(|state| state.reserved_lsn)
            .map_err(|_| Error::CorruptWal("wal coordinator mutex poisoned"))
    }

    pub fn durable_lsn(&self) -> Result<Lsn> {
        self.shared
            .state
            .lock()
            .map(|state| state.durable_lsn)
            .map_err(|_| Error::CorruptWal("wal coordinator mutex poisoned"))
    }

    pub fn prune_segments_below_checkpoint_lsn(&self, checkpoint_lsn: Lsn) -> Result<usize> {
        let keep_segment = segment_for_lsn(checkpoint_lsn, self.config.segment_bytes);
        let active_segment = self
            .shared
            .state
            .lock()
            .map(|state| segment_for_lsn(state.reserved_lsn, self.config.segment_bytes))
            .map_err(|_| Error::CorruptWal("wal coordinator mutex poisoned"))?;
        let mut removed = 0_usize;
        for candidate in segment_numbers_on_disk(&self.dir)? {
            if candidate < keep_segment && candidate < active_segment {
                let path = segment_path(&self.dir, candidate);
                if let Err(err) = std::fs::remove_file(path)
                    && err.kind() != std::io::ErrorKind::NotFound
                {
                    return Err(err.into());
                }
                removed += 1;
            }
        }
        Ok(removed)
    }

    fn append_with_payload(
        &self,
        kind: WalRecordKind,
        tx_id: TxId,
        payload: Vec<u8>,
    ) -> Result<WalAppend> {
        let encoded_len = WAL_HEADER_LEN
            .checked_add(payload.len())
            .ok_or(Error::CorruptWal("record length overflow"))?;
        if encoded_len as u64 > self.config.segment_bytes {
            return Err(Error::CorruptWal("record larger than wal segment"));
        }
        if encoded_len > self.config.wal_buffer_bytes {
            return Err(Error::CorruptWal("record larger than wal buffer"));
        }

        let mut state = self.wait_for_wal_buffer(encoded_len)?;
        let append = enqueue_reserved_record(
            &mut state,
            self.config.segment_bytes,
            kind,
            tx_id,
            payload,
            encoded_len as u64,
        )?;
        self.shared.cvar.notify_all();
        Ok(append)
    }

    fn wait_for_wal_buffer(
        &self,
        encoded_len: usize,
    ) -> Result<std::sync::MutexGuard<'_, WalCoordinatorState>> {
        if encoded_len as u64 > self.config.segment_bytes {
            return Err(Error::CorruptWal("record larger than wal segment"));
        }
        if encoded_len > self.config.wal_buffer_bytes {
            return Err(Error::CorruptWal("record larger than wal buffer"));
        }

        let mut state = self
            .shared
            .state
            .lock()
            .map_err(|_| Error::CorruptWal("wal coordinator mutex poisoned"))?;

        loop {
            check_wal_failure(&state)?;
            let available = self
                .config
                .wal_buffer_bytes
                .saturating_sub(state.pending_bytes);
            if available >= encoded_len {
                return Ok(state);
            }
            state = self
                .shared
                .cvar
                .wait(state)
                .map_err(|_| Error::CorruptWal("wal coordinator wait poisoned"))?;
        }
    }
}

impl Drop for WalCoordinator {
    fn drop(&mut self) {
        if let Ok(mut state) = self.shared.state.lock() {
            state.shutdown = true;
            self.shared.cvar.notify_all();
        }
        if let Ok(mut writer) = self.writer.lock()
            && let Some(writer) = writer.take()
        {
            let _ = writer.join();
        }
    }
}

fn wal_writer_loop(mut wal: WalManager, config: WalConfig, shared: Arc<WalCoordinatorShared>) {
    loop {
        let mut batch = Vec::new();
        let mut flush_target = Lsn::ZERO;
        let mut should_flush = false;
        let shutdown;

        {
            let mut state = match shared.state.lock() {
                Ok(state) => state,
                Err(_) => return,
            };

            while state.pending.is_empty()
                && state.flush_requested_lsn <= state.durable_lsn
                && !state.shutdown
            {
                state = match shared.cvar.wait(state) {
                    Ok(state) => state,
                    Err(_) => return,
                };
            }

            if state.shutdown && state.pending.is_empty() {
                return;
            }

            let mut batch_bytes = 0_usize;
            while let Some(record) = state.pending.pop_front() {
                batch_bytes += record.encoded.len();
                state.pending_bytes = state.pending_bytes.saturating_sub(record.encoded.len());
                batch.push(record);
                if batch_bytes >= config.wal_write_batch_bytes.max(1) {
                    break;
                }
            }

            if state.flush_requested_lsn > state.durable_lsn {
                flush_target = state.flush_requested_lsn;
                should_flush = true;
            }
            shutdown = state.shutdown;
            shared.cvar.notify_all();
        }

        let mut last_written = Lsn::ZERO;
        for record in batch {
            last_written = record.append.end_lsn;
            if let Err(_err) = wal.write_encoded(record.append, &record.encoded) {
                publish_wal_failure(&shared);
                return;
            }
        }

        if should_flush {
            wait_for_group_commit_window(&shared, &config, &mut wal, flush_target);
            if let Err(_err) = drain_until(
                &shared,
                &mut wal,
                flush_target,
                config.wal_write_batch_bytes,
            ) {
                publish_wal_failure(&shared);
                return;
            }
            match wal.flush() {
                Ok(durable_lsn) => {
                    if let Ok(mut state) = shared.state.lock() {
                        state.durable_lsn = durable_lsn;
                        if state.flush_requested_lsn <= durable_lsn {
                            state.flush_requested_lsn = Lsn::ZERO;
                        }
                        shared.cvar.notify_all();
                    } else {
                        return;
                    }
                }
                Err(_err) => {
                    publish_wal_failure(&shared);
                    return;
                }
            }
        } else if shutdown && last_written != Lsn::ZERO {
            match wal.flush() {
                Ok(durable_lsn) => {
                    if let Ok(mut state) = shared.state.lock() {
                        state.durable_lsn = durable_lsn;
                        shared.cvar.notify_all();
                    } else {
                        return;
                    }
                }
                Err(_err) => {
                    publish_wal_failure(&shared);
                    return;
                }
            }
        }
    }
}

fn wait_for_group_commit_window(
    shared: &Arc<WalCoordinatorShared>,
    config: &WalConfig,
    wal: &mut WalManager,
    flush_target: Lsn,
) {
    let delay = Duration::from_micros(config.group_commit_delay_us);
    if delay.is_zero() {
        return;
    }
    let durable = wal.durable_lsn().0;
    if flush_target.0.saturating_sub(durable) >= config.group_commit_max_batch_bytes {
        return;
    }
    if let Ok(state) = shared.state.lock() {
        let _ = shared.cvar.wait_timeout(state, delay);
    }
}

fn drain_until(
    shared: &Arc<WalCoordinatorShared>,
    wal: &mut WalManager,
    target_lsn: Lsn,
    max_batch_bytes: usize,
) -> Result<()> {
    loop {
        if wal.written_lsn() >= target_lsn {
            return Ok(());
        }

        let mut batch = Vec::new();
        {
            let mut state = shared
                .state
                .lock()
                .map_err(|_| Error::CorruptWal("wal coordinator mutex poisoned"))?;
            let mut batch_bytes = 0_usize;
            while let Some(record) = state.pending.pop_front() {
                batch_bytes += record.encoded.len();
                state.pending_bytes = state.pending_bytes.saturating_sub(record.encoded.len());
                batch.push(record);
                if batch_bytes >= max_batch_bytes.max(1) {
                    break;
                }
                if batch
                    .last()
                    .map(|record| record.append.end_lsn >= target_lsn)
                    .unwrap_or(false)
                {
                    break;
                }
            }
            shared.cvar.notify_all();
        }

        if batch.is_empty() {
            let mut state = shared
                .state
                .lock()
                .map_err(|_| Error::CorruptWal("wal coordinator mutex poisoned"))?;
            while state.pending.is_empty() && state.failure.is_none() && !state.shutdown {
                state = shared
                    .cvar
                    .wait(state)
                    .map_err(|_| Error::CorruptWal("wal coordinator wait poisoned"))?;
            }
            check_wal_failure(&state)?;
            continue;
        }

        for record in batch {
            wal.write_encoded(record.append, &record.encoded)?;
        }
    }
}

fn reserve_queued_append(
    reserved_lsn: &mut Lsn,
    segment_bytes: u64,
    encoded_len: u64,
) -> Result<WalAppend> {
    if encoded_len > segment_bytes {
        return Err(Error::CorruptWal("record larger than wal segment"));
    }
    let current_segment = segment_for_lsn(*reserved_lsn, segment_bytes);
    let current_offset = offset_for_lsn(*reserved_lsn, segment_bytes);
    let start_lsn = if current_offset > 0 && current_offset + encoded_len > segment_bytes {
        Lsn(current_segment * segment_bytes)
    } else {
        *reserved_lsn
    };
    let append = WalAppend {
        start_lsn,
        end_lsn: Lsn(start_lsn.0 + encoded_len),
    };
    *reserved_lsn = append.end_lsn;
    Ok(append)
}

fn enqueue_reserved_record(
    state: &mut WalCoordinatorState,
    segment_bytes: u64,
    kind: WalRecordKind,
    tx_id: TxId,
    payload: Vec<u8>,
    encoded_len: u64,
) -> Result<WalAppend> {
    let append = reserve_queued_append(&mut state.reserved_lsn, segment_bytes, encoded_len)?;
    let record = WalRecord {
        lsn: append.start_lsn,
        prev_lsn: state.prev_lsn,
        tx_id,
        kind,
        payload,
    };
    let encoded = record.encode()?;
    state.prev_lsn = append.start_lsn;
    state.pending_bytes += encoded.len();
    state.pending.push_back(QueuedWalRecord { append, encoded });
    Ok(append)
}

fn check_wal_failure(state: &WalCoordinatorState) -> Result<()> {
    if let Some(message) = state.failure {
        return Err(Error::CorruptWal(message));
    }
    Ok(())
}

fn publish_wal_failure(shared: &Arc<WalCoordinatorShared>) {
    if let Ok(mut state) = shared.state.lock() {
        state.failure = Some("wal writer failed");
        shared.cvar.notify_all();
    }
}

impl<Fs: FileSystem> WalManager<Fs> {
    pub fn create_with_fs(path: impl AsRef<Path>, config: WalConfig, fs: Fs) -> Result<Self> {
        validate_config(&config)?;
        let dir = path.as_ref().to_path_buf();
        fs.create_dir_all(&dir)?;
        let active_segment = 1;
        let active_offset = 0;
        let active_file = fs.open_rw_create(&segment_path(&dir, active_segment))?;
        Ok(Self {
            dir,
            fs,
            config,
            active_segment,
            active_offset,
            active_file,
            written_lsn: Lsn::ZERO,
            durable_lsn: Lsn::ZERO,
            prev_lsn: Lsn::ZERO,
        })
    }

    pub fn open_with_fs(path: impl AsRef<Path>, config: WalConfig, fs: Fs) -> Result<Self> {
        validate_config(&config)?;
        let dir = path.as_ref().to_path_buf();
        fs.create_dir_all(&dir)?;
        let mut scan = WalReader::new_with_fs(&dir, config.clone(), fs);
        let report = scan.scan_report()?;
        let records = report.records;
        let fs = scan.into_fs();

        let written_lsn = records
            .last()
            .map(|record| Lsn(record.lsn.0 + record.encoded_len() as u64))
            .unwrap_or(Lsn::ZERO);
        let prev_lsn = records.last().map(|record| record.lsn).unwrap_or(Lsn::ZERO);
        let active_segment = segment_for_lsn(written_lsn, config.segment_bytes);
        let active_offset = offset_for_lsn(written_lsn, config.segment_bytes);
        let active_file = fs.open_rw_create(&segment_path(&dir, active_segment))?;
        active_file.set_len(active_offset)?;

        Ok(Self {
            dir,
            fs,
            config,
            active_segment,
            active_offset,
            active_file,
            written_lsn,
            durable_lsn: Lsn::ZERO,
            prev_lsn,
        })
    }

    pub fn append(
        &mut self,
        kind: WalRecordKind,
        tx_id: TxId,
        payload: Vec<u8>,
    ) -> Result<WalAppend> {
        let encoded_len = WAL_HEADER_LEN
            .checked_add(payload.len())
            .ok_or(Error::CorruptWal("record length overflow"))?;
        let append = self.reserve_append(encoded_len as u64)?;
        let record = WalRecord {
            lsn: append.start_lsn,
            prev_lsn: self.prev_lsn,
            tx_id,
            kind,
            payload,
        };
        let encoded = record.encode()?;
        self.write_encoded(append, &encoded)?;
        Ok(append)
    }

    pub fn flush(&mut self) -> Result<Lsn> {
        // Lane E failpoint: armed before the fsync that establishes WAL
        // durability so the harness can simulate "fsync skipped" or kernel
        // crash mid-fsync.
        crate::fail_point!("wal::flush");
        self.active_file.sync_data()?;
        self.durable_lsn = self.written_lsn;
        Ok(self.durable_lsn)
    }

    pub fn written_lsn(&self) -> Lsn {
        self.written_lsn
    }

    pub fn durable_lsn(&self) -> Lsn {
        self.durable_lsn
    }

    pub fn wal_dir(&self) -> &Path {
        &self.dir
    }

    pub fn write_encoded(&mut self, append: WalAppend, encoded: &[u8]) -> Result<()> {
        // Lane E failpoint: armed before the WAL segment write so harnesses
        // can inject torn-write or panic faults at the precise moment the
        // record would land on disk.
        crate::fail_point!("wal::write_encoded");
        if encoded.len() > self.config.segment_bytes as usize {
            return Err(Error::CorruptWal("record larger than wal segment"));
        }

        let expected_lsn =
            Lsn((self.active_segment - 1) * self.config.segment_bytes + self.active_offset);
        if expected_lsn != append.start_lsn
            && self.active_offset > 0
            && self.active_offset + encoded.len() as u64 > self.config.segment_bytes
        {
            self.rotate_segment()?;
        }

        let expected_lsn =
            Lsn((self.active_segment - 1) * self.config.segment_bytes + self.active_offset);
        if expected_lsn != append.start_lsn {
            return Err(Error::CorruptWal(
                "record lsn does not match write position",
            ));
        }

        self.active_file.write_all_at(self.active_offset, encoded)?;
        self.active_offset += encoded.len() as u64;
        self.written_lsn = append.end_lsn;
        self.prev_lsn = append.start_lsn;
        Ok(())
    }

    fn rotate_segment(&mut self) -> Result<()> {
        self.active_file.sync_data()?;
        self.active_segment += 1;
        self.active_offset = 0;
        self.active_file = self
            .fs
            .open_rw_create(&segment_path(&self.dir, self.active_segment))?;
        Ok(())
    }

    fn reserve_append(&mut self, encoded_len: u64) -> Result<WalAppend> {
        if encoded_len > self.config.segment_bytes {
            return Err(Error::CorruptWal("record larger than wal segment"));
        }

        if self.active_offset > 0 && self.active_offset + encoded_len > self.config.segment_bytes {
            self.rotate_segment()?;
        }

        let lsn = Lsn((self.active_segment - 1) * self.config.segment_bytes + self.active_offset);
        let end_lsn = Lsn(lsn.0 + encoded_len);
        Ok(WalAppend {
            start_lsn: lsn,
            end_lsn,
        })
    }

    fn segment_numbers(&self) -> Result<Vec<u64>> {
        if !self.dir.exists() {
            return Ok(Vec::new());
        }

        let mut segments = Vec::new();
        for name in self.fs.read_dir_names(&self.dir)? {
            if let Some(segment) = parse_segment_name(&name) {
                segments.push(segment);
            }
        }
        segments.sort_unstable();
        Ok(segments)
    }
}

#[derive(Debug)]
pub struct WalReader<Fs: FileSystem = StdFileSystem> {
    dir: PathBuf,
    fs: Fs,
    config: WalConfig,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalScanReport {
    pub records: Vec<WalRecord>,
    pub valid_end_lsn: Lsn,
    pub torn_tail: bool,
}

impl WalReader<StdFileSystem> {
    pub fn new(path: impl AsRef<Path>, config: WalConfig) -> Self {
        Self::new_with_fs(path, config, StdFileSystem)
    }
}

impl<Fs: FileSystem> WalReader<Fs> {
    pub fn new_with_fs(path: impl AsRef<Path>, config: WalConfig, fs: Fs) -> Self {
        Self {
            dir: path.as_ref().to_path_buf(),
            fs,
            config,
        }
    }

    pub fn scan(&mut self) -> Result<Vec<WalRecord>> {
        Ok(self.scan_report()?.records)
    }

    pub fn scan_report(&mut self) -> Result<WalScanReport> {
        validate_config(&self.config)?;
        let segments = self.segment_numbers()?;
        let mut records = Vec::new();
        let mut stopped_at_tail = false;

        for (segment_index, segment) in segments.iter().enumerate() {
            let is_last_segment = segment_index + 1 == segments.len();
            let path = segment_path(&self.dir, *segment);
            let mut file = self.fs.open_rw_existing(&path)?;
            let file_len = file.len()?;
            let mut offset = 0_u64;

            while offset < file_len {
                if stopped_at_tail {
                    return Err(Error::CorruptWal("wal bytes after torn tail"));
                }

                let is_tail_candidate = is_last_segment;
                let remaining = file_len - offset;
                if remaining < WAL_HEADER_LEN as u64 {
                    if is_tail_candidate {
                        stopped_at_tail = true;
                        break;
                    }
                    return Err(Error::CorruptWal("partial record header before final tail"));
                }

                let mut header = vec![0; WAL_HEADER_LEN];
                file.read_exact_at(offset, &mut header)?;
                let payload_len = read_u32(&header, 12)? as u64;
                let record_len = match (WAL_HEADER_LEN as u64).checked_add(payload_len) {
                    Some(record_len) => record_len,
                    None if is_tail_candidate => {
                        stopped_at_tail = true;
                        break;
                    }
                    None => return Err(Error::CorruptWal("record length overflow")),
                };

                if record_len > self.config.segment_bytes {
                    if is_tail_candidate {
                        stopped_at_tail = true;
                        break;
                    }
                    return Err(Error::CorruptWal("record length exceeds segment size"));
                }

                if remaining < record_len {
                    if is_tail_candidate {
                        stopped_at_tail = true;
                        break;
                    }
                    return Err(Error::CorruptWal("partial record body before final tail"));
                }

                let mut encoded = vec![0; record_len as usize];
                encoded[..WAL_HEADER_LEN].copy_from_slice(&header);
                file.read_exact_at(
                    offset + WAL_HEADER_LEN as u64,
                    &mut encoded[WAL_HEADER_LEN..],
                )?;

                match WalRecord::decode(&encoded) {
                    Ok(record) => {
                        validate_record_position(
                            &record,
                            *segment,
                            offset,
                            self.config.segment_bytes,
                        )?;
                        records.push(record);
                        offset += record_len;
                    }
                    Err(_) if is_tail_candidate && offset + record_len >= file_len => {
                        stopped_at_tail = true;
                        break;
                    }
                    Err(err) => return Err(err),
                }
            }
        }

        let valid_end_lsn = records
            .last()
            .map(|record| Lsn(record.lsn.0 + record.encoded_len() as u64))
            .unwrap_or(Lsn::ZERO);
        Ok(WalScanReport {
            records,
            valid_end_lsn,
            torn_tail: stopped_at_tail,
        })
    }

    pub fn into_fs(self) -> Fs {
        self.fs
    }

    fn segment_numbers(&self) -> Result<Vec<u64>> {
        let mut segments = Vec::new();
        for name in self.fs.read_dir_names(&self.dir)? {
            if let Some(segment) = parse_segment_name(&name) {
                segments.push(segment);
            }
        }
        segments.sort_unstable();
        Ok(segments)
    }
}

fn validate_config(config: &WalConfig) -> Result<()> {
    if config.segment_bytes < WAL_HEADER_LEN as u64 {
        return Err(Error::CorruptWal("wal segment size too small"));
    }
    if config.wal_buffer_bytes < WAL_HEADER_LEN {
        return Err(Error::CorruptWal("wal buffer too small"));
    }
    if config.wal_write_batch_bytes == 0 {
        return Err(Error::CorruptWal("wal write batch must be nonzero"));
    }
    Ok(())
}

fn validate_record_position(
    record: &WalRecord,
    segment: u64,
    offset: u64,
    segment_bytes: u64,
) -> Result<()> {
    let expected = (segment - 1)
        .checked_mul(segment_bytes)
        .and_then(|base| base.checked_add(offset))
        .ok_or(Error::CorruptWal("lsn overflow"))?;
    if record.lsn.0 != expected {
        return Err(Error::CorruptWal(
            "record lsn does not match segment position",
        ));
    }
    Ok(())
}

fn segment_for_lsn(lsn: Lsn, segment_bytes: u64) -> u64 {
    lsn.0 / segment_bytes + 1
}

fn offset_for_lsn(lsn: Lsn, segment_bytes: u64) -> u64 {
    lsn.0 % segment_bytes
}

fn segment_path(dir: &Path, segment: u64) -> PathBuf {
    dir.join(format!("{segment:020}{WAL_SEGMENT_EXT}"))
}

fn segment_numbers_on_disk(dir: &Path) -> Result<Vec<u64>> {
    let mut segments = Vec::new();
    if !dir.exists() {
        return Ok(segments);
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if let Some(name) = entry.file_name().to_str()
            && let Some(segment) = parse_segment_name(name)
        {
            segments.push(segment);
        }
    }
    segments.sort_unstable();
    Ok(segments)
}

fn parse_segment_name(name: &str) -> Option<u64> {
    let number = name.strip_suffix(WAL_SEGMENT_EXT)?;
    if number.len() != 20 || !number.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    number.parse().ok()
}
