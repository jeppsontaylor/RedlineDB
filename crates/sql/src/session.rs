use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};

use redlinedb_kernel::engine::Txn;
use redlinedb_kernel::format::RowId;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BeginMode {
    Deferred,
    Immediate,
    Exclusive,
}

#[derive(Debug, Default)]
pub struct SessionState {
    pub tx: Option<Txn>,
    pub failed: bool,
    pub changes: usize,
    pub last_insert_rowid: Option<i64>,
    pub unique_guards: Vec<UniqueKeyGuard>,
}

impl SessionState {
    #[allow(dead_code)]
    pub fn clear(&mut self) {
        self.tx = None;
        self.failed = false;
        self.changes = 0;
        self.last_insert_rowid = None;
        self.unique_guards.clear();
    }
}

#[derive(Debug, Default)]
pub struct UniqueLockTable {
    shards: Vec<Mutex<HashMap<Vec<u8>, UniqueLockState>>>,
    cvars: Vec<Condvar>,
}

#[derive(Clone, Copy, Debug, Default)]
struct UniqueLockState {
    owner: u64,
    depth: usize,
}

#[derive(Debug)]
pub struct UniqueKeyGuard {
    table: Arc<UniqueLockTable>,
    shard: usize,
    key: Vec<u8>,
    owner: u64,
}

impl UniqueLockTable {
    pub fn new(shards: usize) -> Arc<Self> {
        let shards = shards.max(1);
        let mut tables = Vec::with_capacity(shards);
        let mut cvars = Vec::with_capacity(shards);
        for _ in 0..shards {
            tables.push(Mutex::new(HashMap::new()));
            cvars.push(Condvar::new());
        }
        Arc::new(Self {
            shards: tables,
            cvars,
        })
    }

    pub fn lock(self: &Arc<Self>, key: Vec<u8>, owner: u64) -> UniqueKeyGuard {
        let shard = self.shard(&key);
        let mut map = self.shards[shard].lock().expect("unique lock poisoned");
        loop {
            let state = map.entry(key.clone()).or_default();
            if state.owner == 0 || state.owner == owner {
                state.owner = owner;
                state.depth += 1;
                return UniqueKeyGuard {
                    table: Arc::clone(self),
                    shard,
                    key,
                    owner,
                };
            }
            map = self.cvars[shard].wait(map).expect("unique lock poisoned");
        }
    }

    fn unlock(&self, shard: usize, key: Vec<u8>, owner: u64) {
        if let Ok(mut map) = self.shards[shard].lock() {
            if let Some(state) = map.get_mut(&key)
                && state.owner == owner
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

#[allow(dead_code)]
pub(crate) fn _keep_rowid_use(_: RowId) {}
