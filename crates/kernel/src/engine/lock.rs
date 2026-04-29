use crate::format::{RelId, RowId, TxId};
use crate::{Error, Result};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Condvar, Mutex, RwLock};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct RowKey {
    pub(crate) rel_id: RelId,
    pub(crate) row_id: RowId,
}

#[derive(Debug, Default)]
struct LockShard {
    owners: Mutex<HashMap<RowKey, TxId>>,
    cvar: Condvar,
}

#[derive(Debug)]
pub struct RowLockManager {
    shards: Vec<LockShard>,
    timeout: RwLock<Duration>,
}

impl RowLockManager {
    pub fn new(shard_count: usize, timeout: Duration) -> Self {
        let shard_count = shard_count.max(1);
        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shards.push(LockShard::default());
        }
        Self {
            shards,
            timeout: RwLock::new(timeout),
        }
    }

    pub fn set_timeout(&self, timeout: Duration) {
        *self.timeout.write().expect("row lock timeout poisoned") = timeout;
    }

    pub fn lock(&self, rel_id: RelId, row_id: RowId, tx_id: TxId) -> Result<()> {
        let key = RowKey { rel_id, row_id };
        let shard = self.shard(key);
        let timeout = *self.timeout.read().expect("row lock timeout poisoned");
        let deadline = Instant::now() + timeout;
        let mut owners = shard
            .owners
            .lock()
            .map_err(|_| Error::CorruptPage("row lock shard poisoned"))?;

        loop {
            match owners.get(&key).copied() {
                None => {
                    owners.insert(key, tx_id);
                    return Ok(());
                }
                Some(owner) if owner == tx_id => return Ok(()),
                Some(_) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return Err(Error::LockTimeout);
                    }
                    let wait = deadline.saturating_duration_since(now);
                    let (next_owners, timeout) = shard
                        .cvar
                        .wait_timeout(owners, wait)
                        .map_err(|_| Error::CorruptPage("row lock wait poisoned"))?;
                    owners = next_owners;
                    if timeout.timed_out() {
                        return Err(Error::LockTimeout);
                    }
                }
            }
        }
    }

    pub fn unlock(&self, rel_id: RelId, row_id: RowId, tx_id: TxId) {
        let key = RowKey { rel_id, row_id };
        let shard = self.shard(key);
        if let Ok(mut owners) = shard.owners.lock()
            && owners.get(&key).copied() == Some(tx_id)
        {
            owners.remove(&key);
            shard.cvar.notify_all();
        }
    }

    fn shard(&self, key: RowKey) -> &LockShard {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        &self.shards[hasher.finish() as usize % self.shards.len()]
    }
}
