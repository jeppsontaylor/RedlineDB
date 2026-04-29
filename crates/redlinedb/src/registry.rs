use std::collections::HashMap;
use std::fs::{self, File, OpenOptions as FsOpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use crate::error::{Error, ErrorCode, Result};
use crate::options::OpenOptions;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct OpenFingerprint {
    pub read_only: bool,
    pub durability: crate::options::Durability,
    pub memory_cache_bytes: usize,
    pub optimizer: crate::options::OptimizerOptions,
    pub query_memory: crate::options::QueryMemoryOptions,
    pub stats: crate::options::AnalyzeOptions,
    pub process_owner_lock: bool,
}

impl OpenFingerprint {
    fn from_options(options: &OpenOptions) -> Self {
        Self {
            read_only: options.read_only,
            durability: options.durability,
            memory_cache_bytes: options.memory.cache_bytes,
            optimizer: options.optimizer.clone(),
            query_memory: options.query_memory.clone(),
            stats: options.stats.clone(),
            process_owner_lock: options.process_owner_lock,
        }
    }

    fn compatible_with(&self, other: &Self) -> bool {
        self.durability == other.durability
            && self.memory_cache_bytes == other.memory_cache_bytes
            && self.optimizer == other.optimizer
            && self.query_memory == other.query_memory
            && self.stats == other.stats
            && self.process_owner_lock == other.process_owner_lock
    }
}

pub(crate) struct DatabaseEntry {
    pub db: Arc<redlinedb_sql::Database>,
    pub fingerprint: OpenFingerprint,
    pub _owner_lock: Option<Arc<File>>,
    pub path: PathBuf,
    pub interrupt: Arc<AtomicBool>,
    pub busy_timeout: Mutex<Duration>,
}

#[derive(Default)]
struct Registry {
    entries: HashMap<PathBuf, Weak<DatabaseEntry>>,
}

static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();

fn registry() -> &'static Mutex<Registry> {
    REGISTRY.get_or_init(|| Mutex::new(Registry::default()))
}

pub(crate) fn open_database(
    path: impl AsRef<Path>,
    options: &OpenOptions,
    create: bool,
) -> Result<Arc<DatabaseEntry>> {
    let path = normalize_path(path.as_ref(), create || options.create)?;
    let fingerprint = OpenFingerprint::from_options(options);
    let mut registry = registry().lock().expect("registry poisoned");
    if let Some(existing) = registry.entries.get(&path).and_then(Weak::upgrade) {
        if existing.fingerprint.read_only && !fingerprint.read_only {
            return Err(Error::new(
                ErrorCode::Busy,
                "database already open read-only in this process",
            ));
        }
        if !existing.fingerprint.compatible_with(&fingerprint) {
            return Err(Error::new(
                ErrorCode::Misuse,
                "database already open with incompatible options",
            ));
        }
        return Ok(existing);
    }

    if create {
        fs::create_dir_all(&path)?;
    }
    if !path.exists() && !create {
        return Err(Error::new(
            ErrorCode::NotFound,
            "database directory does not exist",
        ));
    }

    let sql_options = crate::sql_options(options);
    let db = if create || options.create {
        redlinedb_sql::Database::create(&path, sql_options)?
    } else {
        redlinedb_sql::Database::open(&path, sql_options)?
    };

    let owner_lock = if options.process_owner_lock && !options.read_only {
        Some(Arc::new(acquire_owner_lock(&path)?))
    } else {
        None
    };

    let entry = Arc::new(DatabaseEntry {
        db,
        fingerprint,
        _owner_lock: owner_lock,
        path: path.clone(),
        interrupt: Arc::new(AtomicBool::new(false)),
        busy_timeout: Mutex::new(options.busy_timeout),
    });
    registry.entries.insert(path, Arc::downgrade(&entry));
    Ok(entry)
}

fn normalize_path(path: &Path, create: bool) -> Result<PathBuf> {
    if path.exists() {
        return Ok(fs::canonicalize(path)?);
    }
    if create {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent)?;
        }
        fs::create_dir_all(path)?;
        return Ok(fs::canonicalize(path)?);
    }
    Ok(path.to_path_buf())
}

fn acquire_owner_lock(path: &Path) -> Result<File> {
    let lock_path = path.join("owner.lock");
    let file = FsOpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(lock_path)?;
    lock_owner_file(&file)?;
    Ok(file)
}

#[cfg(unix)]
fn lock_owner_file(file: &File) -> Result<()> {
    use std::os::unix::io::AsRawFd;

    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc == 0 {
        Ok(())
    } else {
        Err(Error::new(
            ErrorCode::Busy,
            format!("database already open: {}", io::Error::last_os_error()),
        ))
    }
}

#[cfg(not(unix))]
fn lock_owner_file(_file: &File) -> Result<()> {
    Ok(())
}
