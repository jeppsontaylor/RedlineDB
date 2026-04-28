#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::ffi::{CStr, CString};
use std::fs;
use std::os::raw::{c_char, c_int, c_uchar, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use redlinedb_kernel::error::Error as KernelError;
use redlinedb_sql::{DbOptions, Error as SqlError, Step};

const RLDB_OK: c_int = 0;
const RLDB_ERROR: c_int = 1;
const RLDB_INTERNAL: c_int = 2;
const RLDB_BUSY: c_int = 5;
const RLDB_LOCKED: c_int = 6;
const RLDB_INTERRUPT: c_int = 9;
const RLDB_IOERR: c_int = 10;
const RLDB_SCHEMA: c_int = 17;
const RLDB_CONSTRAINT: c_int = 19;
const RLDB_MISMATCH: c_int = 20;
const RLDB_MISUSE: c_int = 21;
const RLDB_RANGE: c_int = 25;
const RLDB_NOTADB: c_int = 26;
const RLDB_ROW: c_int = 100;
const RLDB_DONE: c_int = 101;

const RLDB_NULL: c_int = 0;
const RLDB_INTEGER: c_int = 1;
const RLDB_REAL: c_int = 2;
const RLDB_TEXT: c_int = 3;
const RLDB_BLOB: c_int = 4;

#[repr(C)]
pub struct rldb_config {
    pub struct_size: u32,
    pub flags: u32,
    pub durability: u32,
    pub cache_bytes: u64,
    pub work_mem_bytes: u64,
    pub max_spill_bytes: u64,
    pub statement_cache_capacity: u32,
    pub busy_timeout_ms: u32,
}

#[allow(non_camel_case_types)]
pub struct rldb {
    db: Arc<redlinedb_sql::Database>,
    conn: Arc<redlinedb_sql::Connection>,
    path: PathBuf,
    last_code: AtomicI32,
    last_message: Mutex<CString>,
    interrupted: AtomicBool,
    active_statements: AtomicUsize,
}

#[allow(non_camel_case_types)]
pub struct rldb_stmt {
    db: *mut rldb,
    stmt: redlinedb_sql::Statement,
    column_names: Vec<CString>,
    text_cache: Vec<CString>,
}

#[allow(non_camel_case_types)]
pub struct rldb_backup {
    src_path: PathBuf,
    dst_path: PathBuf,
    done: bool,
    remaining: i64,
    pagecount: i64,
}

fn api<T>(f: impl FnOnce() -> T) -> Result<T, c_int> {
    catch_unwind(AssertUnwindSafe(f)).map_err(|_| RLDB_INTERNAL)
}

fn flatten_code(result: Result<Result<c_int, c_int>, c_int>) -> c_int {
    match result {
        Ok(Ok(code)) => code,
        Ok(Err(code)) => code,
        Err(code) => code,
    }
}

fn map_error(err: SqlError) -> c_int {
    match err {
        SqlError::Kernel(KernelError::LockTimeout)
        | SqlError::Kernel(KernelError::SerializationFailure) => RLDB_BUSY,
        SqlError::Kernel(KernelError::WriteConflict) => RLDB_LOCKED,
        SqlError::Kernel(KernelError::DatatypeMismatch) | SqlError::DatatypeMismatch => {
            RLDB_MISMATCH
        }
        SqlError::Kernel(KernelError::ConstraintViolation(_))
        | SqlError::ConstraintViolation(_) => RLDB_CONSTRAINT,
        SqlError::Kernel(KernelError::SchemaChanged) => RLDB_SCHEMA,
        SqlError::Kernel(KernelError::ObjectNotFound)
        | SqlError::UnknownTable(_)
        | SqlError::UnknownColumn(_) => RLDB_NOTADB,
        SqlError::ParameterOutOfRange(_) => RLDB_RANGE,
        SqlError::TransactionState(_) | SqlError::Bind(_) => RLDB_MISUSE,
        SqlError::Parse(_) => RLDB_ERROR,
        SqlError::UnsupportedSql(_) => RLDB_MISUSE,
        _ => RLDB_ERROR,
    }
}

fn sql_result<T>(result: std::result::Result<T, SqlError>) -> std::result::Result<T, c_int> {
    result.map_err(map_error)
}

fn io<T>(result: std::io::Result<T>) -> std::result::Result<T, c_int> {
    result.map_err(|_| RLDB_IOERR)
}

fn db_options_from_config(config: Option<&rldb_config>) -> DbOptions {
    let mut options = DbOptions::default();
    if let Some(config) = config {
        let page_size = options.engine.page_size.max(1);
        options.engine.buffer_pool_pages = (config.cache_bytes as usize / page_size).max(16);
        options.query_memory.work_mem_bytes = config.work_mem_bytes as usize;
        options.query_memory.max_spill_bytes = config.max_spill_bytes as usize;
        options.query_memory.batch_rows = config.statement_cache_capacity.max(1) as usize;
    }
    options
}

fn open_handle(path: &CStr, config: Option<&rldb_config>) -> Result<*mut rldb, c_int> {
    let path = path.to_str().map_err(|_| RLDB_MISMATCH)?;
    let options = db_options_from_config(config);
    let db = if Path::new(path).exists() {
        sql_result(redlinedb_sql::Database::open(path, options))?
    } else {
        sql_result(redlinedb_sql::Database::create(path, options))?
    };
    let conn = db.connect();
    let handle = Box::new(rldb {
        db,
        conn,
        path: PathBuf::from(path),
        last_code: AtomicI32::new(RLDB_OK),
        last_message: Mutex::new(CString::new("").unwrap()),
        interrupted: AtomicBool::new(false),
        active_statements: AtomicUsize::new(0),
    });
    Ok(Box::into_raw(handle))
}

fn with_db<R>(db: *mut rldb, f: impl FnOnce(&rldb) -> R) -> Result<R, c_int> {
    if db.is_null() {
        return Err(RLDB_MISUSE);
    }
    Ok(f(unsafe { &*db }))
}

fn refresh_text_cache(stmt: &mut rldb_stmt) -> Result<(), c_int> {
    stmt.text_cache.clear();
    for index in 0..stmt.stmt.column_count() {
        if let Ok(text) = stmt.stmt.column_text(index) {
            stmt.text_cache
                .push(CString::new(text).map_err(|_| RLDB_MISMATCH)?);
        } else {
            stmt.text_cache.push(CString::new("").unwrap());
        }
    }
    Ok(())
}

fn to_hex(bytes: &[u8]) -> CString {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = Vec::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(*byte >> 4) as usize]);
        out.push(HEX[(*byte & 0x0f) as usize]);
    }
    CString::new(out).unwrap_or_else(|_| CString::new("blob").unwrap())
}

fn exec_value(stmt: &redlinedb_sql::Statement, index: usize) -> Result<Option<CString>, c_int> {
    if let Ok(text) = stmt.column_text(index) {
        Ok(Some(CString::new(text).map_err(|_| RLDB_MISMATCH)?))
    } else if let Ok(blob) = stmt.column_blob(index) {
        Ok(Some(to_hex(blob)))
    } else if let Ok(v) = stmt.column_i64(index) {
        Ok(Some(CString::new(v.to_string()).unwrap()))
    } else if let Ok(v) = stmt.column_f64(index) {
        Ok(Some(CString::new(v.to_string()).unwrap()))
    } else {
        Ok(Some(CString::new("").unwrap()))
    }
}

fn recursive_copy(src: &Path, dst: &Path) -> std::io::Result<()> {
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let name = entry.file_name();
        if name == "owner.lock" {
            continue;
        }
        let src_path = entry.path();
        let dst_path = dst.join(&name);
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            fs::create_dir_all(&dst_path)?;
            recursive_copy(&src_path, &dst_path)?;
        } else if file_type.is_file() {
            fs::copy(&src_path, &dst_path)?;
        }
    }
    Ok(())
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_open(path: *const c_char, out_db: *mut *mut rldb) -> c_int {
    flatten_code(api(|| {
        if path.is_null() || out_db.is_null() {
            return Err(RLDB_MISUSE);
        }
        let handle = open_handle(unsafe { CStr::from_ptr(path) }, None)?;
        unsafe {
            *out_db = handle;
        }
        Ok(RLDB_OK)
    }))
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_open_v2(
    path: *const c_char,
    config: *const rldb_config,
    out_db: *mut *mut rldb,
) -> c_int {
    flatten_code(api(|| {
        if path.is_null() || out_db.is_null() {
            return Err(RLDB_MISUSE);
        }
        let config = if config.is_null() {
            None
        } else {
            Some(unsafe { &*config })
        };
        let handle = open_handle(unsafe { CStr::from_ptr(path) }, config)?;
        unsafe {
            *out_db = handle;
        }
        Ok(RLDB_OK)
    }))
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_close(db: *mut rldb) -> c_int {
    flatten_code(api(|| {
        let db_ref = unsafe { &*db };
        if db_ref.active_statements.load(Ordering::Relaxed) != 0 {
            return Err(RLDB_BUSY);
        }
        unsafe {
            drop(Box::from_raw(db));
        }
        Ok(RLDB_OK)
    }))
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_close_v2(db: *mut rldb) -> c_int {
    rldb_close(db)
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_prepare_v2(
    db: *mut rldb,
    sql: *const c_char,
    nbytes: c_int,
    out_stmt: *mut *mut rldb_stmt,
    tail: *mut *const c_char,
) -> c_int {
    flatten_code(api(|| {
        if db.is_null() || sql.is_null() || out_stmt.is_null() {
            return Err(RLDB_MISUSE);
        }
        let db_ref = unsafe { &*db };
        let sql_cstr = unsafe { CStr::from_ptr(sql) };
        let sql_text = if nbytes < 0 {
            sql_cstr.to_str().map_err(|_| RLDB_MISMATCH)?.to_owned()
        } else {
            let bytes = unsafe {
                std::slice::from_raw_parts(sql_cstr.as_ptr() as *const u8, nbytes as usize)
            };
            std::str::from_utf8(bytes)
                .map_err(|_| RLDB_MISMATCH)?
                .to_owned()
        };
        let stmt = sql_result(db_ref.conn.clone().prepare(&sql_text))?;
        let mut boxed = Box::new(rldb_stmt {
            db,
            stmt,
            column_names: Vec::new(),
            text_cache: Vec::new(),
        });
        for index in 0..boxed.stmt.column_count() {
            boxed
                .column_names
                .push(CString::new(boxed.stmt.column_name(index)).map_err(|_| RLDB_MISMATCH)?);
        }
        boxed
            .text_cache
            .resize_with(boxed.stmt.column_count(), || CString::new("").unwrap());
        db_ref.active_statements.fetch_add(1, Ordering::Relaxed);
        unsafe {
            *out_stmt = Box::into_raw(boxed);
            if !tail.is_null() {
                *tail = sql_cstr.as_ptr().wrapping_add(sql_text.len());
            }
        }
        Ok(RLDB_OK)
    }))
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_step(stmt: *mut rldb_stmt) -> c_int {
    flatten_code(api(|| {
        let stmt = unsafe { &mut *stmt };
        if unsafe { (*stmt.db).interrupted.load(Ordering::Relaxed) } {
            return Err(RLDB_INTERRUPT);
        }
        match sql_result(stmt.stmt.step())? {
            Step::Row => {
                refresh_text_cache(stmt)?;
                Ok(RLDB_ROW)
            }
            Step::Done => Ok(RLDB_DONE),
        }
    }))
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_reset(stmt: *mut rldb_stmt) -> c_int {
    flatten_code(api(|| {
        let stmt = unsafe { &mut *stmt };
        sql_result(stmt.stmt.reset())?;
        stmt.text_cache.clear();
        Ok(RLDB_OK)
    }))
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_finalize(stmt: *mut rldb_stmt) -> c_int {
    flatten_code(api(|| {
        if stmt.is_null() {
            return Err(RLDB_MISUSE);
        }
        let boxed = unsafe { Box::from_raw(stmt) };
        unsafe {
            (*boxed.db)
                .active_statements
                .fetch_sub(1, Ordering::Relaxed);
        }
        Ok(RLDB_OK)
    }))
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_clear_bindings(stmt: *mut rldb_stmt) -> c_int {
    flatten_code(api(|| {
        let stmt = unsafe { &mut *stmt };
        stmt.stmt.clear_bindings();
        Ok(RLDB_OK)
    }))
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_bind_null(stmt: *mut rldb_stmt, index: c_int) -> c_int {
    flatten_code(api(|| {
        let stmt = unsafe { &mut *stmt };
        sql_result(stmt.stmt.bind_null(index as usize))?;
        Ok(RLDB_OK)
    }))
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_bind_int64(stmt: *mut rldb_stmt, index: c_int, value: i64) -> c_int {
    flatten_code(api(|| {
        let stmt = unsafe { &mut *stmt };
        sql_result(stmt.stmt.bind_i64(index as usize, value))?;
        Ok(RLDB_OK)
    }))
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_bind_double(stmt: *mut rldb_stmt, index: c_int, value: f64) -> c_int {
    flatten_code(api(|| {
        let stmt = unsafe { &mut *stmt };
        sql_result(stmt.stmt.bind_f64(index as usize, value))?;
        Ok(RLDB_OK)
    }))
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_bind_text(
    stmt: *mut rldb_stmt,
    index: c_int,
    value: *const c_char,
    nbytes: c_int,
) -> c_int {
    flatten_code(api(|| {
        if value.is_null() {
            return Err(RLDB_MISUSE);
        }
        let stmt = unsafe { &mut *stmt };
        let bytes = if nbytes < 0 {
            unsafe { CStr::from_ptr(value) }.to_bytes().to_vec()
        } else {
            unsafe { std::slice::from_raw_parts(value as *const u8, nbytes as usize) }.to_vec()
        };
        let text = String::from_utf8(bytes).map_err(|_| RLDB_MISMATCH)?;
        sql_result(stmt.stmt.bind_text(index as usize, text))?;
        Ok(RLDB_OK)
    }))
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_bind_blob(
    stmt: *mut rldb_stmt,
    index: c_int,
    value: *const c_void,
    nbytes: c_int,
) -> c_int {
    flatten_code(api(|| {
        if value.is_null() {
            return Err(RLDB_MISUSE);
        }
        let stmt = unsafe { &mut *stmt };
        let slice = unsafe { std::slice::from_raw_parts(value as *const u8, nbytes as usize) };
        sql_result(stmt.stmt.bind_blob(index as usize, slice.to_vec()))?;
        Ok(RLDB_OK)
    }))
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_parameter_count(stmt: *mut rldb_stmt) -> c_int {
    if stmt.is_null() {
        return RLDB_MISUSE;
    }
    unsafe { (*stmt).stmt.parameter_count() as c_int }
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_bind_parameter_index(stmt: *mut rldb_stmt, name: *const c_char) -> c_int {
    flatten_code(api(|| {
        if name.is_null() {
            return Err(RLDB_MISUSE);
        }
        let stmt = unsafe { &mut *stmt };
        let name = unsafe { CStr::from_ptr(name) }
            .to_str()
            .map_err(|_| RLDB_MISMATCH)?;
        Ok(stmt.stmt.parameter_index(name).unwrap_or(0) as c_int)
    }))
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_column_count(stmt: *mut rldb_stmt) -> c_int {
    if stmt.is_null() {
        return RLDB_MISUSE;
    }
    unsafe { (*stmt).stmt.column_count() as c_int }
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_column_name(stmt: *mut rldb_stmt, index: c_int) -> *const c_char {
    if stmt.is_null() {
        return ptr::null();
    }
    unsafe {
        let stmt = &*stmt;
        stmt.column_names
            .get(index as usize)
            .map(|value| value.as_ptr())
            .unwrap_or(ptr::null())
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_column_type(stmt: *mut rldb_stmt, index: c_int) -> c_int {
    flatten_code(api(|| {
        let stmt = unsafe { &mut *stmt };
        if stmt.stmt.column_text(index as usize).is_ok() {
            Ok(RLDB_TEXT)
        } else if stmt.stmt.column_blob(index as usize).is_ok() {
            Ok(RLDB_BLOB)
        } else if stmt.stmt.column_i64(index as usize).is_ok() {
            Ok(RLDB_INTEGER)
        } else if stmt.stmt.column_f64(index as usize).is_ok() {
            Ok(RLDB_REAL)
        } else {
            Ok(RLDB_NULL)
        }
    }))
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_column_int64(stmt: *mut rldb_stmt, index: c_int) -> i64 {
    if stmt.is_null() {
        return 0;
    }
    unsafe { (*stmt).stmt.column_i64(index as usize).unwrap_or(0) }
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_column_double(stmt: *mut rldb_stmt, index: c_int) -> f64 {
    if stmt.is_null() {
        return 0.0;
    }
    unsafe { (*stmt).stmt.column_f64(index as usize).unwrap_or(0.0) }
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_column_text(stmt: *mut rldb_stmt, index: c_int) -> *const c_uchar {
    if stmt.is_null() {
        return ptr::null();
    }
    unsafe {
        let stmt = &*stmt;
        stmt.text_cache
            .get(index as usize)
            .map(|value| value.as_ptr() as *const c_uchar)
            .unwrap_or(ptr::null())
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_column_blob(stmt: *mut rldb_stmt, index: c_int) -> *const c_void {
    if stmt.is_null() {
        return ptr::null();
    }
    unsafe {
        match (*stmt).stmt.column_blob(index as usize) {
            Ok(blob) => blob.as_ptr() as *const c_void,
            Err(_) => ptr::null(),
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_column_bytes(stmt: *mut rldb_stmt, index: c_int) -> c_int {
    flatten_code(api(|| {
        let stmt = unsafe { &mut *stmt };
        if let Ok(text) = stmt.stmt.column_text(index as usize) {
            Ok(text.len() as c_int)
        } else if let Ok(blob) = stmt.stmt.column_blob(index as usize) {
            Ok(blob.len() as c_int)
        } else {
            Ok(8)
        }
    }))
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_exec(
    db: *mut rldb,
    sql: *const c_char,
    callback: Option<
        extern "C" fn(*mut c_void, c_int, *mut *mut c_char, *mut *mut c_char) -> c_int,
    >,
    ctx: *mut c_void,
    _errmsg: *mut *mut c_char,
) -> c_int {
    flatten_code(api(|| {
        if db.is_null() || sql.is_null() {
            return Err(RLDB_MISUSE);
        }
        let db_ref = unsafe { &*db };
        let sql_text = unsafe { CStr::from_ptr(sql) }
            .to_str()
            .map_err(|_| RLDB_MISMATCH)?;
        let mut stmt = sql_result(db_ref.conn.clone().prepare(sql_text))?;
        while let Step::Row = sql_result(stmt.step())? {
            if let Some(callback) = callback {
                let column_count = stmt.column_count();
                let mut value_strings: Vec<Option<CString>> = Vec::with_capacity(column_count);
                let mut name_strings: Vec<CString> = Vec::with_capacity(column_count);
                for index in 0..column_count {
                    name_strings
                        .push(CString::new(stmt.column_name(index)).map_err(|_| RLDB_MISMATCH)?);
                    value_strings.push(exec_value(&stmt, index)?);
                }
                let mut argv: Vec<*mut c_char> = value_strings
                    .iter_mut()
                    .map(|value| {
                        value
                            .as_mut()
                            .map(|s| s.as_ptr() as *mut c_char)
                            .unwrap_or(ptr::null_mut())
                    })
                    .collect();
                let mut colnames: Vec<*mut c_char> = name_strings
                    .iter_mut()
                    .map(|value| value.as_ptr() as *mut c_char)
                    .collect();
                let rc = callback(
                    ctx,
                    column_count as c_int,
                    argv.as_mut_ptr(),
                    colnames.as_mut_ptr(),
                );
                if rc != 0 {
                    return Err(RLDB_ERROR);
                }
            }
        }
        Ok(RLDB_OK)
    }))
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_errcode(db: *mut rldb) -> c_int {
    with_db(db, |db| db.last_code.load(Ordering::Relaxed)).unwrap_or(RLDB_MISUSE)
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_errmsg(db: *mut rldb) -> *const c_char {
    with_db(db, |db| {
        db.last_message
            .lock()
            .map(|value| value.as_ptr())
            .unwrap_or(ptr::null())
    })
    .unwrap_or(ptr::null())
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_free(ptr: *mut c_void) {
    if !ptr.is_null() {
        unsafe {
            drop(CString::from_raw(ptr as *mut c_char));
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_interrupt(db: *mut rldb) {
    let _ = with_db(db, |db| db.interrupted.store(true, Ordering::Relaxed));
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_busy_timeout(_db: *mut rldb, _milliseconds: c_int) -> c_int {
    RLDB_OK
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_changes(db: *mut rldb) -> c_int {
    with_db(db, |db| db.conn.changes() as c_int).unwrap_or(RLDB_MISUSE)
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_last_insert_rowid(db: *mut rldb) -> i64 {
    with_db(db, |db| db.conn.last_insert_rowid().unwrap_or(0)).unwrap_or(0)
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_checkpoint(db: *mut rldb) -> c_int {
    flatten_code(api(|| {
        let db = unsafe { &*db };
        sql_result(db.db.checkpoint())?;
        Ok(RLDB_OK)
    }))
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_vacuum(db: *mut rldb) -> c_int {
    flatten_code(api(|| {
        let db = unsafe { &*db };
        sql_result(db.db.vacuum())?;
        Ok(RLDB_OK)
    }))
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_stats_json(db: *mut rldb, out_json: *mut *mut c_char) -> c_int {
    flatten_code(api(|| {
        if out_json.is_null() {
            return Err(RLDB_MISUSE);
        }
        let db = unsafe { &*db };
        let stats = sql_result(db.db.stats())?;
        let json = format!(
            "{{\"schema_epoch\":{},\"resident_heap_pages\":{},\"wal_written_lsn\":{},\"wal_durable_lsn\":{}}}",
            db.db.schema_epoch().0,
            stats.resident_heap_pages,
            stats.wal_written_lsn.0,
            stats.wal_durable_lsn.0
        );
        let c = CString::new(json).unwrap();
        unsafe {
            *out_json = c.into_raw();
        }
        Ok(RLDB_OK)
    }))
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_backup_init(
    src: *mut rldb,
    dst_path: *const c_char,
    _dst_config: *const rldb_config,
    out: *mut *mut rldb_backup,
) -> c_int {
    flatten_code(api(|| {
        if src.is_null() || dst_path.is_null() || out.is_null() {
            return Err(RLDB_MISUSE);
        }
        let src_ref = unsafe { &*src };
        let dst = unsafe { CStr::from_ptr(dst_path) }
            .to_str()
            .map_err(|_| RLDB_MISMATCH)?
            .to_owned();
        let backup = Box::new(rldb_backup {
            src_path: src_ref.path.clone(),
            dst_path: PathBuf::from(dst),
            done: false,
            remaining: 1,
            pagecount: 1,
        });
        unsafe {
            *out = Box::into_raw(backup);
        }
        Ok(RLDB_OK)
    }))
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_backup_step(backup: *mut rldb_backup, _batches: c_int) -> c_int {
    flatten_code(api(|| {
        if backup.is_null() {
            return Err(RLDB_MISUSE);
        }
        let backup = unsafe { &mut *backup };
        if !backup.done {
            if backup.dst_path.exists() {
                io(fs::remove_dir_all(&backup.dst_path))?;
            }
            io(fs::create_dir_all(&backup.dst_path))?;
            io(recursive_copy(&backup.src_path, &backup.dst_path))?;
            backup.done = true;
            backup.remaining = 0;
        }
        Ok(RLDB_DONE)
    }))
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_backup_finish(backup: *mut rldb_backup) -> c_int {
    if backup.is_null() {
        return RLDB_MISUSE;
    }
    RLDB_OK
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_backup_close(backup: *mut rldb_backup) -> c_int {
    if backup.is_null() {
        return RLDB_MISUSE;
    }
    unsafe {
        drop(Box::from_raw(backup));
    }
    RLDB_OK
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_backup_remaining(backup: *mut rldb_backup) -> c_int {
    if backup.is_null() {
        return RLDB_MISUSE;
    }
    unsafe { (*backup).remaining as c_int }
}

#[unsafe(no_mangle)]
pub extern "C" fn rldb_backup_pagecount(backup: *mut rldb_backup) -> c_int {
    if backup.is_null() {
        return RLDB_MISUSE;
    }
    unsafe { (*backup).pagecount as c_int }
}
