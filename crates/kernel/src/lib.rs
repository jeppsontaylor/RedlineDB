//! RedlineDB transactional storage kernel primitives.
//!
//! This crate starts with the correctness-critical foundation: typed storage IDs,
//! explicit on-disk page/WAL encodings, checksums, and MVCC visibility rules.

pub mod catalog;
pub mod engine;
pub mod error;
pub mod format;
pub mod heap;
pub mod index;
pub mod io;
pub mod storage;
pub mod txn;
pub mod wal;

pub use error::{Error, Result};
