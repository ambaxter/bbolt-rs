//! bbolt-rs is an implementation of the [etcd-io/bbolt](https://github.com/etcd-io/bbolt) database in Rust.
//! It successfully reads and commits, but it has some limitations.
//!
//! Features:
//! * Arena memory allocation per transaction
//! * Explicitly designed to prevent transaction dependant resources from escaping the transaction.
//! * RwLock based transactions
//! * File backed database
//! * Memory backed database
//! * Miri tested to prevent memory errors in unsafe blocks
//! * Simple and straightforward public APIs
//!
//! Currently not supported:
//! * Tx.copy
//! * Most of the main application
//! * A variety of DB Options including
//!   * no freelist sync
//!   * file open timeout
//! * Panic handling during bench
//!
//!
//! ## Feature flags
#![doc = document_features::document_features!()]

mod arch;
mod bucket;
mod common;
mod cursor;
mod db;
mod node;
#[cfg(test)]
mod test_support;
mod tx;

mod iter;
pub mod util;

pub use bucket::{BucketApi, BucketImpl, BucketRwApi, BucketRwImpl, BucketStats};
pub use common::errors::{Error, Result};
pub use common::ids::{PgId, TxId};
pub use common::page::PageInfo;
pub use cursor::{CursorApi, CursorImpl, CursorRwApi, CursorRwImpl};
pub use db::{Bolt, BoltOptions, BoltOptionsBuilder, DbApi, DbInfo, DbPath, DbRwAPI, DbStats};
pub use iter::{BucketIter, BucketIterMut, EntryIter};
pub use tx::check::TxCheck;
pub use tx::{TxApi, TxImpl, TxRef, TxRwApi, TxRwImpl, TxRwRef, TxRwRefApi, TxStats};
