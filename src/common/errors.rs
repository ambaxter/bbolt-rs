use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
  // These errors can be returned when opening or calling methods on a DB.
  /// DatabaseNotOpen is returned when a DB instance is accessed before it
  /// is opened or after it is closed.
  #[error("database not open")]
  DatabaseNotOpen,
  /// DatabaseOpen is returned when opening a database that is
  /// already open.
  #[error("database already open")]
  DatabaseOpen,
  /// InvalidDatabase is returned when both meta pages on a database are invalid.
  /// This typically occurs when a file is not a bolt database.
  #[error("invalid database - meta_can_read: `{0}`")]
  InvalidDatabase(bool),
  /// InvalidMapping is returned when the database file fails to get mapped.
  #[error("database isn't correctly mapped")]
  InvalidMapping,
  /// ErrVersionMismatch is returned when the data file was created with a
  /// different version of Bolt.
  #[error("version mismatch")]
  VersionMismatch,
  /// Checksum is returned when either meta page checksum does not match.
  #[error("checksum mismatch")]
  ChecksumMismatch,
  /// Timeout is returned when a database cannot obtain an exclusive lock
  /// on the data file after the timeout passed to Open().
  #[error("timeout")]
  Timeout,
  // These errors can occur when beginning or committing a Tx.
  /// TxNotWritable is returned when performing a write operation on a
  /// read-only transaction.
  /// TODO: Can this even happen anymore?
  #[error("tx not writable")]
  TxNotWritable,
  /// TxClosed is returned when committing or rolling back a transaction
  /// that has already been committed or rolled back.
  /// TODO: Can this even happen anymore?
  #[error("tx closed")]
  TxClosed,
  /// DatabaseReadOnly is returned when a mutating transaction is started on a
  /// read-only database.
  /// TODO: Can this even happen anymore?
  #[error("database is in read-only mode")]
  DatabaseReadOnly,
  /// FreePagesNotLoaded is returned when a readonly transaction without
  /// preloading the free pages is trying to access the free pages.
  #[error("free pages are not pre-loaded")]
  FreePagesNotLoaded,
  // These errors can occur when putting or deleting a value or a bucket.
  /// BucketNotFound is returned when trying to access a bucket that has
  /// not been created yet.
  #[error("bucket not found")]
  BucketNotFound,
  /// BucketExists is returned when creating a bucket that already exists.
  #[error("bucket already exists")]
  BucketExists,
  /// BucketNameRequired is returned when creating a bucket with a blank name.
  #[error("bucket name required")]
  BucketNameRequired,
  /// KeyRequired is returned when inserting a zero-length key.
  #[error("key required")]
  KeyRequired,
  /// KeyTooLarge is returned when inserting a key that is larger than MaxKeySize.
  #[error("key too large")]
  KeyTooLarge,
  /// ValueTooLarge is returned when inserting a value that is larger than MaxValueSize.
  #[error("value too large")]
  ValueTooLarge,
  /// IncompatibleValue is returned when trying create or delete a bucket
  /// on an existing non-bucket key or when trying to create or delete a
  /// non-bucket key on an existing bucket key.
  #[error("incompatible value")]
  IncompatibleValue,

  #[error("mmap too large")]
  MMapTooLarge,
  // Chained errors from other sources
  #[error(transparent)]
  IO(#[from] io::Error),
  #[error(transparent)]
  Other(#[from] anyhow::Error),
}

impl PartialEq for Error {
  fn eq(&self, other: &Self) -> bool {
    match (self, other) {
      (&Error::DatabaseNotOpen, &Error::DatabaseNotOpen) => true,
      (Error::DatabaseOpen, Error::DatabaseOpen) => true,
      (Error::InvalidDatabase(_), Error::InvalidDatabase(_)) => true,
      (Error::InvalidMapping, Error::InvalidMapping) => true,
      (Error::VersionMismatch, Error::VersionMismatch) => true,
      (Error::ChecksumMismatch, Error::ChecksumMismatch) => true,
      (Error::Timeout, Error::Timeout) => true,
      (Error::TxNotWritable, Error::TxNotWritable) => true,
      (Error::TxClosed, Error::TxClosed) => true,
      (Error::DatabaseReadOnly, Error::DatabaseReadOnly) => true,
      (Error::FreePagesNotLoaded, Error::FreePagesNotLoaded) => true,
      (Error::BucketNotFound, Error::BucketNotFound) => true,
      (Error::BucketExists, Error::BucketExists) => true,
      (Error::BucketNameRequired, Error::BucketNameRequired) => true,
      (Error::KeyRequired, Error::KeyRequired) => true,
      (Error::KeyTooLarge, Error::KeyTooLarge) => true,
      (Error::ValueTooLarge, Error::ValueTooLarge) => true,
      (Error::IncompatibleValue, Error::IncompatibleValue) => true,
      (Error::MMapTooLarge, Error::MMapTooLarge) => true,
      _ => false,
    }
  }
}

impl Eq for Error {}

pub type Result<T, E = Error> = std::result::Result<T, E>;
