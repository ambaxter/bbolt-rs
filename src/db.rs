use crate::arch::size::MAX_MAP_SIZE;
use crate::bucket::BucketRwIApi;
use crate::common::bucket::BucketHeader;
use crate::common::bump::PinBump;
use crate::common::defaults::{
  DEFAULT_ALLOC_SIZE, DEFAULT_MAX_BATCH_DELAY, DEFAULT_MAX_BATCH_SIZE, DEFAULT_PAGE_SIZE, MAGIC,
  MAX_MMAP_STEP, PGID_NO_FREE_LIST, VERSION,
};
use crate::common::lock::LockGuard;
use crate::common::meta::{MappedMetaPage, Meta};
use crate::common::page::{CoerciblePage, MutPage, PageHeader, RefPage};
use crate::common::pool::{SyncPool, SyncReusable};
use crate::common::self_owned::SelfOwned;
use crate::common::tree::MappedLeafPage;
use crate::common::{BVec, PgId, SplitRef, TxId};
use crate::freelist::{Freelist, MappedFreeListPage};
use crate::tx::{
  TxClosingState, TxIApi, TxImpl, TxRef, TxRwApi, TxRwCell, TxRwImpl, TxRwRef, TxStats,
};
use crate::{Error, TxApi, TxRwRefApi};
use aligners::{alignment, AlignedBytes};
use anyhow::anyhow;
use fs4::FileExt;
use memmap2::{Advice, MmapOptions, MmapRaw};
use monotonic_timer::{Guard, Timer};
use parking_lot::{Mutex, MutexGuard, RwLock};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::{mpsc, Arc, OnceLock, Weak};
use std::time::Duration;
#[cfg(feature = "try-begin")]
use std::time::Instant;
use std::{fs, io, mem, thread};
use typed_builder::TypedBuilder;

/// Read-only DB API
pub trait DbApi: Clone + Send + Sync
where
  Self: Sized,
{
  /// Begin starts a new transaction.
  ///
  /// Multiple read-only transactions can be used concurrently but only one
  /// write transaction can be used at a time. Starting multiple write transactions
  /// will cause the calls to block and be serialized until the current write
  /// transaction finishes.
  ///
  /// Transactions should not be dependent on one another. Opening a read
  /// transaction and a write transaction in the same goroutine can cause the
  /// writer to deadlock because the database periodically needs to re-mmap itself
  /// as it grows and it cannot do that while a read transaction is open.
  ///
  /// If a long running read transaction (for example, a snapshot transaction) is
  /// needed, you might want to set BoltOptions.initial_map_size to a large enough value
  /// to avoid potential blocking of write transaction.
  ///
  /// IMPORTANT: You must drop the read-only transactions after you are finished or
  /// else the database will not reclaim old pages.
  ///
  /// ```rust
  /// use bbolt_rs::*;
  ///
  /// fn main() -> Result<()> {
  ///   let mut db = Bolt::open_mem()?;
  ///
  ///   db.update(|mut tx| {
  ///     let mut b = tx.create_bucket_if_not_exists("test")?;
  ///     b.put("key", "value")?;
  ///     Ok(())
  ///   })?;
  ///
  ///   let tx = db.begin()?;
  ///   let b = tx.bucket("test").unwrap();
  ///   assert_eq!(Some(b"value".as_ref()), b.get("key"));
  ///
  ///   Ok(())
  /// }
  /// ```
  fn begin(&self) -> crate::Result<impl TxApi>;

  #[cfg(feature = "try-begin")]
  fn try_begin(&self) -> crate::Result<Option<impl TxApi>>;

  #[cfg(feature = "try-begin")]
  fn try_begin_for(&self, duration: Duration) -> crate::Result<Option<impl TxApi>>;

  #[cfg(feature = "try-begin")]
  fn try_begin_until(&self, instant: Instant) -> crate::Result<Option<impl TxApi>>;

  /// View executes a function within the context of a managed read-only transaction.
  /// Any error that is returned from the function is returned from the View() method.
  ///
  /// ```rust
  /// use bbolt_rs::*;
  ///
  /// fn main() -> Result<()> {
  ///   let mut db = Bolt::open_mem()?;
  ///
  ///   db.update(|mut tx| {
  ///     let mut b = tx.create_bucket_if_not_exists("test")?;
  ///     b.put("key", "value")?;
  ///     Ok(())
  ///   })?;
  ///
  ///   db.view(|tx| {
  ///     let b = tx.bucket("test").unwrap();
  ///     assert_eq!(Some(b"value".as_ref()), b.get("key"));
  ///     Ok(())
  ///   })?;
  ///
  ///   Ok(())
  /// }
  /// ```
  fn view<'tx, F: Fn(TxRef<'tx>) -> crate::Result<()>>(&'tx self, f: F) -> crate::Result<()>;

  /// Stats retrieves ongoing performance stats for the database.
  ///
  /// This is only updated when a transaction closes.
  fn stats(&self) -> Arc<DbStats>;

  // TODO:
  //fn path(&self) -> &Path;

  /// Close releases all database resources.
  ///
  /// It will block waiting for any open transactions to finish
  /// before closing the database and returning.
  ///
  /// Once closed, other instances return [Error::DatabaseNotOpen]
  ///
  /// ```rust
  /// use bbolt_rs::*;
  ///
  /// fn main() -> Result<()> {
  ///   let mut db = Bolt::open_mem()?;
  ///   let cloned = db.clone();
  ///   db.update(|mut tx| {
  ///     let mut b = tx.create_bucket_if_not_exists("test")?;
  ///     b.put("key", "value")?;
  ///     Ok(())
  ///   })?;
  ///
  ///   db.close();
  ///   assert_eq!(Some(Error::DatabaseNotOpen), cloned.begin().err());
  ///   Ok(())
  /// }
  /// ```
  fn close(self);
}

/// RW DB API
pub trait DbRwAPI: DbApi {
  /// Starts a new transaction.
  /// Multiple read-only transactions can be used concurrently but only one
  /// write transaction can be used at a time. Starting multiple write transactions
  /// will cause the calls to block and be serialized until the current write
  /// transaction finishes.
  ///
  /// Transactions should not be dependent on one another. Opening a read
  /// transaction and a write transaction in the same goroutine can cause the
  /// writer to deadlock because the database periodically needs to re-mmap itself
  /// as it grows and it cannot do that while a read transaction is open.
  ///
  /// If a long running read transaction (for example, a snapshot transaction) is
  /// needed, you might want to set BoltOptions.initial_map_size to a large enough value
  /// to avoid potential blocking of write transaction.
  ///
  /// Dropping the transaction will cause it to rollback.
  ///
  /// ```rust
  /// use bbolt_rs::*;
  ///
  /// fn main() -> Result<()> {
  ///   let mut db = Bolt::open_mem()?;
  ///
  ///   let mut tx = db.begin_rw()?;
  ///   let mut b = tx.create_bucket_if_not_exists("test")?;
  ///   b.put("key", "value")?;
  ///   tx.commit()?;
  ///
  ///   db.view(|tx| {
  ///     let b = tx.bucket("test").unwrap();
  ///     assert_eq!(Some(b"value".as_ref()), b.get("key"));
  ///     Ok(())
  ///   })?;
  ///
  ///   Ok(())
  /// }
  /// ```
  fn begin_rw(&mut self) -> crate::Result<impl TxRwApi>;

  #[cfg(feature = "try-begin")]
  fn try_begin_rw(&self) -> crate::Result<Option<impl TxRwApi>>;

  #[cfg(feature = "try-begin")]
  fn try_begin_rw_for(&self, duration: Duration) -> crate::Result<Option<impl TxRwApi>>;

  #[cfg(feature = "try-begin")]
  fn try_begin_rw_until(&self, instant: Instant) -> crate::Result<Option<impl TxRwApi>>;

  /// Executes a function within the context of a read-write managed transaction.
  ///
  /// If no error is returned from the function then the transaction is committed.
  /// If an error is returned then the entire transaction is rolled back.
  /// Any error that is returned from the function or returned from the commit is
  /// returned from the Update() method.
  ///
  /// ```rust
  /// use bbolt_rs::*;
  ///
  /// fn main() -> Result<()> {
  ///   let mut db = Bolt::open_mem()?;
  ///
  ///   db.update(|mut tx| {
  ///     let mut b = tx.create_bucket_if_not_exists("test")?;
  ///     b.put("key", "value")?;
  ///     Ok(())
  ///   })?;
  ///
  ///   db.view(|tx| {
  ///     let b = tx.bucket("test").unwrap();
  ///     assert_eq!(Some(b"value".as_ref()), b.get("key"));
  ///     Ok(())
  ///   })?;
  ///
  ///   Ok(())
  /// }
  /// ```
  fn update<'tx, F: FnMut(TxRwRef<'tx>) -> crate::Result<()>>(
    &'tx mut self, f: F,
  ) -> crate::Result<()>;

  /// Calls a function as part of a batch. It behaves similar to Update,
  /// except:
  ///
  /// 1. concurrent Batch calls can be combined into a single Bolt
  /// transaction.
  ///
  /// 2. the function passed to Batch may be called multiple times,
  /// regardless of whether it returns error or not.
  ///
  /// This means that Batch function side effects must be idempotent and
  /// take permanent effect only after a successful return is seen in
  /// caller.
  ///
  /// The maximum batch size and delay can be adjusted with MaxBatchSize
  /// and MaxBatchDelay, respectively.
  ///
  /// Batch is only useful when there are multiple threads calling it.
  ///
  /// ```rust
  /// use bbolt_rs::*;
  ///
  /// fn main() -> Result<()> {
  ///   let mut db = Bolt::open_mem()?;
  ///
  ///   db.batch(|mut tx| {
  ///     let mut b = tx.create_bucket_if_not_exists("test")?;
  ///     b.put("key", "value")?;
  ///     Ok(())
  ///   })?;
  ///
  ///   db.view(|tx| {
  ///     let b = tx.bucket("test").unwrap();
  ///     assert_eq!(Some(b"value".as_ref()), b.get("key"));
  ///     Ok(())
  ///   })?;
  ///
  ///   Ok(())
  /// }
  /// ```
  fn batch<F>(&mut self, f: F) -> crate::Result<()>
  where
    F: FnMut(&mut TxRwRef) -> crate::Result<()> + Send + Sync + Clone + 'static;

  /// Executes fdatasync() against the database file handle.
  ///
  /// This is not necessary under normal operation, however, if you use NoSync
  /// then it allows you to force the database file to sync against the disk.
  /// ```rust
  /// use bbolt_rs::*;
  ///
  /// fn main() -> Result<()> {
  ///   let mut db = Bolt::open_mem()?;
  ///
  ///   db.batch(|mut tx| {
  ///     let mut b = tx.create_bucket_if_not_exists("test")?;
  ///     b.put("key", "value")?;
  ///     Ok(())
  ///   })?;
  ///
  ///   db.sync()?;
  ///
  ///   Ok(())
  /// }
  /// ```
  fn sync(&mut self) -> crate::Result<()>;
}

#[derive(Default)]
/// Stats represents statistics about the database.
pub struct DbStats {
  /// global, ongoing stats.
  tx_stats: TxStats,

  // Freelist stats
  /// total number of free pages on the freelist
  free_page_n: AtomicI64,
  /// total number of pending pages on the freelist
  pending_page_n: AtomicI64,
  /// total bytes allocated in free pages
  free_alloc: AtomicI64,
  /// total bytes used by the freelist
  free_list_in_use: AtomicI64,

  // transaction stats
  /// total number of started read transactions
  tx_n: AtomicI64,
  /// number of currently open read transactions
  open_tx_n: AtomicI64,
}

impl DbStats {
  /// global, ongoing stats.
  pub fn tx_stats(&self) -> &TxStats {
    &self.tx_stats
  }

  /// total number of free pages on the freelist
  pub fn free_page_n(&self) -> i64 {
    self.free_page_n.load(Ordering::Acquire)
  }

  pub(crate) fn set_free_page_n(&self, value: i64) {
    self.free_page_n.store(value, Ordering::Release);
  }

  /// total number of pending pages on the freelist
  pub fn pending_page_n(&self) -> i64 {
    self.pending_page_n.load(Ordering::Acquire)
  }

  pub(crate) fn set_pending_page_n(&self, value: i64) {
    self.pending_page_n.store(value, Ordering::Release);
  }

  /// total bytes allocated in free pages
  pub fn free_alloc(&self) -> i64 {
    self.free_alloc.load(Ordering::Acquire)
  }

  pub(crate) fn set_free_alloc(&self, value: i64) {
    self.free_alloc.store(value, Ordering::Release);
  }

  /// total bytes used by the freelist
  pub fn free_list_in_use(&self) -> i64 {
    self.free_list_in_use.load(Ordering::Acquire)
  }

  pub(crate) fn set_free_list_in_use(&self, value: i64) {
    self.free_list_in_use.store(value, Ordering::Release);
  }

  /// total number of started read transactions
  pub fn tx_n(&self) -> i64 {
    self.tx_n.load(Ordering::Acquire)
  }

  pub(crate) fn inc_tx_n(&self, delta: i64) {
    self.tx_n.fetch_add(delta, Ordering::AcqRel);
  }

  /// number of currently open read transactions
  pub fn open_tx_n(&self) -> i64 {
    self.open_tx_n.load(Ordering::Acquire)
  }

  pub(crate) fn sub(&self, rhs: &DbStats) -> DbStats {
    let diff = self.clone();
    diff.inc_tx_n(-rhs.tx_n());
    diff.tx_stats.sub_assign(&rhs.tx_stats);
    diff
  }
}

impl Clone for DbStats {
  fn clone(&self) -> Self {
    DbStats {
      tx_stats: self.tx_stats.clone(),
      free_page_n: self.free_page_n().into(),
      pending_page_n: self.pending_page_n().into(),
      free_alloc: self.free_alloc().into(),
      free_list_in_use: self.free_list_in_use().into(),
      tx_n: self.tx_n().into(),
      open_tx_n: self.open_tx_n().into(),
    }
  }
}

pub struct DbState {
  txs: Vec<TxId>,
  rwtx: Option<TxId>,
  is_open: bool,
  current_meta: Meta,
}

impl DbState {
  fn new(current_meta: Meta) -> DbState {
    DbState {
      txs: vec![],
      rwtx: None,
      is_open: true,
      current_meta,
    }
  }
}

fn mmap_size(page_size: usize, size: u64) -> crate::Result<u64> {
  for i in 15..=30usize {
    if size <= 1 << i {
      return Ok(1 << i);
    }
  }
  if size > MAX_MAP_SIZE.bytes() as u64 {
    return Err(Error::MMapTooLarge);
  }

  let mut sz = size;
  let remainder = sz % MAX_MMAP_STEP.bytes() as u64;
  if remainder > 0 {
    sz += MAX_MMAP_STEP.bytes() as u64 - remainder;
  }

  let ps = page_size as u64;
  if sz % ps != 0 {
    sz = ((sz / ps) + 1) * ps;
  }

  if sz > MAX_MAP_SIZE.bytes() as u64 {
    sz = MAX_MAP_SIZE.bytes() as u64;
  }

  Ok(sz)
}

pub(crate) trait DBBackend: Send + Sync {
  fn page_size(&self) -> usize;
  fn data_size(&self) -> u64;
  fn meta(&self) -> Meta {
    let meta0 = self.meta0();
    let meta1 = self.meta1();
    let (meta_a, meta_b) = {
      if meta1.meta.txid() > meta0.meta.txid() {
        (meta1.meta, meta0.meta)
      } else {
        (meta0.meta, meta1.meta)
      }
    };
    if meta_a.validate().is_ok() {
      return meta_a;
    } else if meta_b.validate().is_ok() {
      return meta_b;
    }
    panic!("bolt.db.meta: invalid meta page")
  }

  fn meta0(&self) -> MappedMetaPage;

  fn meta1(&self) -> MappedMetaPage;

  fn page<'tx>(&self, pg_id: PgId) -> RefPage<'tx>;

  /// grow grows the size of the database to the given `size`.
  fn grow(&self, size: u64) -> crate::Result<()>;

  /// mmap opens the underlying memory-mapped file and initializes the meta references.
  /// min_size is the minimum size that the new mmap can be.
  fn mmap(&mut self, min_size: u64, tx: TxRwCell) -> crate::Result<()>;

  fn fsync(&self) -> crate::Result<()>;
  fn write_all_at(&self, buffer: &[u8], offset: u64) -> crate::Result<usize>;

  fn freelist(&self) -> MutexGuard<Freelist>;
}

struct ClosedBackend {}

impl DBBackend for ClosedBackend {
  fn page_size(&self) -> usize {
    unreachable!()
  }

  fn data_size(&self) -> u64 {
    unreachable!()
  }

  fn meta0(&self) -> MappedMetaPage {
    unreachable!()
  }

  fn meta1(&self) -> MappedMetaPage {
    unreachable!()
  }

  fn page<'tx>(&self, _pg_id: PgId) -> RefPage<'tx> {
    unreachable!()
  }

  fn grow(&self, _size: u64) -> crate::Result<()> {
    unreachable!()
  }

  fn mmap(&mut self, _min_size: u64, _tx: TxRwCell) -> crate::Result<()> {
    unreachable!()
  }

  fn fsync(&self) -> crate::Result<()> {
    unreachable!()
  }

  fn write_all_at(&self, _buffer: &[u8], _offset: u64) -> crate::Result<usize> {
    unreachable!()
  }

  fn freelist(&self) -> MutexGuard<Freelist> {
    unreachable!()
  }
}

struct MemBackend {
  mmap: Mutex<AlignedBytes<alignment::Page>>,
  freelist: OnceLock<Mutex<Freelist>>,
  page_size: usize,
  alloc_size: u64,
  /// current on disk file size
  file_size: u64,
  data_size: u64,
}

unsafe impl Send for MemBackend {}
unsafe impl Sync for MemBackend {}

impl DBBackend for MemBackend {
  fn page_size(&self) -> usize {
    self.page_size
  }

  fn data_size(&self) -> u64 {
    self.data_size
  }

  fn meta0(&self) -> MappedMetaPage {
    // Safe because we will never actually mutate this ptr
    unsafe { MappedMetaPage::new(self.mmap.lock().as_ptr().cast_mut()) }
  }

  fn meta1(&self) -> MappedMetaPage {
    // Safe because we will never actually mutate this ptr
    unsafe { MappedMetaPage::new(self.mmap.lock().as_ptr().add(self.page_size).cast_mut()) }
  }

  fn page<'tx>(&self, pg_id: PgId) -> RefPage<'tx> {
    let mmap = self.mmap.lock();
    debug_assert!(((pg_id.0 as usize + 1) * self.page_size) <= mmap.len());
    unsafe {
      RefPage::new(
        mmap
          .as_ptr()
          .offset(pg_id.0 as isize * self.page_size as isize),
      )
    }
  }

  fn grow(&self, size: u64) -> crate::Result<()> {
    let mut mmap = self.mmap.lock();
    let mut new_mmap = AlignedBytes::new_zeroed(size as usize);
    new_mmap[0..mmap.len()].copy_from_slice(&mmap);
    mem::swap(mmap.deref_mut(), &mut new_mmap);
    Ok(())
  }

  fn mmap(&mut self, min_size: u64, _tx: TxRwCell) -> crate::Result<()> {
    let mut size = {
      let mmap = self.mmap.lock();
      if mmap.len() < self.page_size * 2 {
        return Err(Error::MMapFileSizeTooSmall);
      }
      (mmap.len() as u64).max(min_size)
    };
    size = mmap_size(self.page_size, size)?;
    let r0 = self.meta0().meta.validate();
    let r1 = self.meta1().meta.validate();

    if r0.is_err() && r1.is_err() {
      return r0;
    }

    self.data_size = size;
    Ok(())
  }

  fn fsync(&self) -> crate::Result<()> {
    Ok(())
  }

  fn write_all_at(&self, buffer: &[u8], offset: u64) -> crate::Result<usize> {
    let mut mmap = self.mmap.lock();
    let write_to = &mut mmap[offset as usize..offset as usize + buffer.len()];
    write_to.copy_from_slice(buffer);
    let written = write_to.len();
    Ok(written)
  }

  fn freelist(&self) -> MutexGuard<Freelist> {
    self
      .freelist
      .get_or_init(|| {
        let meta = self.meta();
        let freelist_pgid = meta.free_list();
        let refpage = self.page(freelist_pgid);
        let freelist_page = MappedFreeListPage::coerce_ref(&refpage).unwrap();
        let freelist = freelist_page.read();
        Mutex::new(freelist)
      })
      .lock()
  }
}

struct FileState {
  file: File,
  /// current on disk file size
  file_size: u64,
}

impl Deref for FileState {
  type Target = File;

  fn deref(&self) -> &Self::Target {
    &self.file
  }
}

impl DerefMut for FileState {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.file
  }
}

pub struct FileBackend {
  path: PathBuf,
  file: Mutex<FileState>,
  page_size: usize,
  mmap: Option<MmapRaw>,
  freelist: OnceLock<Mutex<Freelist>>,
  alloc_size: u64,
  data_size: u64,
  use_mlock: bool,
  grow_async: bool,
  read_only: bool,
}

impl FileBackend {
  fn invalidate(&mut self) {
    self.data_size = 0;
  }

  fn munmap(&mut self) -> crate::Result<()> {
    self.mmap = None;
    self.invalidate();
    Ok(())
  }

  fn has_synced_free_list(&self) -> bool {
    self.meta().free_list() != PGID_NO_FREE_LIST
  }

  fn mmap_unlock(&mut self) -> crate::Result<()> {
    if let Some(mmap) = &mut self.mmap {
      mmap.unlock()?;
    }
    Ok(())
  }

  fn mmap_lock(&mut self) -> crate::Result<()> {
    if let Some(mmap) = &mut self.mmap {
      mmap.lock()?;
    }
    Ok(())
  }

  fn mmap_relock(&mut self) -> crate::Result<()> {
    self.mmap_unlock()?;
    self.mmap_lock()?;
    Ok(())
  }

  fn get_page_size(file: &mut File) -> crate::Result<usize> {
    // Read the first meta page to determine the page size.
    let meta0_can_read = match Self::get_page_size_from_first_meta(file) {
      Ok(page_size) => return Ok(page_size),
      // We cannot read the page size from page 0, but can read page 0.
      Err(Error::InvalidDatabase(meta_can_read)) => meta_can_read,
      Err(e) => return Err(e),
    };

    // Read the second meta page to determine the page size.
    let meta1_can_read = match Self::get_page_size_from_second_meta(file) {
      Ok(page_size) => return Ok(page_size),
      // We cannot read the page size from page 1, but can read page 1.
      Err(Error::InvalidDatabase(meta_can_read)) => meta_can_read,
      Err(e) => return Err(e),
    };

    // If we can't read the page size from both pages, but can read
    // either page, then we assume it's the same as the OS or the one
    // given, since that's how the page size was chosen in the first place.
    //
    // If both pages are invalid, and (this OS uses a different page size
    // from what the database was created with or the given page size is
    // different from what the database was created with), then we are out
    // of luck and cannot access the database.
    if meta0_can_read || meta1_can_read {
      return Ok(DEFAULT_PAGE_SIZE.bytes() as usize);
    }
    Err(Error::InvalidDatabase(false))
  }

  //TODO: These can be done better
  fn get_page_size_from_first_meta(file: &mut File) -> crate::Result<usize> {
    // we need this aligned to Page so we don't hit any runtime issues
    let mut buffer = AlignedBytes::<alignment::Page>::new_zeroed(4096);
    let refpage = RefPage::new(buffer.as_ptr());
    let mut meta_can_read = false;
    let bw = file
      .seek(SeekFrom::Start(0))
      .and_then(|_| file.read(&mut buffer))
      .map_err(|_| Error::InvalidDatabase(meta_can_read))?;
    if bw == buffer.len() {
      meta_can_read = true;
      if let Some(meta_page) = MappedMetaPage::coerce_ref(&refpage) {
        if meta_page.meta.validate().is_ok() {
          let page_size = meta_page.meta.page_size();
          return Ok(page_size as usize);
        }
      }
    }
    Err(Error::InvalidDatabase(meta_can_read))
  }

  fn get_page_size_from_second_meta(file: &mut File) -> crate::Result<usize> {
    let mut meta_can_read = false;
    let metadata = file.metadata()?;
    let file_size = metadata.len();
    // we need this aligned to Page so we don't hit any runtime issues
    let mut buffer = AlignedBytes::<alignment::Page>::new_zeroed(4096);
    for i in 0..15u64 {
      let pos = 1024u64 << i;
      if file_size < 1024 || pos >= file_size - 1024 {
        break;
      }
      let bw = file
        .seek(SeekFrom::Start(pos))
        .and_then(|_| file.read(&mut buffer))
        .map_err(|_| Error::InvalidDatabase(meta_can_read))? as u64;
      if bw == buffer.len() as u64 || bw == file_size - pos {
        meta_can_read = true;
        if let Some(meta_page) = MappedMetaPage::coerce_ref(&RefPage::new(buffer.as_ptr())) {
          if meta_page.meta.validate().is_ok() {
            return Ok(meta_page.meta.page_size() as usize);
          }
        }
      }
      // reset the buffer
      buffer.fill(0);
    }
    Err(Error::InvalidDatabase(meta_can_read))
  }
}

impl DBBackend for FileBackend {
  fn page_size(&self) -> usize {
    self.page_size
  }

  fn data_size(&self) -> u64 {
    self.data_size
  }

  fn meta0(&self) -> MappedMetaPage {
    self
      .mmap
      .as_ref()
      .map(|mmap| unsafe { MappedMetaPage::new(mmap.as_mut_ptr()) })
      .unwrap()
  }

  fn meta1(&self) -> MappedMetaPage {
    self
      .mmap
      .as_ref()
      .map(|mmap| unsafe { MappedMetaPage::new(mmap.as_mut_ptr().add(self.page_size)) })
      .unwrap()
  }

  fn page<'tx>(&self, pg_id: PgId) -> RefPage<'tx> {
    let page_addr = pg_id.0 as usize * self.page_size;
    let page_ptr = unsafe { self.mmap.as_ref().unwrap().as_ptr().add(page_addr) };
    RefPage::new(page_ptr)
  }

  fn grow(&self, mut size: u64) -> crate::Result<()> {
    // Ignore if the new size is less than available file size.
    let file_size = self.file.lock().file_size;
    if size <= file_size {
      return Ok(());
    }
    // If the data is smaller than the alloc size then only allocate what's needed.
    // Once it goes over the allocation size then allocate in chunks.
    if self.data_size <= self.alloc_size {
      size = self.data_size;
    } else {
      size += self.alloc_size;
    }

    // Truncate and fsync to ensure file size metadata is flushed.
    // https://github.com/boltdb/bolt/issues/284
    if self.grow_async && !self.read_only {
      let file_lock = self.file.lock();
      #[cfg(unix)]
      if self.use_mlock {
        self.mmap.as_ref().unwrap().unlock()?;
      }
      if cfg!(not(target_os = "windows")) {
        file_lock.set_len(size)?;
      }
      file_lock.sync_all()?;
      #[cfg(unix)]
      if self.use_mlock {
        self.mmap.as_ref().unwrap().lock()?;
      }
    }

    // TODO: This is overkill. Move file_size behind the file mutex
    self.file.lock().file_size = size;
    Ok(())
  }

  /// mmap opens the underlying memory-mapped file and initializes the meta references.
  /// min_size is the minimum size that the new mmap can be.
  fn mmap(&mut self, min_size: u64, tx: TxRwCell) -> crate::Result<()> {
    let file_lock = self.file.lock();
    let info = file_lock.metadata()?;
    if info.len() < (self.page_size * 2) as u64 {
      return Err(Error::MMapFileSizeTooSmall);
    }
    let file_size = info.len();
    let mut size = file_size.max(min_size);

    size = mmap_size(self.page_size, size)?;
    if let Some(mmap) = self.mmap.take() {
      #[cfg(unix)]
      if self.use_mlock {
        mmap.unlock()?;
      }
      tx.cell.bound().own_in();
    }

    let mmap = MmapOptions::new()
      .len(size as usize)
      .map_raw(&**file_lock)?;
    #[cfg(unix)]
    {
      mmap.advise(Advice::Random)?;
      if self.use_mlock {
        mmap.lock()?;
      }
    }

    self.mmap = Some(mmap);

    let r0 = self.meta0().meta.validate();
    let r1 = self.meta1().meta.validate();

    if r0.is_err() && r1.is_err() {
      return r0;
    }

    self.data_size = size;
    Ok(())
  }

  fn fsync(&self) -> crate::Result<()> {
    self.file.lock().sync_all().map_err(Error::IO)
  }

  // TODO: take all of the pages and handle it here
  fn write_all_at(&self, buffer: &[u8], offset: u64) -> crate::Result<usize> {
    let mut file_lock = self.file.lock();
    file_lock.seek(SeekFrom::Start(offset)).map_err(Error::IO)?;
    file_lock
      .write_all(buffer)
      .map_err(Error::IO)
      .map(|_| buffer.len())
  }

  fn freelist(&self) -> MutexGuard<Freelist> {
    self
      .freelist
      .get_or_init(|| {
        let meta = self.meta();
        let freelist_pgid = meta.free_list();
        let refpage = self.page(freelist_pgid);
        let freelist_page = MappedFreeListPage::coerce_ref(&refpage).unwrap();
        let freelist = freelist_page.read();
        Mutex::new(freelist)
      })
      .lock()
  }
}

impl Drop for FileBackend {
  fn drop(&mut self) {
    if !self.read_only {
      match self.file.lock().unlock() {
        Ok(_) => {}
        // TODO: log error
        Err(_) => {
          todo!("log unlock error")
        }
      }
    }
  }
}

pub(crate) enum AllocateResult<'tx> {
  Page(SelfOwned<AlignedBytes<alignment::Page>, MutPage<'tx>>),
  PageWithNewSize(SelfOwned<AlignedBytes<alignment::Page>, MutPage<'tx>>, u64),
}

pub(crate) trait DbIApi<'tx>: 'tx {
  fn page(&self, pg_id: PgId) -> RefPage<'tx>;

  fn is_page_free(&self, pg_id: PgId) -> bool;

  fn remove_tx(&self, rem_tx: TxId, tx_stats: Arc<TxStats>);
  fn allocate(&self, tx: TxRwCell, page_count: u64) -> AllocateResult<'tx>;

  fn free_page(&self, txid: TxId, p: &PageHeader);
  fn free_pages(&self);

  fn freelist_count(&self) -> u64;

  fn freelist_copyall(&self, all: &mut BVec<PgId>);

  fn commit_freelist(&self, tx: TxRwCell<'tx>) -> crate::Result<AllocateResult<'tx>>;

  fn write_all_at(&self, buf: &[u8], offset: u64) -> crate::Result<usize>;

  fn fsync(&self) -> crate::Result<()>;
  fn repool_allocated(&self, page: AlignedBytes<alignment::Page>);

  fn remove_rw_tx(&self, tx_closing_state: TxClosingState, rem_tx: TxId, tx_stats: Arc<TxStats>);

  fn grow(&self, size: u64) -> crate::Result<()>;
}
pub(crate) trait DbMutIApi<'tx>: DbIApi<'tx> {
  fn mmap_to_new_size(&mut self, min_size: u64, tx: TxRwCell) -> crate::Result<()>;
}

impl<'tx> DbIApi<'tx> for LockGuard<'tx, DbShared> {
  fn page(&self, pg_id: PgId) -> RefPage<'tx> {
    match self {
      LockGuard::R(guard) => guard.page(pg_id),
      LockGuard::U(guard) => guard.borrow().page(pg_id),
    }
  }

  fn is_page_free(&self, pg_id: PgId) -> bool {
    match self {
      LockGuard::R(guard) => guard.is_page_free(pg_id),
      LockGuard::U(guard) => guard.borrow().is_page_free(pg_id),
    }
  }

  fn remove_tx(&self, rem_tx: TxId, tx_stats: Arc<TxStats>) {
    match self {
      LockGuard::R(guard) => guard.remove_tx(rem_tx, tx_stats),
      LockGuard::U(guard) => guard.borrow().remove_tx(rem_tx, tx_stats),
    }
  }

  fn allocate(&self, tx: TxRwCell, page_count: u64) -> AllocateResult<'tx> {
    match self {
      LockGuard::R(guard) => guard.allocate(tx, page_count),
      LockGuard::U(guard) => guard.borrow().allocate(tx, page_count),
    }
  }

  fn free_page(&self, txid: TxId, p: &PageHeader) {
    match self {
      LockGuard::R(guard) => guard.free_page(txid, p),
      LockGuard::U(guard) => guard.borrow().free_page(txid, p),
    }
  }

  fn free_pages(&self) {
    match self {
      LockGuard::R(guard) => guard.free_pages(),
      LockGuard::U(guard) => guard.borrow().free_pages(),
    }
  }

  fn freelist_count(&self) -> u64 {
    match self {
      LockGuard::R(guard) => guard.freelist_count(),
      LockGuard::U(guard) => guard.borrow().freelist_count(),
    }
  }

  fn freelist_copyall(&self, all: &mut BVec<PgId>) {
    match self {
      LockGuard::R(guard) => guard.freelist_copyall(all),
      LockGuard::U(guard) => guard.borrow().freelist_copyall(all),
    }
  }

  fn commit_freelist(&self, tx: TxRwCell<'tx>) -> crate::Result<AllocateResult<'tx>> {
    match self {
      LockGuard::R(guard) => guard.commit_freelist(tx),
      LockGuard::U(guard) => guard.borrow().commit_freelist(tx),
    }
  }

  fn write_all_at(&self, buf: &[u8], offset: u64) -> crate::Result<usize> {
    match self {
      LockGuard::R(guard) => guard.write_all_at(buf, offset),
      LockGuard::U(guard) => guard.borrow().write_all_at(buf, offset),
    }
  }

  fn fsync(&self) -> crate::Result<()> {
    match self {
      LockGuard::R(guard) => guard.fsync(),
      LockGuard::U(guard) => guard.borrow().fsync(),
    }
  }

  fn repool_allocated(&self, page: AlignedBytes<alignment::Page>) {
    match self {
      LockGuard::R(guard) => guard.repool_allocated(page),
      LockGuard::U(guard) => guard.borrow().repool_allocated(page),
    }
  }

  fn remove_rw_tx(&self, tx_closing_state: TxClosingState, rem_tx: TxId, tx_stats: Arc<TxStats>) {
    match self {
      LockGuard::R(guard) => guard.remove_rw_tx(tx_closing_state, rem_tx, tx_stats),
      LockGuard::U(guard) => guard
        .borrow()
        .remove_rw_tx(tx_closing_state, rem_tx, tx_stats),
    }
  }

  fn grow(&self, size: u64) -> crate::Result<()> {
    match self {
      LockGuard::R(guard) => guard.grow(size),
      LockGuard::U(guard) => guard.borrow().grow(size),
    }
  }
}

// In theory things are wired up ok. Here's hoping Miri is happy
pub struct DbShared {
  pub(crate) stats: Arc<DbStats>,
  pub(crate) db_state: Arc<Mutex<DbState>>,
  page_pool: Mutex<Vec<AlignedBytes<alignment::Page>>>,
  pub(crate) backend: Box<dyn DBBackend>,
  pub(crate) options: BoltOptions,
}

// Safe because this is all protected by RwLock
unsafe impl Sync for DbShared {}
unsafe impl Send for DbShared {}

impl<'tx> DbIApi<'tx> for DbShared {
  fn page(&self, pg_id: PgId) -> RefPage<'tx> {
    self.backend.page(pg_id)
  }

  fn is_page_free(&self, pg_id: PgId) -> bool {
    self.backend.freelist().freed(pg_id)
  }

  fn remove_tx(&self, rem_tx: TxId, tx_stats: Arc<TxStats>) {
    let mut records = self.db_state.lock();
    if let Some(pos) = records.txs.iter().position(|tx| *tx == rem_tx) {
      records.txs.swap_remove(pos);
    }

    let n = records.txs.len();
    self.stats.open_tx_n.store(n as i64, Ordering::Release);
    self.stats.tx_stats.add_assign(&tx_stats);
  }

  fn allocate(&self, tx: TxRwCell, page_count: u64) -> AllocateResult<'tx> {
    let tx_id = tx.api_id();
    let high_water = tx.meta().pgid();
    let bytes = if page_count == 1 && !self.page_pool.lock().is_empty() {
      let mut page = self.page_pool.lock().pop().unwrap();
      page.fill(0);
      page
    } else {
      AlignedBytes::new_zeroed(page_count as usize * self.backend.page_size())
    };

    //TODO: This should reside in tx.allocate
    {
      let tx = tx.cell.borrow();
      let stats = tx.r.stats.as_ref().unwrap();
      stats.inc_page_count(page_count as i64);
      stats.inc_page_alloc((page_count * tx.r.meta.page_size() as u64) as i64);
    }

    let mut mut_page = SelfOwned::new_with_map(bytes, |b| MutPage::new(b.as_mut_ptr()));
    mut_page.overflow = (page_count - 1) as u32;

    if let Some(pid) = self.backend.freelist().allocate(tx_id, page_count) {
      mut_page.id = pid;
      return AllocateResult::Page(mut_page);
    }

    // Resize mmap() if we're at the end.
    mut_page.id = high_water;
    let min_size = (high_water.0 + page_count + 1) * self.backend.page_size() as u64;
    tx.split_r_mut().meta.set_pgid(high_water + page_count);
    if min_size > self.backend.data_size() {
      AllocateResult::PageWithNewSize(mut_page, min_size)
    } else {
      AllocateResult::Page(mut_page)
    }
  }

  fn free_page(&self, txid: TxId, p: &PageHeader) {
    self.backend.freelist().free(txid, p)
  }

  fn free_pages(&self) {
    let mut state = self.db_state.lock();
    let mut freelist = self.backend.freelist();
    // Free all pending pages prior to earliest open transaction.

    state.txs.sort();
    let mut min_id = TxId(0xFFFFFFFFFFFFFFFF);
    if !state.txs.is_empty() {
      min_id = *state.txs.first().unwrap();
    }
    if min_id.0 > 0 {
      freelist.release(min_id - 1);
    }

    // Release unused txid extents.
    for t in &state.txs {
      freelist.release_range(min_id, *t - 1);
      min_id = *t + 1;
    }
    freelist.release_range(min_id, TxId(0xFFFFFFFFFFFFFFFF));
    // Any page both allocated and freed in an extent is safe to release.
  }

  fn freelist_count(&self) -> u64 {
    self.backend.freelist().count()
  }

  fn freelist_copyall(&self, all: &mut BVec<PgId>) {
    self.backend.freelist().copy_all(all)
  }

  fn commit_freelist(&self, tx: TxRwCell<'tx>) -> crate::Result<AllocateResult<'tx>> {
    // Allocate new pages for the new free list. This will overestimate
    // the size of the freelist but not underestimate the size (which would be bad).
    let count = {
      let page_size = self.backend.page_size();
      let freelist_size = self.backend.freelist().size();
      (freelist_size / page_size as u64) + 1
    };

    let mut freelist_page = self.allocate(tx, count);
    {
      let page = match &mut freelist_page {
        AllocateResult::Page(page) => page,
        AllocateResult::PageWithNewSize(page, _) => page,
      };
      self
        .backend
        .freelist()
        .write(MappedFreeListPage::mut_into(page))
    }

    Ok(freelist_page)
  }

  fn write_all_at(&self, buf: &[u8], offset: u64) -> crate::Result<usize> {
    self.backend.write_all_at(buf, offset)
  }

  fn fsync(&self) -> crate::Result<()> {
    self.backend.fsync()
  }

  fn repool_allocated(&self, page: AlignedBytes<alignment::Page>) {
    self.page_pool.lock().push(page);
  }

  fn remove_rw_tx(&self, tx_closing_state: TxClosingState, rem_tx: TxId, tx_stats: Arc<TxStats>) {
    let mut state = self.db_state.lock();

    let page_size = self.backend.page_size();
    let mut freelist = self.backend.freelist();
    if tx_closing_state.is_rollback() {
      freelist.rollback(rem_tx);
      if tx_closing_state.is_physical_rollback() {
        let freelist_page_id = self.backend.meta().free_list();
        let freelist_page_ref = self.backend.page(freelist_page_id);
        let freelist_page = MappedFreeListPage::coerce_ref(&freelist_page_ref).unwrap();
        freelist.reload(freelist_page);
      }
    }

    let free_list_free_n = freelist.free_count();
    let free_list_pending_n = freelist.pending_count();
    let free_list_alloc = freelist.size();

    let new_meta = self.backend.meta();
    state.current_meta = new_meta;

    state.rwtx = None;

    self.stats.set_free_page_n(free_list_free_n as i64);
    self.stats.set_pending_page_n(free_list_pending_n as i64);
    self
      .stats
      .set_free_alloc(((free_list_free_n + free_list_pending_n) * page_size as u64) as i64);
    self.stats.set_free_list_in_use(free_list_alloc as i64);
    self.stats.tx_stats.add_assign(&tx_stats);
  }

  fn grow(&self, size: u64) -> crate::Result<()> {
    self.backend.grow(size)
  }
}

impl<'tx> DbMutIApi<'tx> for DbShared {
  fn mmap_to_new_size(&mut self, min_size: u64, tx: TxRwCell) -> crate::Result<()> {
    self.backend.as_mut().mmap(min_size, tx)
  }
}

/// Database options
#[derive(Clone, Default, Debug, PartialEq, Eq, TypedBuilder)]
#[builder(doc)]
pub struct BoltOptions {
  // TODO: How do we handle this?
  #[builder(
    default,
    setter(
      strip_option,
      skip,
      doc = "Timeout is the amount of time to wait to obtain a file lock. \
    When set to zero it will wait indefinitely."
    )
  )]
  timeout: Option<Duration>,
  #[builder(
    default,
    setter(
      skip,
      doc = "Sets the DB.NoGrowSync flag before memory mapping the file."
    )
  )]
  no_grow_sync: bool,
  // TODO: How do we handle this?
  #[builder(
    default,
    setter(
      skip,
      doc = "Do not sync freelist to disk.\
    This improves the database write performance under normal operation,\
    but requires a full database re-sync during recovery."
    )
  )]
  no_freelist_sync: bool,
  #[builder(setter(
    strip_bool,
    doc = "Sets whether to load the free pages when opening the db file.\
    Note when opening db in write mode, bbolt will always load the free pages."
  ))]
  preload_freelist: bool,
  //mmap_flags,
  #[builder(
    default,
    setter(
      strip_option,
      doc = "InitialMmapSize is the initial mmap size of the database in bytes. \
    Read transactions won't block write transaction if the InitialMmapSize is \
    large enough to hold database mmap size."
    )
  )]
  /// initial_mmap_size is the initial mmap size of the database
  /// in bytes. Read transactions won't block write transaction
  /// if the initial_mmap_size is large enough to hold database mmap
  /// size. (See DB.Begin for more information)
  ///
  /// If <=0, the initial map size is 0.
  /// If initial_mmap_size is smaller than the previous database size,
  /// it takes no effect.
  initial_mmap_size: Option<u64>,
  #[builder(default, setter(strip_option))]
  /// PageSize overrides the default OS page size.
  page_size: Option<usize>,
  /// NoSync sets the initial value of DB.NoSync. Normally this can just be
  /// set directly on the DB itself when returned from Open(), but this option
  /// is useful in APIs which expose Options but not the underlying DB.
  #[builder(setter(strip_bool))]
  no_sync: bool,
  /// Mlock locks database file in memory when set to true.
  /// It prevents potential page faults, however
  /// used memory can't be reclaimed. (UNIX only)
  #[builder(setter(strip_bool))]
  mlock: bool,
  #[builder(
    default,
    setter(strip_option, doc = "max_batch_size is the maximum size of a batch.")
  )]
  max_batch_size: Option<u32>,
  #[builder(
    default,
    setter(
      strip_option,
      doc = "max_batch_delay is the maximum delay before a batch starts."
    )
  )]
  max_batch_delay: Option<Duration>,
  #[builder(default = false, setter(skip))]
  /// Open database in read-only mode. Uses flock(..., LOCK_SH |LOCK_NB) to
  /// grab a shared lock (UNIX).
  read_only: bool,
}

impl BoltOptions {
  #[inline]
  pub(crate) fn timeout(&self) -> Option<Duration> {
    if cfg!(use_timeout) {
      self.timeout
    } else {
      None
    }
  }

  #[inline]
  pub(crate) fn no_grow_sync(&self) -> bool {
    self.no_grow_sync
  }

  #[inline]
  pub(crate) fn no_freelist_sync(&self) -> bool {
    self.no_freelist_sync
  }

  #[inline]
  pub(crate) fn preload_freelist(&self) -> bool {
    if self.read_only {
      self.preload_freelist
    } else {
      true
    }
  }

  #[inline]
  pub(crate) fn initial_map_size(&self) -> Option<u64> {
    self.initial_mmap_size
  }

  #[inline]
  pub(crate) fn page_size(&self) -> Option<usize> {
    self.page_size
  }

  #[inline]
  pub(crate) fn no_sync(&self) -> bool {
    self.no_sync
  }

  #[inline]
  pub(crate) fn mlock(&self) -> bool {
    if cfg!(use_mlock) {
      self.mlock
    } else {
      false
    }
  }

  #[inline]
  pub(crate) fn read_only(&self) -> bool {
    self.read_only
  }

  /// Open creates and opens a database at the given path.
  /// If the file does not exist then it will be created automatically.
  pub fn open<T: AsRef<Path>>(self, path: T) -> crate::Result<Bolt> {
    Bolt::open_path(path, self)
  }

  /// Opens a database as read-only at the given path.
  /// If the file does not exist then it will be created automatically.
  pub fn open_ro<T: AsRef<Path>>(mut self, path: T) -> crate::Result<impl DbApi> {
    self.read_only = true;
    Bolt::open_path(path, self)
  }

  /// Opens an in-memory database
  pub fn open_mem(self) -> crate::Result<Bolt> {
    Bolt::new_mem_with_options(self)
  }
}

type BatchFn = dyn FnMut(&mut TxRwRef) -> crate::Result<()> + Send + Sync + 'static;

struct Call {
  f: Box<BatchFn>,
  err: SyncSender<crate::Result<()>>,
}

struct ScheduledBatch {
  timer_guard: Option<Guard>,
  calls: Vec<Call>,
}

impl ScheduledBatch {
  fn cancel_schedule(&mut self) {
    if let Some(guard) = self.timer_guard.take() {
      guard.ignore()
    }
  }

  fn run(&mut self, db: &mut Bolt) {
    'retry: loop {
      if self.calls.is_empty() {
        break;
      }
      let mut fail_idx = None;
      let _ = db.update(|mut tx| {
        for (i, call) in self.calls.iter_mut().enumerate() {
          let result = (call.f)(&mut tx);
          if result.is_err() {
            fail_idx = Some(i);
            return result;
          }
        }
        Ok(())
      });
      if let Some(idx) = fail_idx {
        let call = self.calls.remove(idx);
        call.err.send(Err(Error::TrySolo)).unwrap();
        continue 'retry;
      }
      for call in &self.calls {
        call.err.send(Ok(())).unwrap()
      }
      break;
    }
  }
}

struct InnerBatcher {
  timer: Timer,
  batch_pool: Arc<SyncPool<ScheduledBatch>>,
  scheduled: Mutex<SyncReusable<ScheduledBatch>>,
}

impl InnerBatcher {
  fn new(parent: &Arc<Batcher>) -> InnerBatcher {
    let b = Arc::downgrade(parent);
    let timer = Timer::new();
    let batch_pool = SyncPool::new(
      || ScheduledBatch {
        timer_guard: None,
        calls: Vec::with_capacity(0),
      },
      |batch| {
        batch.timer_guard = None;
        batch.calls.clear();
      },
    );
    let guard = timer.schedule_with_delay(parent.max_batch_delay, move || {
      let batcher = b.upgrade().unwrap();
      let mut db = Bolt {
        inner: batcher.db.upgrade().unwrap(),
      };
      batcher.take_batch().run(&mut db)
    });

    let mut scheduled = batch_pool.pull();
    scheduled.timer_guard = Some(guard);
    InnerBatcher {
      timer,
      batch_pool,
      scheduled: scheduled.into(),
    }
  }
}

struct Batcher {
  inner: OnceLock<InnerBatcher>,
  db: Weak<InnerDB>,
  max_batch_delay: Duration,
  max_batch_size: u32,
}

impl Batcher {
  fn new(db: Weak<InnerDB>, max_batch_delay: Duration, max_batch_size: u32) -> Arc<Batcher> {
    Arc::new(Batcher {
      inner: Default::default(),
      db,
      max_batch_delay,
      max_batch_size,
    })
  }

  fn inner<'a>(self: &'a Arc<Batcher>) -> &'a InnerBatcher {
    self.inner.get_or_init(move || InnerBatcher::new(self))
  }

  fn batch<F>(self: &Arc<Batcher>, mut db: Bolt, mut f: F) -> crate::Result<()>
  where
    F: FnMut(&mut TxRwRef) -> crate::Result<()> + Send + Sync + Clone + 'static,
  {
    if self.max_batch_size == 0 || self.max_batch_delay.is_zero() {
      return Err(Error::BatchDisabled);
    }
    let inner = self.inner();
    let (call_len, rx) = {
      let mut batch = inner.scheduled.lock();

      let (tx, rx): (SyncSender<crate::Result<()>>, Receiver<crate::Result<()>>) =
        mpsc::sync_channel(1);
      batch.calls.push(Call {
        f: Box::new(f.clone()),
        err: tx,
      });
      (batch.calls.len(), rx)
    };
    if call_len > self.max_batch_size as usize {
      let mut immediate = self.take_batch();
      if !immediate.calls.is_empty() {
        let mut i_db = db.clone();
        thread::spawn(move || immediate.run(&mut i_db));
      }
    }

    let result = rx.recv().unwrap();
    if Err(Error::TrySolo) == result {
      db.update(|mut tx| f(&mut tx))?;
    }
    Ok(())
  }

  fn schedule_batch(self: &Arc<Batcher>) -> Guard {
    let inner = self.inner();
    let b = Arc::downgrade(self);
    inner
      .timer
      .schedule_with_delay(self.max_batch_delay, move || {
        let batcher = b.upgrade().unwrap();
        let mut db = Bolt {
          inner: batcher.db.upgrade().unwrap(),
        };
        batcher.take_batch().run(&mut db)
      })
  }

  fn take_batch(self: &Arc<Batcher>) -> SyncReusable<ScheduledBatch> {
    let inner = self.inner();
    let mut swap_batch = inner.batch_pool.pull();
    let mut lock = inner.scheduled.lock();
    mem::swap(&mut swap_batch, &mut *lock);
    swap_batch.cancel_schedule();
    let guard = self.schedule_batch();
    lock.timer_guard = Some(guard);
    swap_batch
  }
}

/// A BBolt Database
pub struct InnerDB {
  // TODO: Save a single Bump for RW transactions with a size hint?
  bump_pool: Arc<SyncPool<Pin<Box<PinBump>>>>,
  db: RwLock<DbShared>,
  stats: Arc<DbStats>,
  db_state: Arc<Mutex<DbState>>,
  batcher: Arc<Batcher>,
}

unsafe impl Send for InnerDB {}
unsafe impl Sync for InnerDB {}

/// The Bolt Database
#[derive(Clone)]
pub struct Bolt {
  inner: Arc<InnerDB>,
}

impl Bolt {
  /// Open creates and opens a database at the given path.
  /// If the file does not exist then it will be created automatically.
  pub fn open<T: AsRef<Path>>(path: T) -> crate::Result<Self> {
    Bolt::open_path(path, BoltOptions::default())
  }

  /// Opens a database as read-only at the given path.
  /// If the file does not exist then it will be created automatically.
  pub fn open_ro<T: AsRef<Path>>(path: T) -> crate::Result<impl DbApi> {
    Bolt::open_path(
      path,
      BoltOptions {
        read_only: true,
        ..Default::default()
      },
    )
  }

  fn open_path<T: AsRef<Path>>(path: T, db_options: BoltOptions) -> crate::Result<Self> {
    let path = path.as_ref();
    let read_only = db_options.read_only();
    let mut file = if db_options.read_only() {
      let file = fs::OpenOptions::new().read(true).open(path)?;
      file.lock_shared()?;
      file
    } else {
      let mut file = fs::OpenOptions::new().write(true).read(true).open(path)?;
      file.lock_exclusive()?;
      if !path.exists() || path.metadata()?.len() == 0 {
        let page_size = db_options
          .page_size()
          .unwrap_or(DEFAULT_PAGE_SIZE.bytes() as usize);
        Bolt::init(path, &mut file, page_size)?;
      }
      file
    };
    let page_size = FileBackend::get_page_size(&mut file)?;
    assert!(page_size > 0, "invalid page size");

    let file_size = file.metadata()?.len();
    let data_size = if let Some(initial_mmap_size) = db_options.initial_map_size() {
      file_size.max(initial_mmap_size)
    } else {
      file_size
    };
    let options = MmapOptions::new()
      .offset(0)
      .len(data_size as usize)
      .to_owned();
    let mmap = if read_only {
      options.map_raw_read_only(&file)?
    } else {
      options.map_raw(&file)?
    };
    #[cfg(unix)]
    {
      mmap.advise(Advice::Random)?;
      if db_options.mlock() {
        mmap.lock()?;
      }
    }
    let backend = FileBackend {
      path: path.into(),
      file: Mutex::new(FileState { file, file_size }),
      page_size,
      mmap: Some(mmap),
      freelist: OnceLock::new(),
      alloc_size: DEFAULT_ALLOC_SIZE.bytes() as u64,
      data_size,
      use_mlock: db_options.mlock(),
      grow_async: !db_options.no_grow_sync(),
      read_only,
    };
    let mut free_count = 0u64;
    if db_options.preload_freelist() {
      free_count = backend.freelist().free_count();
    }
    let meta = backend.meta();
    if meta.free_list() == PGID_NO_FREE_LIST {
      return Err(Error::Other(anyhow!(
        "PGID_NO_FREE_LIST not currently supported"
      )));
    }
    let db_state = Arc::new(Mutex::new(DbState::new(meta)));
    let stats = DbStats {
      free_page_n: (free_count as i64).into(),
      ..Default::default()
    };
    let arc_stats = Arc::new(stats);
    let bump_pool = SyncPool::new(
      || Box::pin(PinBump::default()),
      |bump| Pin::as_mut(bump).reset(),
    );

    let inner = Arc::new_cyclic(|weak| InnerDB {
      bump_pool,
      db: RwLock::new(DbShared {
        stats: arc_stats.clone(),
        db_state: db_state.clone(),
        backend: Box::new(backend),
        page_pool: Mutex::new(vec![]),
        options: db_options.clone(),
      }),
      stats: arc_stats,
      db_state,
      batcher: Arc::new(Batcher {
        inner: Default::default(),
        db: weak.clone(),
        max_batch_delay: db_options
          .max_batch_delay
          .unwrap_or(DEFAULT_MAX_BATCH_DELAY),
        max_batch_size: db_options.max_batch_size.unwrap_or(DEFAULT_MAX_BATCH_SIZE),
      }),
    });
    Ok(Bolt { inner })
  }

  /// Opens an in-memory database
  pub fn open_mem() -> crate::Result<Self> {
    Bolt::new_mem_with_options(BoltOptions::default())
  }

  pub(crate) fn new_mem_with_options(db_options: BoltOptions) -> crate::Result<Self> {
    let page_size = db_options
      .page_size()
      .unwrap_or(DEFAULT_PAGE_SIZE.bytes() as usize);
    let mut mmap = Bolt::init_page(page_size);
    let file_size = mmap.len() as u64;
    let data_size = if let Some(initial_mmap_size) = db_options.initial_map_size() {
      file_size.max(initial_mmap_size)
    } else {
      file_size
    };
    if file_size < data_size {
      let mut new_mmap = AlignedBytes::new_zeroed(data_size as usize);
      new_mmap
        .split_at_mut(file_size as usize)
        .0
        .copy_from_slice(&mmap);
      mmap = new_mmap;
    }
    let backend = MemBackend {
      mmap: Mutex::new(mmap),
      freelist: OnceLock::new(),
      page_size,
      alloc_size: DEFAULT_ALLOC_SIZE.bytes() as u64,
      file_size,
      data_size,
    };
    let free_count = backend.freelist().free_count();
    let meta = backend.meta();
    let db_state = Arc::new(Mutex::new(DbState::new(meta)));
    let stats = DbStats {
      free_page_n: (free_count as i64).into(),
      ..Default::default()
    };
    let arc_stats = Arc::new(stats);
    let bump_pool = SyncPool::new(
      || Box::pin(PinBump::default()),
      |bump| Pin::as_mut(bump).reset(),
    );

    let inner = Arc::new_cyclic(|weak| InnerDB {
      bump_pool,
      db: RwLock::new(DbShared {
        stats: arc_stats.clone(),
        db_state: db_state.clone(),
        backend: Box::new(backend),
        page_pool: Mutex::new(vec![]),
        options: db_options.clone(),
      }),
      stats: arc_stats,
      db_state,
      batcher: Arc::new(Batcher {
        inner: Default::default(),
        db: weak.clone(),
        max_batch_delay: db_options
          .max_batch_delay
          .unwrap_or(DEFAULT_MAX_BATCH_DELAY),
        max_batch_size: db_options.max_batch_size.unwrap_or(DEFAULT_MAX_BATCH_SIZE),
      }),
    });

    Ok(Bolt { inner })
  }

  fn init_page(page_size: usize) -> AlignedBytes<alignment::Page> {
    let mut buffer = AlignedBytes::<alignment::Page>::new_zeroed(page_size * 4);
    for (i, page_bytes) in buffer.chunks_mut(page_size).enumerate() {
      let mut page = MutPage::new(page_bytes.as_mut_ptr());
      if i < 2 {
        let meta_page = MappedMetaPage::mut_into(&mut page);
        meta_page.page.id = PgId(i as u64);
        let meta = &mut meta_page.meta;
        meta.set_magic(MAGIC);
        meta.set_version(VERSION);
        meta.set_page_size(page_size as u32);
        meta.set_free_list(PgId(2));
        meta.set_root(BucketHeader::new(PgId(3), 0));
        meta.set_pgid(PgId(4));
        meta.set_txid(TxId(i as u64));
        meta.set_checksum(meta.sum64());
      } else if i == 2 {
        let free_list = MappedFreeListPage::mut_into(&mut page);
        free_list.id = PgId(2);
        free_list.count = 0;
      } else if i == 3 {
        let leaf_page = MappedLeafPage::mut_into(&mut page);
        leaf_page.id = PgId(3);
        leaf_page.count = 0;
      }
    }
    buffer
  }

  fn init(path: &Path, db: &mut File, page_size: usize) -> io::Result<usize> {
    let buffer = Bolt::init_page(page_size);
    #[cfg(unix)]
    {
      use std::os::unix::fs::PermissionsExt;
      let metadata = db.metadata()?;
      let mut permissions = metadata.permissions();
      permissions.set_mode(0o600);
      fs::set_permissions(path, permissions)?;
    }
    db.write_all(&buffer)?;
    db.flush()?;
    Ok(buffer.len())
  }

  fn require_open(state: &DbState) -> crate::Result<()> {
    if !state.is_open {
      return Err(Error::DatabaseNotOpen);
    }
    Ok(())
  }

  pub(crate) fn begin_tx(&self) -> crate::Result<TxImpl> {
    let mut state = self.inner.db_state.lock();
    Bolt::require_open(&state)?;
    let lock = self.inner.db.read();
    let bump = self.inner.bump_pool.pull();
    let meta = state.current_meta;
    let txid = meta.txid();
    state.txs.push(txid);
    self.inner.stats.inc_tx_n(1);
    self
      .inner
      .stats
      .open_tx_n
      .store(state.txs.len() as i64, Ordering::Release);
    Ok(TxImpl::new(bump, lock, meta))
  }

  #[cfg(feature = "try-begin")]
  pub(crate) fn try_begin_tx<'a, F>(&'a self, f: F) -> crate::Result<Option<TxImpl>>
  where
    F: Fn() -> Option<RwLockReadGuard<'a, DbShared>>,
  {
    let mut state = self.inner.db_state.lock();
    Bolt::require_open(&state)?;
    if let Some(lock) = f() {
      let bump = self.inner.bump_pool.pull();
      let meta = state.current_meta;
      let txid = meta.txid();
      state.txs.push(txid);
      self.inner.stats.inc_tx_n(1);
      self
        .inner
        .stats
        .open_tx_n
        .store(state.txs.len() as i64, Ordering::Release);
      Ok(Some(TxImpl::new(bump, lock, meta)))
    } else {
      Ok(None)
    }
  }

  pub(crate) fn begin_rw_tx(&mut self) -> crate::Result<TxRwImpl> {
    let lock = self.inner.db.upgradable_read();
    lock.free_pages();
    let mut state = self.inner.db_state.lock();
    Bolt::require_open(&state)?;
    let bump = self.inner.bump_pool.pull();
    let mut meta = state.current_meta;
    let txid = meta.txid() + 1;
    meta.set_txid(txid);
    state.rwtx = Some(txid);
    Ok(TxRwImpl::new(bump, lock, meta))
  }

  #[cfg(feature = "try-begin")]
  pub(crate) fn try_begin_rw_tx<'a, F>(&'a self, f: F) -> crate::Result<Option<TxRwImpl>>
  where
    F: Fn() -> Option<RwLockUpgradableReadGuard<'a, DbShared>>,
  {
    if let Some(lock) = f() {
      lock.free_pages();
      let mut state = self.inner.db_state.lock();
      Bolt::require_open(&state)?;
      let bump = self.inner.bump_pool.pull();
      let mut meta = state.current_meta;
      let txid = meta.txid() + 1;
      meta.set_txid(txid);
      state.rwtx = Some(txid);
      Ok(Some(TxRwImpl::new(bump, lock, meta)))
    } else {
      Ok(None)
    }
  }
}

impl DbApi for Bolt {
  fn begin(&self) -> crate::Result<impl TxApi> {
    self.begin_tx()
  }

  #[cfg(feature = "try-begin")]
  fn try_begin(&self) -> crate::Result<Option<impl TxApi>> {
    self.try_begin_tx(|| self.inner.db.try_read())
  }

  #[cfg(feature = "try-begin")]
  fn try_begin_for(&self, duration: Duration) -> crate::Result<Option<impl TxApi>> {
    self.try_begin_tx(|| self.inner.db.try_read_for(duration))
  }

  #[cfg(feature = "try-begin")]
  fn try_begin_until(&self, instant: Instant) -> crate::Result<Option<impl TxApi>> {
    self.try_begin_tx(|| self.inner.db.try_read_until(instant))
  }

  fn view<'tx, F: FnMut(TxRef<'tx>) -> crate::Result<()>>(
    &'tx self, mut f: F,
  ) -> crate::Result<()> {
    let tx = self.begin_tx()?;
    let tx_ref = tx.get_ref();
    let r = f(tx_ref);
    r
  }

  fn stats(&self) -> Arc<DbStats> {
    self.inner.stats.clone()
  }

  fn close(self) {
    let mut lock = self.inner.db.write();
    let mut state = self.inner.db_state.lock();
    if Bolt::require_open(&state).is_ok() {
      state.is_open = false;
      let mut closed_db: Box<dyn DBBackend> = Box::new(ClosedBackend {});
      mem::swap(&mut closed_db, &mut lock.backend);
      lock.page_pool.lock().clear();
      self.inner.bump_pool.clear();
      if let Some(inner_batcher) = self.inner.batcher.inner.get() {
        inner_batcher.batch_pool.clear();
      }
    }
  }
}

impl DbRwAPI for Bolt {
  fn begin_rw(&mut self) -> crate::Result<impl TxRwApi> {
    self.begin_rw_tx()
  }

  #[cfg(feature = "try-begin")]
  fn try_begin_rw(&self) -> crate::Result<Option<impl TxRwApi>> {
    self.try_begin_rw_tx(|| self.inner.db.try_upgradable_read())
  }

  #[cfg(feature = "try-begin")]
  fn try_begin_rw_for(&self, duration: Duration) -> crate::Result<Option<impl TxRwApi>> {
    self.try_begin_rw_tx(|| self.inner.db.try_upgradable_read_for(duration))
  }

  #[cfg(feature = "try-begin")]
  fn try_begin_rw_until(&self, instant: Instant) -> crate::Result<Option<impl TxRwApi>> {
    self.try_begin_rw_tx(|| self.inner.db.try_upgradable_read_until(instant))
  }

  fn update<'tx, F: FnMut(TxRwRef<'tx>) -> crate::Result<()>>(
    &'tx mut self, mut f: F,
  ) -> crate::Result<()> {
    let txrw = self.begin_rw_tx()?;
    let tx_ref = txrw.get_ref();
    match f(tx_ref) {
      Ok(_) => {
        txrw.commit()?;
        Ok(())
      }
      Err(e) => {
        let _ = txrw.rollback();
        Err(e)
      }
    }
  }

  fn batch<F>(&mut self, f: F) -> crate::Result<()>
  where
    F: FnMut(&mut TxRwRef) -> crate::Result<()> + Send + Sync + Clone + 'static,
  {
    self.inner.batcher.batch(self.clone(), f)
  }

  fn sync(&mut self) -> crate::Result<()> {
    self.inner.db.write().backend.fsync()?;
    Ok(())
  }
}

#[cfg(test)]
mod test {
  use std::io::Write;
  use crate::db::DbStats;
  use crate::test_support::{temp_file, TestDb};
  use crate::{Bolt, BucketApi, BucketRwApi, DbApi, DbRwAPI, Error, TxApi, TxRwRefApi};
  use std::sync::mpsc::{channel, sync_channel};
  use std::sync::Arc;
  use std::thread;

  #[test]
  fn test_open() -> crate::Result<()> {
    let db = TestDb::new()?;
    db.clone_db().close();
    Ok(())
  }

  #[test]
  //#[cfg(feature = "long-tests")]
  fn test_open_multiple_threads() -> crate::Result<()> {
    let instances = 30;
    let iterations = 30;
    let mut threads = Vec::new();
    let temp_file = Arc::new(temp_file()?);
    let (tx, rx) = channel();
    for _ in 0..iterations {
      for _ in 0..instances {
        let t_file = temp_file.clone();
        let t_tx = tx.clone();
        let handle = thread::spawn(move || {
          let db = Bolt::open(t_file.path());
          if let Some(error) = db.err() {
            let s = format!("{}", &error);
            t_tx.send(error).unwrap();
          }
        });
        threads.push(handle);
      }
      while let Some(handle) = threads.pop() {
        handle.join().unwrap();
      }
    }
    drop(tx);
    if let Ok(error) = rx.try_recv() {
      panic!("Fatal error: {}", error);
    }
    Ok(())
  }

  #[test]
  fn test_open_err_path_required() -> crate::Result<()> {
    let r = Bolt::open("");
    assert!(r.is_err());
    Ok(())
  }

  #[test]
  fn test_open_err_not_exists() -> crate::Result<()> {
    let file = temp_file()?;
    let path = file.path().join("bad-path");
    let r = Bolt::open(path);
    assert!(r.is_err());
    Ok(())
  }

  #[test]
  fn test_open_err_invalid() -> crate::Result<()> {
    let mut file = temp_file()?;
    file.as_file_mut().write_all(b"this is not a bolt database")?;
    let r = Bolt::open(file.path());
    assert_eq!(Some(Error::InvalidDatabase(false)), r.err());
    Ok(())
  }

  #[test]
  #[ignore]
  fn test_open_err_version_mismatch() {
    todo!()
  }

  #[test]
  #[ignore]
  fn test_open_err_checksum() {
    todo!()
  }

  #[test]
  #[ignore]
  fn test_open_read_page_size_from_meta1_os() {
    todo!()
  }

  #[test]
  #[ignore]
  fn test_open_read_page_size_from_meta1_given() {
    todo!()
  }

  #[test]
  #[ignore]
  fn test_open_size() {
    todo!()
  }

  #[test]
  #[ignore]
  #[cfg(feature = "long-tests")]
  fn test_open_size_large() {
    todo!()
  }

  #[test]
  #[ignore]
  fn test_open_check() {
    todo!()
  }

  #[test]
  #[ignore]
  fn test_open_meta_init_write_error() {
    todo!()
  }

  #[test]
  #[ignore]
  fn test_open_file_too_small() {
    todo!()
  }

  #[test]
  #[ignore]
  fn test_db_open_initial_mmap_size() {
    todo!()
  }

  #[test]
  #[ignore]
  fn test_db_open_read_only() {
    todo!()
  }

  #[test]
  #[ignore]
  fn test_open_big_page() {
    todo!()
  }

  #[test]
  #[ignore]
  fn test_open_recover_free_list() {
    todo!()
  }

  #[test]
  #[ignore]
  fn test_db_begin_err_database_not_open() {
    todo!()
  }

  #[test]
  #[ignore]
  fn test_db_begin_rw() {
    todo!()
  }

  #[test]
  #[ignore]
  fn test_db_concurrent_write_to() {
    todo!()
  }

  #[test]
  #[ignore]
  fn test_db_begin_rw_closed() {
    todo!()
  }

  #[test]
  #[ignore]
  fn test_db_close_pending_tx_rw() {
    todo!()
  }

  #[test]
  #[ignore]
  fn test_db_close_pending_tx_ro() {
    todo!()
  }

  #[test]
  #[ignore]
  fn test_db_update() {
    todo!()
  }

  #[test]
  #[ignore]
  fn test_db_update_closed() {
    todo!()
  }

  #[test]
  #[ignore]
  fn test_db_update_manual_commit() {
    todo!()
  }

  #[test]
  #[ignore]
  fn test_db_update_manual_rollback() {
    todo!()
  }

  #[test]
  #[ignore]
  fn test_db_view_manual_commit() {
    todo!()
  }

  #[test]
  #[ignore]
  fn test_db_update_panic() {
    todo!()
  }

  #[test]
  #[ignore]
  fn test_db_view_error() {
    todo!()
  }

  #[test]
  #[ignore]
  fn test_db_view_panic() {
    todo!()
  }

  #[test]
  fn test_db_stats() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      tx.create_bucket("widgets")?;
      Ok(())
    })?;
    let stats = db.stats();
    assert_eq!(2, stats.tx_stats.page_count());
    assert_eq!(0, stats.free_page_n());
    assert_eq!(2, stats.pending_page_n());
    Ok(())
  }

  #[test]
  #[ignore]
  fn test_db_consistency() {
    todo!()
  }

  #[test]
  fn test_dbstats_sub() {
    let a = DbStats::default();
    let b = DbStats::default();
    a.tx_stats.inc_page_count(3);
    a.set_free_page_n(4);
    b.tx_stats.inc_page_count(10);
    b.set_free_page_n(14);
    let diff = b.sub(&a);
    assert_eq!(7, diff.tx_stats.page_count());
    assert_eq!(14, diff.free_page_n());
  }

  #[test]
  #[ignore]
  fn test_db_batch() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let _ = tx.create_bucket("widgets")?;
      Ok(())
    })?;

    let n = 2;
    let mut threads = Vec::with_capacity(n);
    for i in 0..n {
      let mut t_db = db.clone_db();
      let join = thread::spawn(move || {
        t_db.batch(move |tx| {
          let mut b = tx.bucket_mut("widgets").unwrap();
          b.put(format!("{}", i), "")
        })
      });
      threads.push(join);
    }

    for t in threads {
      t.join().unwrap()?;
    }

    db.view(|tx| {
      let b = tx.bucket("widgets").unwrap();
      for i in 0..n {
        let g = b.get(format!("{}", i));
        assert!(g.is_some(), "key not found {}", i);
      }
      Ok(())
    })?;

    Ok(())
  }

  #[test]
  #[ignore]
  fn test_db_batch_panic() {
    todo!()
  }

  #[test]
  #[ignore]
  fn test_db_batch_full() {
    todo!()
  }

  #[test]
  #[ignore]
  fn test_db_batch_time() {
    todo!()
  }

  #[test]
  #[ignore]
  fn test_dbunmap() {
    todo!()
  }

  #[test]
  #[ignore]
  fn benchmark_dbbatch_automatic() {
    todo!()
  }

  #[test]
  #[ignore]
  fn benchmark_dbbatch_single() {
    todo!()
  }

  #[test]
  #[ignore]
  fn benchmark_dbbatch_manual10x100() {
    todo!()
  }
}
