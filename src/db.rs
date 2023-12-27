use crate::arch::size::MAX_MAP_SIZE;
use crate::bucket::BucketRwIAPI;
use crate::common::bucket::InBucket;
use crate::common::defaults::{
  DEFAULT_ALLOC_SIZE, DEFAULT_PAGE_SIZE, MAGIC, MAX_MMAP_STEP, PGID_NO_FREE_LIST, VERSION,
};
use crate::common::memory::LCell;
use crate::common::meta::{MappedMetaPage, Meta};
use crate::common::page::{CoerciblePage, MutPage, Page, RefPage, FREE_LIST_PAGE_FLAG};
use crate::common::self_owned::SelfOwned;
use crate::common::tree::MappedLeafPage;
use crate::common::{PgId, SplitRef, TxId};
use crate::freelist::{Freelist, MappedFreeListPage};
use crate::tx::{TxCell, TxIAPI, TxImpl, TxRef, TxRwCell, TxRwImpl, TxRwRef, TxStats};
use crate::{Error, TxApi, TxRwApi};
use aligners::{alignment, Aligned, AlignedBytes};
use bumpalo::Bump;
use fs4::FileExt;
use itertools::{min, Itertools};
use lockfree_object_pool::LinearObjectPool;
use memmap2::{Advice, MmapOptions, MmapRaw};
use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use size::consts::MEGABYTE;
use std::cell::{RefCell, RefMut};
use std::fs::File;
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::ops::{Deref, DerefMut, Sub};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fs, io, mem};

pub trait DbApi: Clone + Send + Sync
where
  Self: Sized,
{
  type TxType<'tx>: TxApi<'tx>
  where
    Self: 'tx;

  /// Close releases all database resources.
  /// It will block waiting for any open transactions to finish
  /// before closing the database and returning.
  fn close(self);

  /// Begin starts a new transaction.
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
  /// needed, you might want to set DB.InitialMmapSize to a large enough value
  /// to avoid potential blocking of write transaction.
  ///
  /// IMPORTANT: You must close read-only transactions after you are finished or
  /// else the database will not reclaim old pages.
  fn begin<'tx>(&'tx self) -> Self::TxType<'tx>;

  /// View executes a function within the context of a managed read-only transaction.
  /// Any error that is returned from the function is returned from the View() method.
  ///
  /// Attempting to manually rollback within the function will cause a panic.
  fn view<'tx, F: FnMut(TxRef<'tx>) -> crate::Result<()>>(&'tx self, f: F) -> crate::Result<()>;

  /// Stats retrieves ongoing performance stats for the database.
  /// This is only updated when a transaction closes.
  fn stats(&self) -> Arc<RwLock<DbStats>>;
}

pub trait DbRwAPI: DbApi {
  type TxRwType<'tx>: TxRwApi<'tx>
  where
    Self: 'tx;

  /// Begin starts a new transaction.
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
  /// needed, you might want to set DB.InitialMmapSize to a large enough value
  /// to avoid potential blocking of write transaction.
  ///
  /// IMPORTANT: You must close read-only transactions after you are finished or
  /// else the database will not reclaim old pages.
  fn begin_mut<'tx>(&'tx mut self) -> Self::TxRwType<'tx>;

  /// Update executes a function within the context of a read-write managed transaction.
  /// If no error is returned from the function then the transaction is committed.
  /// If an error is returned then the entire transaction is rolled back.
  /// Any error that is returned from the function or returned from the commit is
  /// returned from the Update() method.
  ///
  /// Attempting to manually commit or rollback within the function will cause a panic.
  fn update<'tx, F: FnMut(TxRwRef<'tx>) -> crate::Result<()>>(
    &'tx mut self, f: F,
  ) -> crate::Result<()>;

  /// Sync executes fdatasync() against the database file handle.
  ///
  /// This is not necessary under normal operation, however, if you use NoSync
  /// then it allows you to force the database file to sync against the disk.
  fn sync(&mut self) -> crate::Result<()>;
}

#[derive(Copy, Clone, Default)]
/// Stats represents statistics about the database.
pub struct DbStats {
  /// global, ongoing stats.
  tx_stats: TxStats,

  // Freelist stats
  /// total number of free pages on the freelist
  free_page_n: i64,
  /// total number of pending pages on the freelist
  pending_page_n: i64,
  /// total bytes allocated in free pages
  free_alloc: i64,
  /// total bytes used by the freelist
  free_list_in_use: i64,

  // transaction stats
  /// total number of started read transactions
  tx_n: i64,
  /// number of currently open read transactions
  open_tx_n: i64,
}

impl Sub<&DbStats> for &DbStats {
  type Output = DbStats;

  fn sub(self, rhs: &DbStats) -> Self::Output {
    DbStats {
      tx_stats: self.tx_stats - rhs.tx_stats,
      free_page_n: self.free_page_n - rhs.free_page_n,
      pending_page_n: self.pending_page_n - rhs.pending_page_n,
      free_alloc: self.free_alloc - rhs.free_alloc,
      free_list_in_use: self.free_list_in_use - rhs.free_list_in_use,
      tx_n: self.tx_n - rhs.tx_n,
      open_tx_n: self.open_tx_n - rhs.open_tx_n,
    }
  }
}

pub struct DbInfo {}

pub struct TxRecords {
  txs: Vec<TxId>,
  rwtx: Option<TxId>,
  freelist: Freelist,
}

impl TxRecords {
  fn new(freelist: Freelist) -> TxRecords {
    TxRecords {
      txs: vec![],
      rwtx: None,
      freelist,
    }
  }

  /// freePages releases any pages associated with closed read-only transactions.
  fn free_pages(&mut self) {
    self.txs.sort();
    let mut minid = TxId(0xFFFFFFFFFFFFFFFF);
    if self.txs.len() > 0 {
      minid = *self.txs.first().unwrap();
    }
    if minid.0 > 0 {
      self.freelist.release(minid - 1);
    }

    for txid in &self.txs {
      self.freelist.release_range(minid, *txid - 1);
      minid = *txid + 1;
    }
    self.freelist.release_range(minid, TxId(0xFFFFFFFFFFFFFFFF));
  }
}

pub struct DBBackend {
  path: PathBuf,
  file: File,
  page_size: usize,
  mmap: Option<MmapRaw>,
  meta0: MappedMetaPage,
  meta1: MappedMetaPage,
  page_pool: Vec<AlignedBytes<alignment::Page>>,
  alloc_size: u64,
  /// current on disk file size
  file_size: u64,
  data_size: u64,
  use_mlock: bool,
  grow_async: bool,
  read_only: bool,
}

impl DBBackend {
  fn invalidate(&mut self) {
    self.data_size = 0;
  }

  fn munmap(&mut self) -> crate::Result<()> {
    self.mmap = None;
    self.invalidate();
    Ok(())
  }

  pub(crate) fn meta(&self) -> Meta {
    let (meta_a, meta_b) = {
      if self.meta1.meta.txid() > self.meta0.meta.txid() {
        (self.meta1.meta, self.meta0.meta)
      } else {
        (self.meta0.meta, self.meta1.meta)
      }
    };
    if meta_a.validate().is_ok() {
      return meta_a;
    } else if meta_b.validate().is_ok() {
      return meta_b;
    }
    panic!("bolt.db.meta: invalid meta page")
  }

  fn has_synced_free_list(&self) -> bool {
    self.meta().free_list() != PGID_NO_FREE_LIST
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
    file
      .seek(SeekFrom::Start(0))
      .and_then(|_| file.read(&mut buffer))
      .map_err(|_| Error::InvalidDatabase(meta_can_read))?;
    meta_can_read = true;
    if let Some(meta_page) = MappedMetaPage::coerce_ref(&refpage) {
      if meta_page.meta.validate().is_ok() {
        let page_size = meta_page.meta.page_size();
        return Ok(page_size as usize);
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
      if pos >= file_size - 1024 {
        break;
      }
      let bw = file
        .seek(SeekFrom::Start(0))
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

  /// mmap opens the underlying memory-mapped file and initializes the meta references.
  /// min_size is the minimum size that the new mmap can be.
  fn mmap(&mut self, min_size: u64, tx: TxRwCell) -> crate::Result<()> {
    let info = self.file.metadata()?;
    if info.len() < (self.page_size * 2) as u64 {
      return Err(Error::MMapFileSizeTooSmall);
    }
    let file_size = info.len();
    let mut size = file_size.max(min_size);

    size = DBBackend::mmap_size(self.page_size, size)?;
    if let Some(mmap) = self.mmap.take() {
      mmap.unlock()?;
      tx.cell.1.own_in();
    }

    let mmap = MmapOptions::new().len(size as usize).map_raw(&self.file)?;
    mmap.advise(Advice::Random)?;
    if self.use_mlock {
      mmap.lock()?;
    }

    self.meta0 = unsafe { MappedMetaPage::new(mmap.as_mut_ptr()) };
    self.meta1 = unsafe { MappedMetaPage::new(mmap.as_mut_ptr().add(self.page_size)) };

    let r0 = self.meta0.meta.validate();
    let r1 = self.meta1.meta.validate();

    if r0.is_err() && r1.is_err() {
      return r0;
    }

    self.mmap = Some(mmap);
    self.data_size = size;
    Ok(())
  }

  pub(crate) fn page<'tx>(&self, pg_id: PgId) -> RefPage<'tx> {
    let page_addr = pg_id.0 as usize * self.page_size;
    let page_ptr = unsafe { self.mmap.as_ref().unwrap().as_ptr().add(page_addr) };
    RefPage::new(page_ptr)
  }

  pub(crate) fn grow(&self, size: u64) -> crate::Result<()> {
    todo!()
  }
}

impl Drop for DBBackend {
  fn drop(&mut self) {
    if !self.read_only {
      match self.file.unlock() {
        Ok(_) => {}
        // TODO: log error
        Err(e) => {
          todo!("log unlock error")
        }
      }
    }
  }
}

pub(crate) trait DbIAPI<'tx>: 'tx {
  fn page(&self, pg_id: PgId) -> RefPage<'tx>;

  fn is_page_free(&self, pg_id: PgId) -> bool;

  fn remove_tx(&self, rem_tx: TxId, tx_stats: TxStats);
}

pub(crate) trait DbRwIAPI<'tx>: DbIAPI<'tx> {
  fn free_page(&self, txid: TxId, p: &Page);

  fn free_pages(&mut self);

  fn allocate(
    &mut self, tx: TxRwCell<'tx>, count: u64,
  ) -> crate::Result<SelfOwned<AlignedBytes<alignment::Page>, MutPage<'tx>>>;

  fn grow(&mut self, size: u64) -> crate::Result<()>;

  fn commit_freelist(
    &mut self, tx: TxRwCell<'tx>,
  ) -> crate::Result<SelfOwned<AlignedBytes<alignment::Page>, MutPage<'tx>>>;

  fn remove_rw_tx(&mut self, rem_tx: TxId, tx_stats: TxStats);

  fn repool_allocated(&mut self, page: AlignedBytes<alignment::Page>);
  fn fsync(&mut self) -> crate::Result<()>;
  fn write_at(&mut self, buf: &[u8], offset: u64) -> crate::Result<usize>;
}

pub(crate) enum DbGuard<'tx> {
  Read(RwLockReadGuard<'tx, DBShared>),
  Write(RefCell<RwLockWriteGuard<'tx, DBShared>>),
}

impl<'tx> DbGuard<'tx> {
  pub(crate) fn get_rw(&self) -> Option<RefMut<DBShared>> {
    match self {
      DbGuard::Read(_) => None,
      DbGuard::Write(guard) => Some(RefMut::map(guard.borrow_mut(), |g| &mut **g)),
    }
  }
}

impl<'tx> DbIAPI<'tx> for DbGuard<'tx> {
  fn page(&self, pg_id: PgId) -> RefPage<'tx> {
    match self {
      DbGuard::Read(guard) => guard.page(pg_id),
      DbGuard::Write(guard) => guard.borrow().page(pg_id),
    }
  }

  fn is_page_free(&self, pg_id: PgId) -> bool {
    match self {
      DbGuard::Read(guard) => guard.is_page_free(pg_id),
      DbGuard::Write(guard) => guard.borrow().is_page_free(pg_id),
    }
  }

  fn remove_tx(&self, rem_tx: TxId, tx_stats: TxStats) {
    match self {
      DbGuard::Read(guard) => guard.remove_tx(rem_tx, tx_stats),
      DbGuard::Write(guard) => guard.borrow().remove_tx(rem_tx, tx_stats),
    }
  }
}

// In theory things are wired up ok. Here's hoping Miri is happy
pub struct DBShared {
  pub(crate) stats: Arc<RwLock<DbStats>>,
  pub(crate) records: Mutex<TxRecords>,
  pub(crate) backend: DBBackend,
}

impl<'tx> DbIAPI<'tx> for DBShared {
  fn page(&self, pg_id: PgId) -> RefPage<'tx> {
    self.backend.page(pg_id)
  }

  fn is_page_free(&self, pg_id: PgId) -> bool {
    self.records.lock().freelist.freed(pg_id)
  }

  fn remove_tx(&self, rem_tx: TxId, tx_stats: TxStats) {
    let mut records = self.records.lock();
    let mut stats = self.stats.write();
    if let Some(pos) = records.txs.iter().position(|tx| *tx == rem_tx) {
      records.txs.swap_remove(pos);
    }

    let n = records.txs.len();
    stats.open_tx_n = n as i64;
    stats.tx_stats += tx_stats;
  }
}

impl<'tx> DbRwIAPI<'tx> for DBShared {
  fn free_pages(&mut self) {
    todo!()
  }

  fn free_page(&self, txid: TxId, p: &Page) {
    self.records.lock().freelist.free(txid, p)
  }

  fn allocate(
    &mut self, tx: TxRwCell, count: u64,
  ) -> crate::Result<SelfOwned<AlignedBytes<alignment::Page>, MutPage<'tx>>> {
    let tx_id = tx.api_id();
    let high_water = tx.meta().pgid();
    let mut r = self.records.lock();
    let bytes = if count == 1 && !self.backend.page_pool.is_empty() {
      self.backend.page_pool.pop().unwrap()
    } else {
      AlignedBytes::new_zeroed(count as usize * self.backend.page_size)
    };

    let mut mut_page = SelfOwned::new_with_map(bytes, |b| MutPage::new(b.as_mut_ptr()));
    mut_page.overflow = (count - 1) as u32;

    if let Some(pid) = r.freelist.allocate(tx_id, count) {
      mut_page.id = pid;
      return Ok(mut_page);
    }

    // TODO: How do we signal back that the file is at a new high water mark?
    // TODO: When remapping the file how do we signal that we should own/dereference before we go further?
    // I think we can just pass in the tx when we grow the db size

    // Resize mmap() if we're at the end.
    mut_page.id = high_water;
    let min_size = (high_water.0 + count + 1) * self.backend.page_size as u64;
    if min_size > self.backend.data_size {
      self.backend.mmap(min_size, tx)?;
    }

    tx.split_r_mut().meta.set_pgid(high_water + count);
    Ok(mut_page)
  }

  /// grow grows the size of the database to the given `size`.
  fn grow(&mut self, mut size: u64) -> crate::Result<()> {
    // Ignore if the new size is less than available file size.
    if size <= self.backend.file_size {
      return Ok(());
    }
    // If the data is smaller than the alloc size then only allocate what's needed.
    // Once it goes over the allocation size then allocate in chunks.
    if self.backend.data_size <= self.backend.alloc_size {
      size = self.backend.data_size;
    } else {
      size += self.backend.alloc_size;
    }

    // Truncate and fsync to ensure file size metadata is flushed.
    // https://github.com/boltdb/bolt/issues/284
    if let Some(mmap) = self.backend.mmap.take() {
      if self.backend.use_mlock {
        mmap.unlock()?;
      }
    }
    self.backend.file.set_len(size)?;
    self.backend.file.sync_all()?;
    //By the time we hit this the dereference has already occurred during tx.spill
    let mmap = MmapRaw::map_raw(&self.backend.file)?;
    mmap.advise(Advice::Random)?;
    if self.backend.use_mlock {
      mmap.lock()?;
    }
    self.backend.file_size = size;
    self.backend.mmap = Some(mmap);
    Ok(())
  }

  fn commit_freelist(
    &mut self, tx: TxRwCell<'tx>,
  ) -> crate::Result<SelfOwned<AlignedBytes<alignment::Page>, MutPage<'tx>>> {
    // Allocate new pages for the new free list. This will overestimate
    // the size of the freelist but not underestimate the size (which would be bad).
    let count = {
      let records = self.records.lock();
      let page_size = self.backend.page_size;
      let freelist_size = records.freelist.size();
      (freelist_size / page_size as u64) + 1
    };

    let mut freelist_page = self.allocate(tx, count)?;

    let records = self.records.lock();
    records
      .freelist
      .write(MappedFreeListPage::mut_into(&mut freelist_page));

    Ok(freelist_page)
  }

  fn remove_rw_tx(&mut self, rem_tx: TxId, tx_stats: TxStats) {
    let mut records = self.records.lock();
    let mut stats = self.stats.write();
    let page_size = self.backend.page_size;

    let free_list_free_n = records.freelist.free_count();
    let free_list_pending_n = records.freelist.pending_count();
    let free_list_alloc = records.freelist.size();

    records.rwtx = None;

    stats.free_page_n = free_list_free_n as i64;
    stats.pending_page_n = free_list_pending_n as i64;
    stats.free_alloc = ((free_list_free_n + free_list_pending_n) * page_size as u64) as i64;
    stats.free_list_in_use = free_list_alloc as i64;
    stats.tx_stats += tx_stats;
  }

  fn write_at(&mut self, buf: &[u8], offset: u64) -> crate::Result<usize> {
    self
      .backend
      .file
      .seek(SeekFrom::Start(offset))
      .map_err(|e| Error::IO(e))?;
    self.backend.file.write(buf).map_err(|e| Error::IO(e))
  }

  fn fsync(&mut self) -> crate::Result<()> {
    self.backend.file.sync_all().map_err(|e| Error::IO(e))
  }

  fn repool_allocated(&mut self, mut page: AlignedBytes<alignment::Page>) {
    page.fill(0);
    self.backend.page_pool.push(page);
  }
}

#[derive(Clone)]
pub struct DB {
  // TODO: Save a single Bump for RW transactions with a size hint
  bump_pool: Arc<LinearObjectPool<Bump>>,
  db: Arc<RwLock<DBShared>>,
  stats: Arc<RwLock<DbStats>>,
}
unsafe impl Send for DB {}
unsafe impl Sync for DB {}

impl DB {
  /// Open creates and opens a database at the given path.
  /// If the file does not exist then it will be created automatically.
  /// Passing in nil options will cause Bolt to open the database with the default options.
  pub fn new<T: Into<PathBuf>>(path: T) -> crate::Result<Self> {
    let path = path.into();

    if !path.exists() || path.metadata()?.len() == 0 {
      let page_size = DEFAULT_PAGE_SIZE.bytes() as usize;
      DB::init(&path, page_size)?;
    }
    let mut file = fs::OpenOptions::new()
      .write(true)
      .create(true)
      .read(true)
      .open(&path)?;
    file.lock_exclusive()?;

    let page_size = DBBackend::get_page_size(&mut file)?;
    assert!(page_size > 0, "invalid page size");

    let file_size = file.metadata()?.len();
    let options = MmapOptions::new();
    let open_options = options.clone();
    let mut mmap = open_options.map_raw(&file)?;
    mmap.advise(Advice::Random)?;
    let (meta0, meta1) = unsafe {
      (
        MappedMetaPage::new(mmap.as_mut_ptr()),
        MappedMetaPage::new(mmap.as_mut_ptr().add(page_size)),
      )
    };
    let backend = DBBackend {
      path,
      file,
      page_size,
      mmap: Some(mmap),
      meta0,
      meta1,
      page_pool: vec![],
      alloc_size: DEFAULT_ALLOC_SIZE.bytes() as u64,
      file_size,
      data_size: 0,
      use_mlock: false,
      grow_async: false,
      read_only: false,
    };
    let freelist_pgid = backend.meta().free_list();
    let refpage = backend.page(freelist_pgid);
    let freelist_page = MappedFreeListPage::coerce_ref(&refpage).unwrap();
    let freelist = freelist_page.read();
    let free_count = freelist.free_count();
    let mut records = TxRecords::new(freelist);
    let mut stats = DbStats::default();
    stats.free_page_n = free_count as i64;
    let arc_stats = Arc::new(RwLock::new(stats));
    Ok(DB {
      bump_pool: Arc::new(LinearObjectPool::new(|| Bump::new(), |b| b.reset())),
      db: Arc::new(RwLock::new(DBShared {
        stats: arc_stats.clone(),
        records: Mutex::new(records),
        backend,
      })),
      stats: arc_stats.clone(),
    })
  }

  fn init(path: &Path, page_size: usize) -> io::Result<usize> {
    let mut buffer = AlignedBytes::<alignment::Page>::new_zeroed(page_size * 4);
    for (i, page_bytes) in buffer.chunks_mut(page_size).enumerate() {
      let mut page = MutPage::new(page_bytes.as_mut_ptr());
      if i < 2 {
        let mut meta_page = MappedMetaPage::mut_into(&mut page);
        meta_page.page.id = PgId(i as u64);
        let meta = &mut meta_page.meta;
        meta.set_magic(MAGIC);
        meta.set_version(VERSION);
        meta.set_page_size(page_size as u32);
        meta.set_free_list(PgId(2));
        meta.set_root(InBucket::new(PgId(3), 0));
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
    let mut db = fs::OpenOptions::new()
      .write(true)
      .create(true)
      .read(true)
      .open(&path)?;
    db.write_all(&buffer)?;
    db.flush()?;
    Ok(buffer.len())
  }
}

impl DbApi for DB {
  type TxType<'tx> =  TxImpl<'tx> where Self: 'tx ;

  fn close(self) {
    todo!()
  }

  fn begin<'tx>(&'tx self) -> Self::TxType<'tx> {
    let bump = self.bump_pool.pull_owned();
    TxImpl::new(bump, self.db.read())
  }

  fn view<'tx, F: FnMut(TxRef<'tx>) -> crate::Result<()>>(
    &'tx self, mut f: F,
  ) -> crate::Result<()> {
    let bump = self.bump_pool.pull_owned();
    let tx = TxImpl::new(bump, self.db.read());
    let tx_ref = tx.get_ref();
    let r = f(tx_ref);
    let _ = tx.rollback();
    r
  }

  fn stats(&self) -> Arc<RwLock<DbStats>> {
    self.stats.clone()
  }
}

impl DbRwAPI for DB {
  type TxRwType<'tx> = TxRwImpl<'tx> where Self: 'tx;

  fn begin_mut<'tx>(&'tx mut self) -> Self::TxRwType<'tx> {
    let bump = self.bump_pool.pull_owned();
    TxRwImpl::new(bump, self.db.write())
  }

  fn update<'tx, F: FnMut(TxRwRef<'tx>) -> crate::Result<()>>(
    &'tx mut self, mut f: F,
  ) -> crate::Result<()> {
    let bump = self.bump_pool.pull_owned();
    let txrw = TxRwImpl::new(bump, self.db.write());
    let tx_ref = txrw.get_ref();
    match f(tx_ref) {
      Ok(_) => txrw.commit(),
      Err(e) => {
        let _ = txrw.rollback();
        Err(e)
      }
    }
  }

  fn sync(&mut self) -> crate::Result<()> {
    self.db.write().backend.file.sync_all()?;
    Ok(())
  }
}

#[cfg(test)]
mod test {

  #[test]
  fn test_open() {
    todo!()
  }

  #[test]
  fn test_open_multiple_goroutines() {
    todo!()
  }

  #[test]
  fn test_open_err_path_required() {
    todo!()
  }

  #[test]
  fn test_open_err_not_exists() {
    todo!()
  }

  #[test]
  fn test_open_err_invalid() {
    todo!()
  }

  #[test]
  fn test_open_err_version_mismatch() {
    todo!()
  }

  #[test]
  fn test_open_err_checksum() {
    todo!()
  }

  #[test]
  fn test_open_read_page_size_from_meta1_os() {
    todo!()
  }

  #[test]
  fn test_open_read_page_size_from_meta1_given() {
    todo!()
  }

  #[test]
  fn test_open_size() {
    todo!()
  }

  #[test]
  fn test_open_size_large() {
    todo!()
  }

  #[test]
  fn test_open_check() {
    todo!()
  }

  #[test]
  fn test_open_meta_init_write_error() {
    todo!()
  }

  #[test]
  fn test_open_file_too_small() {
    todo!()
  }

  #[test]
  fn test_db_open_initial_mmap_size() {
    todo!()
  }

  #[test]
  fn test_db_open_read_only() {
    todo!()
  }

  #[test]
  fn test_open_big_page() {
    todo!()
  }

  #[test]
  fn test_open_recover_free_list() {
    todo!()
  }

  #[test]
  fn test_db_begin_err_database_not_open() {
    todo!()
  }

  #[test]
  fn test_db_begin_rw() {
    todo!()
  }

  #[test]
  fn test_db_concurrent_write_to() {
    todo!()
  }

  #[test]
  fn test_db_begin_rw_closed() {
    todo!()
  }

  #[test]
  fn test_db_close_pending_tx_rw() {
    todo!()
  }

  #[test]
  fn test_db_close_pending_tx_ro() {
    todo!()
  }

  #[test]
  fn test_db_update() {
    todo!()
  }

  #[test]
  fn test_db_update_closed() {
    todo!()
  }

  #[test]
  fn test_db_update_manual_commit() {
    todo!()
  }

  #[test]
  fn test_db_update_manual_rollback() {
    todo!()
  }

  #[test]
  fn test_db_view_manual_commit() {
    todo!()
  }

  #[test]
  fn test_db_update_panic() {
    todo!()
  }

  #[test]
  fn test_db_view_error() {
    todo!()
  }

  #[test]
  fn test_db_view_panic() {
    todo!()
  }

  #[test]
  fn test_db_stats() {
    todo!()
  }

  #[test]
  fn test_db_consistency() {
    todo!()
  }

  #[test]
  fn test_dbstats_sub() {
    todo!()
  }

  #[test]
  fn test_db_batch() {
    todo!()
  }

  #[test]
  fn test_db_batch_panic() {
    todo!()
  }

  #[test]
  fn test_db_batch_full() {
    todo!()
  }

  #[test]
  fn test_db_batch_time() {
    todo!()
  }

  #[test]
  fn test_dbunmap() {
    todo!()
  }

  #[test]
  fn example_db_update() {
    todo!()
  }

  #[test]
  fn example_db_view() {
    todo!()
  }

  #[test]
  fn example_db_begin() {
    todo!()
  }

  #[test]
  fn benchmark_dbbatch_automatic() {
    todo!()
  }

  #[test]
  fn benchmark_dbbatch_single() {
    todo!()
  }

  #[test]
  fn benchmark_dbbatch_manual10x100() {
    todo!()
  }
}
