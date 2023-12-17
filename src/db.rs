use crate::arch::size::MAX_MAP_SIZE;
use crate::common::bucket::InBucket;
use crate::common::defaults::{DEFAULT_PAGE_SIZE, MAGIC, MAX_MMAP_STEP, PGID_NO_FREE_LIST};
use crate::common::memory::SCell;
use crate::common::meta::{MappedMetaPage, Meta};
use crate::common::page::{CoerciblePage, MutPage, RefPage};
use crate::common::selfowned::SelfOwned;
use crate::common::tree::MappedLeafPage;
use crate::common::{PgId, TxId};
use crate::freelist::{Freelist, MappedFreeListPage};
use crate::tx::{TxCell, TxRwCell, TxStats};
use crate::{Error, TxApi, TxRwApi};
use aligners::{alignment, AlignedBytes, Aligned};
use bumpalo::Bump;
use fs4::FileExt;
use itertools::{Itertools, min};
use memmap2::{Advice, MmapOptions, MmapRaw};
use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::cell::{RefCell, RefMut};
use std::fs::File;
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::ops::{Deref, Sub};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fs, io, mem};

pub trait DbApi: Clone
where
  Self: Sized,
{
  type TxType<'tx>: TxApi<'tx>
  where
    Self: 'tx;

  /// Open creates and opens a database at the given path.
  /// If the file does not exist then it will be created automatically.
  /// Passing in nil options will cause Bolt to open the database with the default options.
  fn open<T: Into<PathBuf>>(path: T) -> crate::Result<Self>;

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
  fn view<'tx, F: FnMut(TxCell<'tx>) -> crate::Result<()>>(&mut self, f: F) -> crate::Result<()>;

  /// Stats retrieves ongoing performance stats for the database.
  /// This is only updated when a transaction closes.
  fn stats(&self) -> DbStats;
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
  fn update<'tx, F: FnMut(TxRwCell<'tx>) -> crate::Result<()>>(
    &mut self, f: F,
  ) -> crate::Result<()>;

  /// Sync executes fdatasync() against the database file handle.
  ///
  /// This is not necessary under normal operation, however, if you use NoSync
  /// then it allows you to force the database file to sync against the disk.
  fn sync(&mut self) -> crate::Result<()>;
}

#[derive(Copy, Clone)]
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

pub struct DBRecords {
  bump_pool: Vec<Bump>,
  txs: Vec<TxId>,
  rwtx: Option<TxId>,
  freelist: Freelist,
  stats: DbStats,
  page_pool: Vec<AlignedBytes::<alignment::Page>>
}

impl DBRecords {
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

  pub(crate) fn remove_rw_tx<'tx>(&mut self, page_size: usize, rem_tx: TxId, tx_stats: TxStats, mut bump: Bump, db_lock: RwLockWriteGuard<'tx, DBShared>) {

    let free_list_free_n = self.freelist.free_count();
    let free_list_pending_n = self.freelist.pending_count();
    let free_list_alloc = self.freelist.size();

    bump.reset();
    self.bump_pool.push(bump);

    self.rwtx = None;
    drop(db_lock);



    self.stats.free_page_n = free_list_free_n as i64;
    self.stats.pending_page_n = free_list_pending_n as i64;
    self.stats.free_alloc = ((free_list_free_n + free_list_pending_n) * page_size as u64) as i64;
    self.stats.free_list_in_use = free_list_alloc as i64;
    self.stats.tx_stats += tx_stats;
  }

  pub(crate) fn remove_tx<'tx>(&mut self, rem_tx: TxId, tx_stats: TxStats, mut bump: Bump, db_lock: RwLockReadGuard<'tx, DBShared>) {
    if let Some(pos) = self.txs.iter().position(|tx| *tx == rem_tx) {
      self.txs.swap_remove(pos);
    }

    bump.reset();
    self.bump_pool.push(bump);
    drop(db_lock);

    let n = self.txs.len();
    self.stats.open_tx_n = n as i64;
    self.stats.tx_stats += tx_stats;
  }
}

pub struct DBBackend {
  path: PathBuf,
  file: File,
  page_size: usize,
  mmap: Option<MmapRaw>,
  meta0: MappedMetaPage,
  meta1: MappedMetaPage,
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

  fn mmap_size(page_size: usize, size: u64) -> crate::Result<usize> {
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

    Ok(sz as usize)
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
    let mut meta_can_read = false;
    if let Ok(Some(meta_page)) = file
      .seek(SeekFrom::Start(0))
      .and_then(|_| file.read(&mut buffer))
      .map(|_| {
        meta_can_read = true;
        MappedMetaPage::coerce_ref(&RefPage::new(buffer.as_ptr()))
      })
    {
      if meta_page.meta.validate().is_ok() {
        return Ok(meta_page.meta.page_size() as usize);
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
      file.seek(SeekFrom::Start(pos))?;
      let bw = file.read(&mut buffer)? as u64;
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

// In theory things are wired up ok. Here's hoping Miri is happy
pub struct DBShared {
  pub(crate) records: Mutex<DBRecords>,
  pub(crate) backend: DBBackend,
}

impl DBShared {

  // TODO: We are probably violating something here in &self
  fn page(&self, pgid: PgId) -> RefPage {
    let r = self.records.lock();
    // Disallow returning free pages
    if r.freelist.freed(pgid) {
      panic!("db.page: Page {} in the free list", pgid);
    }
    let addr =  pgid.0 as usize * self.backend.page_size;
    // Disallow returning anything beyond the existing file boundaries
    if (addr + self.backend.page_size) as u64 > self.backend.data_size {
      panic!("db.page: Page {} beyond data_size", pgid);
    }
    let mmap = self.backend.mmap.as_ref().unwrap();
    let page_ptr = unsafe {mmap.as_ptr().add(pgid.0 as usize * self.backend.page_size) };
    RefPage::new(page_ptr)
  }

  fn allocate<'tx>(&mut self, tx_id: TxId, count: u64, tx: TxRwCell<'tx>) -> crate::Result<SelfOwned<AlignedBytes<alignment::Page>, MutPage<'tx>>> {
    let r = self.records.lock();
    let bytes = if count == 1 && !r.page_pool.is_empty() {
      r.page_pool.pop().unwrap()
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
    Ok(mut_page)
  }

}

pub(crate) trait DbIAPI<'tx>: 'tx {}

struct Lest<'tx, T>
where
  T: Deref<Target = DBShared>,
{
  s: SCell<'tx, T>,
}

impl<'tx, T> Lest<'tx, T>
where
  T: Deref<Target = DBShared>,
{
  fn ll(&self) {
    self.s.borrow().backend.meta0.meta.page_size() > 3;
  }
}

struct TxOwned<T: Deref<Target = DBShared>> {
  b: Bump,
  l: RefCell<T>,
}

#[derive(Clone, Copy)]
struct DBRef<'tx, T: Deref<Target = DBShared>> {
  b: &'tx Bump,
  l: &'tx RefCell<T>,
}

struct RORsrc<'tx, T: Deref<Target = DBShared> + Unpin> {
  l: SelfOwned<TxOwned<T>, DBRef<'tx, T>>,
}

impl<'tx, T: Deref<Target = DBShared> + Unpin> RORsrc<'tx, T> {
  fn new(lock: T) -> Self {
    let shared = TxOwned {
      b: Default::default(),
      l: RefCell::new(lock),
    };
    let so = SelfOwned::new_with_map(shared, |o| DBRef { b: &o.b, l: &o.l });
    RORsrc { l: so }
  }

  fn test(lock: RwLockReadGuard<'tx, DBShared>) -> RORsrc<'tx, RwLockReadGuard<'tx, DBShared>> {
    RORsrc::new(lock)
  }

  fn release(self) {
    let TxOwned { mut b, l } = self.l.into_owner();
    b.reset();
    let mut lock = l.into_inner();
  }
}

pub(crate) trait DbMutIAPI<'tx>: DbIAPI<'tx> {}

#[derive(Clone)]
pub struct DB {
  db: Arc<RwLock<DBShared>>,
  read_only: bool,
}

impl DB {
  pub fn new<T: Into<PathBuf>>(path: T) -> io::Result<Self> {
    let path = path.into();
    let page_size = DEFAULT_PAGE_SIZE.bytes() as usize;
    if !path.exists() || path.metadata()?.len() == 0 {
      DB::init(&path, page_size)?;
    }
    let db = fs::OpenOptions::new()
      .write(true)
      .create(true)
      .read(true)
      .open(&path)?;
    db.lock_exclusive()?;
    let mut mmap = MmapOptions::new().map_raw(&db)?;
    mmap.advise(Advice::Random)?;
    todo!()
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
        meta.set_version(1);
        meta.set_page_size(page_size as u32);
        meta.set_free_list(PgId(2));
        meta.set_root(InBucket::new(PgId(3), 0));
        meta.set_pgid(PgId(4));
        meta.set_txid(TxId(i as u64));
        meta.set_checksum(meta.checksum());
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

  fn load_free_list(&mut self) {}
}
