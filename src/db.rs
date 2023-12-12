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
use crate::tx::{Tx, TxMut};
use crate::{Error, TxAPI, TxMutAPI};
use aligners::{alignment, AlignedBytes};
use bumpalo::Bump;
use fs4::FileExt;
use itertools::min;
use memmap2::{Advice, MmapOptions, MmapRaw};
use parking_lot::{Mutex, RwLock, RwLockReadGuard};
use std::cell::{RefCell, RefMut};
use std::fs::File;
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fs, io, mem};

pub trait DbAPI: Clone
where
  Self: Sized,
{
  fn open() -> Self;
  fn close(self);
}

pub trait DbMutAPI: DbAPI {}

pub struct DBRecords {
  txs: Vec<TxId>,
  rwtx: Option<TxId>,
  freelist: Freelist,
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

  pub fn close(self) {}

  pub fn begin(&self) -> impl TxAPI<Tx> {
    todo!()
  }

  pub fn begin_mut(&mut self) -> impl TxMutAPI {
    todo!()
  }

  pub fn update<'tx, F: FnMut(TxMut<'tx>)>(&'tx mut self, f: F) {}

  pub fn view<'tx, F: FnMut(Tx<'tx>)>(&'tx self, f: F) {}

  pub fn batch<'tx, F: FnMut(TxMut<'tx>)>(&mut self, f: F) {}

  pub fn sync(&mut self) {}
}
