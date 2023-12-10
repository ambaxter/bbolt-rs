use crate::arch::size::MAX_MAP_SIZE;
use crate::common::bucket::InBucket;
use crate::common::defaults::{DEFAULT_PAGE_SIZE, MAGIC, MAX_MMAP_STEP};
use crate::common::meta::{MappedMetaPage, Meta};
use crate::common::page::{CoerciblePage, MutPage, RefPage};
use crate::common::tree::MappedLeafPage;
use crate::common::{PgId, TxId};
use crate::freelist::MappedFreeListPage;
use crate::Error;
use aligners::{alignment, AlignedBytes};
use fs4::FileExt;
use memmap2::{Advice, MmapOptions, MmapRaw};
use std::cell::RefMut;
use std::fs::File;
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::{fs, io};

pub struct DB {
  path: PathBuf,
  page_size: usize,
  file: File,
  mmap: Option<MmapRaw>,
  alloc_size: u64,
  /// current on disk file size
  file_size: u64,
  data_size: u64,
  use_mlock: bool,
  grow_async: bool,
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

  fn invalidate(&mut self) {
    self.data_size = 0;
  }

  fn munmap(&mut self) -> crate::Result<()> {
    self.mmap = None;
    self.invalidate();
    Ok(())
  }

  fn get_page_size_from_first_meta(file: &mut File) -> crate::Result<usize> {
    let mut buf = [0u8; 4096];
    let mut meta_can_read = false;
    //TODO: Definitely make this look prettier
    if let Ok(Some(meta_page)) = file
      .seek(SeekFrom::Start(0))
      .and_then(|_| file.read(&mut buf))
      .map(|_| {
        meta_can_read = true;
        MappedMetaPage::coerce_ref(&RefPage::new(buf.as_ptr()))
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
    todo!()
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
}

impl Drop for DB {
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
