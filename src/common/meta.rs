use crate::common::bucket::InBucket;
use crate::common::defaults::{MAGIC, PGID_NO_FREE_LIST, VERSION};
use crate::common::page::{CoerciblePage, Page, META_PAGE_FLAG};
use crate::common::{PgId, TxId};
use crate::Error::{ChecksumMismatch, InvalidDatabase, VersionMismatch};
use bytemuck::{Pod, Zeroable};
use fnv_rs::{Fnv64, FnvHasher};
use getset::{CopyGetters, Setters};
use std::hash::Hasher;
use std::marker::PhantomData;
use std::mem;
use std::ops::{Deref, DerefMut};

pub const META_HEADER_SIZE: usize = mem::size_of::<Meta>();

#[repr(C)]
#[derive(Debug, Default, Copy, Clone, CopyGetters, Setters, Pod, Zeroable)]
#[getset(get_copy = "pub", set = "pub")]
pub struct Meta {
  magic: u32,
  version: u32,
  page_size: u32,
  flags: u32,
  root: InBucket,
  free_list: PgId,
  pgid: PgId,
  txid: TxId,
  checksum: u64,
}

impl Meta {
  /// validate checks the marker bytes and version of the meta page to ensure it matches this binary.
  pub fn validate(&self) -> Result<(), crate::Error> {
    if self.magic != MAGIC {
      return Err(InvalidDatabase(true));
    } else if self.version != VERSION {
      return Err(VersionMismatch);
    } else if self.checksum != self.sum64() {
      return Err(ChecksumMismatch);
    }
    Ok(())
  }

  /// write writes the meta onto a page.
  pub fn write(&self, mp: &mut MetaPage) {
    if self.root.root() >= self.pgid {
      panic!(
        "root bucket pgid ({}) above high water mark ({})",
        self.root.root(),
        self.pgid
      );
    } else if self.free_list >= self.pgid && self.free_list != PGID_NO_FREE_LIST {
      panic!(
        "freelist pgid ({}) above high water mark ({})",
        self.free_list, self.pgid
      );
    }
    // Page id is either going to be 0 or 1 which we can determine by the transaction ID.
    mp.page.id = PgId(self.txid.0 % 2);
    mp.page.set_meta();
    mp.meta = *self;
    // Calculate the checksum.
    mp.meta.set_checksum(mp.meta.sum64());
  }

  /// generates the checksum for the meta.
  pub fn sum64(&self) -> u64 {
    let mut h = Fnv64::new();
    let (left, _) =
      bytemuck::bytes_of(self).split_at(mem::size_of::<Meta>() - mem::size_of::<u64>());
    h.update(left);
    h.finish()
  }
}

#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable)]
pub struct MetaPage {
  pub page: Page,
  pub meta: Meta,
}

pub struct MappedMetaPage {
  bytes: *mut u8,
  phantom: PhantomData<[u8]>,
}

impl MappedMetaPage {
  pub unsafe fn new(bytes: *mut u8) -> MappedMetaPage {
    MappedMetaPage {
      bytes,
      phantom: PhantomData,
    }
  }
}

impl CoerciblePage for MappedMetaPage {
  #[inline]
  fn page_flag() -> u16 {
    META_PAGE_FLAG
  }

  fn own(bytes: *mut u8) -> MappedMetaPage {
    let mut page = unsafe { Self::new(bytes) };
    page.page.set_meta();
    page
  }
}

impl Deref for MappedMetaPage {
  type Target = MetaPage;

  fn deref(&self) -> &Self::Target {
    unsafe { &*(self.bytes as *const MetaPage) }
  }
}

impl DerefMut for MappedMetaPage {
  fn deref_mut(&mut self) -> &mut Self::Target {
    unsafe { &mut *(self.bytes as *mut MetaPage) }
  }
}

#[cfg(test)]
mod test {
  use super::*;
  use crate::common::defaults::DEFAULT_PAGE_SIZE;
  use crate::test_support::mapped_page;

  #[test]
  fn test() {
    let mut meta_page = mapped_page::<MappedMetaPage>(4096);
    let mut meta = Meta {
      magic: MAGIC,
      version: VERSION,
      page_size: DEFAULT_PAGE_SIZE.bytes() as u32,
      flags: 0,
      root: Default::default(),
      free_list: 5.into(),
      pgid: 10.into(),
      txid: 2.into(),
      checksum: 0,
    };
    meta.write(meta_page.deref_mut());
    assert!(meta_page.meta.validate().is_ok());
    assert!(meta_page.meta.validate().is_ok());
    assert_eq!(10, meta_page.meta.pgid.0);
  }
}
