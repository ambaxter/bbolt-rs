use crate::common::PgId;
use bytemuck::{Pod, Zeroable};
use std::borrow::Cow;
use std::cmp::Ordering;
use std::marker::PhantomData;
use std::mem;
use std::ops::{Deref, DerefMut};

pub const MIN_KEYS_PER_PAGE: usize = 2;

pub const PAGE_HEADER_SIZE: usize = mem::size_of::<Page>();

pub const BRANCH_PAGE_FLAG: u16 = 0x01;
pub const LEAF_PAGE_FLAG: u16 = 0x02;
pub const META_PAGE_FLAG: u16 = 0x04;
pub const FREE_LIST_PAGE_FLAG: u16 = 0x10;

pub const BUCKET_LEAF_FLAG: u32 = 0x01;

//TODO: This needs to be cleaned up.
pub trait CoerciblePage {
  fn page_flag() -> u16;

  #[inline]
  fn set_flag(page: &mut Page) {
    page.flags = Self::page_flag();
  }

  fn own(bytes: *mut u8) -> Self;

  #[inline]
  unsafe fn unchecked_ref<'tx>(mapped_page: &RefPage<'tx>) -> &'tx Self
  where
    Self: Sized,
  {
    &*(mapped_page as *const RefPage as *const Self)
  }

  #[inline]
  unsafe fn unchecked_mut<'tx>(mapped_page: &mut MutPage<'tx>) -> &'tx mut Self
  where
    Self: Sized,
  {
    &mut *(mapped_page as *mut MutPage<'tx> as *mut Self)
  }

  #[inline]
  fn mut_into<'tx>(mapped_page: &mut MutPage<'tx>) -> &'tx mut Self
  where
    Self: Sized,
  {
    Self::set_flag(mapped_page);
    unsafe { Self::unchecked_mut(mapped_page) }
  }

  #[inline]
  fn coerce_ref<'tx>(mapped_page: &RefPage<'tx>) -> Option<&'tx Self>
  where
    Self: Sized,
  {
    if mapped_page.flags == Self::page_flag() {
      Some(unsafe { Self::unchecked_ref(mapped_page) })
    } else {
      None
    }
  }

  #[inline]
  fn coerce_mut<'a>(mapped_page: &mut MutPage<'a>) -> Option<&'a mut Self>
  where
    Self: Sized,
  {
    if mapped_page.flags == Self::page_flag() {
      Some(unsafe { Self::unchecked_mut(mapped_page) })
    } else {
      None
    }
  }
}

/// A read-only view of page aligned, multiple of page-sized section of memory.
/// Always begins with a 'Page' header
#[derive(Copy, Clone)]
pub struct RefPage<'tx> {
  bytes: *const u8,
  phantom: PhantomData<&'tx [u8]>,
}

impl<'tx> RefPage<'tx> {
  pub fn new(bytes: *const u8) -> RefPage<'tx> {
    RefPage {
      bytes,
      phantom: PhantomData,
    }
  }
}

impl<'tx> Deref for RefPage<'tx> {
  type Target = Page;

  fn deref(&self) -> &Self::Target {
    unsafe { &*(self.bytes as *const Page) }
  }
}

/// A mutable view of page aligned, multiple of page-sized section of memory.
/// Always begins with a 'Page' header
pub struct MutPage<'tx> {
  bytes: *mut u8,
  phantom: PhantomData<&'tx mut [u8]>,
}

impl<'tx> MutPage<'tx> {
  pub fn new(bytes: *mut u8) -> MutPage<'tx> {
    MutPage {
      bytes,
      phantom: PhantomData,
    }
  }
}

impl<'tx> AsRef<RefPage<'tx>> for MutPage<'tx> {
  fn as_ref(&self) -> &RefPage<'tx> {
    unsafe { &*(self as *const MutPage<'tx> as *const RefPage<'tx>) }
  }
}

impl<'tx> Deref for MutPage<'tx> {
  type Target = Page;

  fn deref(&self) -> &Self::Target {
    unsafe { &*(self.bytes as *const Page) }
  }
}

impl<'tx> DerefMut for MutPage<'tx> {
  fn deref_mut(&mut self) -> &mut Self::Target {
    unsafe { &mut *(self.bytes as *mut Page) }
  }
}

#[repr(C)]
#[derive(Debug, Copy, Clone, Default, Pod, Zeroable)]
pub struct Page {
  pub id: PgId,
  pub flags: u16,
  pub count: u16,
  //TODO: make setting this unsafe
  pub overflow: u32,
}

impl PartialOrd for Page {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for Page {
  fn cmp(&self, other: &Self) -> Ordering {
    self.id.cmp(&other.id)
  }
}

impl PartialEq for Page {
  fn eq(&self, other: &Self) -> bool {
    self.id == other.id
  }
}

impl Eq for Page {}

impl Page {
  #[inline]
  pub fn set_branch(&mut self) {
    self.flags = BRANCH_PAGE_FLAG;
  }

  #[inline]
  pub fn set_leaf(&mut self) {
    self.flags = LEAF_PAGE_FLAG;
  }

  #[inline]
  pub fn set_meta(&mut self) {
    self.flags = META_PAGE_FLAG;
  }

  #[inline]
  pub fn set_free_list(&mut self) {
    self.flags = FREE_LIST_PAGE_FLAG;
  }

  #[inline]
  pub fn set_undefined(&mut self) {
    self.flags = 0;
  }

  pub fn fast_check(&self, id: PgId) {
    assert_eq!(
      self.id, id,
      "Page expected to be {}, but self identifies as {}",
      id, self.id
    );
    assert!(
      self.flags == BRANCH_PAGE_FLAG
        || self.flags == LEAF_PAGE_FLAG
        || self.flags == META_PAGE_FLAG
        || self.flags == FREE_LIST_PAGE_FLAG,
      "page {}: has unexpected type/flags {}",
      self.id,
      self.flags
    );
  }

  #[inline]
  pub fn is_branch(&self) -> bool {
    self.flags & BRANCH_PAGE_FLAG != 0
  }

  #[inline]
  pub fn is_leaf(&self) -> bool {
    self.flags & LEAF_PAGE_FLAG != 0
  }

  #[inline]
  pub fn is_meta(&self) -> bool {
    self.flags & META_PAGE_FLAG != 0
  }

  #[inline]
  pub fn is_free_list(&self) -> bool {
    self.flags & FREE_LIST_PAGE_FLAG != 0
  }

  #[inline]
  pub fn is_undefined(&self) -> bool {
    self.flags == 0
  }

  pub fn page_type(&self) -> Cow<'static, str> {
    if self.is_branch() {
      Cow::Borrowed("branch")
    } else if self.is_leaf() {
      Cow::Borrowed("leaf")
    } else if self.is_meta() {
      Cow::Borrowed("meta")
    } else if self.is_free_list() {
      Cow::Borrowed("freelist")
    } else {
      Cow::Owned(format!("unknown<{:#02x}>", self.flags))
    }
  }
}

pub struct PageInfo {
  id: u64,
  t: Cow<'static, str>,
  count: u64,
  overflow_count: u64,
}
