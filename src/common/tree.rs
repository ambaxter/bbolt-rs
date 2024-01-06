use crate::common::inode::INode;
use crate::common::memory::{PhantomUnsend, RWSlice};
use crate::common::page::{
  CoerciblePage, Page, BRANCH_PAGE_FLAG, BUCKET_LEAF_FLAG, LEAF_PAGE_FLAG, PAGE_HEADER_SIZE,
};
use crate::common::PgId;
use bytemuck::{Pod, Zeroable};
use getset::{CopyGetters, Setters};
use itertools::izip;
use std::marker::PhantomData;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::slice::{from_raw_parts, from_raw_parts_mut};

pub const BRANCH_PAGE_ELEMENT_SIZE: usize = mem::size_of::<BranchPageElement>();
pub const LEAF_PAGE_ELEMENT_SIZE: usize = mem::size_of::<LeafPageElement>();

pub struct MappedLeafPage {
  bytes: *mut u8,
  phantom: PhantomData<[u8]>,
}

impl MappedLeafPage {
  pub unsafe fn new(bytes: *mut u8) -> MappedLeafPage {
    MappedLeafPage {
      bytes,
      phantom: PhantomData,
    }
  }
}

impl Deref for MappedLeafPage {
  type Target = Page;

  fn deref(&self) -> &Self::Target {
    unsafe { &*(self.bytes as *const Page) }
  }
}

impl DerefMut for MappedLeafPage {
  fn deref_mut(&mut self) -> &mut Self::Target {
    unsafe { &mut *(self.bytes as *mut Page) }
  }
}

impl CoerciblePage for MappedLeafPage {
  #[inline]
  fn page_flag() -> u16 {
    LEAF_PAGE_FLAG
  }

  fn own(bytes: *mut u8) -> MappedLeafPage {
    let mut page = unsafe { Self::new(bytes) };
    page.set_leaf();
    page
  }
}

#[repr(C)]
#[derive(Debug, Copy, Clone, Pod, Zeroable, CopyGetters, Setters)]
#[getset(get_copy = "pub", set = "pub")]
pub struct LeafPageElement {
  flags: u32,
  pos: u32,
  key_size: u32,
  value_size: u32,
  unsend: PhantomUnsend,
}

impl LeafPageElement {
  #[inline]
  pub fn is_bucket_entry(&self) -> bool {
    self.flags & BUCKET_LEAF_FLAG != 0
  }

  pub fn new(flags: u32, pos: u32, key_size: u32, value_size: u32) -> LeafPageElement {
    LeafPageElement {
      flags,
      pos,
      key_size,
      value_size,
      unsend: PhantomData,
    }
  }

  pub(crate) unsafe fn key(&self, page_ptr: *const u8) -> &[u8] {
    let elem_ptr = self as *const LeafPageElement as *const u8;
    let dist = usize::try_from(elem_ptr.offset_from(page_ptr)).unwrap_unchecked();
    let key_ptr = page_ptr.add(dist + self.pos as usize);
    from_raw_parts(key_ptr, self.key_size as usize)
  }
}

#[derive(Debug)]
pub struct LeafElementRef<'tx> {
  elem: &'tx LeafPageElement,
  key_ref: &'tx [u8],
  value_ref: &'tx [u8],
  unsend: PhantomUnsend,
}

impl<'tx> LeafElementRef<'tx> {
  pub fn key(&self) -> &'tx [u8] {
    self.key_ref
  }

  pub fn value(&self) -> &'tx [u8] {
    self.value_ref
  }
}

impl<'tx> Deref for LeafElementRef<'tx> {
  type Target = LeafPageElement;

  fn deref(&self) -> &Self::Target {
    self.elem
  }
}

pub struct MappedBranchPage {
  bytes: *mut u8,
  phantom: PhantomData<[u8]>,
}

impl MappedBranchPage {
  pub unsafe fn new(bytes: *mut u8) -> MappedBranchPage {
    MappedBranchPage {
      bytes,
      phantom: PhantomData,
    }
  }
}

impl Deref for MappedBranchPage {
  type Target = Page;

  fn deref(&self) -> &Self::Target {
    unsafe { &*(self.bytes as *const Page) }
  }
}

impl DerefMut for MappedBranchPage {
  fn deref_mut(&mut self) -> &mut Self::Target {
    unsafe { &mut *(self.bytes as *mut Page) }
  }
}

impl CoerciblePage for MappedBranchPage {
  #[inline]
  fn page_flag() -> u16 {
    BRANCH_PAGE_FLAG
  }

  fn own(bytes: *mut u8) -> MappedBranchPage {
    let mut page = unsafe { Self::new(bytes) };
    page.set_branch();
    page
  }
}

#[repr(C)]
#[derive(Debug, Copy, Clone, Pod, Zeroable, CopyGetters, Setters)]
#[getset(get_copy = "pub", set = "pub")]
pub struct BranchPageElement {
  pgid: PgId,
  pos: u32,
  key_size: u32,
  unsend: PhantomUnsend,
}

impl BranchPageElement {
  pub(crate) unsafe fn key(&self, page_ptr: *const u8) -> &[u8] {
    let elem_ptr = self as *const BranchPageElement as *const u8;
    let dist = usize::try_from(elem_ptr.offset_from(page_ptr)).unwrap_unchecked();
    let key_ptr = page_ptr.add(dist + self.pos as usize);
    from_raw_parts(key_ptr, self.key_size as usize)
  }
}

#[derive(Debug)]
pub struct BranchElementRef<'tx> {
  elem: &'tx BranchPageElement,
  key_ref: &'tx [u8],
  unsend: PhantomUnsend,
}

impl<'tx> BranchElementRef<'tx> {
  pub fn key(&self) -> &'tx [u8] {
    self.key_ref
  }
}

impl<'tx> Deref for BranchElementRef<'tx> {
  type Target = BranchPageElement;

  fn deref(&self) -> &Self::Target {
    self.elem
  }
}

pub trait TreePage<'tx>: Deref<Target = Page> + DerefMut {
  type Elem: 'tx;
  type ElemRef: 'tx;
  unsafe fn page_ptr(&self) -> *mut u8;
  fn page_element_size(&self) -> usize;

  fn iter<'a>(&'a self) -> TreeIterator<'a, 'tx, Self>
  where
    Self: Sized,
  {
    TreeIterator::new(&self)
  }

  fn get_elem(&self, i: u16) -> Option<Self::ElemRef>;
  fn elements(&self) -> &[Self::Elem] {
    unsafe {
      let page_ptr = self.page_ptr();
      let elem_ptr = page_ptr.add(PAGE_HEADER_SIZE).cast::<Self::Elem>();
      from_raw_parts(elem_ptr, self.count as usize)
    }
  }

  fn elements_mut(&mut self) -> &mut [Self::Elem] {
    unsafe {
      let page_ptr = self.page_ptr();
      let elem_ptr = page_ptr.add(PAGE_HEADER_SIZE).cast::<Self::Elem>();
      from_raw_parts_mut(elem_ptr, self.count as usize)
    }
  }

  fn write_element(element: &mut Self::Elem, pos: u32, node: &INode);
  fn write_elements(&mut self, inodes: &[INode]) -> u32 {
    //debug_assert_ne!(0, inodes.len());
    self.count = inodes.len() as u16;
    if self.count == 0 {
      return 0;
    }
    let mut off = PAGE_HEADER_SIZE + self.page_element_size() * inodes.len();
    let page_ptr = unsafe { self.page_ptr() };
    izip!(self.elements_mut(), inodes).for_each(|(elem, inode)| {
      let inode_key = inode.key();
      let inode_value = inode.value();
      assert!(!inode_key.is_empty(), "write: zero-length inode key");
      let size = inode_key.len() + inode_value.len();
      let mut b = RWSlice::new_with_offset(page_ptr, off, size as u32);
      off += size;
      let pos = b.distance_from(elem);
      Self::write_element(elem, pos, inode);
      let (key, value) = b.split_at_mut(inode_key.len());
      key.copy_from_slice(inode_key);
      value.copy_from_slice(inode_value);
    });
    off as u32
  }
}

pub struct TreeIterator<'a, 'tx, T: TreePage<'tx>> {
  page: &'a T,
  i: u16,
  p: PhantomData<&'tx [u8]>,
  unsend: PhantomUnsend,
}

impl<'a, 'tx, T: TreePage<'tx>> TreeIterator<'a, 'tx, T> {
  pub fn new(t: &'a T) -> Self {
    TreeIterator {
      page: t,
      i: 0,
      p: PhantomData,
      unsend: PhantomData,
    }
  }
}

impl<'a, 'tx, T: TreePage<'tx>> Iterator for TreeIterator<'a, 'tx, T> {
  type Item = T::ElemRef;

  fn next(&mut self) -> Option<Self::Item> {
    let item = self.page.get_elem(self.i);
    self.i += 1;
    item
  }
}

impl<'tx> TreePage<'tx> for MappedLeafPage {
  type Elem = LeafPageElement;
  type ElemRef = LeafElementRef<'tx>;

  #[inline]
  unsafe fn page_ptr(&self) -> *mut u8 {
    self.bytes
  }

  #[inline]
  fn page_element_size(&self) -> usize {
    LEAF_PAGE_ELEMENT_SIZE
  }

  fn get_elem(&self, i: u16) -> Option<Self::ElemRef> {
    if i >= self.count {
      None
    } else {
      unsafe {
        let elem_ptr = self
          .bytes
          .add(PAGE_HEADER_SIZE)
          .add(self.page_element_size() * i as usize);
        let elem = &*(elem_ptr as *const LeafPageElement);
        let key_ptr = elem_ptr.add(elem.pos as usize);
        let key_ref = from_raw_parts(key_ptr, elem.key_size as usize);
        let value_ref = from_raw_parts(
          key_ptr.add(elem.key_size as usize),
          elem.value_size as usize,
        );
        Some(LeafElementRef {
          elem,
          key_ref,
          value_ref,
          unsend: Default::default(),
        })
      }
    }
  }

  fn write_element(element: &mut Self::Elem, pos: u32, node: &INode) {
    element.set_pos(pos);
    element.set_flags(node.flags());
    element.set_key_size(node.key().len() as u32);
    element.set_value_size(node.value().len() as u32);
  }
}

impl<'tx> TreePage<'tx> for MappedBranchPage {
  type Elem = BranchPageElement;
  type ElemRef = BranchElementRef<'tx>;

  #[inline]
  unsafe fn page_ptr(&self) -> *mut u8 {
    self.bytes
  }

  #[inline]
  fn page_element_size(&self) -> usize {
    BRANCH_PAGE_ELEMENT_SIZE
  }

  fn get_elem(&self, i: u16) -> Option<Self::ElemRef> {
    if i >= self.count {
      None
    } else {
      unsafe {
        let elem_ptr = self
          .bytes
          .add(PAGE_HEADER_SIZE)
          .add(self.page_element_size() * i as usize);
        let elem = &*(elem_ptr as *const BranchPageElement);
        let key_ptr = elem_ptr.add(elem.pos as usize);
        let key_ref = from_raw_parts(key_ptr, elem.key_size as usize);
        Some(BranchElementRef {
          elem,
          key_ref,
          unsend: Default::default(),
        })
      }
    }
  }

  fn write_element(element: &mut Self::Elem, pos: u32, node: &INode) {
    element.set_pos(pos);
    element.set_pgid(node.pgid());
    element.set_key_size(node.key().len() as u32);
  }
}
