use crate::common::inode::INode;
use crate::common::memory::PhantomUnsend;
use crate::common::page::tree::TreePage;
use crate::common::page::{CoerciblePage, PageHeader, PAGE_HEADER_SIZE};
use bytemuck::{Pod, Zeroable};
use getset::{CopyGetters, Setters};
use std::marker::PhantomData;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::slice::from_raw_parts;

pub const LEAF_PAGE_ELEMENT_SIZE: usize = mem::size_of::<LeafPageElement>();

pub const BUCKET_LEAF_FLAG: u32 = 0x01;
pub const LEAF_PAGE_FLAG: u16 = 0x02;

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
  type Target = PageHeader;

  fn deref(&self) -> &Self::Target {
    unsafe { &*(self.bytes as *const PageHeader) }
  }
}

impl DerefMut for MappedLeafPage {
  fn deref_mut(&mut self) -> &mut Self::Target {
    unsafe { &mut *(self.bytes as *mut PageHeader) }
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

/// `LeafPageElement` represents the on-file layout of a leaf page's element
///
/// `leafPageElement` in Go BBolt
#[repr(C)]
#[derive(Debug, Copy, Clone, Pod, Zeroable, CopyGetters, Setters)]
#[getset(get_copy = "pub", set = "pub")]
pub struct LeafPageElement {
  /// Additional flag for each element. If leaf is a Bucket then 0x01 set
  flags: u32,
  /// The distance from this element's pointer to its key/value location
  pos: u32,
  /// Key length
  key_size: u32,
  /// Value length
  value_size: u32,
  /// To implement !Send
  unsend: PhantomUnsend,
}

impl LeafPageElement {
  pub(crate) fn new(flags: u32, pos: u32, key_size: u32, value_size: u32) -> LeafPageElement {
    LeafPageElement {
      flags,
      pos,
      key_size,
      value_size,
      unsend: Default::default(),
    }
  }

  #[inline]
  pub fn is_bucket_entry(&self) -> bool {
    self.flags & BUCKET_LEAF_FLAG != 0
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
