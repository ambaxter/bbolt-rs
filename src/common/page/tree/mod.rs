use crate::common::inode::INode;
use crate::common::memory::PhantomUnsend;
use crate::common::page::{PageHeader, PAGE_HEADER_SIZE};
use itertools::izip;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::ptr;
use std::slice::{from_raw_parts, from_raw_parts_mut};

pub mod branch;
pub mod leaf;

pub trait TreePage<'tx>: Deref<Target = PageHeader> + DerefMut {
  type Elem: 'tx;
  type ElemRef: 'tx;
  unsafe fn page_ptr(&self) -> *mut u8;
  fn page_element_size(&self) -> usize;

  fn iter<'a>(&'a self) -> TreeIterator<'tx, 'a, Self>
  where
    Self: Sized,
  {
    TreeIterator::new(self)
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
      let (pos, key_slice, value_slice) = unsafe {
        let key_ptr = page_ptr.byte_add(off);
        let value_ptr = key_ptr.byte_add(inode_key.len());
        let pos = key_ptr.offset_from(ptr::from_mut(elem).cast_const().cast::<u8>());
        (
          pos as u32,
          from_raw_parts_mut(key_ptr, inode_key.len()),
          from_raw_parts_mut(value_ptr, inode_value.len()),
        )
      };
      off += inode_key.len() + inode_value.len();
      Self::write_element(elem, pos, inode);
      key_slice.copy_from_slice(inode_key);
      value_slice.copy_from_slice(inode_value);
    });
    off as u32
  }
}

pub struct TreeIterator<'tx, 'a, T: TreePage<'tx>> {
  page: &'a T,
  i: u16,
  p: PhantomData<&'tx [u8]>,
  unsend: PhantomUnsend,
}

impl<'tx, 'a, T: TreePage<'tx>> TreeIterator<'tx, 'a, T> {
  pub fn new(t: &'a T) -> Self {
    TreeIterator {
      page: t,
      i: 0,
      p: PhantomData,
      unsend: PhantomData,
    }
  }
}

impl<'tx, 'a, T: TreePage<'tx>> Iterator for TreeIterator<'tx, 'a, T> {
  type Item = T::ElemRef;

  fn next(&mut self) -> Option<Self::Item> {
    let item = self.page.get_elem(self.i);
    self.i += 1;
    item
  }
}

pub const MIN_KEYS_PER_PAGE: usize = 2;
