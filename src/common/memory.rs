use bumpalo::Bump;
use bytemuck::Pod;
use std::borrow::Borrow;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::fmt;
use std::fmt::Formatter;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::slice::{from_raw_parts, from_raw_parts_mut};

pub struct SCell<'a, T: ?Sized>(&'a RefCell<T>);

impl<'a, T> Clone for SCell<'a, T> {
  fn clone(&self) -> Self {
    *self
  }
}

impl<'a, T> Copy for SCell<'a, T> {}

impl<'a, T> SCell<'a, T> {
  pub fn new_in(x: T, a: &'a Bump) -> SCell<'a, T> {
    SCell(a.alloc(RefCell::new(x)))
  }
}

impl<'a, T> Deref for SCell<'a, T> {
  type Target = RefCell<T>;

  fn deref(&self) -> &Self::Target {
    self.0
  }
}

/// Copy on Demand handling the case where we need to either point to a memory mapped slice or an Bump owned slice.
#[derive(Copy, Clone)]
pub enum CodSlice<'tx, T: Pod> {
  Owned(&'tx [T]),
  Mapped(&'tx [T]),
}

impl<'tx, T: Pod> CodSlice<'tx, T>
where
  T: Clone,
{
  pub fn default_in(_bump: &'tx Bump) -> CodSlice<'tx, T> {
    CodSlice::Owned(&[])
  }

  #[inline]
  pub fn is_mapped(&self) -> bool {
    match self {
      CodSlice::Owned(_) => false,
      CodSlice::Mapped(_) => true,
    }
  }

  #[inline]
  pub fn is_owned(&self) -> bool {
    !self.is_mapped()
  }

  pub fn own_in(&mut self, bump: &'tx Bump) {
    if self.is_mapped() {
      let o = bump.alloc_slice_copy(Deref::deref(self));
      *self = CodSlice::Owned(o);
    }
  }
}

impl<'tx, T: Pod> Deref for CodSlice<'tx, T> {
  type Target = [T];

  #[inline]
  fn deref(&self) -> &'tx Self::Target {
    match self {
      CodSlice::Owned(s) => *s,
      CodSlice::Mapped(m) => *m,
    }
  }
}

impl<'tx, T: Pod> Borrow<[T]> for CodSlice<'tx, T> {
  #[inline]
  fn borrow(&self) -> &[T] {
    self.deref()
  }
}

impl<'tx, T: Pod> PartialEq for CodSlice<'tx, T>
where
  T: PartialEq,
{
  fn eq(&self, other: &Self) -> bool {
    self.deref() == other.deref()
  }
}

impl<'tx, T: Pod> Eq for CodSlice<'tx, T> where T: Eq {}

impl<'tx, T: Pod> PartialOrd for CodSlice<'tx, T>
where
  T: PartialOrd,
{
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    self.deref().partial_cmp(other.deref())
  }
}

impl<'tx, T: Pod> Ord for CodSlice<'tx, T>
where
  T: Ord,
{
  fn cmp(&self, other: &Self) -> Ordering {
    self.deref().cmp(other.deref())
  }
}

impl<'a, T: Pod> fmt::Debug for CodSlice<'a, T>
where
  T: fmt::Debug,
{
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    self.deref().fmt(f)
  }
}

/// Read-write slice to a piece of memory that's either memory mapped or Bump owned
pub struct RWSlice<'tx, T: Pod> {
  ptr: *mut T,
  size: u32,
  p: PhantomData<&'tx mut [T]>,
}

impl<'tx, T: Pod> RWSlice<'tx, T> {
  pub fn new(ptr: *mut T, size: u32) -> RWSlice<'tx, T> {
    RWSlice {
      ptr,
      size,
      p: PhantomData,
    }
  }

  pub fn new_with_offset(ptr: *mut T, offset: usize, size: u32) -> RWSlice<'tx, T> {
    RWSlice {
      ptr: unsafe { ptr.add(offset) },
      size,
      p: PhantomData,
    }
  }

  pub fn distance_from<E>(&self, other: &E) -> u32 {
    ((self.ptr as usize) - (other as *const E as usize)) as u32
  }
}

impl<'tx, T: Pod> Deref for RWSlice<'tx, T> {
  type Target = [T];

  fn deref(&self) -> &'tx Self::Target {
    unsafe { from_raw_parts(self.ptr, self.size as usize) }
  }
}

impl<'tx, T: Pod> DerefMut for RWSlice<'tx, T> {
  fn deref_mut(&mut self) -> &'tx mut Self::Target {
    unsafe { from_raw_parts_mut(self.ptr, self.size as usize) }
  }
}
