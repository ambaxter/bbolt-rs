use crate::common::BVec;
use bumpalo::Bump;
use bytemuck::Pod;
use std::borrow::Borrow;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::fmt::Formatter;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::ptr::slice_from_raw_parts;
use std::slice::{from_raw_parts, from_raw_parts_mut};
use std::{fmt, mem, slice};

/// A borrowed cell backed by `RefCell`
pub struct LCell<'a, T: ?Sized>(&'a RefCell<T>);

impl<'a, T> Clone for LCell<'a, T> {
  fn clone(&self) -> Self {
    *self
  }
}

impl<'a, T> Copy for LCell<'a, T> {}

impl<'a, T> LCell<'a, T> {
  /// Allocates a BCell in a Bumpalo arena
  pub fn new_in(x: T, a: &'a Bump) -> LCell<'a, T> {
    LCell(a.alloc(RefCell::new(x)))
  }
}

impl<'a, T> Deref for LCell<'a, T> {
  type Target = RefCell<T>;

  fn deref(&self) -> &Self::Target {
    self.0
  }
}

impl<'a, T> PartialEq for LCell<'a, T>
where
  T: PartialEq,
{
  fn eq(&self, other: &Self) -> bool {
    self.deref() == other.deref()
  }
}

impl<'a, T> Eq for LCell<'a, T> where T: Eq {}

/// A borrowed cell backed by `RefCell` with a bound value
pub struct BCell<'a, T: Sized, B: Sized>(&'a (RefCell<T>, B));

impl<'a, T, B> Clone for BCell<'a, T, B> {
  fn clone(&self) -> Self {
    *self
  }
}

impl<'a, T, B> Copy for BCell<'a, T, B> {}

impl<'a, T, B: Clone> BCell<'a, T, B> {
  /// Allocates a BCell in a Bumpalo arena
  pub fn new_in(t: T, b: B, a: &'a Bump) -> BCell<'a, T, B> {
    BCell(a.alloc((RefCell::new(t), b)))
  }

  pub fn bound(&self) -> B {
    self.0 .1.clone()
  }
}

impl<'a, T, B> Deref for BCell<'a, T, B> {
  type Target = RefCell<T>;

  fn deref(&self) -> &Self::Target {
    &self.0 .0
  }
}

impl<'a, T, B> PartialEq for BCell<'a, T, B>
where
  T: PartialEq,
{
  fn eq(&self, other: &Self) -> bool {
    self.0 .0 == other.0 .0
  }
}

impl<'a, T, B> Eq for BCell<'a, T, B> where T: Eq {}

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
  pub fn get_ref(&self) -> &'tx [T] {
    match self {
      CodSlice::Owned(s) => s,
      CodSlice::Mapped(m) => m,
    }
  }
}

impl<'tx, T: Pod> Deref for CodSlice<'tx, T> {
  type Target = [T];

  #[inline]
  fn deref(&self) -> &Self::Target {
    match self {
      CodSlice::Owned(s) => s,
      CodSlice::Mapped(m) => m,
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
//TODO: Probably not useful. Might want to retire this after we finish
// complete the code
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

//TODO: use std is_aligned_to when it comes out
pub(crate) trait IsAligned: Copy {
  fn is_aligned_to<U>(self) -> bool;
}

impl IsAligned for *const u8 {
  #[inline(always)]
  fn is_aligned_to<U>(self) -> bool {
    (self as usize & (mem::align_of::<U>() - 1)) == 0
  }
}

impl IsAligned for *mut u8 {
  #[inline(always)]
  fn is_aligned_to<U>(self) -> bool {
    (self as usize & (mem::align_of::<U>() - 1)) == 0
  }
}

// As described in https://github.com/rust-lang/rust/issues/68318#issuecomment-1066221968
pub type PhantomUnsend = PhantomData<*mut ()>;

pub(crate) struct SubArray<'a, T> {
  phantom: PhantomData<&'a [T]>,
  ptr: *const T,
  len: usize,
}

impl<'a, T> SubArray<'a, T> {
  pub(crate) unsafe fn new(ptr: *const T, len: usize) -> Self {
    SubArray {
      phantom: Default::default(),
      ptr,
      len,
    }
  }
}

unsafe impl<'a, T: Send> Send for SubArray<'a, T> {}
unsafe impl<'a, T: Sync> Sync for SubArray<'a, T> {}

impl<'a, T> From<&BVec<'a, T>> for SubArray<'a, T> {
  fn from(value: &BVec<'a, T>) -> Self {
    unsafe { SubArray::new(value.as_ptr(), value.len()) }
  }
}

impl<'a, T> Deref for SubArray<'a, T> {
  type Target = [T];

  fn deref(&self) -> &Self::Target {
    unsafe { from_raw_parts(self.ptr, self.len) }
  }
}

impl<'a, T> DerefMut for SubArray<'a, T> {
  fn deref_mut(&mut self) -> &mut Self::Target {
    unsafe { from_raw_parts_mut(self.ptr.cast_mut(), self.len) }
  }
}

impl<'a, T> AsRef<[T]> for SubArray<'a, T> {
  fn as_ref(&self) -> &[T] {
    self.deref()
  }
}

impl<'a, T> AsMut<[T]> for SubArray<'a, T> {
  fn as_mut(&mut self) -> &mut [T] {
    self.deref_mut()
  }
}

pub(crate) enum VecOrSub<'a, T> {
  Vec(BVec<'a, T>),
  Sub(SubArray<'a, T>),
}

impl<'a, T> VecOrSub<'a, T> {
  pub(crate) fn get_vec(&self) -> &BVec<'a, T> {
    match self {
      VecOrSub::Vec(v) => v,
      VecOrSub::Sub(_) => panic!("sub access"),
    }
  }

  pub(crate) fn get_mut_vec(&mut self) -> &mut BVec<'a, T> {
    match self {
      VecOrSub::Vec(v) => v,
      VecOrSub::Sub(_) => panic!("sub access"),
    }
  }
}

impl<'a, T> Deref for VecOrSub<'a, T> {
  type Target = [T];

  fn deref(&self) -> &Self::Target {
    match self {
      VecOrSub::Vec(v) => v,
      VecOrSub::Sub(s) => s,
    }
  }
}

impl<'a, T> DerefMut for VecOrSub<'a, T> {
  fn deref_mut(&mut self) -> &mut Self::Target {
    match self {
      VecOrSub::Vec(v) => v,
      VecOrSub::Sub(s) => s,
    }
  }
}

impl<'a, T> AsRef<[T]> for VecOrSub<'a, T> {
  fn as_ref(&self) -> &[T] {
    self.deref()
  }
}

impl<'a, T> AsMut<[T]> for VecOrSub<'a, T> {
  fn as_mut(&mut self) -> &mut [T] {
    self.deref_mut()
  }
}

impl<'a, T> From<BVec<'a, T>> for VecOrSub<'a, T> {
  fn from(value: BVec<'a, T>) -> Self {
    VecOrSub::Vec(value)
  }
}

impl<'a, T> From<SubArray<'a, T>> for VecOrSub<'a, T> {
  fn from(value: SubArray<'a, T>) -> Self {
    VecOrSub::Sub(value)
  }
}
