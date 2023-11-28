pub use bumpalo::collections::Vec as BVec;
use bumpalo::Bump;
use bytemuck::{Pod, Zeroable};
use hashbrown::hash_map::DefaultHashBuilder;
use std::cell;
use std::fmt::{Display, Formatter};
use std::ops::{Add, AddAssign, Sub, SubAssign};

pub mod bucket;
pub mod defaults;
mod errors;
pub mod inode;
pub mod memory;
pub mod meta;
pub mod page;
pub mod selfowned;
pub mod tree;
pub mod utility;

pub(crate) const ZERO_PGID: PgId = PgId(0);

//TODO: Look up math support
#[repr(C)]
#[derive(Default, Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Pod, Zeroable)]
pub struct PgId(pub(crate) u64);

impl Display for PgId {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", self.0)
  }
}

impl From<u64> for PgId {
  #[inline(always)]
  fn from(value: u64) -> Self {
    PgId(value)
  }
}

impl From<PgId> for u64 {
  #[inline(always)]
  fn from(value: PgId) -> Self {
    value.0
  }
}

impl Add<u64> for PgId {
  type Output = PgId;

  fn add(self, rhs: u64) -> Self::Output {
    PgId(self.0 + rhs)
  }
}

impl Sub<u64> for PgId {
  type Output = PgId;

  fn sub(self, rhs: u64) -> Self::Output {
    PgId(self.0 - rhs)
  }
}

impl AddAssign<u64> for PgId {
  fn add_assign(&mut self, rhs: u64) {
    self.0 += rhs;
  }
}

impl SubAssign<u64> for PgId {
  fn sub_assign(&mut self, rhs: u64) {
    self.0 -= rhs;
  }
}

impl PartialEq<PgId> for u64 {
  fn eq(&self, other: &PgId) -> bool {
    *self == other.0
  }
}

#[repr(C)]
#[derive(Default, Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Pod, Zeroable)]
pub struct TxId(pub(crate) u64);

impl Display for TxId {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", self.0)
  }
}

impl From<u64> for TxId {
  #[inline(always)]
  fn from(value: u64) -> Self {
    TxId(value)
  }
}

impl From<TxId> for u64 {
  #[inline(always)]
  fn from(value: TxId) -> Self {
    value.0
  }
}

impl Sub<u64> for TxId {
  type Output = TxId;

  fn sub(self, rhs: u64) -> Self::Output {
    TxId(self.0 - rhs)
  }
}

pub type HashMap<'tx, K, V> = hashbrown::HashMap<K, V, DefaultHashBuilder, &'tx Bump>;

pub type HashSet<'tx, K> = hashbrown::HashSet<K, DefaultHashBuilder, &'tx Bump>;

pub(crate) trait IRef<R, W>: Copy + Clone {
  fn borrow_iref(&self) -> (cell::Ref<R>, Option<cell::Ref<W>>);

  fn borrow_mut_iref(&self) -> (cell::RefMut<R>, Option<cell::RefMut<W>>);
}
