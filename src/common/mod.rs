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

pub(crate) const ZERO_PGID: PgId = PgId { id: 0 };

//TODO: Look up math support
#[repr(C)]
#[derive(Default, Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Pod, Zeroable)]
pub struct PgId {
  pub(crate) id: u64,
}

impl PgId {
  #[inline(always)]
  pub const fn new(id: u64) -> PgId {
    PgId { id }
  }
}

impl Display for PgId {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", self.id)
  }
}

impl From<u64> for PgId {
  #[inline(always)]
  fn from(value: u64) -> Self {
    PgId::new(value)
  }
}

impl From<PgId> for u64 {
  #[inline(always)]
  fn from(value: PgId) -> Self {
    value.id
  }
}

impl Add<u64> for PgId {
  type Output = PgId;

  fn add(self, rhs: u64) -> Self::Output {
    PgId::new(self.id + rhs)
  }
}

impl Sub<u64> for PgId {
  type Output = PgId;

  fn sub(self, rhs: u64) -> Self::Output {
    PgId::new(self.id - rhs)
  }
}

impl AddAssign<u64> for PgId {
  fn add_assign(&mut self, rhs: u64) {
    self.id += rhs;
  }
}

impl SubAssign<u64> for PgId {
  fn sub_assign(&mut self, rhs: u64) {
    self.id -= rhs;
  }
}

impl PartialEq<PgId> for u64 {
  fn eq(&self, other: &PgId) -> bool {
    *self == other.id
  }
}

#[repr(C)]
#[derive(Default, Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Pod, Zeroable)]
pub struct TxId {
  pub(crate) id: u64,
}

impl TxId {
  pub const fn new(id: u64) -> TxId {
    TxId { id }
  }
}

impl Display for TxId {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", self.id)
  }
}

impl From<u64> for TxId {
  #[inline(always)]
  fn from(value: u64) -> Self {
    TxId { id: value }
  }
}

impl From<TxId> for u64 {
  #[inline(always)]
  fn from(value: TxId) -> Self {
    value.id
  }
}

impl Sub<u64> for TxId {
  type Output = TxId;

  fn sub(self, rhs: u64) -> Self::Output {
    TxId::new(self.id - rhs)
  }
}

pub type HashMap<'tx, K, V> = hashbrown::HashMap<K, V, DefaultHashBuilder, &'tx Bump>;

pub type HashSet<'tx, K> = hashbrown::HashSet<K, DefaultHashBuilder, &'tx Bump>;

pub(crate) trait CRef<T>: Copy + Clone {
  fn as_cref(&self) -> cell::Ref<T>;

  fn as_cref_mut(&self) -> cell::RefMut<T>;
}

pub(crate) trait CRefMut<T>: Copy + Clone {
  fn as_mut_cref(&self) -> cell::Ref<T>;

  fn as_mut_cref_mut(&self) -> cell::RefMut<T>;
}
