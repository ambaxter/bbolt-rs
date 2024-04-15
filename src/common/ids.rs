use bytemuck::{Pod, Zeroable};
use std::fmt::{Display, Formatter};
use std::ops::{Add, AddAssign, Sub, SubAssign};

macro_rules! id {

  (
    $(#[$meta:meta])*
    $x:ident
  ) => {

    $(#[$meta])*
    #[repr(C)]
    #[derive(Default, Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Pod, Zeroable)]
    pub struct $x(pub u64);

    impl Display for $x {
      fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
      }
    }

    impl From<u64> for $x {
      #[inline(always)]
      fn from(value: u64) -> Self {
        $x(value)
      }
    }

    impl From<$x> for u64 {
      #[inline(always)]
      fn from(value: $x) -> Self {
        value.0
      }
    }

    impl Add<u64> for $x {
      type Output = $x;

      #[inline(always)]
      fn add(self, rhs: u64) -> Self::Output {
        $x(self.0 + rhs)
      }
    }

    impl Sub<u64> for $x {
      type Output = $x;

      #[inline(always)]
      fn sub(self, rhs: u64) -> Self::Output {
        $x(self.0 - rhs)
      }
    }

    impl AddAssign<u64> for $x {
      #[inline(always)]
      fn add_assign(&mut self, rhs: u64) {
        self.0 += rhs;
      }
    }

    impl SubAssign<u64> for $x {
      #[inline(always)]
      fn sub_assign(&mut self, rhs: u64) {
        self.0 -= rhs;
      }
    }

    impl PartialEq<$x> for u64 {
      #[inline(always)]
      fn eq(&self, other: &$x) -> bool {
        *self == other.0
      }
    }
  };
}

id!(
  /// The Page ID. Page address = `PgId` * page_size
  PgId
);

/// Create a PgId
pub(crate) const fn pd(id: u64) -> PgId {
  PgId(id)
}

id!(
  /// The Transaction ID. Monotonic and incremented every commit
  TxId
);

/// Create a TxId
pub(crate) const fn td(id: u64) -> TxId {
  TxId(id)
}
