use bytemuck::{Pod, Zeroable};
use std::fmt::{Display, Formatter};
use std::ops::{Add, AddAssign, Sub, SubAssign};

macro_rules! id {
  ( $x:ident ) => {
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

      fn add(self, rhs: u64) -> Self::Output {
        $x(self.0 + rhs)
      }
    }

    impl Sub<u64> for $x {
      type Output = $x;

      fn sub(self, rhs: u64) -> Self::Output {
        $x(self.0 - rhs)
      }
    }

    impl AddAssign<u64> for $x {
      fn add_assign(&mut self, rhs: u64) {
        self.0 += rhs;
      }
    }

    impl SubAssign<u64> for $x {
      fn sub_assign(&mut self, rhs: u64) {
        self.0 -= rhs;
      }
    }

    impl PartialEq<$x> for u64 {
      fn eq(&self, other: &$x) -> bool {
        *self == other.0
      }
    }
  };
}

id!(PgId);

id!(TxId);
