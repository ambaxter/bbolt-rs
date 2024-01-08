pub use bumpalo::collections::Vec as BVec;
use bumpalo::Bump;
use bytemuck::{Pod, Zeroable};
use hashbrown::hash_map::DefaultHashBuilder;
use std::cell;
use std::fmt::{Display, Formatter};
use std::ops::{Add, AddAssign, Sub, SubAssign};

pub mod bucket;
pub mod defaults;
pub mod errors;
pub mod inode;
pub mod memory;
pub mod meta;
pub mod page;
pub mod self_owned;
pub mod tree;
pub mod utility;

pub mod ids;

pub use ids::{PgId, TxId};

pub(crate) const ZERO_PGID: PgId = PgId(0);

pub type HashMap<'tx, K, V> = hashbrown::HashMap<K, V, DefaultHashBuilder, &'tx Bump>;

pub type HashSet<'tx, K> = hashbrown::HashSet<K, DefaultHashBuilder, &'tx Bump>;

pub(crate) trait SplitRef<R, B, W>: Copy + Clone {
  /// Access the read section of the struct
  fn split_r(&self) -> cell::Ref<R>;

  /// Access the read and optional write section of the struct
  fn split_r_ow(&self) -> (cell::Ref<R>, Option<cell::Ref<W>>);

  /// Access the option write section of the struct
  fn split_ow(&self) -> Option<cell::Ref<W>>;

  /// Access the bound section of the struct
  fn split_bound(&self) -> B;

  /// Access the read, bound, and optional write section of the struct
  fn split_ref(&self) -> (cell::Ref<R>, B, Option<cell::Ref<W>>);

  /// Mutably access the read section of the struct
  fn split_r_mut(&self) -> cell::RefMut<R>;

  /// Mutably access the read and optional write section of the struct
  fn split_r_ow_mut(&self) -> (cell::RefMut<R>, Option<cell::RefMut<W>>);

  /// Mutably access the option write section of the struct
  fn split_ow_mut(&self) -> Option<cell::RefMut<W>>;

  /// Mutably access the read, bound, and optional write section of the struct
  fn split_ref_mut(&self) -> (cell::RefMut<R>, B, Option<cell::RefMut<W>>);
}
