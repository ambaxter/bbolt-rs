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
pub mod selfowned;
pub mod tree;
pub mod utility;

pub mod ids;

pub use ids::{PgId, TxId};

pub(crate) const ZERO_PGID: PgId = PgId(0);

pub type HashMap<'tx, K, V> = hashbrown::HashMap<K, V, DefaultHashBuilder, &'tx Bump>;

pub type HashSet<'tx, K> = hashbrown::HashSet<K, DefaultHashBuilder, &'tx Bump>;

pub(crate) trait IRef<R, W>: Copy + Clone {
  fn borrow_iref(&self) -> (cell::Ref<R>, Option<cell::Ref<W>>);

  fn borrow_mut_iref(&self) -> (cell::RefMut<R>, Option<cell::RefMut<W>>);
}
