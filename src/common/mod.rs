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

//TODO: Refactor this. We don't need all 3 each and every time.
//Once we're done figure out exactly what we need
pub(crate) trait SplitRef<R, T, W>: Copy + Clone {
  fn split_ref(&self) -> (cell::Ref<R>, cell::Ref<T>, Option<cell::Ref<W>>);

  fn split_ref_mut(&self) -> (cell::RefMut<R>, cell::RefMut<T>, Option<cell::RefMut<W>>);
}
