use crate::bucket::{Bucket, BucketAPI, BucketMut, BucketMutAPI};
use crate::common::inode::INode;
use crate::common::memory::{CodSlice, SCell};
use crate::common::{BVec, IRef, PgId};
use crate::tx::{Tx, TxAPI, TxMut};
use std::cell;
use std::cell::{Ref, RefCell, RefMut};
use std::marker::PhantomData;
use std::mem;

pub trait NodeIRef<'tx>: IRef<NodeR<'tx>, NodeW<'tx>> {}

pub struct NodeR<'tx> {
  pub(crate) is_leaf: bool,
  pub(crate) key: CodSlice<'tx, u8>,
  pub(crate) pgid: PgId,
  pub(crate) inodes: BVec<'tx, INode<'tx>>,
}
pub struct NodeW<'tx> {
  bucket: BucketMut<'tx>,
  parent: Option<NodeMut<'tx>>,
  unbalanced: bool,
  spilled: bool,
  children: BVec<'tx, NodeMut<'tx>>,
}

pub struct NodeRW<'tx> {
  r: NodeR<'tx>,
  w: NodeW<'tx>,
}

#[derive(Copy, Clone)]
pub struct Node<'tx> {
  cell: SCell<'tx, NodeR<'tx>>,
}

impl<'tx> IRef<NodeR<'tx>, NodeW<'tx>> for Node<'tx> {
  fn borrow_iref(&self) -> (Ref<NodeR<'tx>>, Option<Ref<NodeW<'tx>>>) {
    (self.cell.borrow(), None)
  }

  fn borrow_mut_iref(&self) -> (RefMut<NodeR<'tx>>, Option<RefMut<NodeW<'tx>>>) {
    (self.cell.borrow_mut(), None)
  }
}

impl<'tx> NodeIRef<'tx> for Node<'tx> {}

#[derive(Copy, Clone)]
pub struct NodeMut<'tx> {
  cell: SCell<'tx, NodeRW<'tx>>,
}

impl<'tx> IRef<NodeR<'tx>, NodeW<'tx>> for NodeMut<'tx> {
  fn borrow_iref(&self) -> (Ref<NodeR<'tx>>, Option<Ref<NodeW<'tx>>>) {
    let (r, w) = Ref::map_split(self.cell.borrow(), |b| (&b.r, &b.w));
    (r, Some(w))
  }

  fn borrow_mut_iref(&self) -> (RefMut<NodeR<'tx>>, Option<RefMut<NodeW<'tx>>>) {
    let (r, w) = RefMut::map_split(self.cell.borrow_mut(), |b| (&mut b.r, &mut b.w));
    (r, Some(w))
  }
}

impl<'tx> NodeIRef<'tx> for NodeMut<'tx> {}
