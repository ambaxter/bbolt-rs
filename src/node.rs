use crate::bucket::{Bucket, BucketMut, TBucket, TBucketMut};
use crate::common::inode::INode;
use crate::common::memory::{CodSlice, SCell};
use crate::common::{BVec, CRef, CRefMut, PgId};
use crate::tx::{TTx, Tx, TxMut};
use std::cell;
use std::cell::{Ref, RefCell, RefMut};
use std::marker::PhantomData;
use std::mem;

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
  w: NodeW<'tx>
}

#[derive(Copy, Clone)]
pub struct Node<'tx> {
  cell: SCell<'tx, NodeR<'tx>>,
}

impl<'tx> CRef<NodeR<'tx>> for Node<'tx> {
  fn as_cref(&self) -> Ref<NodeR<'tx>> {
    self.cell.borrow()
  }

  fn as_cref_mut(&self) -> RefMut<NodeR<'tx>> {
    self.cell.borrow_mut()
  }
}

#[derive(Copy, Clone)]
pub struct NodeMut<'tx> {
  cell: SCell<'tx, NodeRW<'tx>>,
}

impl<'tx> CRef<NodeR<'tx>> for NodeMut<'tx> {
  fn as_cref(&self) -> Ref<NodeR<'tx>> {
    Ref::map(self.cell.borrow(), |n| &n.r)
  }

  fn as_cref_mut(&self) -> RefMut<NodeR<'tx>> {
    RefMut::map(self.cell.borrow_mut(), |n| &mut n.r)
  }
}

impl<'tx> CRefMut<NodeRW<'tx>> for NodeMut<'tx> {
  fn as_mut_cref(&self) -> Ref<NodeRW<'tx>> {
    self.cell.borrow()
  }

  fn as_mut_cref_mut(&self) -> RefMut<NodeRW<'tx>> {
    self.cell.borrow_mut()
  }
}

