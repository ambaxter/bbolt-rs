use crate::bucket::{Bucket, BucketAPI, BucketMut, BucketMutAPI};
use crate::common::inode::INode;
use crate::common::memory::{CodSlice, SCell};
use crate::common::{BVec, IRef, IRefMut, PgId};
use crate::tx::{Tx, TxAPI, TxMut};
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
  w: NodeW<'tx>,
}

#[derive(Copy, Clone)]
pub struct Node<'tx> {
  cell: SCell<'tx, NodeR<'tx>>,
}

impl<'tx> IRef<NodeR<'tx>> for Node<'tx> {
  fn borrow_iref(&self) -> Ref<NodeR<'tx>> {
    self.cell.borrow()
  }

  fn borrow_mut_iref(&self) -> RefMut<NodeR<'tx>> {
    self.cell.borrow_mut()
  }
}

#[derive(Copy, Clone)]
pub struct NodeMut<'tx> {
  cell: SCell<'tx, NodeRW<'tx>>,
}

impl<'tx> IRef<NodeR<'tx>> for NodeMut<'tx> {
  fn borrow_iref(&self) -> Ref<NodeR<'tx>> {
    Ref::map(self.cell.borrow(), |n| &n.r)
  }

  fn borrow_mut_iref(&self) -> RefMut<NodeR<'tx>> {
    RefMut::map(self.cell.borrow_mut(), |n| &mut n.r)
  }
}

impl<'tx> IRefMut<NodeRW<'tx>> for NodeMut<'tx> {
  fn borrow_iref_mut(&self) -> Ref<NodeRW<'tx>> {
    self.cell.borrow()
  }

  fn borrow_mut_iref_mut(&self) -> RefMut<NodeRW<'tx>> {
    self.cell.borrow_mut()
  }
}
