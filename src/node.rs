use crate::bucket::{Bucket, BucketAPI, BucketMut, BucketMutAPI};
use crate::common::inode::INode;
use crate::common::memory::{CodSlice, SCell};
use crate::common::{BVec, IRef, PgId};
use crate::tx::{Tx, TxAPI, TxMut};
use std::cell;
use std::cell::{Ref, RefCell, RefMut};
use std::marker::PhantomData;
use std::mem;

pub(crate) struct NodeImpl {}

impl NodeImpl {}

impl NodeImpl {
  pub(crate) fn del(n: NodeMut, key: &[u8]) {
    todo!()
  }

  pub(crate) fn put<'tx>(
    cell: NodeMut, old_key: &'tx [u8], new_key: &'tx [u8], value: &'tx mut [u8], pg_id: PgId,
    flags: u32,
  ) {
    todo!()
  }

  pub(crate) fn child_at<'tx>(cell: SCell<NodeW<'tx>>, index: u32) -> NodeMut<'tx> {
    todo!()
  }
}

pub struct NodeW<'tx> {
  pub(crate) is_leaf: bool,
  pub(crate) key: CodSlice<'tx, u8>,
  pub(crate) pgid: PgId,
  pub(crate) inodes: BVec<'tx, INode<'tx>>,
  bucket: BucketMut<'tx>,
  parent: Option<NodeMut<'tx>>,
  unbalanced: bool,
  spilled: bool,
  children: BVec<'tx, NodeMut<'tx>>,
}

#[derive(Copy, Clone)]
pub struct NodeMut<'tx> {
  pub(crate) cell: SCell<'tx, NodeW<'tx>>,
}
