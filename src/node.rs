use crate::bucket::{Bucket, BucketAPI, BucketIAPI, BucketMut, BucketMutAPI};
use crate::common::inode::INode;
use crate::common::memory::{CodSlice, SCell};
use crate::common::page::{CoerciblePage, RefPage};
use crate::common::tree::{MappedBranchPage, MappedLeafPage, TreePage};
use crate::common::{BVec, IRef, PgId};
use crate::tx::{Tx, TxAPI, TxIAPI, TxMut};
use bumpalo::Bump;
use std::cell;
use std::cell::{Ref, RefCell, RefMut};
use std::marker::PhantomData;
use std::mem;

pub(crate) struct NodeImpl {}

impl NodeImpl {
  pub(crate) fn del(cell: NodeMut, key: &[u8]) {
    todo!()
  }

  pub(crate) fn put<'tx>(
    cell: NodeMut, old_key: &'tx [u8], new_key: &'tx [u8], value: &'tx [u8], pg_id: PgId,
    flags: u32,
  ) {
    todo!()
  }

  pub(crate) fn child_at<'tx>(cell: SCell<NodeW<'tx>>, index: u32) -> NodeMut<'tx> {
    todo!()
  }
  pub(crate) fn free(cell: NodeMut) {
    todo!()
  }
  pub(crate) fn own_in<'tx>(cell: NodeMut<'tx>, bump: &'tx Bump) {
    todo!()
  }

  pub(crate) fn root(cell: NodeMut) -> NodeMut {
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

impl<'tx> NodeW<'tx> {
  pub(crate) fn read_in<'a>(
    bucket: BucketMut<'tx>, parent: Option<NodeMut<'tx>>, page: &RefPage<'tx>,
  ) -> NodeW<'tx> {
    assert!(page.is_leaf() || page.is_branch(), "Non-tree page read");
    let bump = bucket.tx().bump();
    let mut inodes = BVec::with_capacity_in(page.count as usize, bump);
    INode::read_inodes_in(&mut inodes, page);
    let key = if inodes.len() > 0 {
      CodSlice::Mapped(inodes[0].key())
    } else {
      CodSlice::Mapped(&[])
    };
    NodeW {
      is_leaf: false,
      key,
      pgid: page.id,
      inodes,
      bucket,
      parent,
      unbalanced: false,
      spilled: false,
      children: BVec::with_capacity_in(page.count as usize, bump),
    }
  }
}

#[derive(Copy, Clone)]
pub struct NodeMut<'tx> {
  pub(crate) cell: SCell<'tx, NodeW<'tx>>,
}

impl<'tx> NodeMut<'tx> {
  pub(crate) fn read_in(
    bucket: BucketMut<'tx>, parent: Option<NodeMut<'tx>>, page: &RefPage<'tx>,
  ) -> NodeMut<'tx> {
    NodeMut {
      cell: SCell::new_in(NodeW::read_in(bucket, parent, page), bucket.tx().bump()),
    }
  }
}
