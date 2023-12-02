use crate::bucket::{Bucket, BucketAPI, BucketIAPI, BucketIRef, BucketMut, BucketMutAPI};
use crate::common::inode::INode;
use crate::common::memory::{CodSlice, SCell};
use crate::common::page::{CoerciblePage, MutPage, RefPage};
use crate::common::tree::{MappedBranchPage, MappedLeafPage, TreePage};
use crate::common::{BVec, IRef, PgId};
use crate::tx::{Tx, TxAPI, TxIAPI, TxMut};
use bumpalo::Bump;
use std::cell;
use std::cell::{Ref, RefCell, RefMut};
use std::marker::PhantomData;
use std::mem;

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
    let bump = bucket.api_tx().bump();
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
      cell: SCell::new_in(NodeW::read_in(bucket, parent, page), bucket.api_tx().bump()),
    }
  }

  pub(crate) fn root(self: NodeMut<'tx>) -> NodeMut<'tx> {
    todo!()
  }

  pub(crate) fn min_keys(self: NodeMut<'tx>) -> u32 {
    todo!()
  }

  pub(crate) fn size(self: NodeMut<'tx>) -> usize {
    todo!()
  }

  pub(crate) fn size_less_than(v: usize) -> bool {
    todo!()
  }

  pub(crate) fn page_element_size(self: NodeMut<'tx>) -> usize {
    todo!()
  }

  pub(crate) fn child_at(self: NodeMut<'tx>, index: u32) -> NodeMut<'tx> {
    todo!()
  }

  pub(crate) fn child_index(self: NodeMut<'tx>) -> usize {
    todo!()
  }

  pub(crate) fn num_children(self: NodeMut<'tx>) -> usize {
    todo!()
  }

  pub(crate) fn next_sibling(self: NodeMut<'tx>) -> NodeMut<'tx> {
    todo!()
  }

  pub(crate) fn prev_sibling(self: NodeMut<'tx>) -> NodeMut<'tx> {
    todo!()
  }

  pub(crate) fn put(
    self: NodeMut<'tx>, old_key: &'tx [u8], new_key: &'tx [u8], value: &'tx [u8], pg_id: PgId,
    flags: u32,
  ) {
    todo!()
  }

  pub(crate) fn del(self: NodeMut<'tx>, key: &[u8]) {
    todo!()
  }

  pub(crate) fn write(self: NodeMut<'tx>, page: MutPage<'tx>) {
    todo!()
  }

  pub(crate) fn split(self: NodeMut<'tx>, page_size: usize) -> BVec<'tx, NodeMut<'tx>> {
    todo!()
  }

  pub(crate) fn split_two(self: NodeMut<'tx>, page_size: usize) -> (NodeMut<'tx>, NodeMut<'tx>) {
    todo!()
  }

  pub(crate) fn split_index(self: NodeMut<'tx>, threshold: usize) -> (usize, usize) {
    todo!()
  }

  pub(crate) fn spill(self: NodeMut<'tx>) -> crate::Result<()> {
    todo!()
  }

  pub(crate) fn rebalance(self: NodeMut<'tx>) {
    todo!()
  }

  pub(crate) fn remove_child(self: NodeMut<'tx>, target: NodeMut<'tx>) {
    todo!()
  }

  pub(crate) fn own_in(self: NodeMut<'tx>, bump: &'tx Bump) {
    todo!()
  }

  pub(crate) fn free(self: NodeMut<'tx>) {
    todo!()
  }
}
