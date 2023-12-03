use crate::bucket::{Bucket, BucketAPI, BucketIAPI, BucketMut, BucketMutAPI, BucketMutIAPI};
use crate::common::inode::INode;
use crate::common::memory::{CodSlice, SCell};
use crate::common::page::{CoerciblePage, MutPage, RefPage, PAGE_HEADER_SIZE};
use crate::common::tree::{
  MappedBranchPage, MappedLeafPage, TreePage, BRANCH_PAGE_ELEMENT_SIZE, LEAF_PAGE_ELEMENT_SIZE,
};
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
    let parent = self.cell.borrow().parent;
    match parent {
      None => self,
      Some(p) => p.root(),
    }
  }

  pub(crate) fn min_keys(self: NodeMut<'tx>) -> u32 {
    if self.cell.borrow().is_leaf {
      1
    } else {
      2
    }
  }

  pub(crate) fn size(self: NodeMut<'tx>) -> usize {
    let mut size = PAGE_HEADER_SIZE;
    let elem_size = self.page_element_size();
    let node = self.cell.borrow();
    for inode in &node.inodes {
      size += elem_size + inode.key().len() + inode.value().len();
    }
    size
  }

  pub(crate) fn size_less_than(self: NodeMut<'tx>, v: usize) -> bool {
    let mut size = PAGE_HEADER_SIZE;
    let elem_size = self.page_element_size();
    let node = self.cell.borrow();
    for inode in &node.inodes {
      size += elem_size + inode.key().len() + inode.value().len();
      if size > v {
        return false;
      }
    }
    true
  }

  pub(crate) fn page_element_size(self: NodeMut<'tx>) -> usize {
    if self.cell.borrow().is_leaf {
      LEAF_PAGE_ELEMENT_SIZE
    } else {
      BRANCH_PAGE_ELEMENT_SIZE
    }
  }

  pub(crate) fn child_at(self: NodeMut<'tx>, index: u32) -> NodeMut<'tx> {
    let (bucket, pgid) = {
      let node = self.cell.borrow();
      if node.is_leaf {
        panic!("invalid child_at {} on leaf node", index);
      }
      (node.bucket, node.inodes[index as usize].pgid())
    };
    bucket.node(pgid, Some(self))
  }

  pub(crate) fn child_index(self: NodeMut<'tx>, child: NodeMut<'tx>) -> usize {
    let child_key = child.cell.borrow().key;
    let result = {
      let node = self.cell.borrow();
      node
        .inodes
        .binary_search_by(|probe| probe.key().cmp(&child_key))
    };
    match result {
      Ok(index) => index,
      Err(next_closest) => next_closest,
    }
  }

  pub(crate) fn num_children(self: NodeMut<'tx>) -> usize {
    self.cell.borrow().inodes.len()
  }

  pub(crate) fn next_sibling(self: NodeMut<'tx>) -> Option<NodeMut<'tx>> {
    let parent = self.cell.borrow().parent;
    if let Some(parent_node) = parent {
      let index = parent_node.child_index(self);
      if index >= parent_node.num_children() - 1 {
        return None;
      }
      return Some(parent_node.child_at((index + 1) as u32));
    }
    None
  }

  pub(crate) fn prev_sibling(self: NodeMut<'tx>) -> Option<NodeMut<'tx>> {
    let parent = self.cell.borrow().parent;
    if let Some(parent_node) = parent {
      let index = parent_node.child_index(self);
      if index == 0 {
        return None;
      }
      return Some(parent_node.child_at((index - 1) as u32));
    }
    None
  }

  pub(crate) fn put(
    self: NodeMut<'tx>, old_key: &'tx [u8], new_key: &'tx [u8], value: &'tx [u8], pgid: PgId,
    flags: u32,
  ) {
    let mut borrow = self.cell.borrow_mut();
    if pgid >= borrow.bucket.api_tx().meta().pgid() {
      panic!(
        "pgid {} above high water mark {}",
        pgid,
        borrow.bucket.api_tx().meta().pgid()
      );
    } else if old_key.is_empty() {
      panic!("put: zero-length old key");
    } else if new_key.is_empty() {
      panic!("put: zero-length new key");
    }
    let index = borrow
      .inodes
      .binary_search_by(|probe| probe.key().cmp(old_key));
    let new_node =
      INode::new_owned_in(flags, pgid, new_key, old_key, borrow.bucket.api_tx().bump());
    if new_node.key().is_empty() {
      panic!("put: zero-length new key");
    }
    match index {
      Ok(exact) => *borrow.inodes.get_mut(exact).unwrap() = new_node,
      Err(closest) => borrow.inodes.insert(closest, new_node),
    }
  }

  pub(crate) fn del(self: NodeMut<'tx>, key: &[u8]) {
    let mut borrow = self.cell.borrow_mut();
    let index = borrow.inodes.binary_search_by(|probe| probe.key().cmp(key));
    if let Ok(exact) = index {
      borrow.inodes.remove(exact);
    }
    borrow.unbalanced = true;
  }

  pub(crate) fn write(self: NodeMut<'tx>, page: &mut MutPage<'tx>) {
    let borrow = self.cell.borrow();
    if borrow.is_leaf {
      let mpage = MappedLeafPage::mut_into(page);
      mpage.write_elements(&borrow.inodes);
    } else {
      let mpage = MappedBranchPage::mut_into(page);
      mpage.write_elements(&borrow.inodes);
    }
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
