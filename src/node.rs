use crate::bucket::{Bucket, BucketAPI, BucketIAPI, BucketMut, BucketMutAPI, BucketMutIAPI, MAX_FILL_PERCENT, MIN_FILL_PERCENT};
use crate::common::inode::INode;
use crate::common::memory::{CodSlice, SCell};
use crate::common::page::{CoerciblePage, MutPage, RefPage, MIN_KEYS_PER_PAGE, PAGE_HEADER_SIZE};
use crate::common::tree::{
  MappedBranchPage, MappedLeafPage, TreePage, BRANCH_PAGE_ELEMENT_SIZE, LEAF_PAGE_ELEMENT_SIZE,
};
use crate::common::{BVec, IRef, PgId, ZERO_PGID};
use crate::tx::{Tx, TxAPI, TxIAPI, TxMut, TxMutIAPI};
use bumpalo::Bump;
use hashbrown::Equivalent;
use std::cell;
use std::cell::{Ref, RefCell, RefMut};
use std::marker::PhantomData;
use std::mem;
use std::ops::Deref;

pub struct NodeW<'tx> {
  pub(crate) is_leaf: bool,
  pub(crate) key: CodSlice<'tx, u8>,
  pub(crate) pgid: PgId,
  pub(crate) inodes: BVec<'tx, INode<'tx>>,
  bucket: BucketMut<'tx>,
  parent: Option<NodeMut<'tx>>,
  is_unbalanced: bool,
  is_spilled: bool,
  children: BVec<'tx, NodeMut<'tx>>,
}

impl<'tx> PartialEq for NodeW<'tx> {
  fn eq(&self, other: &Self) -> bool {
    self.pgid == other.pgid && self.key == other.key
  }
}

impl<'tx> Eq for NodeW<'tx> {}

impl<'tx> NodeW<'tx> {

  fn new_parent_in(bucket: BucketMut<'tx>) -> NodeW<'tx> {
    let bump = bucket.api_tx().bump();
    NodeW {
      is_leaf: false,
      key: CodSlice::Owned(&[]),
      //TODO: this usually defines an inline page
      pgid: Default::default(),
      inodes: BVec::new_in(bump),
      bucket,
      parent: None,
      is_unbalanced: false,
      is_spilled: false,
      children: BVec::with_capacity_in(0, bump),
    }
  }

  fn new_child_in(bucket: BucketMut<'tx>, is_leaf: bool, parent: NodeMut<'tx>) -> NodeW<'tx> {
    let bump = bucket.api_tx().bump();
    NodeW {
      is_leaf,
      key: CodSlice::Owned(&[]),
      //TODO: this usually defines an inline page
      pgid: Default::default(),
      inodes: BVec::new_in(bump),
      bucket,
      parent: Some(parent),
      is_unbalanced: false,
      is_spilled: false,
      children: BVec::with_capacity_in(0, bump),
    }
  }

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
      is_unbalanced: false,
      is_spilled: false,
      children: BVec::with_capacity_in(page.count as usize, bump),
    }
  }

  pub(crate) fn page_element_size(&self) -> usize {
    if self.is_leaf {
      LEAF_PAGE_ELEMENT_SIZE
    } else {
      BRANCH_PAGE_ELEMENT_SIZE
    }
  }

  pub(crate) fn min_keys(&self) -> u32 {
    if self.is_leaf {
      1
    } else {
      2
    }
  }

  pub(crate) fn size(&self) -> usize {
    let mut size = PAGE_HEADER_SIZE;
    let elem_size = self.page_element_size();
    for inode in &self.inodes {
      size += elem_size + inode.key().len() + inode.value().len();
    }
    size
  }

  pub(crate) fn size_less_than(&self, v: usize) -> bool {
    let mut size = PAGE_HEADER_SIZE;
    let elem_size = self.page_element_size();
    for inode in &self.inodes {
      size += elem_size + inode.key().len() + inode.value().len();
      if size > v {
        return false;
      }
    }
    true
  }

  pub(crate) fn split_index(&self, threshold: usize) -> (usize, usize) {
    let mut size = PAGE_HEADER_SIZE;
    let mut index = 0;
    if self.inodes.len() <= MIN_KEYS_PER_PAGE {
      return (index, size);
    }
    for (idx, inode) in self
      .inodes
      .split_at(self.inodes.len() - MIN_KEYS_PER_PAGE)
      .0
      .iter()
      .enumerate()
    {
      index = idx;
      let elsize = self.page_element_size() + inode.key().len() + inode.value().len();
      if index >= MIN_KEYS_PER_PAGE && size + elsize > threshold {
        break;
      }
      size += elsize;
    }
    (index, size)
  }

  fn write(&self, p: &mut MutPage) {
    if self.inodes.len() >= 0xFFFF {
      panic!("inode overflow: {} (pgid={})", self.inodes.len(), p.id);
    }
    if self.is_leaf {
      MappedLeafPage::mut_into(p).write_elements(&self.inodes);
    } else {
      MappedBranchPage::mut_into(p).write_elements(&self.inodes);
    }
  }
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub struct NodeMut<'tx> {
  pub(crate) cell: SCell<'tx, NodeW<'tx>>,
}

impl<'tx> NodeMut<'tx> {
  fn new_parent_in(bucket: BucketMut<'tx>) -> NodeMut<'tx> {
    NodeMut{
      cell: SCell::new_in(NodeW::new_parent_in(bucket), bucket.api_tx().bump())
    }
  }

  fn new_child_in(bucket: BucketMut<'tx>, is_leaf: bool, parent: NodeMut<'tx>) -> NodeMut<'tx> {
    NodeMut{
      cell: SCell::new_in(NodeW::new_child_in(bucket, is_leaf, parent), bucket.api_tx().bump())
    }
  }

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
    borrow.is_unbalanced = true;
  }

  pub(crate) fn write(self: NodeMut<'tx>, page: &mut MutPage<'tx>) {
    // TODO: use INode.write_inodes
    let borrow = self.cell.borrow();
    if borrow.is_leaf {
      let mpage = MappedLeafPage::mut_into(page);
      mpage.write_elements(&borrow.inodes);
    } else {
      let mpage = MappedBranchPage::mut_into(page);
      mpage.write_elements(&borrow.inodes);
    }
  }

  pub(crate) fn split(self: NodeMut<'tx>, tx: &TxMut<'tx>, parent_children: &mut BVec<NodeMut<'tx>>) -> BVec<'tx, NodeMut<'tx>> {
    let mut nodes = { BVec::new_in(tx.bump()) };
    let mut node = self;
    loop {
      let (a, b) = node.split_two(tx.page_size(), parent_children);
      nodes.push(a);
      if b.is_none() {
        break;
      }
      node = b.unwrap();
    }
    nodes
  }

  pub(crate) fn split_two(
    self: NodeMut<'tx>, page_size: usize, parent_children: &mut BVec<NodeMut<'tx>>
  ) -> (NodeMut<'tx>, Option<NodeMut<'tx>>) {

    let mut self_borrow = self.cell.borrow_mut();
    if self_borrow.inodes.len() <= MIN_KEYS_PER_PAGE * 2 || self_borrow.size_less_than(page_size) {
      return (self, None);
    }
    let mut fill_percent = self_borrow.bucket.borrow_iref().1.unwrap().fill_percent;
    fill_percent = fill_percent.max(MIN_FILL_PERCENT).min(MAX_FILL_PERCENT);
    let threshold = (page_size as f64 * fill_percent) as usize;
    let (split_index, _) = self_borrow.split_index(threshold);
    let parent = {
      if let Some(parent) = self_borrow.parent {
        parent
      } else {
        let parent = NodeMut::new_parent_in(self_borrow.bucket);
        self_borrow.parent = Some(parent);
        parent_children.push(self);
        parent
      }
    };

    let mut next = NodeMut::new_child_in(self_borrow.bucket, self_borrow.is_leaf, parent);
    parent_children.push(next);

    let mut next_borrow = next.cell.borrow_mut();
    next_borrow.inodes = self_borrow.inodes.split_off(split_index);
    (self, Some(next))

  }


  pub(crate) fn spill_child(self: NodeMut<'tx>, parent_children: &mut BVec<NodeMut<'tx>>) -> crate::Result<()> {
    let(tx, mut children) = {
      let mut self_borrow = self.cell.borrow_mut();
      if self_borrow.is_spilled {
        return Ok(());
      }
      let mut child_swap = BVec::with_capacity_in(0, self_borrow.bucket.api_tx().bump());
      mem::swap(&mut self_borrow.children, &mut child_swap);
      (self_borrow.bucket.api_tx(), child_swap)
    };

    children.sort_by_key(|probe | probe.cell.borrow().inodes[0].key());
    let mut i: usize = 0;
    loop {
      if i < children.len() {
        children[i].spill_child(&mut children)?;
        i += 1;
      } else {
        break;
      }
    }

    let nodes = self.split(tx, parent_children);
    for node in nodes {
      let node_borrow = node.cell.borrow_mut();
      if node_borrow.pgid > ZERO_PGID {
        tx.freelist().free(tx.txid(), &tx.page(node_borrow.pgid));
        node_borrow.pgid = ZERO_PGID;
      }
      let mut p = tx.allocate((node_borrow.size() + tx.page_size() - 1)/ tx.page_size())?;
      if p.id >= tx.meta().pgid() {
        panic!("pgid {} above high water mark {}", p.id, tx.meta().pgid())
      }

      node_borrow.pgid = p.id;
      node_borrow.write(&mut p);
      node_borrow.is_spilled = true;
      if let Some(parent) = node_borrow.parent {
        let key = {
          if node_borrow.key.len() == 0 {
            node_borrow.inodes[0].key()
          } else {
            &node_borrow.key
          }
        };
        parent.put(key, node_borrow.inodes[0].key(), &[], node_borrow.pgid, 0);
        node_borrow.key = node_borrow.inodes[0].key();
      }
    }
    Ok(())

  }

  pub(crate) fn rebalance(self: NodeMut<'tx>) {
    todo!()
  }

  pub(crate) fn remove_child(self: NodeMut<'tx>, target: NodeMut<'tx>) {
    let mut borrow = self.cell.borrow_mut();
    if let Some(pos) = borrow.children.iter().position(|n| *n == target) {
      borrow.children.remove(pos);
    }
  }

  // Descending the tree shouldn't create runtime issues
  // We bend the rules here!
  pub(crate) fn own_in(self: NodeMut<'tx>, bump: &'tx Bump) {
    let mut borrow = self.cell.borrow_mut();
    borrow.key.own_in(bump);
    for inode in &mut borrow.inodes {
      inode.own_in(bump);
    }
    for child in &borrow.children {
      child.own_in(bump);
    }
  }

  pub(crate) fn free(self: NodeMut<'tx>) {
    let (pgid, api_tx) = {
      let borrow = self.cell.borrow();
      if borrow.pgid == ZERO_PGID {
        return;
      }
      (borrow.pgid, borrow.bucket.api_tx())
    };
    let page = api_tx.page(pgid);
    let txid = api_tx.meta().txid();
    api_tx.freelist().free(txid, &page);
  }
}
