use crate::bucket::{
  BucketApi, BucketCell, BucketIAPI, BucketRwApi, BucketRwCell, BucketRwIAPI, MAX_FILL_PERCENT,
  MIN_FILL_PERCENT,
};
use crate::common::inode::INode;
use crate::common::memory::{CodSlice, LCell};
use crate::common::page::{
  CoerciblePage, MutPage, Page, RefPage, MIN_KEYS_PER_PAGE, PAGE_HEADER_SIZE,
};
use crate::common::tree::{
  MappedBranchPage, MappedLeafPage, TreePage, BRANCH_PAGE_ELEMENT_SIZE, LEAF_PAGE_ELEMENT_SIZE,
};
use crate::common::{BVec, PgId, SplitRef, ZERO_PGID};
use crate::tx::{TxApi, TxCell, TxIAPI, TxRwCell, TxRwIAPI};
use bumpalo::Bump;
use hashbrown::Equivalent;
use std::cell;
use std::cell::{Ref, RefCell, RefMut};
use std::marker::PhantomData;
use std::mem;
use std::ops::Deref;

/// NodeW represents an in-memory, deserialized page.
pub struct NodeW<'tx> {
  pub(crate) is_leaf: bool,
  pub(crate) key: CodSlice<'tx, u8>,
  pub(crate) pgid: PgId,
  pub(crate) inodes: BVec<'tx, INode<'tx>>,
  bucket: BucketRwCell<'tx>,
  parent: Option<NodeRwCell<'tx>>,
  is_unbalanced: bool,
  is_spilled: bool,
  pub(crate) children: BVec<'tx, NodeRwCell<'tx>>,
}

impl<'tx> PartialEq for NodeW<'tx> {
  fn eq(&self, other: &Self) -> bool {
    self.pgid == other.pgid && self.key == other.key
  }
}

impl<'tx> Eq for NodeW<'tx> {}

impl<'tx> NodeW<'tx> {
  fn new_parent_in(bucket: BucketRwCell<'tx>) -> NodeW<'tx> {
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

  fn new_child_in(bucket: BucketRwCell<'tx>, is_leaf: bool, parent: NodeRwCell<'tx>) -> NodeW<'tx> {
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
    bucket: BucketRwCell<'tx>, parent: Option<NodeRwCell<'tx>>, page: &RefPage<'tx>,
  ) -> NodeW<'tx> {
    assert!(page.is_leaf() || page.is_branch(), "Non-tree page read");
    let bump = bucket.api_tx().bump();
    let mut inodes = BVec::with_capacity_in(page.count as usize, bump);
    INode::read_inodes_in(&mut inodes, page);
    let _inodes = inodes.as_slice();
    let key = if inodes.len() > 0 {
      CodSlice::Mapped(inodes[0].key())
    } else {
      CodSlice::Mapped(&[])
    };
    NodeW {
      is_leaf: page.is_leaf(),
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

  /// page_element_size returns the size of each page element based on the type of node.
  pub(crate) fn page_element_size(&self) -> usize {
    if self.is_leaf {
      LEAF_PAGE_ELEMENT_SIZE
    } else {
      BRANCH_PAGE_ELEMENT_SIZE
    }
  }

  /// min_keys returns the minimum number of inodes this node should have.
  pub(crate) fn min_keys(&self) -> usize {
    if self.is_leaf {
      1
    } else {
      2
    }
  }

  pub(crate) fn key(&self) -> &'tx [u8] {
    // I solemnly swear the key is owned by the transaction, not by the node
    unsafe { std::mem::transmute(self.key.deref()) }
  }

  /// size returns the size of the node after serialization.
  pub(crate) fn size(&self) -> usize {
    let mut size = PAGE_HEADER_SIZE;
    let elem_size = self.page_element_size();
    for inode in &self.inodes {
      size += elem_size + inode.key().len() + inode.value().len();
    }
    size
  }

  /// size_less_than returns true if the node is less than a given size.
  /// This is an optimization to avoid calculating a large node when we only need
  /// to know if it fits inside a certain page size.
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

  /// splitIndex finds the position where a page will fill a given threshold.
  /// It returns the index as well as the size of the first page.
  /// This is only be called from split().
  pub(crate) fn split_index(&self, threshold: usize) -> (usize, usize) {
    let mut size = PAGE_HEADER_SIZE;
    let mut index = 0;
    if self.inodes.len() <= MIN_KEYS_PER_PAGE {
      return (index, size);
    }

    // Loop until we only have the minimum number of keys required for the second page.
    for (idx, inode) in self
      .inodes
      .split_at(self.inodes.len() - MIN_KEYS_PER_PAGE)
      .0
      .iter()
      .enumerate()
    {
      index = idx;
      let elsize = self.page_element_size() + inode.key().len() + inode.value().len();

      // If we have at least the minimum number of keys and adding another
      // node would put us over the threshold then exit and return.
      if index >= MIN_KEYS_PER_PAGE && size + elsize > threshold {
        break;
      }

      // Add the element size to the total size.
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

  /// del removes a key from the node.
  fn del(&mut self, key: &[u8]) {
    //println!("trace~node.del - key {:?}", key);
    let inodes = self.inodes.as_slice();
    if let Ok(index) = self.inodes.binary_search_by(|probe| probe.key().cmp(key)) {
      self.inodes.remove(index);
      self.is_unbalanced = true;
    } else {
      //println!("trace~node.del - key {:?} not found", key);
    }
  }

  /// removes a node from the list of in-memory children.
  /// This does not affect the inodes.
  fn remove_child(&mut self, target: NodeRwCell<'tx>) {
    if let Some(pos) = self.children.iter().position(|n| *n == target) {
      self.children.remove(pos);
    }
  }
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub struct NodeRwCell<'tx> {
  pub(crate) cell: LCell<'tx, NodeW<'tx>>,
}

impl<'tx> NodeRwCell<'tx> {
  fn new_parent_in(bucket: BucketRwCell<'tx>) -> NodeRwCell<'tx> {
    NodeRwCell {
      cell: LCell::new_in(NodeW::new_parent_in(bucket), bucket.api_tx().bump()),
    }
  }

  fn new_child_in(
    bucket: BucketRwCell<'tx>, is_leaf: bool, parent: NodeRwCell<'tx>,
  ) -> NodeRwCell<'tx> {
    NodeRwCell {
      cell: LCell::new_in(
        NodeW::new_child_in(bucket, is_leaf, parent),
        bucket.api_tx().bump(),
      ),
    }
  }

  pub(crate) fn read_in(
    bucket: BucketRwCell<'tx>, parent: Option<NodeRwCell<'tx>>, page: &RefPage<'tx>,
  ) -> NodeRwCell<'tx> {
    NodeRwCell {
      cell: LCell::new_in(NodeW::read_in(bucket, parent, page), bucket.api_tx().bump()),
    }
  }

  /// root returns the top-level node this node is attached to.
  pub(crate) fn root(self: NodeRwCell<'tx>) -> NodeRwCell<'tx> {
    let parent = self.cell.borrow().parent;
    match parent {
      None => self,
      Some(p) => p.root(),
    }
  }

  /// childAt returns the child node at a given index.
  pub(crate) fn child_at(self: NodeRwCell<'tx>, index: u32) -> NodeRwCell<'tx> {
    let (bucket, pgid) = {
      let self_borrow = self.cell.borrow();
      if self_borrow.is_leaf {
        panic!("invalid child_at {} on leaf node", index);
      }
      (
        self_borrow.bucket,
        self_borrow.inodes[index as usize].pgid(),
      )
    };
    bucket.node(pgid, Some(self))
  }

  /// childIndex returns the index of a given child node.
  pub(crate) fn child_index(self: NodeRwCell<'tx>, child: NodeRwCell<'tx>) -> usize {
    let child_key = child.cell.borrow().key;
    let result = {
      let self_borrow = self.cell.borrow();
      let inodes = self_borrow.inodes.as_slice();
      self_borrow
        .inodes
        .binary_search_by(|probe| probe.key().cmp(&child_key))
    };
    result
      .map_err(|_| child_key.as_ref())
      .expect("node not found")
  }

  /// num_children returns the number of children.
  pub(crate) fn num_children(self: NodeRwCell<'tx>) -> usize {
    self.cell.borrow().inodes.len()
  }

  /// next_sibling returns the next node with the same parent.
  pub(crate) fn next_sibling(self: NodeRwCell<'tx>) -> Option<NodeRwCell<'tx>> {
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

  /// prev_sibling returns the previous node with the same parent.
  pub(crate) fn prev_sibling(self: NodeRwCell<'tx>) -> Option<NodeRwCell<'tx>> {
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

  /// put inserts a key/value.
  pub(crate) fn put(
    self: NodeRwCell<'tx>, old_key: &'tx [u8], new_key: &'tx [u8], value: &'tx [u8], pgid: PgId,
    flags: u32,
  ) {
    let mut self_borrow = self.cell.borrow_mut();
    /*println!(
      "trace~node.put: key: {:?}, value len: {:?}",
      new_key,
      value.len()
    );*/
    if pgid >= self_borrow.bucket.api_tx().meta().pgid() {
      panic!(
        "pgid {} above high water mark {}",
        pgid,
        self_borrow.bucket.api_tx().meta().pgid()
      );
    } else if old_key.is_empty() {
      panic!("put: zero-length old key");
    } else if new_key.is_empty() {
      panic!("put: zero-length new key");
    }

    // Find insertion index.
    let index = self_borrow
      .inodes
      .binary_search_by(|probe| probe.key().cmp(old_key));
    let new_node = INode::new_owned_in(
      flags,
      pgid,
      new_key,
      value,
      self_borrow.bucket.api_tx().bump(),
    );
    if new_node.key().is_empty() {
      panic!("put: zero-length new key");
    }

    // Add capacity and shift nodes if we don't have an exact match and need to insert.
    match index {
      Ok(exact) => *self_borrow.inodes.get_mut(exact).unwrap() = new_node,
      Err(closest) => self_borrow.inodes.insert(closest, new_node),
    }
  }

  /// del removes a key from the node.
  pub(crate) fn del(self: NodeRwCell<'tx>, key: &[u8]) {
/*    println!(
      "trace~node.del root: {:?}, key: {:?}",
      self.cell.borrow().pgid,
      self.cell.borrow().key
    );*/

    self.cell.borrow_mut().del(key);
  }

  pub(crate) fn size(self: NodeRwCell<'tx>) -> usize {
    self.cell.borrow().size()
  }

  /// write writes the items onto one or more pages.
  /// The page should have p.id (might be 0 for meta or bucket-inline page) and p.overflow set
  /// and the rest should be zeroed.
  pub(crate) fn write(self: NodeRwCell<'tx>, page: &mut MutPage<'tx>) {
    // TODO: use INode.write_inodes
    let self_borrow = self.cell.borrow();
    if self_borrow.is_leaf {
      let mpage = MappedLeafPage::mut_into(page);
      mpage.write_elements(&self_borrow.inodes);
    } else {
      let mpage = MappedBranchPage::mut_into(page);
      mpage.write_elements(&self_borrow.inodes);
    }
  }

  /// spill writes the nodes to dirty pages and splits nodes as it goes.
  /// Returns an error if dirty pages cannot be allocated.
  /// The top-most spill function acts as if it is a parent
  pub(crate) fn spill(self) -> crate::Result<()> {
    let (tx, bump) = {
      let mut cell = self.cell.borrow_mut();
/*      println!(
        "trace~node.spill: pgid: {:?}, key: {:?}",
        cell.pgid,
        cell.key()
        cell.key()
      );*/
      if cell.is_spilled {
        return Ok(());
      }
      cell.children.sort_by_key(|child| child.cell.borrow().key());
      (cell.bucket.api_tx(), cell.children.bump())
    };

    // Spill child nodes first. Child nodes can materialize sibling nodes in
    // the case of split-merge so we cannot use a range loop. We have to check
    // the children size on every loop iteration.
    let mut i = 0usize;
    // have to do this workaround as temporaries live for the entire statement
    // https://users.rust-lang.org/t/why-is-this-refcell-borrow-not-dropped-inside-its-block/66134
    // https://github.com/rust-lang/rust/issues/37612#issuecomment-258676414
    let mut child_get = self.cell.borrow().children.get(i).cloned();
    while let Some(child) = child_get {
      child.spill()?;
      i += 1;
      child_get = self.cell.borrow().children.get(i).cloned();
    }

    // We no longer need the child list because it's only used for spill tracking.
    self.cell.borrow_mut().children.clear();

    let page_size = tx.page_size();

    // Split nodes into appropriate sizes. The first node will always be n.
    let nodes = self.split(page_size, bump);
    for node in nodes {
      let node_size = {
        let mut node_cell = node.cell.borrow_mut();
        if node_cell.pgid > ZERO_PGID {
          let ref_page = tx.page(node_cell.pgid);
          let page: &Page = &ref_page;
          tx.freelist_free_page(tx.api_id(), page);
          node_cell.pgid = ZERO_PGID;
        }
        node_cell.size()
      };

      // Allocate contiguous space for the node.
      let mut p = tx.allocate((node_size + tx.page_size() - 1) / tx.page_size())?;

      // Write the node.
      if p.id >= tx.meta().pgid() {
        panic!("pgid {} above high water mark {}", p.id, tx.meta().pgid())
      }
      let mut node_cell = node.cell.borrow_mut();

      node_cell.pgid = p.id;
      node_cell.write(&mut p);
      tx.queue_page(p);
      node_cell.is_spilled = true;

      // Insert into parent inodes.
      if let Some(parent) = node_cell.parent {
        let key: &'tx [u8] = {
          if node_cell.key.len() == 0 {
            node_cell.inodes[0].key()
          } else {
            node_cell.key()
          }
        };
        parent.put(key, node_cell.inodes[0].key(), &[], node_cell.pgid, 0);
        node_cell.key = node_cell.inodes[0].cod_key();
      }

      tx.mut_stats().spill += 1;
    }

    // If the root node split and created a new root then we need to spill that
    // as well. We'll clear out the children to make sure it doesn't try to respill.
    let cell = self.cell.borrow();
    if let Some(parent) = cell.parent {
      drop(cell);
      return parent.spill();
    }
    Ok(())
  }

  /// split breaks up a node into multiple smaller nodes, if appropriate.
  /// This should only be called from the spill() function.
  fn split(self, page_size: usize, bump: &'tx Bump) -> BVec<'tx, NodeRwCell<'tx>> {
    let mut nodes = BVec::new_in(bump);
    let mut node = self;
    loop {
      // Split node into two.
      let (a, ob) = node.split_two(page_size);
      nodes.push(a);
      match ob {
        // If we can't split then exit the loop.
        None => break,
        // Set node to b so it gets split on the next iteration.
        Some(b) => node = b,
      }
    }
    nodes
  }

  fn split_two(self, page_size: usize) -> (NodeRwCell<'tx>, Option<NodeRwCell<'tx>>) {
    let mut cell = self.cell.borrow_mut();
    // Ignore the split if the page doesn't have at least enough nodes for
    // two pages or if the nodes can fit in a single page.
    if cell.inodes.len() <= MIN_KEYS_PER_PAGE * 2 || cell.size_less_than(page_size) {
      return (self, None);
    }
    // Determine the threshold before starting a new node.
    let mut fill_percent = cell.bucket.split_ow().unwrap().fill_percent;
    fill_percent = fill_percent.max(MIN_FILL_PERCENT).min(MAX_FILL_PERCENT);
    let threshold = (page_size as f64 * fill_percent) as usize;

    // Determine split position and sizes of the two pages.
    let (split_index, _) = cell.split_index(threshold);

    // Split node into two separate nodes.
    // If there's no parent then we'll need to create one.
    let parent = {
      if let Some(parent) = cell.parent {
        parent
      } else {
        let parent = NodeRwCell::new_parent_in(cell.bucket);
        cell.parent = Some(parent);
        parent.cell.borrow_mut().children.push(self);
        parent
      }
    };

    // Create a new node and add it to the parent.
    let next = NodeRwCell::new_child_in(cell.bucket, cell.is_leaf, parent);
    parent.cell.borrow_mut().children.push(next);

    let mut next_borrow = next.cell.borrow_mut();
    // Split inodes across two nodes.
    next_borrow.inodes = cell.inodes.split_off(split_index);

    // Update the statistics
    cell.bucket.api_tx().mut_stats().split += 1;

    (self, Some(next))
  }

  /// rebalance attempts to combine the node with sibling nodes if the node fill
  /// size is below a threshold or if there are not enough keys.
  // TODO: Definitely needs optimizing
  pub(crate) fn rebalance(self: NodeRwCell<'tx>) {
    let mut self_borrow = self.cell.borrow_mut();
    // tracing
/*    println!(
      "trace~node.rebalance - page: {:?}, key: {:?}",
      self_borrow.pgid, self_borrow.key
    );*/
    let bucket = self_borrow.bucket;
    if !self_borrow.is_unbalanced {
      return;
    }
    self_borrow.is_unbalanced = false;
    let tx = self_borrow.bucket.api_tx();

    // Ignore if node is above threshold (25%) and has enough keys.
    let threshold = tx.page_size() / 4;
    if self_borrow.size() > threshold && self_borrow.inodes.len() > self_borrow.min_keys() {
      return;
    }

    // Root node has special handling.
    if self_borrow.parent.is_none() {
      // If root node is a branch and only has one node then collapse it.
      if !self_borrow.is_leaf && self_borrow.inodes.len() == 1 {
        // Move root's child up.
        let child = self_borrow
          .bucket
          .node(self_borrow.inodes.first().unwrap().pgid(), Some(self));
        let mut child_borrow = child.cell.borrow_mut();
        self_borrow.inodes.clear();
        mem::swap(&mut self_borrow.inodes, &mut child_borrow.inodes);
        self_borrow.children.clear();
        mem::swap(&mut self_borrow.children, &mut child_borrow.children);

        let (r, _, w) = self_borrow.bucket.split_ref_mut();
        let mut wb = w.unwrap();

        // Reparent all child nodes being moved.
        for inode in &self_borrow.inodes {
          if let Some(child) = wb.nodes.get_mut(&inode.pgid()) {
            child.cell.borrow_mut().parent = Some(self);
          }
        }

        // Remove old child.
        child_borrow.parent = None;
        wb.nodes.remove(&child_borrow.pgid);
        child.free()
      }
      return;
    }
    let parent = self_borrow.parent.unwrap();
    let mut parent_borrow = parent.cell.borrow_mut();

    // If node has no keys then just remove it.
    if self_borrow.inodes.is_empty() {
      parent_borrow.del(self_borrow.key());
      // drop self as we need to inspect self to remove child
      // TODO: rewrite remove child to do the equivalency a cheaper way
      drop(self_borrow);
      parent_borrow.remove_child(self);
      self.free();
      // drop parent_borrow, and bucket to rebalance the parent
      drop(parent_borrow);
      parent.rebalance();
      return;
    }

    assert!(
      parent_borrow.inodes.len() > 1,
      "parent must have at least 2 children"
    );
    drop(self_borrow);
    drop(parent_borrow);

    // Destination node is right sibling if idx == 0, otherwise left sibling.
    let use_next_sibling = parent.child_index(self) == 0;
    let target = if use_next_sibling {
      self.next_sibling().unwrap()
    } else {
      self.prev_sibling().unwrap()
    };
    let mut target_borrow = target.cell.borrow_mut();
    let (r, _, w) = bucket.split_ref_mut();
    let mut wb = w.unwrap();
    let mut self_borrow = self.cell.borrow_mut();

    // If both this node and the target node are too small then merge them.
    if use_next_sibling {
      // Reparent all child nodes being moved.
      for inode in &target_borrow.inodes {
        if let Some(child) = wb.nodes.get(&inode.pgid()).cloned() {
          let child_parent = child.cell.borrow().parent.unwrap();
          child_parent.cell.borrow_mut().remove_child(child);
          child.cell.borrow_mut().parent = Some(self);
          self_borrow.children.push(child);
        }
      }

      // Copy over inodes from target and remove target.
      self_borrow.inodes.append(&mut target_borrow.inodes);
      let parent = self_borrow.parent.unwrap();
      parent.del(target_borrow.key());
      let target_pgid = target_borrow.pgid;
      drop(target_borrow);
      parent.cell.borrow_mut().remove_child(target);
      wb.nodes.remove(&target_pgid);
      target.free();
    } else {
      // Reparent all child nodes being moved.
      for inode in &self_borrow.inodes {
        if let Some(child) = wb.nodes.get(&inode.pgid()).cloned() {
          let child_parent = child.cell.borrow().parent.unwrap();
          child_parent.cell.borrow_mut().remove_child(child);
          child.cell.borrow_mut().parent = Some(target);
          target_borrow.children.push(child);
        }
      }
      // Copy over inodes to target and remove node.
      target_borrow.inodes.append(&mut self_borrow.inodes);
      let parent = self_borrow.parent.unwrap();
      parent.del(self_borrow.key());
      let self_pgid = self_borrow.pgid;
      drop(self_borrow);
      parent.cell.borrow_mut().remove_child(self);
      wb.nodes.remove(&self_pgid);
      self.free();
    }

    // Either this node or the target node was deleted from the parent so rebalance it.
    parent.rebalance();
  }

  // Descending the tree shouldn't create runtime issues
  // We bend the rules here!
  /// own_in causes the node to copy all its inode key/value references to heap memory.
  /// This is required when the mmap is reallocated so inodes are not pointing to stale data.
  pub(crate) fn own_in(self: NodeRwCell<'tx>, bump: &'tx Bump) {
    let mut self_borrow = self.cell.borrow_mut();
    self_borrow.key.own_in(bump);
    for inode in &mut self_borrow.inodes {
      inode.own_in(bump);
    }

    // Recursively own_in children.
    for child in &self_borrow.children {
      child.own_in(bump);
    }

    // Update statistics.
    self_borrow.bucket.api_tx().mut_stats().node_deref += 1;
  }

  /// free adds the node's underlying page to the freelist.
  pub(crate) fn free(self: NodeRwCell<'tx>) {
    let (pgid, api_tx) = {
      let self_borrow = self.cell.borrow();
      if self_borrow.pgid == ZERO_PGID {
        return;
      }
      (self_borrow.pgid, self_borrow.bucket.api_tx())
    };
    let page = api_tx.page(pgid);
    let txid = api_tx.meta().txid();
    api_tx.freelist_free_page(txid, &page);
  }
}

#[cfg(test)]
mod test {
  use crate::bucket::{BucketRwCell, BucketRwIAPI};
  use crate::common::memory::CodSlice;
  use crate::common::page::LEAF_PAGE_FLAG;
  use crate::common::{BVec, SplitRef, ZERO_PGID};
  use crate::node::NodeW;
  use crate::test_support::{TestDb, Unseal};
  use crate::tx::TxIAPI;
  use crate::DbRwAPI;
  use bumpalo::Bump;
  use std::ops::DerefMut;

  #[test]
  fn test_node_put() -> crate::Result<()> {
    let mut test_db = TestDb::new()?;
    let tx = test_db.begin_mut();
    let txrw = tx.unseal();
    let root_bucket = txrw.root_bucket();
    let n = root_bucket.materialize_root();
    n.put(b"baz", b"baz", b"2", ZERO_PGID, 0);
    n.put(b"foo", b"foo", b"0", ZERO_PGID, 0);
    n.put(b"bar", b"bar", b"1", ZERO_PGID, 0);
    n.put(b"foo", b"foo", b"3", ZERO_PGID, LEAF_PAGE_FLAG as u32);

    assert_eq!(3, n.cell.borrow().inodes.len());
    let inode = &n.cell.borrow().inodes[0];
    assert_eq!(b"bar1".split_at(3), (inode.key(), inode.value()));
    let inode = &n.cell.borrow().inodes[1];
    assert_eq!(b"baz2".split_at(3), (inode.key(), inode.value()));
    let inode = &n.cell.borrow().inodes[2];
    assert_eq!(b"foo3".split_at(3), (inode.key(), inode.value()));
    assert_eq!(LEAF_PAGE_FLAG as u32, n.cell.borrow().inodes[2].flags());
    Ok(())
  }

  #[test]
  fn test_node_read_leaf_page() -> crate::Result<()> {
    let mut test_db = TestDb::new()?;
    let tx = test_db.begin_mut();
    let txrw = tx.unseal();
    let root_bucket = txrw.root_bucket();
    root_bucket.materialize_root();
    let n = root_bucket.split_ref().2.unwrap().root_node.unwrap();
    todo!()
  }

  #[test]
  fn test_node_write_leaf_page() -> crate::Result<()> {
    let mut test_db = TestDb::new()?;
    let tx = test_db.begin_mut();
    let txrw = tx.unseal();
    let root_bucket = txrw.root_bucket();
    root_bucket.materialize_root();
    let n = root_bucket.split_ref().2.unwrap().root_node.unwrap();
    todo!()
  }

  #[test]
  fn test_node_split() -> crate::Result<()> {
    let mut test_db = TestDb::new()?;
    let tx = test_db.begin_mut();
    let txrw = tx.unseal();
    let root_bucket = txrw.root_bucket();
    let n = root_bucket.materialize_root();
    n.put(b"00000001", b"00000001", b"0123456701234567", ZERO_PGID, 0);
    n.put(b"00000002", b"00000002", b"0123456701234567", ZERO_PGID, 0);
    n.put(b"00000003", b"00000003", b"0123456701234567", ZERO_PGID, 0);
    n.put(b"00000004", b"00000004", b"0123456701234567", ZERO_PGID, 0);
    n.put(b"00000005", b"00000005", b"0123456701234567", ZERO_PGID, 0);
    let split_nodes = n.split(100, txrw.bump());
    let binding = n.cell.borrow().parent.unwrap();
    let parent_children = &binding.cell.borrow().children;
    assert_eq!(2, parent_children.len());
    assert_eq!(2, parent_children[0].cell.borrow().inodes.len());
    assert_eq!(3, parent_children[1].cell.borrow().inodes.len());
    Ok(())
  }

  #[test]
  fn test_node_split_min_keys() -> crate::Result<()> {
    let mut test_db = TestDb::new()?;
    let tx = test_db.begin_mut();
    let txrw = tx.unseal();
    let root_bucket = txrw.root_bucket();
    let n = root_bucket.materialize_root();
    n.put(b"00000001", b"00000001", b"0123456701234567", ZERO_PGID, 0);
    n.put(b"00000002", b"00000002", b"0123456701234567", ZERO_PGID, 0);
    let split_nodes = n.split(20, txrw.bump());
    assert!(n.cell.borrow().parent.is_none(), "expected none parent");
    Ok(())
  }

  #[test]
  fn test_node_split_single_page() -> crate::Result<()> {
    let mut test_db = TestDb::new()?;
    let tx = test_db.begin_mut();
    let txrw = tx.unseal();
    let root_bucket = txrw.root_bucket();
    let n = root_bucket.materialize_root();
    n.put(b"00000001", b"00000001", b"0123456701234567", ZERO_PGID, 0);
    n.put(b"00000002", b"00000002", b"0123456701234567", ZERO_PGID, 0);
    n.put(b"00000003", b"00000003", b"0123456701234567", ZERO_PGID, 0);
    n.put(b"00000004", b"00000004", b"0123456701234567", ZERO_PGID, 0);
    n.put(b"00000005", b"00000005", b"0123456701234567", ZERO_PGID, 0);
    let split_nodes = n.split(4096, txrw.bump());
    assert!(n.cell.borrow().parent.is_none(), "expected none parent");
    Ok(())
  }
}
