use crate::bucket::{BucketCell, BucketIApi, BucketRwCell, BucketRwIApi};
use crate::common::page::{CoerciblePage, RefPage, BUCKET_LEAF_FLAG};
use crate::common::tree::{MappedBranchPage, MappedLeafPage, TreePage};
use crate::common::{BVec, PgId};
use crate::node::NodeRwCell;
use crate::tx::{TxCell, TxIApi, TxRwCell};
use crate::Error::IncompatibleValue;
use bumpalo::Bump;
use either::Either;
use std::marker::PhantomData;

/// Read-only Cursor API
pub trait CursorApi<'tx> {
  /// First moves the cursor to the first item in the bucket and returns its key and value.
  /// If the bucket is empty then None is returned.
  fn first(&mut self) -> Option<(&'tx [u8], Option<&'tx [u8]>)>;

  /// Last moves the cursor to the last item in the bucket and returns its key and value.
  /// If the bucket is empty then None is returned.
  fn last(&mut self) -> Option<(&'tx [u8], Option<&'tx [u8]>)>;

  /// Next moves the cursor to the next item in the bucket and returns its key and value.
  /// If the cursor is at the end of the bucket then None is returned.
  fn next(&mut self) -> Option<(&'tx [u8], Option<&'tx [u8]>)>;

  /// Prev moves the cursor to the previous item in the bucket and returns its key and value.
  /// If the cursor is at the beginning of the bucket then None is returned.
  fn prev(&mut self) -> Option<(&'tx [u8], Option<&'tx [u8]>)>;

  /// Seek moves the cursor to a given key using a b-tree search and returns it.
  /// If the key does not exist then the next key is used. If no keys
  /// follow, None is returned.
  fn seek<T: AsRef<[u8]>>(&mut self, seek: T) -> Option<(&'tx [u8], Option<&'tx [u8]>)>;
}

/// RW Bucket API
pub trait CursorRwApi<'tx>: CursorApi<'tx> {
  /// Delete removes the current key/value under the cursor from the bucket.
  fn delete(&mut self) -> crate::Result<()>;
}

pub enum CursorImpl<'tx> {
  R(InnerCursor<'tx, TxCell<'tx>, BucketCell<'tx>>),
  RW(InnerCursor<'tx, TxRwCell<'tx>, BucketRwCell<'tx>>),
}

impl<'tx> From<InnerCursor<'tx, TxCell<'tx>, BucketCell<'tx>>> for CursorImpl<'tx> {
  fn from(value: InnerCursor<'tx, TxCell<'tx>, BucketCell<'tx>>) -> Self {
    CursorImpl::R(value)
  }
}

impl<'tx> From<InnerCursor<'tx, TxRwCell<'tx>, BucketRwCell<'tx>>> for CursorImpl<'tx> {
  fn from(value: InnerCursor<'tx, TxRwCell<'tx>, BucketRwCell<'tx>>) -> Self {
    CursorImpl::RW(value)
  }
}

impl<'tx> CursorApi<'tx> for CursorImpl<'tx> {
  fn first(&mut self) -> Option<(&'tx [u8], Option<&'tx [u8]>)> {
    match self {
      CursorImpl::R(r) => r.api_first(),
      CursorImpl::RW(rw) => rw.api_first(),
    }
  }

  fn last(&mut self) -> Option<(&'tx [u8], Option<&'tx [u8]>)> {
    match self {
      CursorImpl::R(r) => r.api_last(),
      CursorImpl::RW(rw) => rw.api_last(),
    }
  }

  fn next(&mut self) -> Option<(&'tx [u8], Option<&'tx [u8]>)> {
    match self {
      CursorImpl::R(r) => r.api_next(),
      CursorImpl::RW(rw) => rw.api_next(),
    }
  }

  fn prev(&mut self) -> Option<(&'tx [u8], Option<&'tx [u8]>)> {
    match self {
      CursorImpl::R(r) => r.api_prev(),
      CursorImpl::RW(rw) => rw.api_prev(),
    }
  }

  fn seek<T: AsRef<[u8]>>(&mut self, seek: T) -> Option<(&'tx [u8], Option<&'tx [u8]>)> {
    match self {
      CursorImpl::R(r) => r.api_seek(seek.as_ref()),
      CursorImpl::RW(rw) => rw.api_seek(seek.as_ref()),
    }
  }
}

pub struct CursorRwImpl<'tx> {
  c: InnerCursor<'tx, TxRwCell<'tx>, BucketRwCell<'tx>>,
}

impl<'tx> CursorRwImpl<'tx> {
  pub(crate) fn new(c: InnerCursor<'tx, TxRwCell<'tx>, BucketRwCell<'tx>>) -> Self {
    CursorRwImpl { c }
  }
}

impl<'tx> From<InnerCursor<'tx, TxRwCell<'tx>, BucketRwCell<'tx>>> for CursorRwImpl<'tx> {
  fn from(value: InnerCursor<'tx, TxRwCell<'tx>, BucketRwCell<'tx>>) -> Self {
    CursorRwImpl::new(value)
  }
}

impl<'tx> CursorApi<'tx> for CursorRwImpl<'tx> {
  fn first(&mut self) -> Option<(&'tx [u8], Option<&'tx [u8]>)> {
    self.c.api_first()
  }

  fn last(&mut self) -> Option<(&'tx [u8], Option<&'tx [u8]>)> {
    self.c.api_last()
  }

  fn next(&mut self) -> Option<(&'tx [u8], Option<&'tx [u8]>)> {
    self.c.api_next()
  }

  fn prev(&mut self) -> Option<(&'tx [u8], Option<&'tx [u8]>)> {
    self.c.api_prev()
  }

  fn seek<T: AsRef<[u8]>>(&mut self, seek: T) -> Option<(&'tx [u8], Option<&'tx [u8]>)> {
    self.c.api_seek(seek.as_ref())
  }
}

impl<'tx> CursorRwApi<'tx> for CursorRwImpl<'tx> {
  fn delete(&mut self) -> crate::Result<()> {
    self.c.api_delete()
  }
}

pub(crate) trait CursorIApi<'tx>: Clone {
  /// See [CursorApi::first]
  fn api_first(&mut self) -> Option<(&'tx [u8], Option<&'tx [u8]>)>;

  fn i_first(&mut self) -> Option<(&'tx [u8], &'tx [u8], u32)>;

  /// See [CursorApi::next]
  fn api_next(&mut self) -> Option<(&'tx [u8], Option<&'tx [u8]>)>;

  /// i_next moves to the next leaf element and returns the key and value.
  /// If the cursor is at the last leaf element then it stays there and returns nil.
  fn i_next(&mut self) -> Option<(&'tx [u8], &'tx [u8], u32)>;

  /// See [CursorApi::prev]
  fn api_prev(&mut self) -> Option<(&'tx [u8], Option<&'tx [u8]>)>;

  /// i_prev moves the cursor to the previous item in the bucket and returns its key and value.
  /// If the cursor is at the beginning of the bucket then a nil key and value are returned.
  fn i_prev(&mut self) -> Option<(&'tx [u8], &'tx [u8], u32)>;

  /// See [CursorApi::last]
  fn api_last(&mut self) -> Option<(&'tx [u8], Option<&'tx [u8]>)>;

  /// i_last moves the cursor to the last leaf element under the last page in the stack.
  fn i_last(&mut self);

  /// key_value returns the key and value of the current leaf element.
  fn key_value(&self) -> Option<(&'tx [u8], &'tx [u8], u32)>;

  /// See [CursorApi::seek]
  fn api_seek(&mut self, seek: &[u8]) -> Option<(&'tx [u8], Option<&'tx [u8]>)>;

  /// i_seek moves the cursor to a given key and returns it.
  /// If the key does not exist then the next key is used.
  fn i_seek(&mut self, seek: &[u8]) -> Option<(&'tx [u8], &'tx [u8], u32)>;

  /// first moves the cursor to the first leaf element under the last page in the stack.
  fn go_to_first_element_on_the_stack(&mut self);

  /// search recursively performs a binary search against a given page/node until it finds a given key.
  fn search(&mut self, key: &[u8], pgid: PgId);

  fn search_inodes(&mut self, key: &[u8]);

  fn search_node(&mut self, key: &[u8], node: NodeRwCell<'tx>);

  fn search_page(&mut self, key: &[u8], page: &RefPage);
}

pub(crate) trait CursorRwIApi<'tx>: CursorIApi<'tx> {
  /// node returns the node that the cursor is currently positioned on.
  fn node(&mut self) -> NodeRwCell<'tx>;

  /// See [CursorRwApi::delete]
  fn api_delete(&mut self) -> crate::Result<()>;
}

#[derive(Clone)]
pub struct ElemRef<'tx> {
  pn: Either<RefPage<'tx>, NodeRwCell<'tx>>,
  index: i32,
}

impl<'tx> ElemRef<'tx> {
  /// count returns the number of inodes or page elements.
  fn count(&self) -> u32 {
    match &self.pn {
      Either::Left(r) => r.count as u32,
      Either::Right(n) => n.cell.borrow().inodes.len() as u32,
    }
  }

  /// is_leaf returns whether the ref is pointing at a leaf page/node.
  fn is_leaf(&self) -> bool {
    match &self.pn {
      Either::Left(r) => r.is_leaf(),
      Either::Right(n) => n.cell.borrow().is_leaf,
    }
  }
}

#[derive(Clone)]
pub(crate) struct InnerCursor<'tx, T: TxIApi<'tx>, B: BucketIApi<'tx, T>> {
  bucket: B,
  stack: BVec<'tx, ElemRef<'tx>>,
  phantom_t: PhantomData<T>,
}

impl<'tx, T: TxIApi<'tx>, B: BucketIApi<'tx, T>> InnerCursor<'tx, T, B> {
  pub(crate) fn new(cell: B, bump: &'tx Bump) -> Self {
    cell
      .tx()
      .split_r()
      .stats
      .as_ref()
      .unwrap()
      .inc_cursor_count(1);
    InnerCursor {
      bucket: cell,
      stack: BVec::with_capacity_in(0, bump),
      phantom_t: PhantomData,
    }
  }
}

impl<'tx, T: TxIApi<'tx>, B: BucketIApi<'tx, T>> CursorIApi<'tx> for InnerCursor<'tx, T, B> {
  fn api_first(&mut self) -> Option<(&'tx [u8], Option<&'tx [u8]>)> {
    let (k, v, flags) = self.i_first()?;
    if (flags & BUCKET_LEAF_FLAG) != 0 {
      return Some((k, None));
    }
    Some((k, Some(v)))
  }

  fn i_first(&mut self) -> Option<(&'tx [u8], &'tx [u8], u32)> {
    self.stack.clear();

    // TODO: Optimize this a bit for the internal API. BucketImpl::root_page_node?
    let root = self.bucket.root();
    let pn = self.bucket.page_node(root);
    self.stack.push(ElemRef { pn, index: 0 });

    self.go_to_first_element_on_the_stack();

    // If we land on an empty page then move to the next value.
    // https://github.com/boltdb/bolt/issues/450
    if self.stack.last().unwrap().count() == 0 {
      self.i_next();
    }

    let (k, v, flags) = self.key_value()?;
    if (flags & BUCKET_LEAF_FLAG) != 0 {
      return Some((k, &[], flags));
    }
    Some((k, v, flags))
  }

  fn api_next(&mut self) -> Option<(&'tx [u8], Option<&'tx [u8]>)> {
    let (k, v, flags) = self.i_next()?;
    if flags & BUCKET_LEAF_FLAG != 0 {
      Some((k, None))
    } else {
      Some((k, Some(v)))
    }
  }

  /// next moves to the next leaf element and returns the key and value.
  /// If the cursor is at the last leaf element then it stays there and returns nil.
  fn i_next(&mut self) -> Option<(&'tx [u8], &'tx [u8], u32)> {
    loop {
      // Attempt to move over one element until we're successful.
      // Move up the stack as we hit the end of each page in our stack.
      let mut stack_exhausted = true;
      let mut new_stack_depth = 0;
      for (depth, elem) in self.stack.iter_mut().enumerate().rev() {
        new_stack_depth = depth + 1;
        if elem.index < elem.count() as i32 - 1 {
          elem.index += 1;
          stack_exhausted = false;
          break;
        }
      }

      // If we've hit the root page then stop and return. This will leave the
      // cursor on the last element of the last page.
      if stack_exhausted {
        return None;
      }

      // Otherwise start from where we left off in the stack and find the
      // first element of the first leaf page.
      self.stack.truncate(new_stack_depth);
      self.go_to_first_element_on_the_stack();

      // If this is an empty page then restart and move back up the stack.
      // https://github.com/boltdb/bolt/issues/450
      if let Some(elem) = self.stack.last() {
        if elem.count() == 0 {
          continue;
        }
      }

      return self.key_value();
    }
  }

  fn api_prev(&mut self) -> Option<(&'tx [u8], Option<&'tx [u8]>)> {
    let (k, v, flags) = self.i_prev()?;
    if flags & BUCKET_LEAF_FLAG != 0 {
      Some((k, None))
    } else {
      Some((k, Some(v)))
    }
  }

  /// prev moves the cursor to the previous item in the bucket and returns its key and value.
  /// If the cursor is at the beginning of the bucket then a nil key and value are returned.
  fn i_prev(&mut self) -> Option<(&'tx [u8], &'tx [u8], u32)> {
    // Attempt to move back one element until we're successful.
    // Move up the stack as we hit the beginning of each page in our stack.
    let mut new_stack_depth = 0;
    let mut stack_exhausted = true;
    for (depth, elem) in self.stack.iter_mut().enumerate().rev() {
      new_stack_depth = depth + 1;
      if elem.index > 0 {
        elem.index -= 1;
        stack_exhausted = false;
        break;
      }
    }
    if stack_exhausted {
      self.stack.truncate(0);
    } else {
      self.stack.truncate(new_stack_depth);
    }

    // If we've hit the end then return None
    if self.stack.is_empty() {
      return None;
    }

    // Move down the stack to find the last element of the last leaf under this branch.
    self.i_last();

    self.key_value()
  }

  fn api_last(&mut self) -> Option<(&'tx [u8], Option<&'tx [u8]>)> {
    self.stack.truncate(0);
    let root = self.bucket.root();
    let pn = self.bucket.page_node(root);
    let mut elem_ref = ElemRef { pn, index: 0 };
    elem_ref.index = elem_ref.count() as i32 - 1;
    self.stack.push(elem_ref);
    self.i_last();

    while !self.stack.is_empty() && self.stack.last().unwrap().count() == 0 {
      self.i_prev();
    }

    if self.stack.is_empty() {
      return None;
    }

    let (k, v, flags) = self.key_value().unwrap();

    if flags & BUCKET_LEAF_FLAG != 0 {
      Some((k, None))
    } else {
      Some((k, Some(v)))
    }
  }

  /// last moves the cursor to the last leaf element under the last page in the stack.

  fn i_last(&mut self) {
    loop {
      // Exit when we hit a leaf page.
      if let Some(elem) = self.stack.last() {
        if elem.is_leaf() {
          break;
        }

        // Keep adding pages pointing to the last element in the stack.
        let pgid = match &elem.pn {
          Either::Left(page) => {
            let branch_page = MappedBranchPage::coerce_ref(page).unwrap();
            branch_page.get_elem(elem.index as u16).unwrap().pgid()
          }
          Either::Right(node) => node.cell.borrow().inodes[elem.index as usize].pgid(),
        };

        let pn = self.bucket.page_node(pgid);
        let mut next_elem = ElemRef { pn, index: 0 };
        next_elem.index = next_elem.count() as i32 - 1;
        self.stack.push(next_elem);
      }
    }
  }

  fn key_value(&self) -> Option<(&'tx [u8], &'tx [u8], u32)> {
    let elem_ref = self.stack.last().unwrap();
    let pn_count = elem_ref.count();

    // If the cursor is pointing to the end of page/node then return nil.
    if pn_count == 0 || elem_ref.index as u32 > pn_count {
      return None;
    }

    match &elem_ref.pn {
      // Retrieve value from page.
      Either::Left(r) => {
        let l = MappedLeafPage::coerce_ref(r).unwrap();
        l.get_elem(elem_ref.index as u16)
          .map(|inode| (inode.key(), inode.value(), inode.flags()))
      }
      // Retrieve value from node.
      Either::Right(n) => {
        let ref_node = n.cell.borrow();
        ref_node
          .inodes
          .get(elem_ref.index as usize)
          .map(|inode| (inode.key(), inode.value(), inode.flags()))
      }
    }
  }

  fn api_seek(&mut self, seek: &[u8]) -> Option<(&'tx [u8], Option<&'tx [u8]>)> {
    let mut vals = self.i_seek(seek);

    if let Some(elem_ref) = self.stack.last() {
      if elem_ref.index >= elem_ref.count() as i32 {
        vals = self.i_next();
      }
    }

    let (k, v, flags) = vals?;
    if flags & BUCKET_LEAF_FLAG != 0 {
      Some((k, None))
    } else {
      Some((k, Some(v)))
    }
  }

  fn i_seek(&mut self, seek: &[u8]) -> Option<(&'tx [u8], &'tx [u8], u32)> {
    self.stack.truncate(0);
    let root = self.bucket.root();
    self.search(seek, root);

    self.key_value()
  }

  /// first moves the cursor to the first leaf element under the last page in the stack.
  fn go_to_first_element_on_the_stack(&mut self) {
    loop {
      let _slice = self.stack.as_slice();
      let pgid = {
        // Exit when we hit a leaf page.
        let r = self.stack.last().unwrap();
        if r.is_leaf() {
          break;
        }

        // Keep adding pages pointing to the first element to the stack.
        match r.pn {
          Either::Left(page) => {
            let branch_page = MappedBranchPage::coerce_ref(&page).unwrap();
            let elem = branch_page.get_elem(r.index as u16).unwrap();
            elem.pgid()
          }
          Either::Right(node) => {
            let node_borrow = node.cell.borrow();
            node_borrow.inodes[r.index as usize].pgid()
          }
        }
      };
      let pn = self.bucket.page_node(pgid);
      self.stack.push(ElemRef { pn, index: 0 })
    }
  }

  /// search recursively performs a binary search against a given page/node until it finds a given key.
  fn search(&mut self, key: &[u8], pgid: PgId) {
    let pn = self.bucket.page_node(pgid);

    if let Either::Left(page) = &pn {
      if !page.is_leaf() && !page.is_branch() {
        panic!("invalid page type: {}, {:X}", page.id, page.flags);
      }
    }

    let elem = ElemRef { pn, index: 0 };

    // If we're on a leaf page/node then find the specific node.
    let elem_is_leaf = elem.is_leaf();

    self.stack.push(elem);

    if elem_is_leaf {
      self.search_inodes(key);
      return;
    }

    match &pn {
      Either::Left(page) => self.search_page(key, page),
      Either::Right(node) => self.search_node(key, *node),
    }
  }

  /// search_inodes searches the leaf node on the top of the stack for a key.
  fn search_inodes(&mut self, key: &[u8]) {
    if let Some(elem) = self.stack.last_mut() {
      let index = match &elem.pn {
        // If we have a page then search its leaf elements.
        Either::Left(page) => {
          let leaf_page = MappedLeafPage::coerce_ref(page).unwrap();
          leaf_page
            .elements()
            .partition_point(|elem| unsafe { elem.key(leaf_page.page_ptr().cast_const()) } < key)
        }
        // If we have a node then search its inodes.
        Either::Right(node) => node
          .cell
          .borrow()
          .inodes
          .partition_point(|inode| inode.key() < key),
      };
      elem.index = index as i32;
    }
  }

  fn search_node(&mut self, key: &[u8], node: NodeRwCell<'tx>) {
    let (index, pgid) = {
      let w = node.cell.borrow();

      let r = w.inodes.binary_search_by_key(&key, |inode| inode.key());
      let index = r.unwrap_or_else(|index| if index > 0 { index - 1 } else { index });
      (index as u32, w.inodes[index].pgid())
    };

    if let Some(elem) = self.stack.last_mut() {
      elem.index = index as i32;
    }

    // Recursively search to the next page.
    self.search(key, pgid)
  }

  fn search_page(&mut self, key: &[u8], page: &RefPage) {
    let branch_page = MappedBranchPage::coerce_ref(page).unwrap();
    let elements = branch_page.elements();
    debug_assert_ne!(0, elements.len());
    let r = branch_page
      .elements()
      .binary_search_by_key(&key, |elem| unsafe {
        elem.key(branch_page.page_ptr().cast_const())
      });
    let index = r.unwrap_or_else(|index| if index > 0 { index - 1 } else { index });

    if let Some(elem) = self.stack.last_mut() {
      elem.index = index as i32;
    }
    let pgid = branch_page.elements()[index].pgid();

    // Recursively search to the next page.
    self.search(key, pgid)
  }
}

impl<'tx, B: BucketRwIApi<'tx>> CursorRwIApi<'tx> for InnerCursor<'tx, TxRwCell<'tx>, B> {
  fn node(&mut self) -> NodeRwCell<'tx> {
    assert!(
      !self.stack.is_empty(),
      "accessing a node with a zero-length cursor stack"
    );

    // If the top of the stack is a leaf node then just return it.
    if let Some(elem_ref) = self.stack.last() {
      if let Either::Right(node) = elem_ref.pn {
        if node.cell.borrow().is_leaf {
          return node;
        }
      }
    }

    // Start from root and traverse down the hierarchy.
    let mut n = {
      match &self.stack.first().unwrap().pn {
        Either::Left(page) => self.bucket.node(page.id, None),
        Either::Right(node) => *node,
      }
    };
    let _stack = &self.stack[0..self.stack.len() - 1];
    for elem in &self.stack[0..self.stack.len() - 1] {
      assert!(!n.cell.borrow().is_leaf, "expected branch node");
      n = n.child_at(elem.index as u32);
    }
    assert!(n.cell.borrow().is_leaf, "expected leaf node");
    n
  }

  fn api_delete(&mut self) -> crate::Result<()> {
    let (k, _, flags) = self.key_value().unwrap();
    if flags & BUCKET_LEAF_FLAG != 0 {
      return Err(IncompatibleValue);
    }
    self.node().del(k);
    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use crate::test_support::TestDb;
  use crate::{
    BucketApi, BucketRwApi, CursorApi, CursorRwApi, DbApi, DbRwAPI, Error, TxApi, TxRwRefApi,
  };

  /// Ensure that a Tx cursor can seek to the appropriate keys.
  #[test]
  fn test_cursor_seek() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let mut b = tx.create_bucket(b"widgets")?;
      b.put(b"foo", b"0001")?;
      b.put(b"bar", b"0002")?;
      b.put(b"baz", b"0003")?;
      let _ = b.create_bucket(b"bkt")?;
      Ok(())
    })?;
    db.view(|tx| {
      let b = tx.bucket(b"widgets").unwrap();
      let mut c = b.cursor();
      // Exact match should go to the key.
      assert_eq!(
        (b"bar".as_slice(), Some(b"0002".as_slice())),
        c.seek(b"bar").unwrap()
      );
      // Inexact match should go to the next key.
      assert_eq!(
        (b"baz".as_slice(), Some(b"0003".as_slice())),
        c.seek(b"bas").unwrap()
      );
      // Low key should go to the first key.
      assert_eq!(
        (b"bar".as_slice(), Some(b"0002".as_slice())),
        c.seek(b"").unwrap()
      );
      // High key should return no key.
      assert_eq!(None, c.seek(b"zzz"));
      // Buckets should return their key but no value.
      assert_eq!((b"bkt".as_slice(), None), c.seek(b"bkt").unwrap());
      Ok(())
    })
  }

  #[test]
  #[cfg(not(miri))]
  fn test_cursor_delete() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    let count = 1000u64;
    let value = [0u8; 100];
    db.update(|mut tx| {
      let mut b = tx.create_bucket(b"widgets")?;
      for i in 0..count {
        let be_i = i.to_be_bytes();
        b.put(be_i, value)?;
      }
      let _ = b.create_bucket(b"sub")?;
      Ok(())
    })?;
    db.must_check();
    db.update(|mut tx| {
      let b = tx.bucket_mut(b"widgets").unwrap();
      let mut c = b.cursor_mut();
      let bound = (count / 2).to_be_bytes();
      let (mut key, _) = c.first().unwrap();
      while key < bound.as_slice() {
        c.delete()?;
        key = c.next().unwrap().0;
      }
      c.seek(b"sub");
      assert_eq!(Err(Error::IncompatibleValue), c.delete());
      Ok(())
    })?;
    db.must_check();
    db.view(|tx| {
      let b = tx.bucket(b"widgets").unwrap();
      let stats = b.stats();
      assert_eq!((count / 2) + 1, stats.key_n as u64);
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  #[cfg(not(miri))]
  fn test_cursor_seek_large() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    let count = 1000u64;
    let value = [0u8; 100];
    db.update(|mut tx| {
      let mut b = tx.create_bucket(b"widgets")?;
      for i in (0..count).step_by(100) {
        for j in (i..i + 100).step_by(2) {
          let k = j.to_be_bytes();
          b.put(k, value)?;
        }
      }
      Ok(())
    })?;
    db.view(|tx| {
      let b = tx.bucket(b"widgets").unwrap();
      let mut c = b.cursor();
      for i in 0..count {
        let seek = i.to_be_bytes();
        let sought = c.seek(seek);

        if i == count - 1 {
          assert!(sought.is_none(), "expected None");
          continue;
        }
        let k = sought.unwrap().0;
        let num = u64::from_be_bytes(k.try_into().unwrap());
        if i % 2 == 0 {
          assert_eq!(num, i, "unexpected num: {}", num)
        } else {
          assert_eq!(num, i + 1, "unexpected num: {}", num)
        }
      }
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  fn test_cursor_empty_bucket() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let _ = tx.create_bucket(b"widgets")?;
      Ok(())
    })?;
    db.view(|tx| {
      let b = tx.bucket(b"widgets").unwrap();
      let mut c = b.cursor();
      let kv = c.first();
      assert_eq!(None, kv, "unexpected kv: {:?}", kv);
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  fn test_cursor_empty_bucket_reverse() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let _ = tx.create_bucket(b"widgets")?;
      Ok(())
    })?;
    db.view(|tx| {
      let b = tx.bucket(b"widgets").unwrap();
      let mut c = b.cursor();
      let kv = c.last();
      assert_eq!(None, kv, "unexpected kv: {:?}", kv);
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  fn test_cursor_iterate_leaf() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let mut b = tx.create_bucket(b"widgets")?;
      b.put(b"baz", [])?;
      b.put(b"foo", [0])?;
      b.put(b"bar", [1])?;
      Ok(())
    })?;
    let tx = db.begin()?;
    {
      let b = tx.bucket(b"widgets").unwrap();
      let mut c = b.cursor();
      assert_eq!(Some((b"bar".as_slice(), Some([1].as_slice()))), c.first());
      assert_eq!(Some((b"baz".as_slice(), Some([].as_slice()))), c.next());
      assert_eq!(Some((b"foo".as_slice(), Some([0].as_slice()))), c.next());
      assert_eq!(None, c.next());
      assert_eq!(None, c.next());
    }
    tx.rollback()
  }

  #[test]
  fn test_cursor_leaf_root_reverse() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let mut b = tx.create_bucket(b"widgets")?;
      b.put(b"baz", [])?;
      b.put(b"foo", [0])?;
      b.put(b"bar", [1])?;
      Ok(())
    })?;
    let tx = db.begin()?;
    {
      let b = tx.bucket(b"widgets").unwrap();
      let mut c = b.cursor();
      assert_eq!(Some((b"foo".as_slice(), Some([0].as_slice()))), c.last());
      assert_eq!(Some((b"baz".as_slice(), Some([].as_slice()))), c.prev());
      assert_eq!(Some((b"bar".as_slice(), Some([1].as_slice()))), c.prev());
      assert_eq!(None, c.prev());
      assert_eq!(None, c.prev());
    }
    tx.rollback()
  }

  #[test]
  fn test_cursor_restart() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let mut b = tx.create_bucket(b"widgets")?;
      b.put("foo", [])?;
      b.put("bar", [])?;
      Ok(())
    })?;
    let tx = db.begin()?;
    {
      let b = tx.bucket(b"widgets").unwrap();
      let mut c = b.cursor();
      assert_eq!(Some((b"bar".as_slice(), Some([].as_slice()))), c.first());
      assert_eq!(Some((b"foo".as_slice(), Some([].as_slice()))), c.next());
      assert_eq!(Some((b"bar".as_slice(), Some([].as_slice()))), c.first());
      assert_eq!(Some((b"foo".as_slice(), Some([].as_slice()))), c.next());
    }
    tx.rollback()
  }

  #[test]
  fn test_cursor_first_empty_pages() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let mut b = tx.create_bucket(b"widgets")?;
      for i in 1..1000u64 {
        b.put(bytemuck::bytes_of(&i), [])?;
      }
      Ok(())
    })?;
    db.update(|mut tx| {
      let mut b = tx.bucket_mut(b"widgets").unwrap();
      for i in 1..600u64 {
        b.delete(bytemuck::bytes_of(&i))?;
      }
      let mut c = b.cursor();
      let mut kv = c.first();
      let mut n = 0;
      while kv.is_some() {
        n += 1;
        kv = c.next();
      }
      assert_eq!(400, n, "unexpected key count");
      Ok(())
    })
  }

  #[test]
  fn test_cursor_last_empty_pages() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let mut b = tx.create_bucket(b"widgets")?;
      for i in 0..1000u64 {
        b.put(bytemuck::bytes_of(&i), [])?;
      }
      Ok(())
    })?;
    db.update(|mut tx| {
      let mut b = tx.bucket_mut(b"widgets").unwrap();
      for i in 200..1000u64 {
        b.delete(bytemuck::bytes_of(&i))?;
      }
      let mut c = b.cursor();
      let mut kv = c.last();
      let mut n = 0;
      while kv.is_some() {
        n += 1;
        kv = c.prev();
      }
      assert_eq!(200, n, "unexpected key count");
      Ok(())
    })
  }

  #[test]
  #[ignore]
  fn test_cursor_quick_check() {
    todo!()
  }

  #[test]
  #[ignore]
  fn test_cursor_quick_check_reverse() {
    todo!()
  }

  #[test]
  #[ignore]
  fn test_cursor_quick_check_buckets_only() {
    todo!()
  }

  #[test]
  #[ignore]
  fn test_cursor_quick_check_buckets_only_reverse() {
    todo!()
  }

  #[test]
  #[ignore]
  fn example_cursor() {
    todo!()
  }

  #[test]
  #[ignore]
  fn example_cursor_reverse() {
    todo!()
  }
}
