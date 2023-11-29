use crate::bucket::{Bucket, BucketAPI, BucketIAPI, BucketIRef, BucketImpl, BucketMut, BucketR};
use crate::common::memory::SCell;
use crate::common::page::{CoerciblePage, RefPage, BUCKET_LEAF_FLAG};
use crate::common::tree::{MappedBranchPage, MappedLeafPage, TreePage};
use crate::common::{BVec, IRef, PgId};
use crate::node::NodeMut;
use crate::tx::{Tx, TxAPI, TxMut};
use either::Either;
use std::io;
use std::marker::PhantomData;

pub trait CursorAPI<'tx>: Clone {
  type BucketType: BucketAPI<'tx>;

  fn bucket(&self) -> Self::BucketType;

  fn first(&mut self) -> Option<(&'tx [u8], Option<&'tx [u8]>)>;

  fn last(&mut self) -> Option<(&'tx [u8], Option<&'tx [u8]>)>;

  fn next(&mut self) -> Option<(&'tx [u8], Option<&'tx [u8]>)>;

  fn prev(&mut self) -> Option<(&'tx [u8], Option<&'tx [u8]>)>;

  fn seek(&mut self, seek: &[u8]) -> Option<(&'tx [u8], Option<&'tx [u8]>)>;
}

pub trait CursorMutAPI<'tx>: CursorAPI<'tx> {
  fn delete(&mut self) -> io::Result<()>;
}

#[derive(Clone)]
pub struct ElemRef<'tx> {
  pn: Either<RefPage<'tx>, NodeMut<'tx>>,
  index: u32,
}

impl<'tx> ElemRef<'tx> {
  fn count(&self) -> u32 {
    match &self.pn {
      Either::Left(r) => r.count as u32,
      Either::Right(n) => n.cell.borrow().inodes.len() as u32,
    }
  }

  fn is_leaf(&self) -> bool {
    match &self.pn {
      Either::Left(r) => r.is_leaf(),
      Either::Right(n) => n.cell.borrow().is_leaf,
    }
  }
}

#[derive(Clone)]
pub struct ICursor<'tx, B: BucketIRef<'tx>> {
  bucket: B,
  stack: BVec<'tx, ElemRef<'tx>>,
}

impl<'tx, B: BucketIRef<'tx>> ICursor<'tx, B> {
  fn api_first(&mut self) -> Option<(&'tx [u8], Option<&'tx [u8]>)> {
    let (k, v, flags) = self.first()?;
    if (flags & BUCKET_LEAF_FLAG) != 0 {
      return Some((k, None));
    }
    Some((k, Some(v)))
  }

  fn first(&mut self) -> Option<(&'tx [u8], &'tx [u8], u32)> {
    self.stack.clear();

    let root = BucketImpl::root(self.bucket);
    let pn = BucketImpl::page_node(self.bucket, root);
    self.stack.push(ElemRef { pn, index: 0 });

    self.go_to_first_element_on_the_stack();

    // If we land on an empty page then move to the next value.
    // https://github.com/boltdb/bolt/issues/450
    if self.stack.last().unwrap().count() == 0 {
      self.next();
    }

    let (k, v, flags) = self.key_value()?;
    if (flags & BUCKET_LEAF_FLAG) != 0 {
      return Some((k, &[], flags));
    }
    Some((k, v, flags))
  }

  fn api_next(&mut self) -> Option<(&'tx [u8], Option<&'tx [u8]>)> {
    let (k, v, flags) = self.next()?;
    if flags & BUCKET_LEAF_FLAG != 0 {
      Some((k, None))
    } else {
      Some((k, Some(v)))
    }
  }

  /// next moves to the next leaf element and returns the key and value.
  /// If the cursor is at the last leaf element then it stays there and returns nil.
  fn next(&mut self) -> Option<(&'tx [u8], &'tx [u8], u32)> {
    loop {
      // Attempt to move over one element until we're successful.
      // Move up the stack as we hit the end of each page in our stack.
      let mut stack_exhausted = true;
      let mut new_stack_depth = 0;
      for (depth, elem) in self.stack.iter_mut().enumerate().rev() {
        new_stack_depth = depth;
        if elem.index < elem.count() - 1 {
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
    let (k, v, flags) = self.prev()?;
    if flags & BUCKET_LEAF_FLAG != 0 {
      Some((k, None))
    } else {
      Some((k, Some(v)))
    }
  }

  /// prev moves the cursor to the previous item in the bucket and returns its key and value.
  /// If the cursor is at the beginning of the bucket then a nil key and value are returned.
  fn prev(&mut self) -> Option<(&'tx [u8], &'tx [u8], u32)> {
    // Attempt to move back one element until we're successful.
    // Move up the stack as we hit the beginning of each page in our stack.
    let mut new_stack_depth = 0;
    for (depth, elem) in self.stack.iter_mut().enumerate().rev() {
      new_stack_depth = depth;
      if elem.index > 0 {
        elem.index -= 1;
      }
    }
    self.stack.truncate(new_stack_depth);

    // If we've hit the end then return None
    if self.stack.is_empty() {
      return None;
    }

    // Move down the stack to find the last element of the last leaf under this branch.
    self.last();

    self.key_value()
  }

  fn api_last(&mut self) -> Option<(&'tx [u8], Option<&'tx [u8]>)> {
    self.stack.truncate(0);
    let root = BucketImpl::root(self.bucket);
    let pn = BucketImpl::page_node(self.bucket, root);
    let mut elem_ref = ElemRef{ pn, index: 0};
    elem_ref.index = elem_ref.count() - 1;
    self.stack.push(elem_ref);
    self.last();


    if let Some( _ ) = self.stack.last() {
      self.prev();
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

  fn last(&mut self) {
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

        let pn = BucketImpl::page_node(self.bucket, pgid);
        let mut next_elem = ElemRef { pn, index: 0 };
        next_elem.index = next_elem.count();
        self.stack.push(next_elem);
      }
    }
  }

  fn key_value(&self) -> Option<(&'tx [u8], &'tx [u8], u32)> {
    let elem_ref = self.stack.last().unwrap();
    let pn_count = elem_ref.count();
    if pn_count == 0 || elem_ref.index > pn_count {
      return None;
    }

    match &elem_ref.pn {
      Either::Left(r) => {
        let l = MappedLeafPage::coerce_ref(r).unwrap();
        let inode = l.get_elem(elem_ref.index as u16).unwrap();
        Some((inode.key(), inode.value(), inode.flags()))
      }
      Either::Right(n) => {
        let ref_node = n.cell.borrow();
        let inode = &ref_node.inodes[elem_ref.index as usize];
        Some((inode.key(), inode.value(), inode.flags()))
      }
    }
  }

  fn api_seek(&mut self, seek: &[u8]) -> Option<(&'tx [u8], Option<&'tx [u8]>)> {

    let mut vals = self.seek(seek);

    if let Some( elem_ref ) = self.stack.last() {
      if elem_ref.index >= elem_ref.count() {
        vals = self.next();
      }
    }

    let (k, v, flags) = vals?;
    if flags & BUCKET_LEAF_FLAG != 0 {
      Some((k, None))
    } else {
      Some((k, Some(v)))
    }
  }

  fn seek(&mut self, seek: &[u8]) -> Option<(&'tx [u8], &'tx [u8], u32)> {
    self.stack.truncate(0);
    let root = BucketImpl::root(self.bucket);
    self.search(seek, root);

    self.key_value()
  }

  /// first moves the cursor to the first leaf element under the last page in the stack.
  fn go_to_first_element_on_the_stack(&mut self) {
    loop {
      let pgid = {
        // Exit when we hit a leaf page.
        let r = self.stack.last().unwrap();
        if r.is_leaf() {
          break;
        }

        match r.pn {
          Either::Left(page) => {
            let branch_page = MappedBranchPage::coerce_ref(&page).unwrap();
            branch_page.get_elem(r.index as u16).unwrap().pgid()
          }
          Either::Right(node) => {
            let node_borrow = node.cell.borrow();
            node_borrow.inodes[r.index as usize].pgid()
          }
        }
      };
      let pn = BucketImpl::page_node(self.bucket, pgid);
      self.stack.push(ElemRef { pn, index: 0 })
    }
  }

  /// search recursively performs a binary search against a given page/node until it finds a given key.
  fn search(&mut self, key: &[u8], pgid: PgId) {
    let pn = BucketImpl::page_node(self.bucket, pgid);

    if let Either::Left(page) = &pn {
      if !page.is_leaf() && !page.is_branch() {
        panic!("invalid page type: {}, {:X}", page.id, page.flags);
      }
    }

    let elem = ElemRef { pn, index: 0 };
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
        Either::Left(page) => {
          let leaf_page = MappedLeafPage::coerce_ref(page).unwrap();
          leaf_page
            .elements()
            .partition_point(|elem| elem.as_ref().key() < key)
        }
        Either::Right(node) => node
          .cell
          .borrow()
          .inodes
          .partition_point(|inode| inode.key() < key),
      };
      elem.index = index as u32;
    }
  }

  fn search_node(&mut self, key: &[u8], node: NodeMut<'tx>) {
    let (index, pgid) = {
      let w = node.cell.borrow();
      let r = w.inodes.binary_search_by_key(&key, |inode| inode.key());
      let index = match r {
        Ok(index) => index,
        Err(index) => {
          if index > 0 {
            index - 1
          } else {
            index
          }
        }
      };
      (index as u32, w.inodes[index].pgid())
    };

    if let Some(elem) = self.stack.last_mut() {
      elem.index = index;
    }

    // Recursively search to the next page.
    self.search(key, pgid)
  }

  fn search_page(&mut self, key: &[u8], page: &RefPage) {
    let branch_page = MappedBranchPage::coerce_ref(page).unwrap();
    let r = branch_page
      .elements()
      .binary_search_by_key(&key, |elem| elem.as_ref().key());
    let index = match r {
      Ok(index) => index,
      Err(index) => {
        if index > 0 {
          index - 1
        } else {
          index
        }
      }
    };

    if let Some(elem) = self.stack.last_mut() {
      elem.index = index as u32;
    }
    let pgid = branch_page.elements()[index].pgid();

    // Recursively search to the next page.
    self.search(key, pgid)
  }
}



#[derive(Clone)]
pub struct Cursor<'tx> {
  c: ICursor<'tx, Bucket<'tx>>,
}

impl<'tx> Cursor<'tx> {
}

#[derive(Clone)]
pub struct CursorMut<'tx> {
  c: ICursor<'tx, BucketMut<'tx>>,
}

impl<'tx> CursorAPI<'tx> for Cursor<'tx> {
  type BucketType = Bucket<'tx>;

  /// Bucket returns the bucket that this cursor was created from.
  fn bucket(&self) -> Self::BucketType {
    self.c.bucket
  }

  /// First moves the cursor to the first item in the bucket and returns its key and value.
  /// If the bucket is empty then a nil key and value are returned.
  /// The returned key and value are only valid for the life of the transaction.
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

  fn seek(&mut self, seek: &[u8]) -> Option<(&'tx [u8], Option<&'tx [u8]>)> {
    self.c.api_seek(seek)
  }
}
