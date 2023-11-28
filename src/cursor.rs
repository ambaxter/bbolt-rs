use crate::bucket::{Bucket, BucketAPI, BucketIAPI, BucketIRef, BucketImpl, BucketMut, BucketR};
use crate::common::memory::SCell;
use crate::common::page::{CoerciblePage, RefPage, BUCKET_LEAF_FLAG};
use crate::common::tree::{MappedBranchPage, MappedLeafPage, TreePage};
use crate::common::{BVec, IRef};
use crate::node::{NodeIRef, NodeR};
use crate::tx::{Tx, TxAPI, TxMut};
use either::Either;
use std::io;
use std::marker::PhantomData;

pub trait CursorAPI<'tx>: Copy + Clone {
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

pub struct ElemRef<'tx, N: NodeIRef<'tx>> {
  pn: Either<RefPage<'tx>, N>,
  index: u32,
}

impl<'tx, N: NodeIRef<'tx>> ElemRef<'tx, N> {
  fn count(&self) -> u32 {
    match &self.pn {
      Either::Left(r) => r.count as u32,
      Either::Right(n) => n.borrow_iref().0.inodes.len() as u32,
    }
  }

  fn is_leaf(&self) -> bool {
    match &self.pn {
      Either::Left(r) => r.is_leaf(),
      Either::Right(n) => n.borrow_iref().0.is_leaf,
    }
  }
}

pub struct NCursor<'tx, B: BucketIRef<'tx>> {
  bucket: B,
  stack: BVec<'tx, ElemRef<'tx, <<B as BucketIAPI<'tx>>::BucketType as BucketIAPI<'tx>>::NodeType>>,
}

impl<'tx, B: BucketIRef<'tx>> NCursor<'tx, B> {
  fn first(&mut self) -> Option<(&'tx [u8], Option<&'tx [u8]>)> {
    let (k, v, flags) = self._first()?;
    if (flags & BUCKET_LEAF_FLAG) != 0 {
      return Some((k, None));
    }
    Some((k, v))
  }
  fn _first(&mut self) -> Option<(&'tx [u8], Option<&'tx [u8]>, u32)> {
    self.stack.clear();

    let root = BucketImpl::root(self.bucket);
    let pn = BucketImpl::page_node(self.bucket, root);
    self.stack.push(ElemRef { pn, index: 0 });

    self.go_to_first_element_on_the_stack();

    // If we land on an empty page then move to the next value.
    // https://github.com/boltdb/bolt/issues/450
    if self.stack.last()?.count() == 0 {
      self.next();
    }

    let (k, v, flags) = self.key_value()?;
    if (flags & BUCKET_LEAF_FLAG) != 0 {
      return Some((k, None, flags));
    }
    Some((k, Some(v), flags))
  }

  fn next(&mut self) {}

  fn key_value(&self) -> Option<(&'tx [u8], &'tx [u8], u32)> {
    let elem_ref = self.stack.last()?;
    let pn_count = elem_ref.count();
    if pn_count == 0 || elem_ref.index > pn_count {
      return None;
    }

    match &elem_ref.pn {
      Either::Left(r) => {
        let l = MappedLeafPage::coerce_ref(r).unwrap();
        let inode = l.get_elem(elem_ref.index as u16)?;
        Some((inode.key(), inode.value(), inode.flags()))
      }
      Either::Right(n) => {
        let ref_node = n.borrow_iref();
        let inode = ref_node.0.inodes.get(elem_ref.index as usize)?;
        Some((inode.key(), inode.value(), inode.flags()))
      }
    }
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
            let node_borrow = node.borrow_iref();
            node_borrow.0.inodes.get(r.index as usize).unwrap().pgid()
          }
        }
      };
      let pn = BucketImpl::page_node(self.bucket, pgid);
      self.stack.push(ElemRef { pn, index: 0 })
    }
  }
}

pub struct NCursorMut<'tx> {
  n: NCursor<'tx, BucketMut<'tx>>,
}

#[derive(Copy, Clone)]
pub struct Cursor<'tx> {
  cell: SCell<'tx, NCursor<'tx, Bucket<'tx>>>,
}

impl<'tx> CursorAPI<'tx> for Cursor<'tx> {
  type BucketType = Bucket<'tx>;

  /// Bucket returns the bucket that this cursor was created from.
  fn bucket(&self) -> Self::BucketType {
    self.cell.borrow().bucket
  }

  /// First moves the cursor to the first item in the bucket and returns its key and value.
  /// If the bucket is empty then a nil key and value are returned.
  /// The returned key and value are only valid for the life of the transaction.
  fn first(&mut self) -> Option<(&'tx [u8], Option<&'tx [u8]>)> {
    self.cell.borrow_mut().first()
  }

  fn last(&mut self) -> Option<(&'tx [u8], Option<&'tx [u8]>)> {
    todo!()
  }

  fn next(&mut self) -> Option<(&'tx [u8], Option<&'tx [u8]>)> {
    todo!()
  }

  fn prev(&mut self) -> Option<(&'tx [u8], Option<&'tx [u8]>)> {
    todo!()
  }

  fn seek(&mut self, seek: &[u8]) -> Option<(&'tx [u8], Option<&'tx [u8]>)> {
    todo!()
  }
}

#[derive(Copy, Clone)]
pub struct CursorMut<'tx> {
  cell: SCell<'tx, NCursorMut<'tx>>,
}
