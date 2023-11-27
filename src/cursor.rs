use crate::bucket::{Bucket, BucketAPI, BucketMut, BucketR};
use crate::common::memory::SCell;
use crate::common::page::{CoerciblePage, RefPage, BUCKET_LEAF_FLAG};
use crate::common::tree::{MappedLeafPage, TreePage};
use crate::common::{BVec, IRef};
use crate::node::NodeR;
use crate::tx::{Tx, TxAPI, TxMut};
use either::Either;
use std::io;
use std::marker::PhantomData;

pub trait CursorAPI<'tx>: Copy + Clone {
  type BucketType: BucketAPI<'tx>;

  fn bucket(&self) -> Self::BucketType;

  fn first(&mut self) -> Option<(&'tx [u8], &'tx [u8])>;

  fn last(&mut self) -> Option<(&'tx [u8], &'tx [u8])>;

  fn next(&mut self) -> Option<(&'tx [u8], &'tx [u8])>;

  fn prev(&mut self) -> Option<(&'tx [u8], &'tx [u8])>;

  fn seek(&mut self, seek: &[u8]) -> Option<(&'tx [u8], &'tx [u8])>;
}

pub trait CursorMutAPI<'tx>: CursorAPI<'tx> {
  fn delete(&mut self) -> io::Result<()>;
}

pub struct ElemRef<'tx, N: IRef<NodeR<'tx>>> {
  pn: Either<RefPage<'tx>, N>,
  index: u32,
}

impl<'tx, N: IRef<NodeR<'tx>>> ElemRef<'tx, N> {
  fn count(&self) -> u32 {
    match &self.pn {
      Either::Left(r) => r.count as u32,
      Either::Right(n) => n.borrow_iref().inodes.len() as u32,
    }
  }

  fn is_leaf(&self) -> bool {
    match &self.pn {
      Either::Left(r) => r.is_leaf(),
      Either::Right(n) => n.borrow_iref().is_leaf,
    }
  }
}

pub struct NCursor<'tx, B: BucketAPI<'tx> + IRef<BucketR<'tx, B::TxType>>> {
  bucket: B,
  stack: BVec<'tx, ElemRef<'tx, B::NodeType>>,
}

impl<'tx, B: BucketAPI<'tx> + IRef<BucketR<'tx, B::TxType>>> NCursor<'tx, B> {
  fn first(&mut self) -> Option<(&'tx [u8], &'tx [u8])> {
    let (k, v, flags) = self._first()?;
    if (flags & BUCKET_LEAF_FLAG) != 0 {
      return Some((k, &[]));
    }
    Some((k, v))
  }
  fn _first(&mut self) -> Option<(&'tx [u8], &'tx [u8], u32)> {
    self.stack.clear();

    let pn = self
      .bucket
      .borrow_iref()
      .page_node::<B>(self.bucket.root(), None);
    self.stack.push(ElemRef { pn, index: 0 });

    // If we land on an empty page then move to the next value.
    // https://github.com/boltdb/bolt/issues/450
    if self.stack.last()?.count() == 0 {
      self.next();
    }

    let (k, v, flags) = self.key_value()?;
    if (flags & BUCKET_LEAF_FLAG) != 0 {
      return Some((k, &[], flags));
    }
    None
  }

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
        let inode = ref_node.inodes.get(elem_ref.index as usize)?;
        Some((inode.key(), inode.value(), inode.flags()))
      }
    }
  }

  fn next(&mut self) {
    todo!("cursor.next")
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
  fn first(&mut self) -> Option<(&'tx [u8], &'tx [u8])> {
    self.cell.borrow_mut().first()
  }

  fn last(&mut self) -> Option<(&'tx [u8], &'tx [u8])> {
    todo!()
  }

  fn next(&mut self) -> Option<(&'tx [u8], &'tx [u8])> {
    todo!()
  }

  fn prev(&mut self) -> Option<(&'tx [u8], &'tx [u8])> {
    todo!()
  }

  fn seek(&mut self, seek: &[u8]) -> Option<(&'tx [u8], &'tx [u8])> {
    todo!()
  }
}

#[derive(Copy, Clone)]
pub struct CursorMut<'tx> {
  cell: SCell<'tx, NCursorMut<'tx>>,
}
