use crate::bucket::BucketIApi;
use crate::common::page::tree::leaf::BUCKET_LEAF_FLAG;
use crate::cursor::{CursorIApi, InnerCursor};
use crate::tx::TxIApi;
use crate::{BucketApi, BucketImpl, BucketRwImpl, TxImpl, TxRef, TxRwImpl, TxRwRef};
use itertools::rev;
use std::marker::PhantomData;

#[derive(Clone)]
struct KvIter<'tx: 'p, 'p> {
  next: InnerCursor<'tx>,
  next_started: bool,
  back: InnerCursor<'tx>,
  back_started: bool,
  p: PhantomData<&'p u8>,
}

impl<'tx: 'p, 'p> KvIter<'tx, 'p> {
  pub(crate) fn new(c: InnerCursor<'tx>) -> KvIter<'tx, 'p> {
    KvIter {
      next: c.clone(),
      next_started: false,
      back: c,
      back_started: false,
      p: PhantomData,
    }
  }
}

impl<'tx: 'p, 'p> Iterator for KvIter<'tx, 'p> {
  type Item = (&'p [u8], &'p [u8], u32);

  fn next(&mut self) -> Option<Self::Item> {
    if self.next_started && self.back_started && self.next == self.back {
      return None;
    }
    if self.next_started {
      self.next.i_next()
    } else {
      self.next_started = true;
      self.next.i_first()
    }
  }
}

impl<'tx: 'p, 'p> DoubleEndedIterator for KvIter<'tx, 'p> {
  fn next_back(&mut self) -> Option<Self::Item> {
    if self.next_started && self.back_started && self.next == self.back {
      return None;
    }
    if self.back_started {
      self.back.i_prev()
    } else {
      self.back_started = true;
      self.back.api_last();
      self.back.key_value()
    }
  }
}

#[derive(Clone)]
pub struct EntryIter<'tx: 'p, 'p> {
  i: KvIter<'tx, 'p>,
}

impl<'tx: 'p, 'p> EntryIter<'tx, 'p> {
  pub(crate) fn new(c: InnerCursor<'tx>) -> EntryIter<'tx, 'p> {
    EntryIter { i: KvIter::new(c) }
  }
}

impl<'tx: 'p, 'p> Iterator for EntryIter<'tx, 'p> {
  type Item = (&'p [u8], &'p [u8]);

  fn next(&mut self) -> Option<Self::Item> {
    while let Some((k, v, flags)) = self.i.next() {
      if flags & BUCKET_LEAF_FLAG == 0 {
        return Some((k, v));
      }
    }
    None
  }
}

impl<'tx: 'p, 'p> DoubleEndedIterator for EntryIter<'tx, 'p> {
  fn next_back(&mut self) -> Option<Self::Item> {
    while let Some((k, v, flags)) = self.i.next_back() {
      if flags & BUCKET_LEAF_FLAG == 0 {
        return Some((k, v));
      }
    }
    None
  }
}

pub struct BucketIter<'tx: 'p, 'p> {
  i: KvIter<'tx, 'p>,
}

impl<'tx: 'p, 'p> BucketIter<'tx, 'p> {
  pub(crate) fn new(c: InnerCursor<'tx>) -> BucketIter<'tx, 'p> {
    BucketIter { i: KvIter::new(c) }
  }
}

impl<'tx: 'p, 'p> Iterator for BucketIter<'tx, 'p> {
  type Item = (&'p [u8], BucketImpl<'tx, 'p>);

  fn next(&mut self) -> Option<Self::Item> {
    while let Some((k, _, flags)) = self.i.next() {
      if flags & BUCKET_LEAF_FLAG != 0 {
        let bucket = BucketImpl::from(self.i.next.bucket.api_bucket(k).unwrap());
        return Some((k, bucket));
      }
    }
    None
  }
}

impl<'tx: 'p, 'p> DoubleEndedIterator for BucketIter<'tx, 'p> {
  fn next_back(&mut self) -> Option<Self::Item> {
    while let Some((k, _, flags)) = self.i.next_back() {
      if flags & BUCKET_LEAF_FLAG != 0 {
        let bucket = BucketImpl::from(self.i.back.bucket.api_bucket(k).unwrap());
        return Some((k, bucket));
      }
    }
    None
  }
}

impl<'tx: 'a, 'a> IntoIterator for &'a TxImpl<'tx> {
  type Item = (&'a [u8], BucketImpl<'tx, 'a>);
  type IntoIter = BucketIter<'tx, 'a>;

  fn into_iter(self) -> Self::IntoIter {
    BucketIter::new(self.tx.api_cursor())
  }
}

impl<'tx: 'a, 'a> IntoIterator for &'a TxRwImpl<'tx> {
  type Item = (&'a [u8], BucketImpl<'tx, 'a>);
  type IntoIter = BucketIter<'tx, 'a>;

  fn into_iter(self) -> Self::IntoIter {
    BucketIter::new(self.tx.api_cursor())
  }
}

impl<'tx: 'a, 'a> IntoIterator for &'a TxRef<'tx> {
  type Item = (&'a [u8], BucketImpl<'tx, 'a>);
  type IntoIter = BucketIter<'tx, 'a>;

  fn into_iter(self) -> Self::IntoIter {
    BucketIter::new(self.tx.api_cursor())
  }
}

impl<'tx: 'a, 'a> IntoIterator for &'a TxRwRef<'tx> {
  type Item = (&'a [u8], BucketImpl<'tx, 'a>);
  type IntoIter = BucketIter<'tx, 'a>;

  fn into_iter(self) -> Self::IntoIter {
    BucketIter::new(self.tx.api_cursor())
  }
}

pub struct BucketIterMut<'tx: 'p, 'p> {
  i: KvIter<'tx, 'p>,
}

impl<'tx: 'p, 'p> BucketIterMut<'tx, 'p> {
  pub(crate) fn new(c: InnerCursor<'tx>) -> BucketIterMut<'tx, 'p> {
    BucketIterMut { i: KvIter::new(c) }
  }
}

impl<'tx: 'p, 'p> Iterator for BucketIterMut<'tx, 'p> {
  type Item = (&'p [u8], BucketRwImpl<'tx, 'p>);

  fn next(&mut self) -> Option<Self::Item> {
    while let Some((k, _, flags)) = self.i.next() {
      if flags & BUCKET_LEAF_FLAG != 0 {
        let bucket = BucketRwImpl::from(self.i.next.bucket.api_bucket(k).unwrap());
        return Some((k, bucket));
      }
    }
    None
  }
}

impl<'tx: 'p, 'p> DoubleEndedIterator for BucketIterMut<'tx, 'p> {
  fn next_back(&mut self) -> Option<Self::Item> {
    while let Some((k, _, flags)) = self.i.next_back() {
      if flags & BUCKET_LEAF_FLAG != 0 {
        let bucket = BucketRwImpl::from(self.i.back.bucket.api_bucket(k).unwrap());
        return Some((k, bucket));
      }
    }
    None
  }
}

impl<'tx: 'a, 'a> IntoIterator for &'a mut TxRwImpl<'tx> {
  type Item = (&'a [u8], BucketRwImpl<'tx, 'a>);
  type IntoIter = BucketIterMut<'tx, 'a>;

  fn into_iter(self) -> Self::IntoIter {
    BucketIterMut::new(self.tx.api_cursor())
  }
}

impl<'tx: 'a, 'a> IntoIterator for &'a mut TxRwRef<'tx> {
  type Item = (&'a [u8], BucketRwImpl<'tx, 'a>);
  type IntoIter = BucketIterMut<'tx, 'a>;

  fn into_iter(self) -> Self::IntoIter {
    BucketIterMut::new(self.tx.api_cursor())
  }
}

pub enum ValueBucket<'tx: 'p, 'p> {
  Value(&'p [u8]),
  Bucket(BucketImpl<'tx, 'p>),
}

pub struct ValueBucketIter<'tx: 'p, 'p> {
  i: KvIter<'tx, 'p>,
}

impl<'tx: 'p, 'p> ValueBucketIter<'tx, 'p> {
  pub(crate) fn new(c: InnerCursor<'tx>) -> ValueBucketIter<'tx, 'p> {
    ValueBucketIter { i: KvIter::new(c) }
  }
}

impl<'tx: 'p, 'p> Iterator for ValueBucketIter<'tx, 'p> {
  type Item = (&'p [u8], ValueBucket<'tx, 'p>);

  fn next(&mut self) -> Option<Self::Item> {
    if let Some((k, v, flags)) = self.i.next() {
      return if flags & BUCKET_LEAF_FLAG == 0 {
        Some((k, ValueBucket::Value(v)))
      } else {
        let bucket = BucketImpl::from(self.i.next.bucket.api_bucket(k).unwrap());
        Some((k, ValueBucket::Bucket(bucket)))
      };
    }
    None
  }
}

impl<'tx: 'p, 'p> DoubleEndedIterator for ValueBucketIter<'tx, 'p> {
  fn next_back(&mut self) -> Option<Self::Item> {
    if let Some((k, v, flags)) = self.i.next_back() {
      return if flags & BUCKET_LEAF_FLAG == 0 {
        Some((k, ValueBucket::Value(v)))
      } else {
        let bucket = BucketImpl::from(self.i.back.bucket.api_bucket(k).unwrap());
        Some((k, ValueBucket::Bucket(bucket)))
      };
    }
    None
  }
}

#[derive(Debug)]
pub enum ValueBucketSeq<'p> {
  Value(&'p [u8]),
  BucketSeq(u64),
}

impl<'p> ValueBucketSeq<'p> {
  pub fn len(&self) -> usize {
    match self {
      ValueBucketSeq::Value(v) => v.len(),
      ValueBucketSeq::BucketSeq(_) => 0,
    }
  }
}

pub struct DbWalker<'tx: 'p, 'p> {
  root_buckets: BucketIter<'tx, 'p>,
  path: Vec<&'p [u8]>,
  cursors: Vec<ValueBucketIter<'tx, 'p>>,
  bucket_seq: bool,
  p: PhantomData<&'p u8>,
}

impl<'tx: 'p, 'p> DbWalker<'tx, 'p> {
  pub(crate) fn new(root_cursor: InnerCursor<'tx>) -> DbWalker<'tx, 'p> {
    DbWalker {
      root_buckets: BucketIter::new(root_cursor),
      path: Vec::new(),
      cursors: Vec::new(),
      bucket_seq: false,
      p: PhantomData,
    }
  }

  pub fn path(&self) -> &[&'p [u8]] {
    if self.bucket_seq {
      self.path.split_last().unwrap().1
    } else {
      &self.path
    }
  }
}

impl<'tx: 'p, 'p> Iterator for DbWalker<'tx, 'p> {
  type Item = (&'p [u8], ValueBucketSeq<'p>);

  fn next(&mut self) -> Option<Self::Item> {
    self.bucket_seq = false;
    loop {
      if self.cursors.is_empty() {
        return if let Some((k, b)) = self.root_buckets.next() {
          let seq = b.sequence();
          self.bucket_seq = true;
          self.path.push(k);
          let i = ValueBucketIter::new(b.b.i_cursor());
          self.cursors.push(i);
          Some((k, ValueBucketSeq::BucketSeq(seq)))
        } else {
          None
        };
      }
      if let Some((k, vb)) = self.cursors.last_mut().and_then(|i| i.next()) {
        return match vb {
          ValueBucket::Value(v) => Some((k, ValueBucketSeq::Value(v))),
          ValueBucket::Bucket(b) => {
            let seq = b.sequence();
            self.bucket_seq = true;
            self.path.push(k);
            let i = ValueBucketIter::new(b.b.i_cursor());
            self.cursors.push(i);
            Some((k, ValueBucketSeq::BucketSeq(seq)))
          }
        };
      } else {
        self.path.pop();
        self.cursors.pop();
      }
    }
  }
}

#[cfg(test)]
mod test {
  use crate::iter::DbWalker;
  use crate::test_support::TestDb;
  use crate::tx::TxIApi;
  use crate::{BucketApi, BucketRwApi, DbRwAPI, TxRwRefApi};

  #[test]
  fn test_tx_for_each_no_error() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let mut b = tx.create_bucket("widgets")?;
      b.put("foo", "bar")?;

      for (k, mut b) in tx.iter_mut_buckets() {
        let mut wb = b.create_bucket("woojits")?;
        wb.put("fooz", "ball")?;
        for (k, wb) in b.iter_buckets() {
          println!("{:?}", k);
          for (k, v) in wb.iter_entries() {
            println!("{:?}", k);
          }
        }
      }
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  fn test_tx_walk_no_error() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let mut b = tx.create_bucket("widgets")?;
      b.put("foo1", "bar")?;
      b.put("foo2", "bar")?;
      let mut wb = b.create_bucket("woojits")?;
      wb.set_sequence(1)?;
      wb.put("fooz1", "ball")?;
      wb.put("fooz2", "ball")?;
      let mut hb = wb.create_bucket("hoojits")?;
      hb.set_sequence(2)?;
      hb.put("fooz1", "bill")?;
      hb.put("fooz2", "bill")?;
      let mut wb = b.create_bucket("wajits")?;
      wb.set_sequence(3)?;
      wb.put("fooz1", "balls")?;
      wb.put("fooz2", "balls")?;
      let mut pb = tx.create_bucket_path(&["one", "two", "three", "four"])?;
      pb.set_sequence(4)?;
      pb.put("five", "six")?;
      Ok(())
    })?;
    let tx = db.begin_tx()?;
    let mut w = DbWalker::new(tx.tx.api_cursor());
    while let Some(i) = w.next() {
      let path = w.path();
      println!("{:?}: {:?}", path, i);
    }
    drop(w);
    let c = DbWalker::new(tx.tx.api_cursor()).count();
    println!("{:?}", c);
    Ok(())
  }
}
