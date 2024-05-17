use crate::bucket::BucketIApi;
use crate::common::page::tree::leaf::BUCKET_LEAF_FLAG;
use crate::cursor::{CursorIApi, InnerCursor};
use crate::tx::TxIApi;
use crate::{BucketApi, BucketImpl, BucketRwImpl, TxImpl, TxRef, TxRwImpl, TxRwRef};
use std::marker::PhantomData;

struct KvIter<'p, 'tx: 'p> {
  c: InnerCursor<'tx>,
  started: bool,
  p: PhantomData<&'p u8>,
}

impl<'p, 'tx: 'p> KvIter<'p, 'tx> {
  pub(crate) fn new(c: InnerCursor<'tx>) -> KvIter<'p, 'tx> {
    KvIter {
      c,
      started: false,
      p: PhantomData,
    }
  }
}

impl<'p, 'tx: 'p> Iterator for KvIter<'p, 'tx> {
  type Item = (&'p [u8], &'p [u8], u32);

  fn next(&mut self) -> Option<Self::Item> {
    if !self.started {
      self.started = true;
      self.c.i_first()
    } else {
      self.c.i_next()
    }
  }
}

pub struct EntryIter<'p, 'tx: 'p> {
  i: KvIter<'p, 'tx>,
}

impl<'p, 'tx: 'p> EntryIter<'p, 'tx> {
  pub(crate) fn new(c: InnerCursor<'tx>) -> EntryIter<'p, 'tx> {
    EntryIter { i: KvIter::new(c) }
  }
}

impl<'p, 'tx: 'p> Iterator for EntryIter<'p, 'tx> {
  type Item = (&'p [u8], &'p [u8]);

  fn next(&mut self) -> Option<Self::Item> {
    for (k, v, flags) in self.i.by_ref() {
      if flags & BUCKET_LEAF_FLAG == 0 {
        return Some((k, v));
      }
    }
    None
  }
}

pub struct BucketIter<'p, 'tx: 'p> {
  i: KvIter<'p, 'tx>,
}

impl<'p, 'tx: 'p> BucketIter<'p, 'tx> {
  pub(crate) fn new(c: InnerCursor<'tx>) -> BucketIter<'p, 'tx> {
    BucketIter { i: KvIter::new(c) }
  }
}

impl<'p, 'tx: 'p> Iterator for BucketIter<'p, 'tx> {
  type Item = (&'p [u8], BucketImpl<'p, 'tx>);

  fn next(&mut self) -> Option<Self::Item> {
    for (k, _, flags) in self.i.by_ref() {
      if flags & BUCKET_LEAF_FLAG != 0 {
        let bucket = BucketImpl::from(self.i.c.bucket.api_bucket(k).unwrap());
        return Some((k, bucket));
      }
    }
    None
  }
}

impl<'a, 'tx: 'a> IntoIterator for &'a TxImpl<'tx> {
  type Item = (&'a [u8], BucketImpl<'a, 'tx>);
  type IntoIter = BucketIter<'a, 'tx>;

  fn into_iter(self) -> Self::IntoIter {
    BucketIter::new(self.tx.api_cursor())
  }
}

impl<'a, 'tx: 'a> IntoIterator for &'a TxRwImpl<'tx> {
  type Item = (&'a [u8], BucketImpl<'a, 'tx>);
  type IntoIter = BucketIter<'a, 'tx>;

  fn into_iter(self) -> Self::IntoIter {
    BucketIter::new(self.tx.api_cursor())
  }
}

impl<'a, 'tx: 'a> IntoIterator for &'a TxRef<'tx> {
  type Item = (&'a [u8], BucketImpl<'a, 'tx>);
  type IntoIter = BucketIter<'a, 'tx>;

  fn into_iter(self) -> Self::IntoIter {
    BucketIter::new(self.tx.api_cursor())
  }
}

impl<'a, 'tx: 'a> IntoIterator for &'a TxRwRef<'tx> {
  type Item = (&'a [u8], BucketImpl<'a, 'tx>);
  type IntoIter = BucketIter<'a, 'tx>;

  fn into_iter(self) -> Self::IntoIter {
    BucketIter::new(self.tx.api_cursor())
  }
}

pub struct BucketIterMut<'p, 'tx: 'p> {
  i: KvIter<'p, 'tx>,
}

impl<'p, 'tx: 'p> BucketIterMut<'p, 'tx> {
  pub(crate) fn new(c: InnerCursor<'tx>) -> BucketIterMut<'p, 'tx> {
    BucketIterMut { i: KvIter::new(c) }
  }
}

impl<'p, 'tx: 'p> Iterator for BucketIterMut<'p, 'tx> {
  type Item = (&'p [u8], BucketRwImpl<'p, 'tx>);

  fn next(&mut self) -> Option<Self::Item> {
    for (k, _, flags) in self.i.by_ref() {
      if flags & BUCKET_LEAF_FLAG != 0 {
        let bucket = BucketRwImpl::from(self.i.c.bucket.api_bucket(k).unwrap());
        return Some((k, bucket));
      }
    }
    None
  }
}

impl<'a, 'tx: 'a> IntoIterator for &'a mut TxRwImpl<'tx> {
  type Item = (&'a [u8], BucketRwImpl<'a, 'tx>);
  type IntoIter = BucketIterMut<'a, 'tx>;

  fn into_iter(self) -> Self::IntoIter {
    BucketIterMut::new(self.tx.api_cursor())
  }
}

impl<'a, 'tx: 'a> IntoIterator for &'a mut TxRwRef<'tx> {
  type Item = (&'a [u8], BucketRwImpl<'a, 'tx>);
  type IntoIter = BucketIterMut<'a, 'tx>;

  fn into_iter(self) -> Self::IntoIter {
    BucketIterMut::new(self.tx.api_cursor())
  }
}

pub enum ValueBucket<'p, 'tx: 'p> {
  Value(&'p [u8]),
  Bucket(BucketImpl<'p, 'tx>),
}

pub struct ValueBucketIter<'p, 'tx: 'p> {
  i: KvIter<'p, 'tx>,
}

impl<'p, 'tx: 'p> ValueBucketIter<'p, 'tx> {
  pub(crate) fn new(c: InnerCursor<'tx>) -> ValueBucketIter<'p, 'tx> {
    ValueBucketIter { i: KvIter::new(c) }
  }
}

impl<'p, 'tx: 'p> Iterator for ValueBucketIter<'p, 'tx> {
  type Item = (&'p [u8], ValueBucket<'p, 'tx>);

  fn next(&mut self) -> Option<Self::Item> {
    if let Some((k, v, flags)) = self.i.by_ref().next() {
      return if flags & BUCKET_LEAF_FLAG == 0 {
        Some((k, ValueBucket::Value(v)))
      } else {
        let bucket = BucketImpl::from(self.i.c.bucket.api_bucket(k).unwrap());
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

pub struct DbWalker<'p, 'tx: 'p> {
  root_buckets: BucketIter<'p, 'tx>,
  path: Vec<&'p [u8]>,
  cursors: Vec<ValueBucketIter<'p, 'tx>>,
  bucket_seq: bool,
  p: PhantomData<&'p u8>,
}

impl<'p, 'tx: 'p> DbWalker<'p, 'tx> {
  pub(crate) fn new(root_cursor: InnerCursor<'tx>) -> DbWalker<'p, 'tx> {
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

impl<'p, 'tx: 'p> Iterator for DbWalker<'p, 'tx> {
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
