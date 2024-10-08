use crate::common::bucket::{BucketHeader, BUCKET_HEADER_SIZE};
use crate::common::cell::{Ref, RefMut};
use crate::common::memory::{BCell, IsAligned};
use crate::common::page::tree::branch::{MappedBranchPage, BRANCH_PAGE_ELEMENT_SIZE};
use crate::common::page::tree::leaf::{
  MappedLeafPage, BUCKET_LEAF_FLAG, LEAF_PAGE_ELEMENT_SIZE, LEAF_PAGE_FLAG,
};
use crate::common::page::tree::TreePage;
use crate::common::page::{CoerciblePage, MutPage, PageHeader, RefPage, PAGE_HEADER_SIZE};
use crate::common::{BVec, HashMap, PgId, SplitRef, ZERO_PGID};
use crate::cursor::{CursorIApi, CursorImpl, CursorRwIApi, CursorRwImpl, InnerCursor, PageNode};
use crate::iter::{BucketIter, BucketIterMut, EntryIter};
use crate::node::NodeRwCell;
use crate::tx::{TxCell, TxIApi, TxRwIApi};
use crate::Error;
use crate::Error::{
  BucketExists, BucketNameRequired, BucketNotFound, IncompatibleValue, KeyRequired, KeyTooLarge,
  ValueTooLarge,
};
use bumpalo::Bump;
use bytemuck::{Pod, Zeroable};
use getset::CopyGetters;
use std::alloc::Layout;
use std::marker::PhantomData;
use std::ops::{AddAssign, Deref};
use std::ptr::slice_from_raw_parts_mut;
use std::slice::{from_raw_parts, from_raw_parts_mut};
use std::{mem, ptr};

/// Read-only Bucket API
pub trait BucketApi<'tx>
where
  Self: Sized,
{
  /// Returns the bucket's root page id.
  ///
  /// ```rust
  /// use bbolt_rs::*;
  ///
  /// fn main() -> Result<()> {
  ///   let mut db = Bolt::open_mem()?;
  ///
  ///   db.update(|mut tx| {
  ///     let mut b = tx.create_bucket_if_not_exists("test")?;
  ///     b.put("key", "value")?;
  ///     Ok(())
  ///   })?;
  ///
  ///   db.view(|tx| {
  ///     let b = tx.bucket("test").unwrap();
  ///     let page_id = b.root();
  ///     let page_info = tx.page(page_id).unwrap();
  ///     println!("{:?}", page_info);
  ///     Ok(())
  ///   })?;
  ///
  ///   Ok(())
  /// }
  /// ```
  fn root(&self) -> PgId;

  /// Returns whether the bucket is writable.
  ///
  /// ```rust
  /// use bbolt_rs::*;
  ///
  /// fn main() -> Result<()> {
  ///   let mut db = Bolt::open_mem()?;
  ///
  ///   db.update(|mut tx| {
  ///     let b = tx.create_bucket_if_not_exists("test")?;
  ///     assert_eq!(true, b.writable());
  ///     Ok(())
  ///   })?;
  ///
  ///   db.view(|tx| {
  ///     let b = tx.bucket("test").unwrap();
  ///     assert_eq!(false, b.writable());
  ///     Ok(())
  ///   })?;
  ///
  ///   Ok(())
  /// }
  /// ```
  fn writable(&self) -> bool;

  /// Creates a cursor associated with the bucket.
  ///
  /// ```rust
  /// use bbolt_rs::*;
  ///
  /// fn main() -> Result<()> {
  ///   let mut db = Bolt::open_mem()?;
  ///
  ///   db.update(|mut tx| {
  ///     let mut b = tx.create_bucket_if_not_exists("test")?;
  ///     b.put("key", "value")?;
  ///     Ok(())
  ///   })?;
  ///
  ///   db.view(|tx| {
  ///     let b = tx.bucket("test").unwrap();
  ///     let mut c = b.cursor();
  ///     let first = c.first();
  ///     assert_eq!(Some((b"key".as_slice(), Some(b"value".as_slice()))), first);
  ///     Ok(())
  ///   })?;
  ///
  ///   Ok(())
  /// }
  /// ```
  fn cursor<'a>(&'a self) -> CursorImpl<'tx, 'a>;

  /// Retrieves a nested bucket by name.
  ///
  /// Returns None if the bucket does not exist.
  ///
  /// ```rust
  /// use bbolt_rs::*;
  ///
  /// fn main() -> Result<()> {
  ///   let mut db = Bolt::open_mem()?;
  ///
  ///   db.update(|mut tx| {
  ///     let mut b = tx.create_bucket_if_not_exists("test")?;
  ///     let mut sub = b.create_bucket_if_not_exists("sub")?;
  ///     sub.put("key", "value")?;
  ///     Ok(())
  ///   })?;
  ///
  ///   db.view(|tx| {
  ///     let b = tx.bucket("test").unwrap();
  ///     assert_eq!(true, b.bucket("sub").is_some());
  ///     assert_eq!(false, b.bucket("no bucket").is_some());
  ///     Ok(())
  ///   })?;
  ///
  ///   Ok(())
  /// }
  /// ```
  fn bucket<'a, T: AsRef<[u8]>>(&'a self, name: T) -> Option<BucketImpl<'tx, 'a>>;

  /// Retrieves the value for a key in the bucket.
  ///
  /// Returns None if the key does not exist or if the key is a nested bucket.
  ///
  /// ```rust
  /// use bbolt_rs::*;
  ///
  /// fn main() -> Result<()> {
  ///   let mut db = Bolt::open_mem()?;
  ///
  ///   db.update(|mut tx| {
  ///     let mut b = tx.create_bucket_if_not_exists("test")?;
  ///     b.put("key", "value")?;
  ///     Ok(())
  ///   })?;
  ///
  ///   db.view(|tx| {
  ///     let b = tx.bucket("test").unwrap();
  ///     let get = b.get("key");
  ///     assert_eq!(Some(b"value".as_slice()), get);
  ///     let get = b.get("no value");
  ///     assert_eq!(None, get);
  ///     Ok(())
  ///   })?;
  ///
  ///   Ok(())
  /// }
  /// ```
  fn get<T: AsRef<[u8]>>(&self, key: T) -> Option<&[u8]>;

  /// Returns the current integer for the bucket without incrementing it.
  ///
  /// ```rust
  /// use bbolt_rs::*;
  ///
  /// fn main() -> Result<()> {
  ///   let mut db = Bolt::open_mem()?;
  ///
  ///   db.update(|mut tx| {
  ///     let mut b = tx.create_bucket_if_not_exists("test")?;
  ///     b.put("key", "value")?;
  ///     Ok(())
  ///   })?;
  ///
  ///   db.view(|tx| {
  ///     let b = tx.bucket("test").unwrap();
  ///     let seq = b.sequence();
  ///     assert_eq!(0, seq);
  ///     Ok(())
  ///   })?;
  ///
  ///   Ok(())
  /// }
  /// ```
  fn sequence(&self) -> u64;

  #[deprecated(since = "1.3.9", note = "please use `iter_*` methods instead")]
  /// Executes a function for each key/value pair in a bucket.
  /// Because this uses a [`crate::CursorApi`], the iteration over keys is in lexicographical order.
  ///
  /// If the provided function returns an error then the iteration is stopped and
  /// the error is returned to the caller.
  ///
  /// ```rust
  /// use bbolt_rs::*;
  ///
  /// fn main() -> Result<()> {
  ///   let mut db = Bolt::open_mem()?;
  ///
  ///   db.update(|mut tx| {
  ///     let mut b = tx.create_bucket_if_not_exists("test")?;
  ///     b.put("key1", "value1")?;
  ///     b.put("key2", "value2")?;
  ///     b.put("key3", "value3")?;
  ///     Ok(())
  ///   })?;
  ///
  ///   db.view(|tx| {
  ///     let b = tx.bucket("test").unwrap();
  ///     b.for_each(|k,v| {
  ///      println!("{:?}, {:?}", k, v);
  ///      Ok(())
  ///     })?;
  ///     Ok(())
  ///   })?;
  ///
  ///   Ok(())
  /// }
  /// ```
  fn for_each<F: FnMut(&'tx [u8], Option<&'tx [u8]>) -> crate::Result<()>>(
    &self, f: F,
  ) -> crate::Result<()>;

  #[deprecated(since = "1.3.9", note = "please use `iter_*` methods instead")]
  /// Executes a function for each bucket in a bucket.
  /// Because this function uses a [`crate::CursorApi`], the iteration over keys is in lexicographical order.
  ///
  /// If the provided function returns an error then the iteration is stopped and
  /// the error is returned to the caller.
  ///
  /// ```rust
  /// use bbolt_rs::*;
  ///
  /// fn main() -> Result<()> {
  ///   let mut db = Bolt::open_mem()?;
  ///
  ///   db.update(|mut tx| {
  ///     let mut b = tx.create_bucket_if_not_exists("test")?;
  ///     let _ = b.create_bucket_if_not_exists("sub1")?;
  ///     let _ = b.create_bucket_if_not_exists("sub2")?;
  ///     let _ = b.create_bucket_if_not_exists("sub3")?;
  ///     Ok(())
  ///   })?;
  ///
  ///   db.view(|tx| {
  ///     let b = tx.bucket("test").unwrap();
  ///     b.for_each_bucket(|k| {
  ///      println!("{:?}", k);
  ///      Ok(())
  ///     })?;
  ///     Ok(())
  ///   })?;
  ///
  ///   Ok(())
  /// }
  /// ```
  fn for_each_bucket<F: FnMut(&'tx [u8]) -> crate::Result<()>>(&self, f: F) -> crate::Result<()>;

  /// Returns stats on a bucket.
  ///
  /// ```rust
  /// use bbolt_rs::*;
  ///
  /// fn main() -> Result<()> {
  ///   let mut db = Bolt::open_mem()?;
  ///
  ///   db.update(|mut tx| {
  ///     let mut b = tx.create_bucket_if_not_exists("test")?;
  ///     b.put("key1", "value1")?;
  ///     b.put("key2", "value2")?;
  ///     b.put("key3", "value3")?;
  ///     Ok(())
  ///   })?;
  ///
  ///   db.view(|tx| {
  ///     let b = tx.bucket("test").unwrap();
  ///     let stats = b.stats();
  ///     assert_eq!(3, stats.key_n());
  ///     Ok(())
  ///   })?;
  ///
  ///   Ok(())
  /// }
  /// ```
  fn stats(&self) -> BucketStats;

  fn iter_entries<'a>(&'a self) -> EntryIter<'tx, 'a>;

  fn iter_buckets<'a>(&'a self) -> BucketIter<'tx, 'a>;
}

/// RW Bucket API
pub trait BucketRwApi<'tx>: BucketApi<'tx> {
  /// Retrieves a nested mutable bucket by name.
  ///
  /// Returns None if the bucket does not exist.
  ///
  /// ```rust
  /// use bbolt_rs::*;
  ///
  /// fn main() -> Result<()> {
  ///   let mut db = Bolt::open_mem()?;
  ///
  ///   db.update(|mut tx| {
  ///     let mut b = tx.create_bucket_if_not_exists("test")?;
  ///     let _ = b.create_bucket_if_not_exists("sub")?;
  ///     Ok(())
  ///   })?;
  ///
  ///   db.update(|mut tx | {
  ///     let mut b = tx.bucket_mut("test").unwrap();
  ///     let mut sub = b.bucket_mut("sub").unwrap();
  ///     sub.put("key1", "value1")?;
  ///     Ok(())
  ///   })?;
  ///
  ///   Ok(())
  /// }
  /// ```
  fn bucket_mut<'a, T: AsRef<[u8]>>(&'a mut self, name: T) -> Option<BucketRwImpl<'tx, 'a>>;

  /// Creates a new bucket at the given key and returns the new bucket.
  ///
  /// Returns an error if the key already exists, if the bucket name is blank, or if the bucket name is too long.
  ///
  /// ```rust
  /// use bbolt_rs::*;
  ///
  /// fn main() -> Result<()> {
  ///   let mut db = Bolt::open_mem()?;
  ///
  ///   db.update(|mut tx| {
  ///     let mut b = tx.create_bucket("test")?;
  ///     let _ = b.create_bucket("sub")?;
  ///     Ok(())
  ///   })?;
  ///
  ///   db.view(|tx| {
  ///     let b = tx.bucket("test").unwrap();
  ///     let _ = b.bucket("sub").unwrap();
  ///     Ok(())
  ///   })?;
  ///
  ///   Ok(())
  /// }
  /// ```
  fn create_bucket<'a, T: AsRef<[u8]>>(
    &'a mut self, key: T,
  ) -> crate::Result<BucketRwImpl<'tx, 'a>>;

  /// CreateBucketIfNotExists creates a new bucket if it doesn't already exist and returns a reference to it.
  ///
  /// Returns an error if the bucket name is blank, or if the bucket name is too long.
  ///
  /// ```rust
  /// use bbolt_rs::*;
  ///
  /// fn main() -> Result<()> {
  ///   let mut db = Bolt::open_mem()?;
  ///
  ///   db.update(|mut tx| {
  ///     let mut b = tx.create_bucket_if_not_exists("test")?;
  ///     let _ = b.create_bucket_if_not_exists("sub")?;
  ///     Ok(())
  ///   })?;
  ///
  ///   db.view(|tx| {
  ///     let b = tx.bucket("test").unwrap();
  ///     let _ = b.bucket("sub").unwrap();
  ///     Ok(())
  ///   })?;
  ///
  ///   Ok(())
  /// }
  /// ```
  fn create_bucket_if_not_exists<'a, T: AsRef<[u8]>>(
    &'a mut self, key: T,
  ) -> crate::Result<BucketRwImpl<'tx, 'a>>;

  /// Cursor creates a cursor associated with the bucket.
  ///
  /// ```rust
  /// use bbolt_rs::*;
  ///
  /// fn main() -> Result<()> {
  ///   let mut db = Bolt::open_mem()?;
  ///
  ///   db.update(|mut tx| {
  ///     let mut b = tx.create_bucket_if_not_exists("test")?;
  ///     b.put("key1", "value1")?;
  ///     b.put("key2", "value2")?;
  ///     b.put("key3", "value3")?;
  ///     Ok(())
  ///   })?;
  ///
  ///   db.update(|mut tx| {
  ///     let mut b = tx.bucket_mut("test").unwrap();
  ///     let mut c = b.cursor_mut();
  ///     c.seek("key2");
  ///     c.delete()?;
  ///     Ok(())
  ///   })?;
  ///
  ///   db.view(|tx| {
  ///     let b = tx.bucket("test").unwrap();
  ///     let mut c = b.cursor();
  ///     let seek = c.seek("key2");
  ///     assert_eq!(Some((b"key3".as_slice(), Some(b"value3".as_slice()))), seek);
  ///     Ok(())
  ///   })?;
  ///
  ///   Ok(())
  /// }
  /// ```
  fn cursor_mut<'a>(&'a self) -> CursorRwImpl<'tx, 'a>;

  /// Deletes a bucket at the given key.
  ///
  /// Returns an error if the bucket does not exist, or if the key represents a non-bucket value.
  ///
  /// ```rust
  /// use bbolt_rs::*;
  ///
  /// fn main() -> Result<()> {
  ///   let mut db = Bolt::open_mem()?;
  ///
  ///   db.update(|mut tx| {
  ///     let mut b = tx.create_bucket_if_not_exists("test")?;
  ///     let _ = b.create_bucket_if_not_exists("sub")?;
  ///     Ok(())
  ///   })?;
  ///
  ///   db.update(|mut tx| {
  ///     let mut b = tx.bucket_mut("test").unwrap();
  ///     b.delete_bucket("sub")?;
  ///     Ok(())
  ///   })?;
  ///
  ///   db.view(|tx| {
  ///     let b = tx.bucket("test").unwrap();
  ///     assert_eq!(false, b.bucket("sub").is_some());
  ///     Ok(())
  ///   })?;
  ///
  ///   Ok(())
  /// }
  /// ```
  fn delete_bucket<T: AsRef<[u8]>>(&mut self, key: T) -> crate::Result<()>;

  /// Sets the value for a key in the bucket.
  ///
  /// If the key exist then its previous value will be overwritten.
  ///
  /// Returns an error if the key is blank, if the key is too large, or if the value is too large.
  ///
  /// ```rust
  /// use bbolt_rs::*;
  ///
  /// fn main() -> Result<()> {
  ///   let mut db = Bolt::open_mem()?;
  ///
  ///   db.update(|mut tx| {
  ///     let mut b = tx.create_bucket_if_not_exists("test")?;
  ///     b.put("key1", "value1")?;
  ///     b.put("key2", "value2")?;
  ///     b.put("key3", "value3")?;
  ///     Ok(())
  ///   })?;
  ///
  ///   db.update(|mut tx| {
  ///     let mut b = tx.create_bucket_if_not_exists("test")?;
  ///     b.put("key2", "new value")?;
  ///     Ok(())
  ///   })?;
  ///
  ///   db.view(|tx| {
  ///     let b = tx.bucket("test").unwrap();
  ///     let get = b.get("key1");
  ///     assert_eq!(Some(b"value1".as_slice()), get);
  ///     let get = b.get("key2");
  ///     assert_eq!(Some(b"new value".as_slice()), get);
  ///     Ok(())
  ///   })?;
  ///
  ///   Ok(())
  /// }
  /// ```
  fn put<T: AsRef<[u8]>, U: AsRef<[u8]>>(&mut self, key: T, data: U) -> crate::Result<()>;

  /// Removes a key from the bucket.
  ///
  /// If the key does not exist then nothing is done.
  ///
  /// ```rust
  /// use bbolt_rs::*;
  ///
  /// fn main() -> Result<()> {
  ///   let mut db = Bolt::open_mem()?;
  ///
  ///   db.update(|mut tx| {
  ///     let mut b = tx.create_bucket_if_not_exists("test")?;
  ///     b.put("key1", "value1")?;
  ///     b.put("key2", "value2")?;
  ///     b.put("key3", "value3")?;
  ///     Ok(())
  ///   })?;
  ///
  ///   db.update(|mut tx| {
  ///     let mut b = tx.create_bucket_if_not_exists("test")?;
  ///     b.delete("key2")?;
  ///     Ok(())
  ///   })?;
  ///
  ///   db.view(|tx| {
  ///     let b = tx.bucket("test").unwrap();
  ///     let get = b.get("key1");
  ///     assert_eq!(Some(b"value1".as_slice()), get);
  ///     let get = b.get("key2");
  ///     assert_eq!(false, get.is_some());
  ///     Ok(())
  ///   })?;
  ///
  ///   Ok(())
  /// }
  /// ```
  fn delete<T: AsRef<[u8]>>(&mut self, key: T) -> crate::Result<()>;

  /// Updates the sequence number for the bucket.
  ///
  /// ```rust
  /// use bbolt_rs::*;
  ///
  /// fn main() -> Result<()> {
  ///   let mut db = Bolt::open_mem()?;
  ///
  ///   db.update(|mut tx| {
  ///     let mut b = tx.create_bucket_if_not_exists("test")?;
  ///     b.set_sequence(42)?;
  ///     Ok(())
  ///   })?;
  ///
  ///   db.view(|tx| {
  ///     let b = tx.bucket("test").unwrap();
  ///     assert_eq!(42, b.sequence());
  ///     Ok(())
  ///   })?;
  ///
  ///   Ok(())
  /// }
  /// ```
  fn set_sequence(&mut self, v: u64) -> crate::Result<()>;

  /// Returns the auto-incremented integer for the bucket.
  ///
  /// ```rust
  /// use bbolt_rs::*;
  ///
  /// fn main() -> Result<()> {
  ///   let mut db = Bolt::open_mem()?;
  ///
  ///   db.update(|mut tx| {
  ///     let mut b = tx.create_bucket_if_not_exists("test")?;
  ///     b.set_sequence(42)?;
  ///     assert_eq!(43, b.next_sequence()?);
  ///     Ok(())
  ///   })?;
  ///
  ///   Ok(())
  /// }
  /// ```
  fn next_sequence(&mut self) -> crate::Result<u64>;

  /// Set the fill percent of the bucket
  ///
  /// ```rust
  /// use bbolt_rs::*;
  ///
  /// fn main() -> Result<()> {
  ///   let mut db = Bolt::open_mem()?;
  ///
  ///   db.update(|mut tx| {
  ///     let mut b = tx.create_bucket_if_not_exists("test")?;
  ///     b.set_fill_percent(0.90);
  ///     Ok(())
  ///   })?;
  ///
  ///   Ok(())
  /// }
  /// ```
  fn set_fill_percent(&mut self, fill_percent: f64);

  fn iter_mut_buckets<'a>(&'a mut self) -> BucketIterMut<'tx, 'a>;

  fn rev_iter_mut_buckets<'a>(&'a mut self) -> BucketIterMut<'tx, 'a>;
}

/// Read-only Bucket
pub struct BucketImpl<'tx: 'p, 'p> {
  pub(crate) b: BucketCell<'tx>,
  p: PhantomData<&'p u8>,
}

impl<'tx, 'p> From<BucketCell<'tx>> for BucketImpl<'tx, 'p> {
  fn from(value: BucketCell<'tx>) -> Self {
    BucketImpl {
      b: value,
      p: PhantomData,
    }
  }
}

impl<'tx, 'p> BucketApi<'tx> for BucketImpl<'tx, 'p> {
  fn root(&self) -> PgId {
    self.b.root()
  }

  fn writable(&self) -> bool {
    self.b.is_writeable()
  }

  fn cursor<'a>(&'a self) -> CursorImpl<'tx, 'a> {
    InnerCursor::new(self.b, self.b.tx().bump()).into()
  }

  fn bucket<'a, T: AsRef<[u8]>>(&'a self, name: T) -> Option<BucketImpl<'tx, 'a>> {
    self.b.api_bucket(name.as_ref()).map(BucketImpl::from)
  }

  fn get<T: AsRef<[u8]>>(&self, key: T) -> Option<&[u8]> {
    self.b.api_get(key.as_ref())
  }

  fn sequence(&self) -> u64 {
    self.b.api_sequence()
  }

  fn for_each<F: FnMut(&'tx [u8], Option<&'tx [u8]>) -> crate::Result<()>>(
    &self, f: F,
  ) -> crate::Result<()> {
    self.b.api_for_each(f)
  }

  fn for_each_bucket<F: FnMut(&'tx [u8]) -> crate::Result<()>>(&self, f: F) -> crate::Result<()> {
    self.b.api_for_each_bucket(f)
  }

  fn stats(&self) -> BucketStats {
    self.b.api_stats()
  }

  fn iter_entries<'a>(&'a self) -> EntryIter<'tx, 'a> {
    EntryIter::new(self.b.i_cursor())
  }

  fn iter_buckets<'a>(&'a self) -> BucketIter<'tx, 'a> {
    BucketIter::new(self.b.i_cursor())
  }
}

/// Read/Write Bucket
pub struct BucketRwImpl<'tx: 'p, 'p> {
  b: BucketCell<'tx>,
  p: PhantomData<&'p u8>,
}

impl<'tx, 'p> From<BucketCell<'tx>> for BucketRwImpl<'tx, 'p> {
  fn from(value: BucketCell<'tx>) -> Self {
    BucketRwImpl {
      b: value,
      p: PhantomData,
    }
  }
}

impl<'tx, 'p> BucketApi<'tx> for BucketRwImpl<'tx, 'p> {
  fn root(&self) -> PgId {
    self.b.root()
  }

  fn writable(&self) -> bool {
    self.b.is_writeable()
  }

  fn cursor<'a>(&'a self) -> CursorImpl<'tx, 'a> {
    InnerCursor::new(self.b, self.b.tx().bump()).into()
  }

  fn bucket<'a, T: AsRef<[u8]>>(&'a self, name: T) -> Option<BucketImpl<'tx, 'a>> {
    self.b.api_bucket(name.as_ref()).map(BucketImpl::from)
  }

  fn get<T: AsRef<[u8]>>(&self, key: T) -> Option<&[u8]> {
    self.b.api_get(key.as_ref())
  }

  fn sequence(&self) -> u64 {
    self.b.api_sequence()
  }

  fn for_each<F: FnMut(&'tx [u8], Option<&'tx [u8]>) -> crate::Result<()>>(
    &self, f: F,
  ) -> crate::Result<()> {
    self.b.api_for_each(f)
  }

  fn for_each_bucket<F: FnMut(&'tx [u8]) -> crate::Result<()>>(&self, f: F) -> crate::Result<()> {
    self.b.api_for_each_bucket(f)
  }

  fn stats(&self) -> BucketStats {
    self.b.api_stats()
  }

  fn iter_entries<'a>(&'a self) -> EntryIter<'tx, 'a> {
    EntryIter::new(self.b.i_cursor())
  }

  fn iter_buckets<'a>(&'a self) -> BucketIter<'tx, 'a> {
    BucketIter::new(self.b.i_cursor())
  }
}

impl<'tx, 'p> BucketRwApi<'tx> for BucketRwImpl<'tx, 'p> {
  fn bucket_mut<'a, T: AsRef<[u8]>>(&'a mut self, name: T) -> Option<BucketRwImpl<'tx, 'a>> {
    self.b.api_bucket(name.as_ref()).map(BucketRwImpl::from)
  }

  fn create_bucket<'a, T: AsRef<[u8]>>(
    &'a mut self, key: T,
  ) -> crate::Result<BucketRwImpl<'tx, 'a>> {
    self
      .b
      .api_create_bucket(key.as_ref())
      .map(BucketRwImpl::from)
  }

  fn create_bucket_if_not_exists<'a, T: AsRef<[u8]>>(
    &'a mut self, key: T,
  ) -> crate::Result<BucketRwImpl<'tx, 'a>> {
    self
      .b
      .api_create_bucket_if_not_exists(key.as_ref())
      .map(BucketRwImpl::from)
  }

  fn cursor_mut<'a>(&'a self) -> CursorRwImpl<'tx, 'a> {
    CursorRwImpl::new(InnerCursor::new(self.b, self.b.tx().bump()))
  }

  fn delete_bucket<T: AsRef<[u8]>>(&mut self, key: T) -> crate::Result<()> {
    self.b.api_delete_bucket(key.as_ref())
  }

  fn put<T: AsRef<[u8]>, U: AsRef<[u8]>>(&mut self, key: T, data: U) -> crate::Result<()> {
    self.b.api_put(key.as_ref(), data.as_ref())
  }

  fn delete<T: AsRef<[u8]>>(&mut self, key: T) -> crate::Result<()> {
    self.b.api_delete(key.as_ref())
  }

  fn set_sequence(&mut self, v: u64) -> crate::Result<()> {
    self.b.api_set_sequence(v)
  }

  fn next_sequence(&mut self) -> crate::Result<u64> {
    self.b.api_next_sequence()
  }

  fn set_fill_percent(&mut self, fill_percent: f64) {
    // TODO: Move to cell api call
    self.b.cell.borrow_mut().w.as_mut().unwrap().fill_percent = fill_percent;
  }

  fn iter_mut_buckets<'a>(&'a mut self) -> BucketIterMut<'tx, 'a> {
    BucketIterMut::new(self.b.i_cursor())
  }

  fn rev_iter_mut_buckets<'a>(&'a mut self) -> BucketIterMut<'tx, 'a> {
    BucketIterMut::new(self.b.i_cursor())
  }
}

/// BucketStats records statistics about resources used by a bucket.
#[derive(Copy, Clone, Eq, PartialEq, Default, Debug, CopyGetters)]
#[getset(get_copy = "pub")]
pub struct BucketStats {
  // Page count statistics.
  /// number of logical branch pages
  branch_page_n: i64,
  /// number of physical branch overflow pages
  branch_overflow_n: i64,
  /// number of logical leaf pages
  leaf_page_n: i64,
  /// number of physical leaf overflow pages
  leaf_overflow_n: i64,

  // Tree statistics.
  /// number of keys/value pairs
  pub(crate) key_n: i64,
  /// number of levels in B+tree
  depth: i64,

  // Page size utilization.
  /// bytes allocated for physical branch pages
  branch_alloc: i64,
  /// bytes actually used for branch data
  branch_in_use: i64,
  /// bytes allocated for physical leaf pages
  leaf_alloc: i64,
  /// bytes actually used for leaf data
  leaf_in_use: i64,

  // Bucket statistics
  /// total number of buckets including the top bucket
  bucket_n: i64,
  /// total number on inlined buckets
  inline_bucket_n: i64,
  /// bytes used for inlined buckets (also accounted for in LeafInuse)
  inline_bucket_in_use: i64,
}

impl AddAssign<BucketStats> for BucketStats {
  fn add_assign(&mut self, rhs: BucketStats) {
    self.branch_page_n += rhs.branch_page_n;
    self.branch_overflow_n += rhs.branch_overflow_n;
    self.leaf_page_n += rhs.leaf_page_n;
    self.leaf_overflow_n += rhs.leaf_overflow_n;

    self.key_n += rhs.key_n;
    if self.depth < rhs.depth {
      self.depth = rhs.depth;
    }

    self.branch_alloc += rhs.branch_alloc;
    self.branch_in_use += rhs.branch_in_use;
    self.leaf_alloc += rhs.leaf_alloc;
    self.leaf_in_use += rhs.leaf_in_use;

    self.bucket_n += rhs.bucket_n;
    self.inline_bucket_n += rhs.inline_bucket_n;
    self.inline_bucket_in_use += rhs.inline_bucket_in_use;
  }
}

/// DefaultFillPercent is the percentage that split pages are filled.
/// This value can be changed by setting Bucket.FillPercent.
const DEFAULT_FILL_PERCENT: f64 = 0.5;

/// MAX_KEY_SIZE is the maximum length of a key, in bytes.
const MAX_KEY_SIZE: u32 = 32768;

/// MaxValueSize is the maximum length of a value, in bytes.
const MAX_VALUE_SIZE: u32 = (1 << 31) - 2;
const INLINE_BUCKET_ALIGNMENT: usize = mem::align_of::<InlineBucket>();
const INLINE_BUCKET_SIZE: usize = mem::size_of::<InlineBucket>();

pub(crate) const MIN_FILL_PERCENT: f64 = 0.1;
pub(crate) const MAX_FILL_PERCENT: f64 = 1.0;

/// A convenience struct representing an inline page header
#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable)]
struct InlineBucket {
  header: BucketHeader,
  page: PageHeader,
}

impl Default for InlineBucket {
  fn default() -> Self {
    InlineBucket {
      header: BucketHeader::new(ZERO_PGID, 0),
      page: PageHeader {
        id: Default::default(),
        flags: LEAF_PAGE_FLAG,
        count: 0,
        overflow: 0,
      },
    }
  }
}

/// The internal Bucket API
pub(crate) trait BucketIApi<'tx> {
  fn new_r_in(
    bump: &'tx Bump, bucket_header: BucketHeader, tx: TxCell<'tx>,
    inline_page: Option<RefPage<'tx>>,
  ) -> BucketCell<'tx>;

  fn new_rw_in(
    bump: &'tx Bump, bucket_header: BucketHeader, tx: TxCell<'tx>,
    inline_page: Option<RefPage<'tx>>,
  ) -> BucketCell<'tx>;

  /// Returns whether the bucket is writable.
  fn is_writeable(&self) -> bool;

  /// Returns the rc ptr Tx of the bucket
  fn tx(self) -> TxCell<'tx>;

  /// Returns the root page id of the bucket
  fn root(self) -> PgId;

  /// Create a new cursor for this Bucket
  fn i_cursor(self) -> InnerCursor<'tx>;

  /// See [BucketApi::bucket]
  fn api_bucket(self, name: &[u8]) -> Option<BucketCell<'tx>>;

  /// Helper method that re-interprets a sub-bucket value
  /// from a parent into a Bucket
  fn open_bucket(self, value: &[u8]) -> BucketCell<'tx>;

  /// See [BucketApi::get]
  fn api_get(self, key: &[u8]) -> Option<&'tx [u8]>;

  /// See [BucketApi::for_each]
  fn api_for_each<F: FnMut(&'tx [u8], Option<&'tx [u8]>) -> crate::Result<()>>(
    self, f: F,
  ) -> crate::Result<()>;

  /// See [BucketApi::for_each_bucket]
  fn api_for_each_bucket<F: FnMut(&'tx [u8]) -> crate::Result<()>>(self, f: F)
    -> crate::Result<()>;

  /// forEachPage iterates over every page in a bucket, including inline pages.
  fn for_each_page<F: FnMut(&RefPage<'tx>, usize, &mut BVec<PgId>)>(self, f: &mut F);

  /// forEachPageNode iterates over every page (or node) in a bucket.
  /// This also includes inline pages.
  fn for_each_page_node<F: FnMut(&PageNode<'tx>, usize) + Copy>(self, f: F);

  fn _for_each_page_node<F: FnMut(&PageNode<'tx>, usize) + Copy>(
    self, root: PgId, depth: usize, f: F,
  );

  fn page_node(self, id: PgId) -> PageNode<'tx>;

  /// See [BucketApi::sequence]
  fn api_sequence(self) -> u64;

  /// Returns the maximum total size of a bucket to make it a candidate for inlining.
  fn max_inline_bucket_size(self) -> usize;

  /// See [BucketApi::stats]
  fn api_stats(self) -> BucketStats;

  fn into_impl<'a>(self) -> BucketImpl<'tx, 'a>;
}

pub(crate) trait BucketRwIApi<'tx>: BucketIApi<'tx> {
  /// Explicitly materialize the root node
  fn materialize_root(self) -> NodeRwCell<'tx>;

  /// See [BucketRwApi::create_bucket]
  fn api_create_bucket(self, key: &[u8]) -> crate::Result<Self>
  where
    Self: Sized;

  /// See [BucketRwApi::create_bucket_if_not_exists]
  fn api_create_bucket_if_not_exists(self, key: &[u8]) -> crate::Result<Self>
  where
    Self: Sized;

  /// See [BucketRwApi::delete_bucket]
  fn api_delete_bucket(self, key: &[u8]) -> crate::Result<()>;

  /// See [BucketRwApi::put]
  fn api_put(self, key: &[u8], value: &[u8]) -> crate::Result<()>;

  /// See [BucketRwApi::delete]
  fn api_delete(self, key: &[u8]) -> crate::Result<()>;

  /// See [BucketRwApi::set_sequence]
  fn api_set_sequence(self, v: u64) -> crate::Result<()>;

  /// See [BucketRwApi::next_sequence]
  fn api_next_sequence(self) -> crate::Result<u64>;

  /// free recursively frees all pages in the bucket.
  fn free(self);

  /// spill writes all the nodes for this bucket to dirty pages.
  fn spill(self, bump: &'tx Bump) -> crate::Result<()>;

  fn write(self, bump: &'tx Bump) -> &'tx [u8];

  /// inlineable returns true if a bucket is small enough to be written inline
  /// and if it contains no subbuckets. Otherwise returns false.
  fn inlineable(self) -> bool;

  /// own_in removes all references to the old mmap.
  fn own_in(self);

  /// node creates a node from a page and associates it with a given parent.
  fn node(self, pgid: PgId, parent: Option<NodeRwCell<'tx>>) -> NodeRwCell<'tx>;

  /// rebalance attempts to balance all nodes.
  fn rebalance(self);
}

pub struct BucketR<'tx> {
  pub(crate) bucket_header: BucketHeader,
  /// inline page reference
  pub(crate) inline_page: Option<RefPage<'tx>>,
  p: PhantomData<&'tx u8>,
}

impl<'tx> BucketR<'tx> {
  pub fn new(in_bucket: BucketHeader) -> BucketR<'tx> {
    BucketR {
      bucket_header: in_bucket,
      inline_page: None,
      p: Default::default(),
    }
  }
}

pub struct BucketW<'tx> {
  /// materialized node for the root page.
  pub(crate) root_node: Option<NodeRwCell<'tx>>,
  /// subbucket cache
  buckets: HashMap<'tx, &'tx [u8], BucketCell<'tx>>,
  /// node cache
  pub(crate) nodes: HashMap<'tx, PgId, NodeRwCell<'tx>>,

  /// Sets the threshold for filling nodes when they split. By default,
  /// the bucket will fill to 50% but it can be useful to increase this
  /// amount if you know that your write workloads are mostly append-only.
  ///
  /// This is non-persisted across transactions so it must be set in every Tx.
  pub(crate) fill_percent: f64,
}

impl<'tx> BucketW<'tx> {
  pub fn new_in(bump: &'tx Bump) -> BucketW<'tx> {
    BucketW {
      root_node: None,
      buckets: HashMap::with_capacity_in(0, bump),
      nodes: HashMap::with_capacity_in(0, bump),
      fill_percent: DEFAULT_FILL_PERCENT,
    }
  }
}

pub struct BucketRW<'tx> {
  pub(crate) r: BucketR<'tx>,
  pub(crate) w: Option<BucketW<'tx>>,
}

#[derive(Copy, Clone)]
pub(crate) struct BucketCell<'tx> {
  pub(crate) cell: BCell<'tx, BucketRW<'tx>, TxCell<'tx>>,
}

impl<'tx> PartialEq for BucketCell<'tx> {
  fn eq(&self, other: &Self) -> bool {
    let self_txid = self.tx().api_id();
    let other_txid = other.tx().api_id();
    let self_header = self.split_r().bucket_header;
    let other_header = other.split_r().bucket_header;
    self_txid == other_txid && self_header == other_header
  }
}

impl<'tx> Eq for BucketCell<'tx> {}

impl<'tx> SplitRef<BucketR<'tx>, TxCell<'tx>, BucketW<'tx>> for BucketCell<'tx> {
  fn split_r(&self) -> Ref<BucketR<'tx>> {
    Ref::map(self.cell.borrow(), |b| &b.r)
  }

  fn split_ref(&self) -> (Ref<BucketR<'tx>>, Ref<Option<BucketW<'tx>>>) {
    let (r, w) = Ref::map_split(self.cell.borrow(), |b| (&b.r, &b.w));
    (r, w)
  }

  fn split_ow(&self) -> Ref<Option<BucketW<'tx>>> {
    Ref::map(self.cell.borrow(), |b| &b.w)
  }

  fn split_bound(&self) -> TxCell<'tx> {
    self.cell.bound()
  }

  fn split_r_mut(&self) -> RefMut<BucketR<'tx>> {
    RefMut::map(self.cell.borrow_mut(), |b| &mut b.r)
  }

  fn split_ow_mut(&self) -> RefMut<Option<BucketW<'tx>>> {
    RefMut::map(self.cell.borrow_mut(), |b| &mut b.w)
  }
}

impl<'tx> BucketIApi<'tx> for BucketCell<'tx> {
  fn new_r_in(
    bump: &'tx Bump, bucket_header: BucketHeader, tx: TxCell<'tx>,
    inline_page: Option<RefPage<'tx>>,
  ) -> BucketCell<'tx> {
    let r = BucketR {
      bucket_header,
      inline_page,
      p: Default::default(),
    };

    BucketCell {
      cell: BCell::new_in(BucketRW { r, w: None }, tx, bump),
    }
  }

  fn new_rw_in(
    bump: &'tx Bump, bucket_header: BucketHeader, tx: TxCell<'tx>,
    inline_page: Option<RefPage<'tx>>,
  ) -> BucketCell<'tx> {
    let r = BucketR {
      bucket_header,
      inline_page,
      p: Default::default(),
    };

    let w = BucketW::new_in(bump);

    BucketCell {
      cell: BCell::new_in(BucketRW { r, w: Some(w) }, tx, bump),
    }
  }

  #[inline(always)]
  fn is_writeable(&self) -> bool {
    self.cell.borrow().w.is_some()
  }

  /// Returns the rc ptr Tx of the bucket
  fn tx(self) -> TxCell<'tx> {
    self.split_bound()
  }

  /// Returns the root page id of the bucket
  fn root(self) -> PgId {
    self.split_r().bucket_header.root()
  }

  fn i_cursor(self) -> InnerCursor<'tx> {
    let tx = self.tx();
    tx.split_r().stats.as_ref().unwrap().inc_cursor_count(1);
    InnerCursor::new(self, tx.bump())
  }

  /// See [BucketApi::bucket]
  fn api_bucket(self, name: &[u8]) -> Option<BucketCell<'tx>> {
    if let Some(w) = self.split_ow().as_ref() {
      if let Some(child) = w.buckets.get(name) {
        return Some(*child);
      }
    }
    let mut c = self.i_cursor();
    // Move cursor to key.
    let (k, v, flags) = c.i_seek(name)?;
    // Return None if the key doesn't exist or it is not a bucket.
    if name != k || (flags & BUCKET_LEAF_FLAG) == 0 {
      return None;
    }

    // Otherwise create a bucket and cache it.
    let child = self.open_bucket(v);
    if let Some(w) = self.split_ow_mut().as_mut() {
      let tx = self.split_bound();
      let bump = tx.bump();
      let name = bump.alloc_slice_copy(name);
      w.buckets.insert(name, child);
    }

    Some(child)
  }

  /// Helper method that re-interprets a sub-bucket value
  /// from a parent into a Bucket
  fn open_bucket(self, mut value: &[u8]) -> BucketCell<'tx> {
    let tx = self.tx();
    let bump = tx.bump();
    // Unaligned access requires a copy to be made.
    //TODO: use std is_aligned_to when it comes out
    if !IsAligned::is_aligned_to::<InlineBucket>(value.as_ptr()) {
      // TODO: Shove this into a centralized function somewhere
      let layout = Layout::from_size_align(value.len(), INLINE_BUCKET_ALIGNMENT).unwrap();
      let new_value = unsafe {
        let data = bump.alloc_layout(layout).as_ptr();
        ptr::write_bytes(data, 0, value.len());
        &mut *slice_from_raw_parts_mut(data, value.len())
      };
      new_value.copy_from_slice(value);
      value = new_value;
    }
    let bucket_header = *bytemuck::from_bytes::<BucketHeader>(value.split_at(BUCKET_HEADER_SIZE).0);
    // Save a reference to the inline page if the bucket is inline.
    let ref_page = if bucket_header.root() == ZERO_PGID {
      assert!(
        value.len() >= INLINE_BUCKET_SIZE,
        "subbucket value not large enough. Expected at least {} bytes. Was {}",
        INLINE_BUCKET_SIZE,
        value.len()
      );
      unsafe {
        let ref_page_ptr = value.as_ptr().add(BUCKET_HEADER_SIZE);
        Some(RefPage::new(ref_page_ptr))
      }
    } else {
      None
    };
    if tx.split_ow().is_some() {
      Self::new_rw_in(bump, bucket_header, tx, ref_page)
    } else {
      Self::new_r_in(bump, bucket_header, tx, ref_page)
    }
  }

  /// See [BucketApi::get]
  fn api_get(self, key: &[u8]) -> Option<&'tx [u8]> {
    if let Some((k, v, flags)) = self.i_cursor().i_seek(key) {
      // Return None if this is a bucket.
      if (flags & BUCKET_LEAF_FLAG) != 0 {
        return None;
      }
      // If our target node isn't the same key as what's passed in then return None.
      if key != k {
        return None;
      }
      Some(v)
    } else {
      None
    }
  }

  /// See [BucketApi::for_each]
  fn api_for_each<F: FnMut(&'tx [u8], Option<&'tx [u8]>) -> crate::Result<()>>(
    self, mut f: F,
  ) -> crate::Result<()> {
    let mut c = self.i_cursor();
    let mut inode = c.api_first();
    while let Some((k, v)) = inode {
      f(k, v)?;
      inode = c.api_next();
    }
    Ok(())
  }

  /// See [BucketApi::for_each_bucket]
  fn api_for_each_bucket<F: FnMut(&'tx [u8]) -> crate::Result<()>>(
    self, mut f: F,
  ) -> crate::Result<()> {
    let mut c = self.i_cursor();
    let mut inode = c.i_first();
    while let Some((k, _, flags)) = inode {
      if flags & BUCKET_LEAF_FLAG != 0 {
        f(k)?;
      }
      inode = c.i_next();
    }
    Ok(())
  }

  /// forEachPage iterates over every page in a bucket, including inline pages.
  fn for_each_page<F: FnMut(&RefPage<'tx>, usize, &mut BVec<PgId>)>(self, f: &mut F) {
    let tx = self.tx();
    let root = {
      let r = self.split_r();
      let root = r.bucket_header.root();
      // If we have an inline page then just use that.
      if let Some(page) = &r.inline_page {
        let mut v = BVec::with_capacity_in(1, tx.bump());
        v.push(root);
        f(page, 0, &mut v);
        return;
      }
      root
    };
    // Otherwise traverse the page hierarchy.
    tx.for_each_page(root, f)
  }

  /// forEachPageNode iterates over every page (or node) in a bucket.
  /// This also includes inline pages.
  fn for_each_page_node<F: FnMut(&PageNode<'tx>, usize) + Copy>(self, mut f: F) {
    let root = {
      let r = self.split_r();
      // If we have an inline page or root node then just use that.
      if let Some(page) = &r.inline_page {
        f(&PageNode::Page(*page), 0);
        return;
      }
      r.bucket_header.root()
    };
    self._for_each_page_node(root, 0, f);
  }

  fn _for_each_page_node<F: FnMut(&PageNode<'tx>, usize) + Copy>(
    self, root: PgId, depth: usize, mut f: F,
  ) {
    let pn = self.page_node(root);

    // Execute function.
    f(&pn, depth);

    // Recursively loop over children.
    match &pn {
      PageNode::Page(page) => {
        if let Some(branch_page) = MappedBranchPage::coerce_ref(page) {
          branch_page.elements().iter().for_each(|elem| {
            self._for_each_page_node(elem.pgid(), depth + 1, f);
          });
        }
      }
      PageNode::Node(node) => {
        let bump = self.tx().bump();
        // To keep with our rules we much copy the inode pgids to temporary storage first
        // This should be unnecessary, but working first *then* optimize
        let v = {
          let node_borrow = node.cell.borrow();
          if node_borrow.is_leaf {
            return;
          }
          let mut v = BVec::with_capacity_in(node_borrow.inodes.len(), bump);
          let ids = node_borrow.inodes.iter().map(|inode| inode.pgid());
          v.extend(ids);
          v
        };
        for pgid in v.into_iter() {
          self._for_each_page_node(pgid, depth + 1, f);
        }
      }
    }
  }

  fn page_node(self, id: PgId) -> PageNode<'tx> {
    let (r, w) = self.split_ref();
    // Inline buckets have a fake page embedded in their value so treat them
    // differently. We'll return the rootNode (if available) or the fake page.
    if r.bucket_header.root() == ZERO_PGID {
      if id != ZERO_PGID {
        panic!("inline bucket non-zero page access(2): {} != 0", id)
      }
      return if let Some(root_node) = w.as_ref().and_then(|wb| wb.root_node) {
        PageNode::Node(root_node)
      } else {
        PageNode::Page(r.inline_page.unwrap())
      };
    }

    // Check the node cache for non-inline buckets.
    if let Some(wb) = w.as_ref() {
      if let Some(node) = wb.nodes.get(&id) {
        return PageNode::Node(*node);
      }
    }

    PageNode::Page(self.tx().mem_page(id))
  }

  /// See [BucketApi::sequence]
  fn api_sequence(self) -> u64 {
    self.split_r().bucket_header.sequence()
  }

  /// Returns the maximum total size of a bucket to make it a candidate for inlining.
  fn max_inline_bucket_size(self) -> usize {
    self.tx().page_size() / 4
  }

  /// See [BucketApi::stats]
  fn api_stats(self) -> BucketStats {
    let mut s = BucketStats::default();
    let mut sub_stats = BucketStats::default();
    let page_size = self.tx().page_size();
    s.bucket_n += 1;
    if self.root() == ZERO_PGID {
      s.inline_bucket_n += 1;
    }
    self.for_each_page(&mut |p, depth, _| {
      if let Some(leaf_page) = MappedLeafPage::coerce_ref(p) {
        s.key_n += p.count as i64;

        // used totals the used bytes for the page
        let mut used = PAGE_HEADER_SIZE;
        if let Some(last_element) = leaf_page.elements().last() {
          // If page has any elements, add all element headers.
          used += LEAF_PAGE_ELEMENT_SIZE * (p.count - 1) as usize;

          // Add all element key, value sizes.
          // The computation takes advantage of the fact that the position
          // of the last element's key/value equals to the total of the sizes
          // of all previous elements' keys and values.
          // It also includes the last element's header.
          used += last_element.pos() as usize
            + last_element.key_size() as usize
            + last_element.value_size() as usize;
        }

        // For inlined bucket just update the inline stats
        if self.root() == ZERO_PGID {
          s.inline_bucket_in_use += used as i64;
        } else {
          // For non-inlined bucket update all the leaf stats
          s.leaf_page_n += 1;
          s.leaf_in_use += used as i64;
          s.leaf_overflow_n += leaf_page.overflow as i64;

          // Collect stats from sub-buckets.
          // Do that by iterating over all element headers
          // looking for the ones with the bucketLeafFlag.
          for leaf_elem in leaf_page.iter() {
            if leaf_elem.is_bucket_entry() {
              // For any bucket element, open the element value
              // and recursively call Stats on the contained bucket.
              sub_stats += self.open_bucket(leaf_elem.value()).api_stats();
            }
          }
        }
      } else if let Some(branch_page) = MappedBranchPage::coerce_ref(p) {
        s.branch_page_n += 1;
        if let Some(last_element) = branch_page.elements().last() {
          // used totals the used bytes for the page
          // Add header and all element headers.
          let mut used =
            PAGE_HEADER_SIZE + (BRANCH_PAGE_ELEMENT_SIZE * (branch_page.count - 1) as usize);
          // Add size of all keys and values.
          // Again, use the fact that last element's position equals to
          // the total of key, value sizes of all previous elements.
          used += last_element.pos() as usize + last_element.key_size() as usize;
          s.branch_in_use += used as i64;
          s.branch_overflow_n += branch_page.overflow as i64;
        }
      }

      if (depth + 1) as i64 > s.depth {
        s.depth = (depth + 1) as i64;
      }
    });
    // Alloc stats can be computed from page counts and pageSize.
    s.branch_alloc = (s.branch_page_n + s.branch_overflow_n) * page_size as i64;
    s.leaf_alloc = (s.leaf_page_n + s.leaf_overflow_n) * page_size as i64;

    // Add the max depth of sub-buckets to get total nested depth.
    s.depth += sub_stats.depth;
    // Add the stats for all sub-buckets
    s += sub_stats;
    s
  }

  fn into_impl<'a>(self) -> BucketImpl<'tx, 'a> {
    self.into()
  }
}

impl<'tx> BucketRwIApi<'tx> for BucketCell<'tx> {
  fn materialize_root(self) -> NodeRwCell<'tx> {
    let root_id = {
      let bucket = self.cell.borrow();
      match bucket.w.as_ref().unwrap().root_node {
        None => bucket.r.bucket_header.root(),
        Some(root_node) => return root_node,
      }
    };
    self.node(root_id, None)
  }

  fn api_create_bucket(self, key: &[u8]) -> crate::Result<Self> {
    if key.is_empty() {
      return Err(BucketNameRequired);
    }
    let mut c = self.i_cursor();

    if let Some((k, _, flags)) = c.i_seek(key) {
      if k == key {
        if flags & BUCKET_LEAF_FLAG != 0 {
          return Err(BucketExists);
        }
        return Err(IncompatibleValue);
      }
    }

    let inline_page = InlineBucket::default();
    let layout = Layout::from_size_align(INLINE_BUCKET_SIZE, INLINE_BUCKET_ALIGNMENT).unwrap();
    let bump = self.tx().bump();
    let data = bump.alloc_layout(layout).as_ptr();

    let value = unsafe {
      ptr::write_bytes(data, 0, INLINE_BUCKET_SIZE);
      from_raw_parts_mut(data, INLINE_BUCKET_SIZE)
    };
    value.copy_from_slice(bytemuck::bytes_of(&inline_page));
    let key = bump.alloc_slice_clone(key) as &[u8];

    c.node().put(key, key, value, ZERO_PGID, BUCKET_LEAF_FLAG);

    self.split_r_mut().inline_page = None;

    Ok(self.api_bucket(key).unwrap())
  }

  fn api_create_bucket_if_not_exists(self, key: &[u8]) -> crate::Result<Self> {
    match self.api_create_bucket(key) {
      Ok(child) => Ok(child),
      Err(error) => {
        if error == BucketExists {
          Ok(self.api_bucket(key).unwrap())
        } else {
          Err(error)
        }
      }
    }
  }

  fn api_delete_bucket(self, key: &[u8]) -> crate::Result<()> {
    let mut c = self.i_cursor();

    let (k, _, flags) = c.i_seek(key).unwrap_or((&[], &[], 0));
    if key != k {
      return Err(BucketNotFound);
    } else if flags & BUCKET_LEAF_FLAG == 0 {
      return Err(IncompatibleValue);
    }

    let child = self.api_bucket(key).unwrap();
    child.api_for_each_bucket(|k| {
      match child.api_delete_bucket(k) {
        Ok(_) => Ok(()),
        // TODO: Ideally we want to properly chain errors here
        Err(e) => Err(Error::Other(e.into())),
      }
    })?;

    {
      let mut self_mut = self.cell.borrow_mut();
      let self_w = self_mut.w.as_mut().unwrap();
      self_w.buckets.remove(key);
      let mut child_mut = child.cell.borrow_mut();
      let child_w = child_mut.w.as_mut().unwrap();
      child_w.nodes.clear();
      child_w.root_node = None;
    }

    child.free();

    c.node().del(key);
    Ok(())
  }

  fn api_put(self, key: &[u8], value: &[u8]) -> crate::Result<()> {
    if key.is_empty() {
      return Err(KeyRequired);
    } else if key.len() > MAX_KEY_SIZE as usize {
      return Err(KeyTooLarge);
    } else if value.len() > MAX_VALUE_SIZE as usize {
      return Err(ValueTooLarge);
    }
    let mut c = self.i_cursor();
    if let Some((k, _, flags)) = c.i_seek(key) {
      if key == k && (flags & BUCKET_LEAF_FLAG) != 0 {
        return Err(IncompatibleValue);
      }
    }

    let bump = self.tx().bump();
    let key = &*bump.alloc_slice_clone(key);
    let value = &*bump.alloc_slice_clone(value);
    c.node().put(key, key, value, ZERO_PGID, 0);
    Ok(())
  }

  fn api_delete(self, key: &[u8]) -> crate::Result<()> {
    let mut c = self.i_cursor();
    if let Some((k, _, flags)) = c.i_seek(key) {
      if key != k {
        return Ok(());
      }
      if flags & BUCKET_LEAF_FLAG != 0 {
        return Err(IncompatibleValue);
      }

      c.node().del(key);
    }
    Ok(())
  }

  fn api_set_sequence(self, v: u64) -> crate::Result<()> {
    self.materialize_root();
    self.split_r_mut().bucket_header.set_sequence(v);
    Ok(())
  }

  fn api_next_sequence(self) -> crate::Result<u64> {
    self.materialize_root();
    let mut r = self.split_r_mut();
    r.bucket_header.inc_sequence();
    Ok(r.bucket_header.sequence())
  }

  fn free(self) {
    if self.split_r().bucket_header.root() == ZERO_PGID {
      return;
    }

    let tx = self.tx();
    let txid = tx.meta().txid();

    self.for_each_page_node(|pn, _| match pn {
      PageNode::Page(page) => tx.freelist_free_page(txid, page),
      PageNode::Node(node) => node.free(),
    });
    self.split_r_mut().bucket_header.set_root(ZERO_PGID);
  }

  /// spill writes all the nodes for this bucket to dirty pages.
  fn spill(self, bump: &'tx Bump) -> crate::Result<()> {
    // To keep with our rules we much copy the bucket entries to temporary storage first
    // This should be unnecessary, but working first *then* optimize
    let v = {
      let bucket = self.cell.borrow();
      let mut v = BVec::with_capacity_in(bucket.w.as_ref().unwrap().buckets.len(), bump);
      // v.extend() would be more idiomatic, but I'm too tired atm to figure out why
      // it's not working
      for (name, child) in &bucket.w.as_ref().unwrap().buckets {
        v.push((*name, *child));
      }
      v
    };

    for (name, child) in v.into_iter() {
      let value = if child.inlineable() {
        child.free();
        child.write(bump)
      } else {
        child.spill(bump)?;
        let layout = Layout::from_size_align(BUCKET_HEADER_SIZE, INLINE_BUCKET_ALIGNMENT).unwrap();
        let inline_bucket_ptr = bump.alloc_layout(layout).as_ptr();
        unsafe {
          let inline_bucket = &mut (*(inline_bucket_ptr as *mut BucketHeader));
          *inline_bucket = child.split_r().bucket_header;
          from_raw_parts(inline_bucket_ptr, BUCKET_HEADER_SIZE)
        }
      };
      if child.cell.borrow().w.as_ref().unwrap().root_node.is_none() {
        continue;
      }
      let mut c = self.i_cursor();
      let (k, _, flags) = c.i_seek(name).unwrap();
      assert_eq!(name, k, "misplaced bucket header");
      assert_ne!(
        flags & BUCKET_LEAF_FLAG,
        0,
        "unexpected bucket header flag: {:x}",
        flags
      );

      c.node().put(name, name, value, ZERO_PGID, BUCKET_LEAF_FLAG);
    }

    let root_node = match self.cell.borrow().w.as_ref().unwrap().root_node {
      None => return Ok(()),
      Some(root_node) => root_node,
    };

    root_node.spill()?;
    {
      let mut self_borrow = self.cell.borrow_mut();
      let new_root = root_node.root();
      self_borrow.w.as_mut().unwrap().root_node = Some(new_root);
      let borrow_root = new_root.cell.borrow_mut();
      let new_pgid = borrow_root.pgid;
      let tx_pgid = self.cell.bound().meta().pgid();
      if new_pgid >= tx_pgid {
        panic!("pgid ({}) above high water mark ({})", new_pgid, tx_pgid);
      }
      self_borrow.r.bucket_header.set_root(new_pgid);
    }
    Ok(())
  }

  fn write(self, bump: &'tx Bump) -> &'tx [u8] {
    let root_node = self.materialize_root();
    let page_size = BUCKET_HEADER_SIZE + root_node.size();
    let layout = Layout::from_size_align(page_size, INLINE_BUCKET_ALIGNMENT).unwrap();
    let inline_bucket_ptr = bump.alloc_layout(layout).as_ptr();

    unsafe {
      let inline_bucket = &mut (*(inline_bucket_ptr as *mut BucketHeader));
      *inline_bucket = self.cell.borrow().r.bucket_header;
      let mut mut_page = MutPage::new(inline_bucket_ptr.add(BUCKET_HEADER_SIZE));
      mut_page.id = PgId(0);
      mut_page.count = 0;
      mut_page.overflow = 0;
      root_node.write(&mut mut_page);
      from_raw_parts(inline_bucket_ptr, page_size)
    }
  }

  fn inlineable(self) -> bool {
    let bucket = self.cell.borrow_mut();

    // Bucket must only contain a single leaf node.
    let n = match bucket.w.as_ref().unwrap().root_node {
      None => return false,
      Some(n) => n,
    };

    let node_ref = n.cell.borrow();
    if !node_ref.is_leaf {
      return false;
    }

    // Bucket is not inlineable if it contains subbuckets or if it goes beyond
    // our threshold for inline bucket size.
    let mut size = PAGE_HEADER_SIZE;
    for inode in node_ref.inodes.deref() {
      size += LEAF_PAGE_ELEMENT_SIZE + inode.key().len() + inode.value().len();

      if inode.flags() & BUCKET_LEAF_FLAG != 0 || size > self.max_inline_bucket_size() {
        return false;
      }
    }

    true
  }

  fn own_in(self) {
    let (bump, root, children) = {
      let tx = self.split_bound();
      let bucket = self.cell.borrow();
      let bump = tx.bump();
      let mut children: BVec<BucketCell<'tx>> =
        BVec::with_capacity_in(bucket.w.as_ref().unwrap().buckets.len(), bump);
      children.extend(bucket.w.as_ref().unwrap().buckets.values());
      (bump, bucket.w.as_ref().unwrap().root_node, children)
    };

    if let Some(node) = root {
      node.root().own_in(bump);
    }

    for child in children.into_iter() {
      child.own_in();
    }
  }

  fn node(self, pgid: PgId, parent: Option<NodeRwCell<'tx>>) -> NodeRwCell<'tx> {
    let inline_page = {
      let self_borrow = self.cell.borrow_mut();

      // Retrieve node if it's already been created.
      if let Some(n) = self_borrow.w.as_ref().unwrap().nodes.get(&pgid) {
        return *n;
      }
      self_borrow.r.inline_page
    };

    // Otherwise create a node and cache it.
    // Use the inline page if this is an inline bucket.
    let page = match inline_page {
      None => self.tx().mem_page(pgid),
      Some(page) => page,
    };

    // Read the page into the node and cache it.
    let n = NodeRwCell::read_in(self, parent, &page);
    let mut bucket = self.cell.borrow_mut();
    let wb = &mut bucket.w;
    match parent {
      None => wb.as_mut().unwrap().root_node = Some(n),
      Some(parent_node) => parent_node.cell.borrow_mut().children.push(n),
    }

    wb.as_mut().unwrap().nodes.insert(pgid, n);

    // Update statistics.
    self
      .split_bound()
      .split_r()
      .stats
      .as_ref()
      .unwrap()
      .inc_node_count(1);

    n
  }

  fn rebalance(self) {
    let bump = self.tx().bump();
    let (nodes, buckets) = {
      let borrow = self.cell.borrow();
      let nodes = BVec::from_iter_in(borrow.w.as_ref().unwrap().nodes.values().cloned(), bump);
      let buckets = BVec::from_iter_in(borrow.w.as_ref().unwrap().buckets.values().cloned(), bump);
      (nodes, buckets)
    };
    let _nodes = nodes.as_slice();
    for node in nodes.into_iter() {
      node.rebalance();
    }
    for bucket in buckets.into_iter() {
      bucket.rebalance();
    }
  }
}

#[cfg(test)]
mod tests {
  use crate::bucket::MAX_VALUE_SIZE;
  use crate::test_support::{quick_check, DummyVec, TestDb};
  use crate::{
    Bolt, BoltOptionsBuilder, BucketApi, BucketRwApi, CursorApi, DbApi, DbRwAPI, Error, TxApi,
    TxRwRefApi,
  };
  use anyhow::anyhow;
  use fake::{Fake, Faker};
  use std::collections::HashMap;
  use std::fs;
  use std::fs::{File, OpenOptions};
  use std::io::{Read, Write};
  use std::path::Path;
  use std::sync::atomic::{AtomicU32, Ordering};

  #[test]
  fn test_bucket_get_non_existent() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let b = tx.create_bucket(b"widgets")?;
      assert_eq!(None, b.get(b"foo"));
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  fn test_bucket_get_from_node() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let mut b = tx.create_bucket(b"widgets")?;
      b.put(b"foo", b"bar")?;
      assert_eq!(Some(b"bar".as_slice()), b.get(b"foo"));
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  fn test_bucket_get_incompatible_value() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let _ = tx.create_bucket(b"widgets")?;
      tx.bucket_mut(b"widgets").unwrap().create_bucket(b"foo")?;
      assert_eq!(None, tx.bucket(b"widgets").unwrap().get(b"foo"));
      Ok(())
    })?;
    Ok(())
  }

  // Ensure that a slice returned from a bucket has a capacity equal to its length.
  // This also allows slices to be appended to since it will require a realloc by Go.
  //
  // https://github.com/boltdb/bolt/issues/544
  #[test]
  #[ignore]
  fn test_bucket_get_capacity() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let mut b = tx.create_bucket(b"widgets")?;
      b.put(b"key", b"val")?;
      Ok(())
    })?;
    db.update(|tx| {
      let b = tx.bucket(b"widgets").unwrap();
      let mut c = b.cursor();
      if let Some((_k, Some(_v))) = c.first() {
        todo!("We don't allow modifying values in place for this first version");
      }
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  fn test_bucket_put() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      {
        let mut b = tx.create_bucket(b"widgets")?;
        b.put(b"foo", b"bar")?;
      }

      assert_eq!(
        Some(b"bar".as_slice()),
        tx.bucket(b"widgets").unwrap().get(b"foo")
      );
      Ok(())
    })
  }

  #[test]
  fn test_bucket_put_repeat() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      {
        let mut b = tx.create_bucket(b"widgets")?;
        b.put(b"foo", b"bar")?;
        b.put(b"foo", b"baz")?;
      }

      assert_eq!(
        Some(b"baz".as_slice()),
        tx.bucket(b"widgets").unwrap().get(b"foo")
      );
      Ok(())
    })
  }

  #[test]
  #[cfg(not(miri))]
  fn test_bucket_put_large() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    let count = 100;
    let factor = 200;
    db.update(|mut tx| {
      let mut b = tx.create_bucket(b"widgets")?;
      for i in 1..count {
        b.put(
          "0".repeat(i * factor).as_bytes(),
          "X".repeat((count - i) * factor).as_bytes(),
        )?;
      }
      Ok(())
    })?;
    db.view(|tx| {
      let b = tx.bucket(b"widgets").unwrap();
      for i in 1..count {
        let v = b.get("0".repeat(i * factor).as_bytes()).unwrap();
        assert_eq!((count - i) * factor, v.len());
        v.iter().all(|c| c == &b'X');
      }
      Ok(())
    })
  }

  #[test]
  #[cfg(all(not(miri), feature = "long-tests"))]

  fn test_db_put_very_large() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    let n = 400000u64;
    let batch_n = 200000u64;

    let v = [0u8; 500];
    for i in (0..n).step_by(batch_n as usize) {
      db.update(|mut tx| {
        let mut b = tx.create_bucket_if_not_exists(b"widgets")?;
        for j in 0..batch_n {
          b.put((i + j).to_be_bytes().as_slice(), v)?;
        }
        Ok(())
      })?;
    }
    Ok(())
  }

  #[test]
  fn test_bucket_put_incompatible_value() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let _ = tx.create_bucket(b"widgets")?;
      tx.bucket_mut(b"widgets").unwrap().create_bucket(b"foo")?;

      assert_eq!(
        Err(Error::IncompatibleValue),
        tx.bucket_mut(b"widgets").unwrap().put(b"foo", b"bar")
      );
      Ok(())
    })
  }

  #[test]
  fn test_bucket_delete() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let mut b = tx.create_bucket(b"widgets")?;
      b.put(b"foo", b"bar")?;
      b.delete(b"foo")?;
      assert_eq!(None, b.get(b"foo"));
      Ok(())
    })
  }

  #[test]
  #[cfg(not(miri))]
  fn test_bucket_delete_large() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    let var = [b'*'; 1024];
    db.update(|mut tx| {
      let mut b = tx.create_bucket(b"widgets")?;
      for i in 0..100 {
        b.put(format!("{}", i).as_bytes(), var)?;
      }
      Ok(())
    })?;
    db.update(|mut tx| {
      let mut b = tx.bucket_mut(b"widgets").unwrap();
      for i in 0..100 {
        b.delete(format!("{}", i).as_bytes())?;
      }
      Ok(())
    })?;
    db.view(|tx| {
      let b = tx.bucket(b"widgets").unwrap();
      for i in 0..100 {
        assert_eq!(None, b.get(format!("{}", i).as_bytes()));
      }
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  #[cfg(all(not(miri), feature = "long-tests"))]
  fn test_bucket_delete_freelist_overflow() -> crate::Result<()> {
    let mut db = TestDb::new()?;

    //TODO: make this based off of page size
    for i in 0u64..8192 {
      db.update(|mut tx| {
        let mut b = tx.create_bucket_if_not_exists(b"0")?;
        for j in 0u64..1000 {
          let mut k = [0u8; 16];
          let (k0, k1) = k.split_at_mut(8);
          k0.copy_from_slice(i.to_be_bytes().as_slice());
          k1.copy_from_slice(j.to_be_bytes().as_slice());
          b.put(k, [])?;
        }
        Ok(())
      })?;
    }
    db.update(|mut tx| {
      let b = tx.bucket_mut(b"0").unwrap();
      let mut c = b.cursor_mut();
      let mut node = c.first();
      while node.is_some() {
        c.delete()?;
        node = c.next();
      }
      Ok(())
    })?;
    let stats = db.stats();
    let free_page_n = stats.free_page_n();
    let pending_page_n = stats.pending_page_n();
    let free_pages = free_page_n + pending_page_n;
    assert!(
      free_pages > 0xFFFF,
      "expected more than 0xFFFF free pages. Got {}",
      free_pages
    );
    db.must_close();
    db.must_reopen();
    let stats = db.stats();
    let reopen_free_pages = stats.free_page_n();
    assert_eq!(
      free_pages, reopen_free_pages,
      "Expected {} free pages, got {:?}",
      free_pages, reopen_free_pages
    );
    Ok(())
  }

  #[test]
  fn test_bucket_delete_non_existing() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let mut b = tx.create_bucket(b"widgets")?;
      let _ = b.create_bucket(b"nested")?;
      Ok(())
    })?;
    db.update(|mut tx| {
      let mut b = tx.bucket_mut(b"widgets").unwrap();
      b.delete(b"foo")?;
      assert!(
        b.bucket(b"nested").is_some(),
        "nested bucket has been deleted"
      );
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  #[cfg(not(miri))]
  fn test_bucket_nested() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      // Create a widgets bucket.
      let mut b = tx.create_bucket(b"widgets")?;

      // Create a widgets/foo bucket.
      let _ = b.create_bucket(b"foo")?;

      // Create a widgets/bar key.
      b.put(b"bar", b"0000")?;
      Ok(())
    })?;
    db.must_check();
    db.update(|mut tx| {
      let mut b = tx.bucket_mut(b"widgets").unwrap();
      b.put(b"bar", b"xxxx")?;
      Ok(())
    })?;
    db.must_check();
    db.update(|mut tx| {
      let mut b = tx.bucket_mut(b"widgets").unwrap();
      for i in 0..10000 {
        let s = format!("{}", i);
        b.put(s.as_bytes(), s.as_bytes())?;
      }
      Ok(())
    })?;
    db.must_check();
    db.update(|mut tx| {
      let mut b = tx.bucket_mut(b"widgets").unwrap();
      {
        let mut foo = b.bucket_mut(b"foo").unwrap();
        foo.put(b"baz", b"yyyy")?;
      }
      b.put(b"bar", b"xxxx")?;
      Ok(())
    })?;
    db.must_check();
    db.view(|tx| {
      let b = tx.bucket(b"widgets").unwrap();
      let foo = b.bucket(b"foo").unwrap();
      assert_eq!(Some(b"yyyy".as_slice()), foo.get(b"baz"));
      assert_eq!(Some(b"xxxx".as_slice()), b.get(b"bar"));

      for i in 0..10000 {
        let s = format!("{}", i);
        assert_eq!(Some(s.as_bytes()), b.get(s.as_bytes()));
      }
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  fn test_bucket_delete_bucket() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let mut b = tx.create_bucket(b"widgets")?;
      let _ = b.create_bucket(b"foo")?;
      assert_eq!(Err(Error::IncompatibleValue), b.delete(b"foo"));
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  // Ensure that deleting a bucket causes nested buckets to be deleted.
  fn test_bucket_delete_bucket_nested() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      {
        let mut widgets = tx.create_bucket(b"widgets")?;
        let mut foo = widgets.create_bucket(b"foo")?;
        let mut bar = foo.create_bucket(b"bar")?;
        bar.put(b"baz", b"bat")?;
      }
      tx.bucket_mut(b"widgets").unwrap().delete_bucket(b"foo")?;
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  // Ensure that deleting a bucket causes nested buckets to be deleted after they have been committed.
  fn test_bucket_delete_bucket_nested2() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let mut widgets = tx.create_bucket(b"widgets")?;
      let mut foo = widgets.create_bucket(b"foo")?;
      let mut bar = foo.create_bucket(b"bar")?;
      bar.put(b"baz", b"bat")?;
      Ok(())
    })?;
    db.update(|mut tx| {
      {
        let widgets = tx.bucket(b"widgets").unwrap();
        let foo = widgets.bucket(b"foo").unwrap();
        let bar = foo.bucket(b"bar").unwrap();
        assert_eq!(Some(b"bat".as_slice()), bar.get(b"baz"));
      }
      tx.delete_bucket(b"widgets")?;
      Ok(())
    })?;
    db.view(|tx| {
      assert!(tx.bucket(b"widgets").is_none());
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  #[cfg(not(miri))]
  // Ensure that deleting a child bucket with multiple pages causes all pages to get collected.
  // NOTE: Consistency check in bolt_test.DB.Close() will panic if pages not freed properly.
  fn test_bucket_delete_bucket_large() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let mut widgets = tx.create_bucket(b"widgets")?;
      let mut foo = widgets.create_bucket(b"foo")?;
      for i in 0..1000 {
        let k = format!("{}", i);
        let v = format!("{:0100}", i);
        foo.put(k.as_bytes(), v.as_bytes())?;
      }
      Ok(())
    })?;
    db.update(|mut tx| {
      tx.delete_bucket(b"widgets")?;
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  fn test_bucket_bucket_incompatible_value() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let mut widgets = tx.create_bucket(b"widgets")?;
      widgets.put(b"foo", b"bar")?;
      assert!(widgets.bucket(b"foo").is_none());
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  fn test_bucket_create_bucket_incompatible_value() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let mut widgets = tx.create_bucket(b"widgets")?;
      widgets.put(b"foo", b"bar")?;
      assert_eq!(
        Some(Error::IncompatibleValue),
        widgets.create_bucket(b"foo").err()
      );
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  fn test_bucket_delete_bucket_incompatible_value() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let mut widgets = tx.create_bucket(b"widgets")?;
      widgets.put(b"foo", b"bar")?;
      assert_eq!(
        Some(Error::IncompatibleValue),
        widgets.delete_bucket(b"foo").err()
      );
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  fn test_bucket_sequence() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let mut bkt = tx.create_bucket(b"0")?;
      assert_eq!(0, bkt.sequence());
      bkt.set_sequence(1000)?;
      assert_eq!(1000, bkt.sequence());
      Ok(())
    })?;
    db.view(|tx| {
      let bkt = tx.bucket(b"0").unwrap();
      assert_eq!(1000, bkt.sequence());
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  fn test_bucket_next_sequence() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let _ = tx.create_bucket(b"widgets")?;
      let _ = tx.create_bucket(b"woojits")?;
      {
        let mut widgets = tx.bucket_mut("widgets").unwrap();
        assert_eq!(1, widgets.next_sequence()?);
        assert_eq!(2, widgets.next_sequence()?);
      }
      let mut woojits = tx.bucket_mut("woojits").unwrap();
      assert_eq!(1, woojits.next_sequence()?);

      Ok(())
    })?;
    Ok(())
  }

  #[test]
  // Ensure that a bucket will persist an autoincrementing sequence even if its
  // the only thing updated on the bucket.
  // https://github.com/boltdb/bolt/issues/296
  fn test_bucket_next_sequence_persist() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      tx.create_bucket(b"widgets")?;
      Ok(())
    })?;
    db.update(|mut tx| {
      let mut widgets = tx.bucket_mut(b"widgets").unwrap();
      assert_eq!(1, widgets.next_sequence()?);
      Ok(())
    })?;
    db.update(|mut tx| {
      let mut widgets = tx.bucket_mut(b"widgets").unwrap();
      assert_eq!(2, widgets.next_sequence()?);
      Ok(())
    })?;
    Ok(())
  }

  fn for_each_collect_kv<'tx, B: BucketApi<'tx>>(
    b: B,
  ) -> crate::Result<Vec<(&'tx [u8], Option<&'tx [u8]>)>> {
    let items = std::cell::RefCell::new(Vec::new());
    b.for_each(|k, v| {
      items.borrow_mut().push((k, v));
      Ok(())
    })?;
    Ok(items.into_inner())
  }

  fn for_each_bucket_collect_k<'tx, B: BucketApi<'tx>>(b: B) -> crate::Result<Vec<&'tx [u8]>> {
    let items = std::cell::RefCell::new(Vec::new());
    b.for_each_bucket(|k| {
      items.borrow_mut().push(k);
      Ok(())
    })?;
    Ok(items.into_inner())
  }

  #[test]
  fn test_bucket_for_each() -> crate::Result<()> {
    let expected_items = [
      (b"bar".as_slice(), Some(b"0002".as_slice())),
      (b"baz".as_slice(), Some(b"0001".as_slice())),
      (b"csubbucket".as_slice(), None),
      (b"foo".as_slice(), Some(b"0000".as_slice())),
    ];
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let mut b = tx.create_bucket(b"widgets")?;
      b.put(b"foo", b"0000")?;
      b.put(b"baz", b"0001")?;
      b.put(b"bar", b"0002")?;
      b.create_bucket(b"csubbucket")?;

      let items = for_each_collect_kv(b)?;
      assert_eq!(
        expected_items.as_slice(),
        &items,
        "what we iterated (ForEach) is not what we put"
      );
      Ok(())
    })?;
    db.view(|tx| {
      let b = tx.bucket(b"widgets").unwrap();
      let items = for_each_collect_kv(b)?;
      assert_eq!(
        expected_items.as_slice(),
        &items,
        "what we iterated (ForEach) is not what we put"
      );
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  fn test_bucket_for_each_bucket() -> crate::Result<()> {
    let expected_items = [b"csubbucket".as_slice(), b"zsubbucket".as_slice()];
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let mut b = tx.create_bucket(b"widgets")?;
      b.put(b"foo", b"0000")?;
      let _ = b.create_bucket(b"zsubbucket")?;
      b.put(b"baz", b"0001")?;
      b.put(b"bar", b"0002")?;
      let _ = b.create_bucket(b"csubbucket")?;

      let items = for_each_bucket_collect_k(b)?;
      assert_eq!(
        expected_items.as_slice(),
        &items,
        "what we iterated (ForEach) is not what we put"
      );
      Ok(())
    })?;
    db.view(|tx| {
      let b = tx.bucket(b"widgets").unwrap();
      let items = for_each_bucket_collect_k(b)?;
      assert_eq!(
        expected_items.as_slice(),
        &items,
        "what we iterated (ForEach) is not what we put"
      );
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  fn test_bucket_for_each_bucket_no_buckets() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let mut b = tx.create_bucket(b"widgets")?;
      b.put(b"foo", b"0000")?;
      b.put(b"baz", b"0001")?;

      let items = for_each_bucket_collect_k(b)?;
      assert!(
        items.is_empty(),
        "what we iterated (ForEach) is not what we put"
      );
      Ok(())
    })?;
    db.view(|tx| {
      let b = tx.bucket(b"widgets").unwrap();
      let items = for_each_bucket_collect_k(b)?;
      assert!(
        items.is_empty(),
        "what we iterated (ForEach) is not what we put"
      );
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  fn test_bucket_for_each_short_circuit() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    let result = db.update(|mut tx| {
      {
        let mut b = tx.create_bucket(b"widgets")?;
        b.put(b"bar", b"0000")?;
        b.put(b"baz", b"0000")?;
        b.put(b"foo", b"0000")?;
      }
      let index = AtomicU32::new(0);
      tx.bucket(b"widgets").unwrap().for_each(|k, _| {
        index.fetch_add(1, Ordering::Relaxed);
        if k == b"baz" {
          return Err(Error::Other(anyhow!("marker")));
        }
        Ok(())
      })?;

      Ok(())
    });
    let e = result.map_err(|e| e.to_string()).err().unwrap();
    assert_eq!("marker", e);
    Ok(())
  }

  #[test]
  fn test_bucket_put_empty_key() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let mut widgets = tx.create_bucket(b"widgets")?;
      assert_eq!(Some(Error::KeyRequired), widgets.put([], []).err());
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  fn test_bucket_put_key_too_large() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    let key = [0u8; 32769];
    db.update(|mut tx| {
      let mut widgets = tx.create_bucket(b"widgets")?;
      assert_eq!(
        Some(Error::KeyTooLarge),
        widgets.put(key.as_slice(), b"bar").err()
      );
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  fn test_bucket_put_value_too_large() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    let value = vec![0u8; MAX_VALUE_SIZE as usize + 1];
    db.update(|mut tx| {
      let mut widgets = tx.create_bucket(b"widgets")?;
      assert_eq!(
        Some(Error::ValueTooLarge),
        widgets.put(b"foo", value.as_slice()).err()
      );
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  #[cfg(feature = "long-tests")]
  fn test_bucket_stats() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    let big_key = "really-big-value";
    for i in 0..500 {
      db.update(|mut tx| {
        let mut b = tx.create_bucket_if_not_exists("woojits")?;
        b.put(format!("{:03}", i), format!("{}", i))?;
        Ok(())
      })?;
    }
    // TODO: Update to support different page sizes
    let long_key_length = 10 * 4096 + 17;
    db.update(|mut tx| {
      tx.bucket_mut("woojits")
        .unwrap()
        .put(big_key, "*".repeat(long_key_length))?;
      Ok(())
    })?;
    db.must_check();

    // TODO: Support pagesize that's not 4096
    let stat_4096 = BucketStats {
      branch_page_n: 1,
      branch_overflow_n: 0,
      leaf_page_n: 7,
      leaf_overflow_n: 10,
      key_n: 501,
      depth: 2,
      branch_alloc: 4096,
      branch_in_use: 149,
      leaf_alloc: 69_632,
      leaf_in_use: 0 +
      7 * 16 + // leaf page header (x LeafPageN)
      501 * 16 + // leaf elements
      500 * 3 + big_key.len() as i64 + // leaf keys
      1 * 10 + 2 * 90 + 3 * 400 + long_key_length as i64, // leaf values: 10 * 1digit, 90*2digits, ...
      bucket_n: 1,
      inline_bucket_n: 0,
      inline_bucket_in_use: 0,
    };
    db.view(|tx| {
      let b = tx.bucket("woojits").unwrap();
      let stats = b.stats();
      assert_eq!(stat_4096, stats, "stats differs from expectations");
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  #[cfg(feature = "long-tests")]
  fn test_bucket_stats_random_fill() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    let mut count = 0;
    let mut rand = StdRng::seed_from_u64(42);
    let mut outer_range = (0..1000).collect_vec();
    outer_range.shuffle(&mut rand);
    for i in outer_range {
      db.update(|mut tx| {
        let mut b = tx.create_bucket_if_not_exists("woojits")?;
        b.set_fill_percent(0.90);
        let mut inner_range = (0..100).collect_vec();
        inner_range.shuffle(&mut rand);
        for j in inner_range {
          let index = (j * 1000) + i;
          b.put(format!("{:015}", index), "0000000000")?;
          count += 1;
        }
        Ok(())
      })?;
    }
    db.must_check();
    db.view(|tx| {
      let b = tx.bucket("woojits").unwrap();
      let stats = b.stats();
      assert_eq!(68, stats.branch_page_n, "unexpected BranchPageN");
      assert_eq!(0, stats.branch_overflow_n, "unexpected BranchOverflowN");
      assert_eq!(2946, stats.leaf_page_n, "unexpected LeafPageN");
      assert_eq!(0, stats.leaf_overflow_n, "unexpected LeafOverflowN");
      assert_eq!(10_0000, stats.key_n, "unexpected KeyN");
      assert_eq!(94_491, stats.branch_in_use, "unexpected BranchInuse");
      assert_eq!(4_147_136, stats.leaf_in_use, "unexpected LeafInuse");
      assert_eq!(278_528, stats.branch_alloc, "unexpected BranchAlloc");
      assert_eq!(12_066_816, stats.leaf_alloc, "unexpected LeafAlloc");
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  fn test_bucket_stats_small() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let mut b = tx.create_bucket("whozawhats")?;
      b.put("foo", "bar")?;
      Ok(())
    })?;
    db.must_check();
    db.view(|tx| {
      let b = tx.bucket("whozawhats").unwrap();
      let stats = b.stats();
      assert_eq!(0, stats.branch_page_n, "unexpected BranchPageN");
      assert_eq!(0, stats.branch_overflow_n, "unexpected BranchOverflowN");
      assert_eq!(0, stats.leaf_page_n, "unexpected LeafPageN");
      assert_eq!(0, stats.leaf_overflow_n, "unexpected LeafOverflowN");
      assert_eq!(1, stats.key_n, "unexpected KeyN");
      assert_eq!(1, stats.depth, "unexpected Depth");
      assert_eq!(0, stats.branch_in_use, "unexpected BranchInuse");
      assert_eq!(0, stats.leaf_in_use, "unexpected LeafInuse");

      //TODO: Fails if page_size != 4096? Db.Info?
      assert_eq!(0, stats.branch_alloc, "unexpected BranchAlloc");
      assert_eq!(0, stats.leaf_alloc, "unexpected LeafAlloc");

      assert_eq!(1, stats.bucket_n, "unexpected BucketN");
      assert_eq!(1, stats.inline_bucket_n, "unexpected InlineBucketN");
      assert_eq!(
        16 + 16 + 6,
        stats.inline_bucket_in_use,
        "unexpected InlineBucketInuse"
      );

      Ok(())
    })?;
    Ok(())
  }

  #[test]
  fn test_bucket_stats_empty_bucket() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      tx.create_bucket("whozawhats")?;
      Ok(())
    })?;
    db.must_check();
    db.view(|tx| {
      let b = tx.bucket("whozawhats").unwrap();
      let stats = b.stats();
      assert_eq!(0, stats.branch_page_n, "unexpected BranchPageN");
      assert_eq!(0, stats.branch_overflow_n, "unexpected BranchOverflowN");
      assert_eq!(0, stats.leaf_page_n, "unexpected LeafPageN");
      assert_eq!(0, stats.leaf_overflow_n, "unexpected LeafOverflowN");
      assert_eq!(0, stats.key_n, "unexpected KeyN");
      assert_eq!(1, stats.depth, "unexpected Depth");
      assert_eq!(0, stats.branch_in_use, "unexpected BranchInuse");
      assert_eq!(0, stats.leaf_in_use, "unexpected LeafInuse");

      //TODO: Fails if page_size != 4096? Db.Info?
      assert_eq!(0, stats.branch_alloc, "unexpected BranchAlloc");
      assert_eq!(0, stats.leaf_alloc, "unexpected LeafAlloc");

      assert_eq!(1, stats.bucket_n, "unexpected BucketN");
      assert_eq!(1, stats.inline_bucket_n, "unexpected InlineBucketN");
      assert_eq!(
        16, stats.inline_bucket_in_use,
        "unexpected InlineBucketInuse"
      );

      Ok(())
    })?;
    Ok(())
  }

  #[test]
  fn test_bucket_stats_nested() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let mut b = tx.create_bucket("foo")?;
      for i in 0..100 {
        let i_str = format!("{:02}", i);
        b.put(&i_str, &i_str)?;
      }
      let mut bar = b.create_bucket("bar")?;
      for i in 0..10 {
        let i_str = format!("{}", i);
        bar.put(&i_str, &i_str)?;
      }
      let mut baz = bar.create_bucket("baz")?;
      for i in 0..10 {
        let i_str = format!("{}", i);
        baz.put(&i_str, &i_str)?;
      }
      Ok(())
    })?;
    db.must_check();

    db.view(|tx| {
      let b = tx.bucket("foo").unwrap();
      let stats = b.stats();
      assert_eq!(0, stats.branch_page_n, "unexpected BranchPageN");
      assert_eq!(0, stats.branch_overflow_n, "unexpected BranchOverflowN");
      assert_eq!(2, stats.leaf_page_n, "unexpected LeafPageN");
      assert_eq!(0, stats.leaf_overflow_n, "unexpected LeafOverflowN");
      assert_eq!(122, stats.key_n, "unexpected KeyN");
      assert_eq!(3, stats.depth, "unexpected Depth");
      assert_eq!(0, stats.branch_in_use, "unexpected BranchInuse");

      let mut foo = 16; // foo (pghdr)
      foo += 101 * 16; // foo leaf elements
      foo += 100 * 2 + 100 * 2; // foo leaf key/values
      foo += 3 + 16; // foo -> bar key/value

      let mut bar = 16; // bar (pghdr)
      bar += 11 * 16; // bar leaf elements
      bar += 10 + 10; // bar leaf key/values
      bar += 3 + 16; //bar -> baz key/value

      let mut baz = 16; // baz (inline) (pghdr)
      baz += 10 * 16; // baz leaf elements
      baz += 10 + 10; // baz leaf key/values

      assert_eq!(foo + bar + baz, stats.leaf_in_use, "unexpected LeafInuse");

      //TODO: Fails if page_size != 4096? Db.Info?
      assert_eq!(0, stats.branch_alloc, "unexpected BranchAlloc");
      assert_eq!(
        (page_size::get() * 2) as i64,
        stats.leaf_alloc,
        "unexpected LeafAlloc"
      );

      assert_eq!(3, stats.bucket_n, "unexpected BucketN");
      assert_eq!(1, stats.inline_bucket_n, "unexpected InlineBucketN");
      assert_eq!(
        baz, stats.inline_bucket_in_use,
        "unexpected InlineBucketInuse"
      );
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  #[cfg(feature = "long-tests")]
  fn test_bucket_stats_large() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    let mut index = 0;
    for _ in 0..100 {
      db.update(|mut tx| {
        let mut b = tx.create_bucket_if_not_exists("widgets")?;
        for _ in 0..1000 {
          let i_str = format!("{}", index);
          b.put(&i_str, &i_str)?;
          index += 1;
        }
        Ok(())
      })?;
    }

    db.must_check();

    // TODO: Support pagesize that's not 4096
    let stat_4096 = BucketStats {
      branch_page_n: 13,
      branch_overflow_n: 0,
      leaf_page_n: 1196,
      leaf_overflow_n: 0,
      key_n: 100_000,
      depth: 3,
      branch_alloc: 53_248,
      branch_in_use: 25_257,
      leaf_alloc: 4_898_816,
      leaf_in_use: 2_596_916,
      bucket_n: 1,
      inline_bucket_n: 0,
      inline_bucket_in_use: 0,
    };

    db.view(|tx| {
      let b = tx.bucket("widgets").unwrap();
      let stats = b.stats();
      assert_eq!(stat_4096, stats, "stats differs from expectations");
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  fn test_bucket_put_single() -> crate::Result<()> {
    let mut index = 0;
    quick_check(5, |d: &mut DummyVec| {
      let mut db = TestDb::new().unwrap();

      let mut m = HashMap::new();

      db.update(|mut tx| {
        tx.create_bucket("widgets")?;
        Ok(())
      })
      .unwrap();

      for entry in &d.values {
        db.update(|mut tx| {
          let mut b = tx.bucket_mut("widgets").unwrap();
          b.put(&entry.key, &entry.value).unwrap();
          m.insert(entry.key.clone(), entry.value.clone());
          Ok(())
        })
        .unwrap();
        db.must_check();
      }

      db.view(|tx| {
        let b = tx.bucket("widgets").unwrap();
        for (i, entry) in d.values.iter().enumerate() {
          let value = b.get(&entry.key).unwrap();
          assert_eq!(
            &entry.value,
            value,
            "values mismatch [run {}] ({} of {})",
            index,
            i,
            d.values.len()
          );
          //todo- copy temp if fails
        }
        Ok(())
      })
      .unwrap();
      index += 1;
      true
    });
    Ok(())
  }

  #[test]
  fn test_bucket_put_multiple() -> crate::Result<()> {
    let mut index = 0;
    quick_check(5, |d: &mut DummyVec| {
      let mut db = TestDb::new().unwrap();

      let mut m = HashMap::new();

      db.update(|mut tx| {
        tx.create_bucket("widgets")?;
        Ok(())
      })
      .unwrap();

      db.update(|mut tx| {
        let mut b = tx.bucket_mut("widgets").unwrap();
        for entry in &d.values {
          b.put(&entry.key, &entry.value).unwrap();
          m.insert(entry.key.clone(), entry.value.clone());
        }
        Ok(())
      })
      .unwrap();

      db.must_check();

      db.view(|tx| {
        let b = tx.bucket("widgets").unwrap();
        for (i, entry) in d.values.iter().enumerate() {
          let value = b.get(&entry.key).unwrap();
          assert_eq!(
            &entry.value,
            value,
            "values mismatch [run {}] ({} of {})",
            index,
            i,
            d.values.len()
          );
          //todo- copy temp if fails
        }
        Ok(())
      })
      .unwrap();
      index += 1;
      true
    });
    Ok(())
  }

  #[test]
  fn test_bucket_delete_quick() -> crate::Result<()> {
    let mut index = 0;
    quick_check(5, |d: &mut DummyVec| {
      let mut db = TestDb::new().unwrap();

      db.update(|mut tx| {
        tx.create_bucket("widgets")?;
        Ok(())
      })
      .unwrap();

      db.update(|mut tx| {
        let mut b = tx.bucket_mut("widgets").unwrap();
        for entry in &d.values {
          b.put(&entry.key, &entry.value).unwrap();
        }
        Ok(())
      })
      .unwrap();

      db.must_check();

      for (i, entry) in d.values.iter().enumerate() {
        db.update(|mut tx| {
          let mut b = tx.bucket_mut("widgets").unwrap();
          b.delete(&entry.key).unwrap();
          Ok(())
        })
        .unwrap();
        db.must_check();
      }

      db.view(|tx| {
        let b = tx.bucket("widgets").unwrap();
        for entry in &d.values {
          let value = b.get(&entry.key);
          assert!(
            value.is_none(),
            "bucket should be empty; found {:?}",
            &entry.key[0..3]
          );
          //todo- copy temp if fails
        }
        Ok(())
      })
      .unwrap();
      index += 1;
      true
    });
    Ok(())
  }
}
