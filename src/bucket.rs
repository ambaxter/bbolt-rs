use crate::common::bucket::{InBucket, IN_BUCKET_SIZE};
use crate::common::memory::{BCell, IsAligned, LCell};
use crate::common::meta::MetaPage;
use crate::common::page::{CoerciblePage, Page, RefPage, BUCKET_LEAF_FLAG, PAGE_HEADER_SIZE};
use crate::common::tree::{
  MappedBranchPage, MappedLeafPage, TreePage, BRANCH_PAGE_ELEMENT_SIZE, LEAF_PAGE_ELEMENT_SIZE,
};
use crate::common::{BVec, HashMap, PgId, SplitRef, ZERO_PGID};
use crate::cursor::{
  CursorApi, CursorIAPI, CursorImpl, CursorRwIAPI, CursorRwImpl, ElemRef, InnerCursor,
};
use crate::node::{NodeRwCell, NodeW};
use crate::tx::{TxApi, TxCell, TxIAPI, TxImplTODORenameMe, TxR, TxRwCell, TxRwIAPI, TxW};
use crate::Error::{
  BucketExists, BucketNameRequired, BucketNotFound, IncompatibleValue, KeyRequired, KeyTooLarge,
  ValueTooLarge,
};
use crate::{CursorRwApi, Error, TxRwApi};
use bumpalo::Bump;
use bytemuck::{Pod, Zeroable};
use either::Either;
use std::alloc::Layout;
use std::cell::{Ref, RefCell, RefMut};
use std::io::BufRead;
use std::marker::PhantomData;
use std::mem;
use std::ops::{AddAssign, Deref, DerefMut};
use std::ptr::slice_from_raw_parts_mut;
use std::rc::{Rc, Weak};

pub trait BucketApi<'tx>
where
  Self: Sized,
{
  type CursorType: CursorApi<'tx>;

  /// Root returns the root of the bucket.
  fn root(&self) -> PgId;

  /// Writable returns whether the bucket is writable.
  fn is_writeable(&self) -> bool;

  /// Cursor creates a cursor associated with the bucket.
  /// The cursor is only valid as long as the transaction is open.
  /// Do not use a cursor after the transaction is closed.
  fn cursor(&self) -> Self::CursorType;

  /// Bucket retrieves a nested bucket by name.
  /// Returns nil if the bucket does not exist.
  /// The bucket instance is only valid for the lifetime of the transaction.
  fn bucket(&self, name: &[u8]) -> Option<Self>;

  /// Get retrieves the value for a key in the bucket.
  /// Returns a nil value if the key does not exist or if the key is a nested bucket.
  /// The returned value is only valid for the life of the transaction.
  fn get(&self, key: &[u8]) -> Option<&'tx [u8]>;

  /// Sequence returns the current integer for the bucket without incrementing it.
  fn sequence(&self) -> u64;

  /// ForEach executes a function for each key/value pair in a bucket.
  /// Because ForEach uses a Cursor, the iteration over keys is in lexicographical order.
  /// If the provided function returns an error then the iteration is stopped and
  /// the error is returned to the caller. The provided function must not modify
  /// the bucket; this will result in undefined behavior.
  fn for_each<F: Fn(&[u8]) -> crate::Result<()>>(&self, f: F) -> crate::Result<()>;

  fn for_each_bucket<F: Fn(&[u8]) -> crate::Result<()>>(&self, f: F) -> crate::Result<()>;

  /// Stats returns stats on a bucket.
  fn stats(&self) -> BucketStats;
}

pub trait BucketRwApi<'tx>: BucketApi<'tx> {
  type CursorRwType: CursorRwApi<'tx>;

  /// CreateBucket creates a new bucket at the given key and returns the new bucket.
  /// Returns an error if the key already exists, if the bucket name is blank, or if the bucket name is too long.
  /// The bucket instance is only valid for the lifetime of the transaction.
  fn create_bucket(&mut self, key: &[u8]) -> crate::Result<Self>;

  /// CreateBucketIfNotExists creates a new bucket if it doesn't already exist and returns a reference to it.
  /// Returns an error if the bucket name is blank, or if the bucket name is too long.
  /// The bucket instance is only valid for the lifetime of the transaction.
  fn create_bucket_if_not_exists(&mut self, key: &[u8]) -> crate::Result<Self>;

  /// Cursor creates a cursor associated with the bucket.
  /// The cursor is only valid as long as the transaction is open.
  /// Do not use a cursor after the transaction is closed.
  fn cursor_mut(&self) -> Self::CursorRwType;

  /// DeleteBucket deletes a bucket at the given key.
  /// Returns an error if the bucket does not exist, or if the key represents a non-bucket value.
  fn delete_bucket(&mut self, key: &[u8]) -> crate::Result<()>;

  /// Put sets the value for a key in the bucket.
  /// If the key exist then its previous value will be overwritten.
  /// Supplied value must remain valid for the life of the transaction.
  /// Returns an error if the bucket was created from a read-only transaction, if the key is blank, if the key is too large, or if the value is too large.
  fn put(&mut self, key: &[u8], data: &[u8]) -> crate::Result<()>;

  /// Delete removes a key from the bucket.
  /// If the key does not exist then nothing is done and a nil error is returned.
  /// Returns an error if the bucket was created from a read-only transaction.
  fn delete(&mut self, key: &[u8]) -> crate::Result<()>;

  /// SetSequence updates the sequence number for the bucket.
  fn set_sequence(&mut self, v: u64) -> crate::Result<()>;

  /// NextSequence returns an autoincrementing integer for the bucket.
  fn next_sequence(&mut self) -> crate::Result<u64>;
}

pub struct BucketImpl<'tx> {
  b: BucketCell<'tx>,
}

impl<'tx> From<BucketCell<'tx>> for BucketImpl<'tx> {
  fn from(value: BucketCell<'tx>) -> Self {
    BucketImpl { b: value }
  }
}

impl<'tx> BucketApi<'tx> for BucketImpl<'tx> {
  type CursorType = CursorImpl<'tx, InnerCursor<'tx, TxCell<'tx>, BucketCell<'tx>>>;

  fn root(&self) -> PgId {
    self.b.root()
  }

  fn is_writeable(&self) -> bool {
    self.b.is_writeable()
  }

  fn cursor(&self) -> Self::CursorType {
    CursorImpl::new(InnerCursor::new(self.b, self.b.api_tx().bump()))
  }

  fn bucket(&self, name: &[u8]) -> Option<Self> {
    self.b.api_bucket(name).map(|b| BucketImpl { b })
  }

  fn get(&self, key: &[u8]) -> Option<&'tx [u8]> {
    self.b.api_get(key)
  }

  fn sequence(&self) -> u64 {
    self.b.api_sequence()
  }

  fn for_each<F: Fn(&[u8]) -> crate::Result<()>>(&self, f: F) -> crate::Result<()> {
    self.b.api_for_each(f)
  }

  fn for_each_bucket<F: Fn(&[u8]) -> crate::Result<()>>(&self, f: F) -> crate::Result<()> {
    self.b.api_for_each_bucket(f)
  }

  fn stats(&self) -> BucketStats {
    todo!()
  }
}

pub struct BucketRwImpl<'tx> {
  b: BucketRwCell<'tx>,
}

impl<'tx> From<BucketRwCell<'tx>> for BucketRwImpl<'tx> {
  fn from(value: BucketRwCell<'tx>) -> Self {
    BucketRwImpl { b: value }
  }
}

impl<'tx> BucketApi<'tx> for BucketRwImpl<'tx> {
  type CursorType = CursorImpl<'tx, InnerCursor<'tx, TxRwCell<'tx>, BucketRwCell<'tx>>>;

  fn root(&self) -> PgId {
    self.b.root()
  }

  fn is_writeable(&self) -> bool {
    self.b.is_writeable()
  }

  fn cursor(&self) -> Self::CursorType {
    CursorImpl::new(InnerCursor::new(self.b, self.b.api_tx().bump()))
  }

  fn bucket(&self, name: &[u8]) -> Option<Self> {
    self.b.api_bucket(name).map(|b| BucketRwImpl { b })
  }

  fn get(&self, key: &[u8]) -> Option<&'tx [u8]> {
    self.b.api_get(key)
  }

  fn sequence(&self) -> u64 {
    self.b.api_sequence()
  }

  fn for_each<F: Fn(&[u8]) -> crate::Result<()>>(&self, f: F) -> crate::Result<()> {
    self.b.api_for_each(f)
  }

  fn for_each_bucket<F: Fn(&[u8]) -> crate::Result<()>>(&self, f: F) -> crate::Result<()> {
    self.b.api_for_each_bucket(f)
  }

  fn stats(&self) -> BucketStats {
    todo!()
  }
}

impl<'tx> BucketRwApi<'tx> for BucketRwImpl<'tx> {
  type CursorRwType = CursorRwImpl<'tx, InnerCursor<'tx, TxRwCell<'tx>, BucketRwCell<'tx>>>;

  fn create_bucket(&mut self, key: &[u8]) -> crate::Result<Self> {
    self.b.api_create_bucket(key).map(|b| BucketRwImpl { b })
  }

  fn create_bucket_if_not_exists(&mut self, key: &[u8]) -> crate::Result<Self> {
    self
      .b
      .api_create_bucket_if_not_exists(key)
      .map(|b| BucketRwImpl { b })
  }

  fn cursor_mut(&self) -> Self::CursorRwType {
    CursorRwImpl::new(InnerCursor::new(self.b, self.b.api_tx().bump()))
  }

  fn delete_bucket(&mut self, key: &[u8]) -> crate::Result<()> {
    self.b.api_delete_bucket(key)
  }

  fn put(&mut self, key: &[u8], data: &[u8]) -> crate::Result<()> {
    self.b.api_put(key, data)
  }

  fn delete(&mut self, key: &[u8]) -> crate::Result<()> {
    self.b.api_delete(key)
  }

  fn set_sequence(&mut self, v: u64) -> crate::Result<()> {
    todo!()
  }

  fn next_sequence(&mut self) -> crate::Result<u64> {
    todo!()
  }
}

/// BucketStats records statistics about resources used by a bucket.
#[derive(Copy, Clone, Default)]
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
  key_n: i64,
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
const INLINE_PAGE_ALIGNMENT: usize = mem::align_of::<InlinePage>();
const INLINE_PAGE_SIZE: usize = mem::size_of::<InlinePage>();

pub(crate) const MIN_FILL_PERCENT: f64 = 0.1;
pub(crate) const MAX_FILL_PERCENT: f64 = 1.0;

/// A convenience struct representing an inline page header
#[repr(C)]
#[derive(Copy, Clone, Default, Pod, Zeroable)]
struct InlinePage {
  header: InBucket,
  page: Page,
}

/// The internal Bucket API
pub(crate) trait BucketIAPI<'tx, T: TxIAPI<'tx>>:
  SplitRef<BucketR<'tx>, Weak<T>, InnerBucketW<'tx, T, Self>>
{
  fn new_in(
    bump: &'tx Bump, bucket_header: InBucket, tx: Weak<T>, inline_page: Option<RefPage<'tx>>,
  ) -> Self;

  /// Returns whether the bucket is writable.
  fn is_writeable(&self) -> bool;

  /// Returns the rc ptr Tx of the bucket
  fn api_tx(self) -> Rc<T>;

  /// Returns the weak ptr to the Tx of the bucket
  fn weak_tx(self) -> Weak<T>;

  /// Returns the root page id of the bucket
  fn root(self) -> PgId {
    self.split_ref().0.bucket_header.root()
  }

  /// Create a new cursor for this Bucket
  fn i_cursor(self) -> InnerCursor<'tx, T, Self> {
    let tx = self.api_tx();
    tx.split_r_mut().stats.cursor_count += 1;
    InnerCursor::new(self, tx.bump())
  }

  /// See [BucketApi::bucket]
  fn api_bucket(self, name: &[u8]) -> Option<Self> {
    if let Some(w) = self.split_ow() {
      if let Some(child) = w.buckets.get(name) {
        return Some(*child);
      }
    }
    let mut c = self.i_cursor();
    // Move cursor to key.
    let (k, v, flags) = c.i_seek(name)?;
    // Return None if the key doesn't exist or it is not a bucket.
    if !(name == k) || (flags & BUCKET_LEAF_FLAG) == 0 {
      return None;
    }

    // Otherwise create a bucket and cache it.
    let child = self.open_bucket(v);

    if let (_, tx, Some(mut w)) = self.split_ref_mut() {
      let bump = tx.upgrade().unwrap().bump();
      let name = bump.alloc_slice_copy(name);
      w.buckets.insert(name, child);
    }

    Some(child)
  }

  /// Helper method that re-interprets a sub-bucket value
  /// from a parent into a Bucket
  fn open_bucket(self, mut value: &[u8]) -> Self {
    let tx = self.api_tx();
    let bump = tx.bump();
    // Unaligned access requires a copy to be made.
    //TODO: use std is_aligned_to when it comes out
    if !IsAligned::is_aligned_to::<InlinePage>(value.as_ptr()) {
      // TODO: Shove this into a centralized function somewhere
      let layout = Layout::from_size_align(value.len(), INLINE_PAGE_ALIGNMENT).unwrap();
      let new_value = unsafe {
        let mut new_value = bump.alloc_layout(layout);
        let new_value_ptr = new_value.as_mut() as *mut u8;
        &mut *slice_from_raw_parts_mut(new_value_ptr, value.len())
      };
      new_value.copy_from_slice(value);
      value = new_value;
    }
    let bucket_header = *bytemuck::from_bytes::<InBucket>(value.split_at(IN_BUCKET_SIZE).0);
    // Save a reference to the inline page if the bucket is inline.
    let ref_page = if bucket_header.root() == ZERO_PGID {
      assert!(
        value.len() >= INLINE_PAGE_SIZE,
        "subbucket value not large enough. Expected at least {} bytes. Was {}",
        INLINE_PAGE_SIZE,
        value.len()
      );
      unsafe {
        let ref_page_ptr = value.as_ptr().add(IN_BUCKET_SIZE);
        Some(RefPage::new(ref_page_ptr))
      }
    } else {
      None
    };
    Self::new_in(bump, bucket_header, Rc::downgrade(&tx), ref_page)
  }

  /// See [BucketApi::get]
  fn api_get(self, key: &[u8]) -> Option<&'tx [u8]> {
    let (k, v, flags) = self.i_cursor().i_seek(key).unwrap();
    // Return None if this is a bucket.
    if (flags & BUCKET_LEAF_FLAG) != 0 {
      return None;
    }
    // If our target node isn't the same key as what's passed in then return None.
    if key != k {
      return None;
    }
    Some(v)
  }

  /// See [BucketApi::for_each]
  fn api_for_each<F: Fn(&[u8]) -> crate::Result<()>>(self, f: F) -> crate::Result<()> {
    let mut c = self.i_cursor();
    let mut inode = c.i_first();
    while let Some((k, _, flags)) = inode {
      f(k)?;
      inode = c.i_next();
    }
    Ok(())
  }

  /// See [BucketApi::for_each_bucket]
  fn api_for_each_bucket<F: FnMut(&[u8]) -> crate::Result<()>>(
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
  fn for_each_page<F: FnMut(&RefPage, usize, &[PgId])>(self, mut f: F) {
    let root = {
      let r = self.split_r();
      let root = r.bucket_header.root();
      // If we have an inline page then just use that.
      if let Some(page) = &r.inline_page {
        f(page, 0, &[root]);
        return;
      }
      root
    };
    // Otherwise traverse the page hierarchy.
    TxImplTODORenameMe::for_each_page(self.api_tx().deref(), root, f);
  }

  /// forEachPageNode iterates over every page (or node) in a bucket.
  /// This also includes inline pages.
  fn for_each_page_node<F: FnMut(&Either<RefPage, NodeRwCell<'tx>>, usize) + Copy>(self, mut f: F) {
    let root = {
      let r = self.split_r();
      // If we have an inline page or root node then just use that.
      if let Some(page) = &r.inline_page {
        f(&Either::Left(*page), 0);
        return;
      }
      r.bucket_header.root()
    };
    self._for_each_page_node(root, 0, f);
  }

  fn _for_each_page_node<F: FnMut(&Either<RefPage, NodeRwCell<'tx>>, usize) + Copy>(
    self, root: PgId, depth: usize, mut f: F,
  ) {
    let pn = self.page_node(root);

    // Execute function.
    f(&pn, depth);

    // Recursively loop over children.
    match &pn {
      Either::Left(page) => {
        if let Some(branch_page) = MappedBranchPage::coerce_ref(page) {
          branch_page.elements().iter().for_each(|elem| {
            self._for_each_page_node(elem.pgid(), depth + 1, f);
          });
        }
      }
      Either::Right(node) => {
        let bump = self.api_tx().bump();
        // To keep with our rules we much copy the inode pgids to temporary storage first
        // This should be unnecessary, but working first *then* optimize
        let v = {
          let node_borrow = node.cell.borrow();
          let mut v = BVec::with_capacity_in(node_borrow.inodes.len(), bump);
          let ids = node_borrow.inodes.iter().map(|inode| inode.pgid());
          v.extend(ids);
          v
        };
        v.into_iter()
          .for_each(|pgid| self._for_each_page_node(pgid, depth + 1, f));
      }
    }
  }

  fn page_node(self, id: PgId) -> Either<RefPage<'tx>, NodeRwCell<'tx>> {
    let (r, w) = self.split_r_ow();
    // Inline buckets have a fake page embedded in their value so treat them
    // differently. We'll return the rootNode (if available) or the fake page.
    if r.bucket_header.root() == ZERO_PGID {
      if id != ZERO_PGID {
        panic!("inline bucket non-zero page access(2): {} != 0", id)
      }
      return if let Some(root_node) = &w.map(|wb| wb.root_node).flatten() {
        Either::Right(*root_node)
      } else {
        Either::Left(r.inline_page.unwrap())
      };
    }

    // Check the node cache for non-inline buckets.
    if let Some(wb) = &w {
      if let Some(node) = wb.nodes.get(&id) {
        return Either::Right(*node);
      }
    }

    Either::Left(self.api_tx().page(id))
  }

  /// See [BucketApi::sequence]
  fn api_sequence(self) -> u64 {
    self.split_ref().0.bucket_header.sequence()
  }

  /// Returns the maximum total size of a bucket to make it a candidate for inlining.
  fn max_inline_bucket_size(self) -> usize {
    self.api_tx().page_size() / 4
  }

  /// See [BucketApi::stats]
  fn api_stats(self) -> BucketStats {
    let mut s = BucketStats::default();
    let mut sub_stats = BucketStats::default();
    let page_size = self.api_tx().page_size();
    s.bucket_n += 1;
    if self.root() == ZERO_PGID {
      s.inline_bucket_n += 1;
    }
    self.for_each_page(|p, depth, stack| {
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
          for leaf_elem in leaf_page.elements() {
            if leaf_elem.is_bucket_entry() {
              // For any bucket element, open the element value
              // and recursively call Stats on the contained bucket.
              sub_stats += self.open_bucket(leaf_elem.as_ref().value()).api_stats();
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
}

pub(crate) trait BucketRwIAPI<'tx>: BucketIAPI<'tx, TxRwCell<'tx>> {
  /// Explicitly materialize the root node
  fn materialize_root(self) -> NodeRwCell<'tx>;

  /// See [BucketRwApi::create_bucket]
  fn api_create_bucket(self, key: &[u8]) -> crate::Result<Self>;

  /// See [BucketRwApi::create_bucket_if_not_exists]
  fn api_create_bucket_if_not_exists(self, key: &[u8]) -> crate::Result<Self>;

  /// See [BucketRwApi::delete_bucket]
  fn api_delete_bucket(self, key: &[u8]) -> crate::Result<()>;

  /// See [BucketRwApi::put]
  fn api_put(self, key: &[u8], value: &[u8]) -> crate::Result<()>;

  /// See [BucketRwApi::delete]
  fn api_delete(self, key: &[u8]) -> crate::Result<()>;

  /// See [BucketRwApi::set_sequence]
  fn api_set_sequence(cell: BucketRwCell<'tx>, v: u64) -> crate::Result<()>;

  /// See [BucketRwApi::next_sequence]
  fn api_next_sequence(cell: BucketRwCell<'tx>) -> crate::Result<u64>;

  /// free recursively frees all pages in the bucket.
  fn free(self);

  /// spill writes all the nodes for this bucket to dirty pages.
  fn spill(self, bump: &'tx Bump) -> crate::Result<()>;

  /// inlineable returns true if a bucket is small enough to be written inline
  /// and if it contains no subbuckets. Otherwise returns false.
  fn inlineable(self) -> bool;

  /// own_in removes all references to the old mmap.
  fn own_in(self);

  /// node creates a node from a page and associates it with a given parent.
  fn node(self, pgid: PgId, parent: Option<NodeRwCell<'tx>>) -> NodeRwCell<'tx>;
}

pub struct BucketR<'tx> {
  pub(crate) bucket_header: InBucket,
  /// inline page reference
  pub(crate) inline_page: Option<RefPage<'tx>>,
  p: PhantomData<&'tx u8>,
}

impl<'tx> BucketR<'tx> {
  pub fn new(in_bucket: InBucket) -> BucketR<'tx> {
    BucketR {
      bucket_header: in_bucket,
      inline_page: None,
      p: Default::default(),
    }
  }
}

pub struct InnerBucketW<'tx, T: TxIAPI<'tx>, B: BucketIAPI<'tx, T>> {
  /// materialized node for the root page.
  pub(crate) root_node: Option<NodeRwCell<'tx>>,
  /// subbucket cache
  buckets: HashMap<'tx, &'tx [u8], B>,
  /// node cache
  pub(crate) nodes: HashMap<'tx, PgId, NodeRwCell<'tx>>,


  /// Sets the threshold for filling nodes when they split. By default,
  /// the bucket will fill to 50% but it can be useful to increase this
  /// amount if you know that your write workloads are mostly append-only.
  ///
  /// This is non-persisted across transactions so it must be set in every Tx.
  pub(crate) fill_percent: f64,
  phantom_t: PhantomData<T>,
}

impl<'tx, T: TxIAPI<'tx>, B: BucketIAPI<'tx, T>> InnerBucketW<'tx, T, B> {
  pub fn new_in(bump: &'tx Bump) -> InnerBucketW<'tx, T, B> {
    InnerBucketW {
      root_node: None,
      buckets: HashMap::new_in(bump),
      nodes: HashMap::new_in(bump),
      fill_percent: DEFAULT_FILL_PERCENT,
      phantom_t: PhantomData,
    }
  }
}

pub type BucketW<'tx> = InnerBucketW<'tx, TxRwCell<'tx>, BucketRwCell<'tx>>;

pub struct BucketRW<'tx> {
  r: BucketR<'tx>,
  w: BucketW<'tx>,
}

impl<'tx> BucketRW<'tx> {
  pub fn new_in(bump: &'tx Bump, in_bucket: InBucket) -> BucketRW<'tx> {
    BucketRW {
      r: BucketR::new(in_bucket),
      w: BucketW::new_in(bump),
    }
  }
}

#[derive(Copy, Clone)]
pub struct BucketCell<'tx> {
  cell: BCell<'tx, BucketR<'tx>, Weak<TxCell<'tx>>>,
}

impl<'tx> BucketIAPI<'tx, TxCell<'tx>> for BucketCell<'tx> {
  fn new_in(
    bump: &'tx Bump, bucket_header: InBucket, tx: Weak<TxCell<'tx>>,
    inline_page: Option<RefPage<'tx>>,
  ) -> Self {
    let r = BucketR {
      bucket_header,
      inline_page,
      p: Default::default(),
    };

    BucketCell {
      cell: BCell::new_in(r, tx, bump),
    }
  }

  #[inline(always)]
  fn is_writeable(&self) -> bool {
    false
  }

  #[inline(always)]
  fn api_tx(self) -> Rc<TxCell<'tx>> {
    self.split_bound().upgrade().unwrap()
  }

  fn weak_tx(self) -> Weak<TxCell<'tx>> {
    self.split_bound().clone()
  }
}

impl<'tx> SplitRef<BucketR<'tx>, Weak<TxCell<'tx>>, InnerBucketW<'tx, TxCell<'tx>, BucketCell<'tx>>>
  for BucketCell<'tx>
{
  fn split_r(&self) -> Ref<BucketR<'tx>> {
    self.cell.0.borrow()
  }

  fn split_r_ow(
    &self,
  ) -> (
    Ref<BucketR<'tx>>,
    Option<Ref<InnerBucketW<'tx, TxCell<'tx>, BucketCell<'tx>>>>,
  ) {
    (self.cell.0.borrow(), None)
  }

  fn split_ow(&self) -> Option<Ref<InnerBucketW<'tx, TxCell<'tx>, BucketCell<'tx>>>> {
    None
  }

  fn split_bound(&self) -> Weak<TxCell<'tx>> {
    self.cell.1.clone()
  }

  fn split_ref(
    &self,
  ) -> (
    Ref<BucketR<'tx>>,
    Weak<TxCell<'tx>>,
    Option<Ref<InnerBucketW<'tx, TxCell<'tx>, BucketCell<'tx>>>>,
  ) {
    (self.cell.0.borrow(), self.cell.1.clone(), None)
  }

  fn split_r_mut(&self) -> RefMut<BucketR<'tx>> {
    self.cell.0.borrow_mut()
  }

  fn split_r_ow_mut(
    &self,
  ) -> (
    RefMut<BucketR<'tx>>,
    Option<RefMut<InnerBucketW<'tx, TxCell<'tx>, BucketCell<'tx>>>>,
  ) {
    (self.cell.0.borrow_mut(), None)
  }

  fn split_ow_mut(&self) -> Option<RefMut<InnerBucketW<'tx, TxCell<'tx>, BucketCell<'tx>>>> {
    None
  }

  fn split_ref_mut(
    &self,
  ) -> (
    RefMut<BucketR<'tx>>,
    Weak<TxCell<'tx>>,
    Option<RefMut<InnerBucketW<'tx, TxCell<'tx>, BucketCell<'tx>>>>,
  ) {
    (self.cell.0.borrow_mut(), self.cell.1.clone(), None)
  }
}

#[derive(Copy, Clone)]
pub struct BucketRwCell<'tx> {
  cell: BCell<'tx, BucketRW<'tx>, Weak<TxRwCell<'tx>>>,
}

impl<'tx> SplitRef<BucketR<'tx>, Weak<TxRwCell<'tx>>, BucketW<'tx>> for BucketRwCell<'tx> {
  fn split_r(&self) -> Ref<BucketR<'tx>> {
    Ref::map(self.cell.0.borrow(), |b| &b.r)
  }

  fn split_r_ow(&self) -> (Ref<BucketR<'tx>>, Option<Ref<BucketW<'tx>>>) {
    let (r, w) = Ref::map_split(self.cell.0.borrow(), |b| (&b.r, &b.w));
    (r, Some(w))
  }

  fn split_ow(&self) -> Option<Ref<BucketW<'tx>>> {
    Some(Ref::map(self.cell.0.borrow(), |b| &b.w))
  }

  fn split_bound(&self) -> Weak<TxRwCell<'tx>> {
    self.cell.1.clone()
  }

  fn split_ref(
    &self,
  ) -> (
    Ref<BucketR<'tx>>,
    Weak<TxRwCell<'tx>>,
    Option<Ref<BucketW<'tx>>>,
  ) {
    let (r, w) = Ref::map_split(self.cell.0.borrow(), |b| (&b.r, &b.w));
    (r, self.cell.1.clone(), Some(w))
  }

  fn split_r_mut(&self) -> RefMut<BucketR<'tx>> {
    RefMut::map(self.cell.0.borrow_mut(), |b| &mut b.r)
  }

  fn split_r_ow_mut(&self) -> (RefMut<BucketR<'tx>>, Option<RefMut<BucketW<'tx>>>) {
    let (r, w) = RefMut::map_split(self.cell.0.borrow_mut(), |b| (&mut b.r, &mut b.w));
    (r, Some(w))
  }

  fn split_ow_mut(&self) -> Option<RefMut<BucketW<'tx>>> {
    Some(RefMut::map(self.cell.0.borrow_mut(), |b| &mut b.w))
  }

  fn split_ref_mut(
    &self,
  ) -> (
    RefMut<BucketR<'tx>>,
    Weak<TxRwCell<'tx>>,
    Option<RefMut<BucketW<'tx>>>,
  ) {
    let (r, w) = RefMut::map_split(self.cell.0.borrow_mut(), |b| (&mut b.r, &mut b.w));
    (r, self.cell.1.clone(), Some(w))
  }
}

impl<'tx> BucketIAPI<'tx, TxRwCell<'tx>> for BucketRwCell<'tx> {
  fn new_in(
    bump: &'tx Bump, bucket_header: InBucket, tx: Weak<TxRwCell<'tx>>,
    inline_page: Option<RefPage<'tx>>,
  ) -> Self {
    let r = BucketR {
      bucket_header,
      inline_page,
      p: Default::default(),
    };

    let w = BucketW::new_in(bump);

    BucketRwCell {
      cell: BCell::new_in(BucketRW { r, w }, tx, bump),
    }
  }

  #[inline(always)]
  fn is_writeable(&self) -> bool {
    true
  }

  fn api_tx(self) -> Rc<TxRwCell<'tx>> {
    self.split_bound().upgrade().unwrap()
  }

  fn weak_tx(self) -> Weak<TxRwCell<'tx>> {
    self.split_bound().clone()
  }
}

impl<'tx> BucketRwIAPI<'tx> for BucketRwCell<'tx> {
  fn materialize_root(self) -> NodeRwCell<'tx> {
    let mut root_id = ZERO_PGID;
    if let (r, Some(w)) = self.split_r_ow() {
      match w.root_node {
        None => root_id = r.bucket_header.root(),
        Some(root_node) => return root_node,
      }
    }
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

    let mut inline_page = InlinePage::default();
    inline_page.page.set_leaf();
    let layout = Layout::from_size_align(INLINE_PAGE_SIZE, INLINE_PAGE_ALIGNMENT).unwrap();
    let bump = self.api_tx().bump();
    let value = unsafe {
      let mut data = bump.alloc_layout(layout);
      &mut *slice_from_raw_parts_mut(data.as_mut() as *mut u8, INLINE_PAGE_SIZE)
    };
    value.copy_from_slice(bytemuck::bytes_of(&inline_page));
    let key = bump.alloc_slice_clone(key) as &[u8];

    c.node().put(key, key, value, ZERO_PGID, BUCKET_LEAF_FLAG);

    self.split_r_mut().inline_page = None;

    return Ok(self.api_bucket(key).unwrap());
  }

  fn api_create_bucket_if_not_exists(self, key: &[u8]) -> crate::Result<Self> {
    match self.api_create_bucket(key) {
      Ok(child) => Ok(child),
      Err(error) => {
        if error == BucketExists {
          return Ok(self.api_bucket(key).unwrap());
        } else {
          return Err(error);
        }
      }
    }
  }

  fn api_delete_bucket(self, key: &[u8]) -> crate::Result<()> {
    let mut c = self.i_cursor();

    let (k, _, flags) = c.i_seek(key).unwrap();
    if key != k {
      return Err(BucketNotFound);
    } else if flags & BUCKET_LEAF_FLAG != 0 {
      return Err(IncompatibleValue);
    }

    let child = self.api_bucket(key).unwrap();
    child.api_for_each_bucket(|k| {
      match self.api_delete_bucket(k) {
        Ok(_) => Ok(()),
        // TODO: Ideally we want to properly chain errors here
        Err(e) => Err(Error::Other(e.into())),
      }
    })?;

    if let Some(mut w) = self.split_ow_mut() {
      w.buckets.remove(key);
    }

    if let Some(mut w) = child.split_ow_mut() {
      w.nodes.clear();
      w.root_node = None;
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
    let (k, _, flags) = c.i_seek(key).unwrap();

    if (flags & BUCKET_LEAF_FLAG) != 0 || key != k {
      return Err(IncompatibleValue);
    }
    let bump = self.api_tx().bump();
    let key = &*bump.alloc_slice_clone(key);
    let value = &*bump.alloc_slice_clone(value);
    c.node().put(key, key, value, ZERO_PGID, 0);
    Ok(())
  }

  fn api_delete(self, key: &[u8]) -> crate::Result<()> {
    let mut c = self.i_cursor();
    let (k, _, flags) = c.i_seek(key).unwrap();

    if key != k {
      return Ok(());
    }

    if flags & BUCKET_LEAF_FLAG != 0 {
      return Err(IncompatibleValue);
    }

    c.node().del(key);

    Ok(())
  }

  fn api_set_sequence(cell: BucketRwCell<'tx>, v: u64) -> crate::Result<()> {
    cell.materialize_root();
    cell.split_r_mut().bucket_header.set_sequence(v);
    Ok(())
  }

  fn api_next_sequence(cell: BucketRwCell<'tx>) -> crate::Result<u64> {
    cell.materialize_root();
    let mut r = cell.split_r_mut();
    r.bucket_header.inc_sequence();
    Ok(r.bucket_header.sequence())
  }

  fn free(self) {
    if self.split_r().bucket_header.root() == ZERO_PGID {
      return;
    }

    let txid = self.api_tx().meta().txid();

    self.for_each_page_node(|pn, depth| match pn {
      Either::Left(page) => self.api_tx().freelist().free(txid, page),
      Either::Right(node) => node.free(),
    });
  }

  /// spill writes all the nodes for this bucket to dirty pages.
  fn spill(self, bump: &'tx Bump) -> crate::Result<()> {
    // To keep with our rules we much copy the bucket entries to temporary storage first
    // This should be unnecessary, but working first *then* optimize
    let v = {
      let bucket_mut = self.split_ow();
      let w = bucket_mut.unwrap();
      let mut v = BVec::with_capacity_in(w.buckets.len(), bump);
      // v.extend() would be more idiomatic, but I'm too tired atm to figure out why
      // it's not working
      w.buckets.iter().for_each(|(k, b)| {
        v.push((*k, *b));
      });
      v
    };

    for (name, child) in v.into_iter() {}

    todo!()
  }

  fn inlineable(self) -> bool {
    let b = self.split_ow();
    let w = b.unwrap();

    // Bucket must only contain a single leaf node.
    let n = match w.root_node {
      None => return false,
      Some(n) => n,
    };
    let node_ref = n.cell.borrow();
    if node_ref.is_leaf {
      return false;
    }

    // Bucket is not inlineable if it contains subbuckets or if it goes beyond
    // our threshold for inline bucket size.
    let mut size = PAGE_HEADER_SIZE;
    for inode in &node_ref.inodes {
      size += LEAF_PAGE_ELEMENT_SIZE + inode.key().len() + inode.value().len();

      if inode.flags() & BUCKET_LEAF_FLAG != 0 {
        return false;
      } else if size > self.max_inline_bucket_size() {
        return false;
      }
    }

    true
  }

  fn own_in(self) {
    let (bump, root, children) = {
      let (r, tx, w) = self.split_ref();
      let bump = tx.upgrade().unwrap().bump();
      let wb = w.unwrap();
      let mut children: BVec<BucketRwCell<'tx>> = BVec::with_capacity_in(wb.buckets.len(), bump);
      children.extend(wb.buckets.values());
      (bump, wb.root_node, children)
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
      let (r, w) = self.split_r_ow_mut();
      let wb = w.unwrap();

      // Retrieve node if it's already been created.
      if let Some(n) = wb.nodes.get(&pgid) {
        return *n;
      }
      r.inline_page
    };

    // Otherwise create a node and cache it.
    // Use the inline page if this is an inline bucket.
    let page = match inline_page {
      None => self.api_tx().page(pgid),
      Some(page) => page,
    };

    // Read the page into the node and cache it.
    let n = NodeRwCell::read_in(self, parent, &page);
    let w = self.split_ow_mut();
    let mut wb = w.unwrap();
    match parent {
      None => wb.root_node = Some(n),
      Some(parent_node) => parent_node.cell.borrow_mut().children.push(n),
    }

    wb.nodes.insert(pgid, n);
    n
  }
}
