use crate::bucket::{
  BucketCell, BucketIAPI, BucketImpl, BucketRW, BucketRwCell, BucketRwIAPI, BucketRwImpl,
};
use crate::common::defaults::DEFAULT_PAGE_SIZE;
use crate::common::memory::SCell;
use crate::common::meta::Meta;
use crate::common::page::{MutPage, PageInfo, RefPage};
use crate::common::selfowned::SelfOwned;
use crate::common::{BVec, HashMap, PgId, SplitRef, TxId};
use crate::cursor::{CursorImpl, CursorRwIAPI, CursorRwImpl, InnerCursor};
use crate::db::{DBShared, Pager};
use crate::freelist::Freelist;
use crate::node::NodeRwCell;
use crate::{BucketApi, CursorApi, CursorRwApi};
use bumpalo::Bump;
use parking_lot::{RwLockReadGuard, RwLockWriteGuard};
use std::borrow::Cow;
use std::cell::{Ref, RefCell, RefMut};
use std::marker::{PhantomData, PhantomPinned};
use std::mem::MaybeUninit;
use std::ops::{AddAssign, Deref, DerefMut, Sub};
use std::pin::Pin;
use std::ptr::{addr_of, addr_of_mut};
use std::rc::Rc;
use std::time::Duration;
use std::{cell, mem};

pub trait TxApi<'tx> {
  type CursorType: CursorApi<'tx>;
  type BucketType: BucketApi<'tx>;

  /// ID returns the transaction id.
  fn id(&self) -> TxId;

  /// Size returns current database size in bytes as seen by this transaction.
  fn size(&self) -> u64;

  /// Writable returns whether the transaction can perform write operations.
  fn writeable(&self) -> bool;

  /// Cursor creates a cursor associated with the root bucket.
  /// All items in the cursor will return a nil value because all root bucket keys point to buckets.
  /// The cursor is only valid as long as the transaction is open.
  /// Do not use a cursor after the transaction is closed.
  fn cursor(&self) -> Self::CursorType;

  /// Stats retrieves a copy of the current transaction statistics.
  fn stats(&self) -> TxStats;

  /// Bucket retrieves a bucket by name.
  /// Returns nil if the bucket does not exist.
  /// The bucket instance is only valid for the lifetime of the transaction.
  fn bucket(&self, name: &[u8]) -> Option<Self::BucketType>;

  fn for_each<F: FnMut(&[u8], Self::BucketType)>(&self, f: F) -> crate::Result<()>;

  /// Rollback closes the transaction and ignores all previous updates. Read-only
  /// transactions must be rolled back and not committed.
  fn rollback(self) -> crate::Result<()>;

  /// Page returns page information for a given page number.
  /// This is only safe for concurrent use when used by a writable transaction.
  fn page(&self, id: PgId) -> crate::Result<Option<PageInfo>>;
}

pub trait TxRwApi<'tx>: TxApi<'tx> {
  type CursorRwType: CursorRwApi<'tx>;

  /// Cursor creates a cursor associated with the root bucket.
  /// All items in the cursor will return a nil value because all root bucket keys point to buckets.
  /// The cursor is only valid as long as the transaction is open.
  /// Do not use a cursor after the transaction is closed.
  fn cursor_mut(&mut self) -> Self::CursorRwType;

  /// CreateBucket creates a new bucket.
  /// Returns an error if the bucket already exists, if the bucket name is blank, or if the bucket name is too long.
  /// The bucket instance is only valid for the lifetime of the transaction.
  fn create_bucket(&mut self, name: &[u8]) -> crate::Result<Self::BucketType>;

  /// CreateBucketIfNotExists creates a new bucket if it doesn't already exist.
  /// Returns an error if the bucket name is blank, or if the bucket name is too long.
  /// The bucket instance is only valid for the lifetime of the transaction.
  fn create_bucket_if_not_exists(&mut self, name: &[u8]) -> crate::Result<Self::BucketType>;

  /// DeleteBucket deletes a bucket.
  /// Returns an error if the bucket cannot be found or if the key represents a non-bucket value.
  fn delete_bucket(&mut self, name: &[u8]) -> crate::Result<()>;

  /// Commit writes all changes to disk and updates the meta page.
  /// Returns an error if a disk write error occurs, or if Commit is
  /// called on a read-only transaction.
  fn commit(self) -> crate::Result<()>;
}

#[derive(Copy, Clone, Default)]
pub struct TxStats {
  // Page statistics.
  //
  /// number of page allocations
  page_count: i64,
  /// total bytes allocated
  page_alloc: i64,

  // Cursor statistics.
  //
  /// number of cursors created
  cursor_count: i64,

  // Node statistics
  //
  /// number of node allocations
  node_count: i64,
  /// number of node dereferences
  node_deref: i64,

  // Rebalance statistics.
  //
  /// number of node rebalances
  rebalance: i64,
  /// total time spent rebalancing
  rebalance_time: Duration,

  // Split/Spill statistics.
  //
  /// number of nodes split
  split: i64,
  /// number of nodes spilled
  spill: i64,
  /// total time spent spilling
  spill_time: Duration,

  // Write statistics.
  //
  /// number of writes performed
  write: i64,
  /// total time spent writing to disk
  write_time: Duration,
}

impl AddAssign<TxStats> for TxStats {
  fn add_assign(&mut self, rhs: TxStats) {
    self.page_count += rhs.page_count;
    self.page_alloc += rhs.page_alloc;
    self.cursor_count += rhs.cursor_count;
    self.node_count += rhs.node_count;
    self.node_deref += rhs.node_deref;
    self.rebalance += rhs.rebalance;
    self.rebalance_time += rhs.rebalance_time;
    self.split += rhs.split;
    self.spill += rhs.spill;
    self.spill_time += rhs.spill_time;
    self.write += rhs.write;
    self.write_time += rhs.write_time;
  }
}

impl Sub<TxStats> for TxStats {
  type Output = TxStats;

  fn sub(self, rhs: TxStats) -> Self::Output {
    TxStats {
      page_count: self.page_count - rhs.page_count,
      page_alloc: self.page_alloc - rhs.page_alloc,
      cursor_count: self.cursor_count - rhs.cursor_count,
      node_count: self.node_count - rhs.node_count,
      node_deref: self.node_deref - rhs.node_deref,
      rebalance: self.rebalance - rhs.rebalance,
      rebalance_time: self.rebalance_time - rhs.rebalance_time,
      split: self.split - rhs.split,
      spill: self.spill - rhs.spill,
      spill_time: self.spill_time - rhs.spill_time,
      write: self.write - rhs.write,
      write_time: self.write_time - rhs.write_time,
    }
  }
}

//TODO: now that the layout has been filled out these aren't needed anymore I don't think
pub(crate) trait TxIAPI<'tx>: SplitRef<TxR<'tx>, Self::BucketType, TxW<'tx>> {
  type BucketType: BucketIAPI<'tx, Self>;

  fn bump(self) -> &'tx Bump {
    let (r, _, _) = self.split_ref();
    r.b
  }

  fn page_size(self) -> usize {
    let (r, _, _) = self.split_ref();
    r.page_size
  }

  fn meta<'a>(&'a self) -> Ref<'a, Meta>
  where
    'tx: 'a,
  {
    let (r, _, _) = self.split_ref();
    Ref::map(r, |tx| &tx.meta)
  }

  fn page(self, id: PgId) -> RefPage<'tx> {
    let (r, _, _) = self.split_ref();
    r.pager.page(id)
  }

  fn api_id(self) -> TxId {
    let (r, _, _) = self.split_ref();
    r.meta.txid()
  }

  fn api_size(self) -> u64 {
    let (r, _, _) = self.split_ref();
    r.meta.pgid().0 * r.meta.page_size() as u64
  }

  fn api_writeable(self) -> bool {
    let (_, _, w) = self.split_ref();
    w.is_some()
  }

  fn api_cursor(self) -> InnerCursor<'tx, Self, Self::BucketType> {
    let root_bucket = self.root_bucket();
    root_bucket.i_cursor()
  }

  fn api_stats(self) -> TxStats {
    let (r, _, _) = self.split_ref();
    r.stats
  }

  fn root_bucket(self) -> Self::BucketType {
    *self.split_ref().1
  }

  fn api_bucket(self, name: &[u8]) -> Option<Self::BucketType> {
    let root_bucket = self.root_bucket();
    root_bucket.api_bucket(name)
  }

  fn api_for_each<F: FnMut(&[u8], Self::BucketType)>(&self, mut f: F) -> crate::Result<()> {
    let root_bucket = self.root_bucket();
    // TODO: Are we calling the right function?
    root_bucket.api_for_each_bucket(|k| {
      let bucket = root_bucket.api_bucket(k).unwrap();
      Ok(f(k, bucket))
    })
  }

  fn api_rollback(self) -> crate::Result<()> {
    todo!()
  }

  fn non_physical_rollback(self) -> crate::Result<()>;

  fn rollback(self) -> crate::Result<()> {
    let (mut r, _, _) = self.split_ref_mut();
    r.is_rollback = true;
    Ok(())
  }

  fn api_page(&self, id: PgId) -> crate::Result<Option<PageInfo>> {
    let (r, _, _) = self.split_ref();
    if id >= r.meta.pgid() {
      return Ok(None);
    }
    //TODO: Check if freelist loaded
    //WHEN: Freelists can be unloaded

    let p = r.pager.page(id);
    let id = p.id;
    let count = p.count as u64;
    let overflow_count = p.overflow as u64;

    let t = if r.pager.page_is_free(id) {
      Cow::Borrowed("free")
    } else {
      p.page_type()
    };
    let info = PageInfo {
      id: id.0,
      t,
      count,
      overflow_count,
    };
    Ok(Some(info))
  }

  fn close(self) -> crate::Result<()>;
}

pub(crate) trait TxRwIAPI<'tx>: TxIAPI<'tx> {
  type CursorRwType: CursorRwIAPI<'tx>;
  fn freelist(self) -> RefMut<'tx, Freelist>;

  fn allocate(self, count: usize) -> crate::Result<MutPage<'tx>>;

  fn api_cursor_mut(self) -> Self::CursorRwType;

  fn api_create_bucket(self, name: &[u8]) -> crate::Result<Self::BucketType>;

  fn api_create_bucket_if_not_exist(self, name: &[u8]) -> crate::Result<Self::BucketType>;

  fn api_delete_bucket(self, name: &[u8]) -> crate::Result<()>;

  fn api_commit(self) -> crate::Result<()>;
}

pub(crate) struct TxImplTODORenameMe {}

impl TxImplTODORenameMe {
  pub(crate) fn for_each_page<'tx, T: TxIAPI<'tx>, F: FnMut(&RefPage, usize, &[PgId])>(
    cell: &T, root: PgId, f: F,
  ) {
    todo!()
  }
}

pub struct TxR<'tx> {
  b: &'tx Bump,
  page_size: usize,
  pager: &'tx dyn Pager,
  stats: TxStats,
  meta: Meta,
  is_rollback: bool,
  p: PhantomData<&'tx u8>,
}

pub struct TxW<'tx> {
  pages: HashMap<'tx, PgId, MutPage<'tx>>,
  //TODO: We leak memory when this drops. Need special handling here
  commit_handlers: BVec<'tx, Box<dyn FnMut()>>,
  p: PhantomData<&'tx u8>,
}

pub struct TxRW<'tx> {
  r: TxR<'tx>,
  w: TxW<'tx>,
}

#[derive(Copy, Clone)]
pub struct TxCell<'tx> {
  pub(crate) cell: SCell<'tx, (TxR<'tx>, BucketCell<'tx>)>,
}

impl<'tx> SplitRef<TxR<'tx>, BucketCell<'tx>, TxW<'tx>> for TxCell<'tx> {
  fn split_ref(&self) -> (Ref<TxR<'tx>>, Ref<BucketCell<'tx>>, Option<Ref<TxW<'tx>>>) {
    let (r, bucket) = Ref::map_split(self.cell.borrow(), |c| (&c.0, &c.1));
    (r, bucket, None)
  }

  fn split_ref_mut(
    &self,
  ) -> (
    RefMut<TxR<'tx>>,
    RefMut<BucketCell<'tx>>,
    Option<RefMut<TxW<'tx>>>,
  ) {
    let (r, bucket) = RefMut::map_split(self.cell.borrow_mut(), |c| (&mut c.0, &mut c.1));
    (r, bucket, None)
  }
}

impl<'tx> TxIAPI<'tx> for TxCell<'tx> {
  type BucketType = BucketCell<'tx>;

  fn non_physical_rollback(self) -> crate::Result<()> {
    todo!()
  }

  fn close(self) -> crate::Result<()> {
    todo!()
  }
}

#[derive(Copy, Clone)]
pub struct TxRwCell<'tx> {
  pub(crate) cell: SCell<'tx, (TxRW<'tx>, BucketRwCell<'tx>)>,
}

impl<'tx> SplitRef<TxR<'tx>, BucketRwCell<'tx>, TxW<'tx>> for TxRwCell<'tx> {
  fn split_ref(&self) -> (Ref<TxR<'tx>>, Ref<BucketRwCell<'tx>>, Option<Ref<TxW<'tx>>>) {
    let (bucket, rw) = Ref::map_split(self.cell.borrow(), |c| (&c.1, &c.0));
    let (r, w) = Ref::map_split(rw, |b| (&b.r, &b.w));
    (r, bucket, Some(w))
  }

  fn split_ref_mut(
    &self,
  ) -> (
    RefMut<TxR<'tx>>,
    RefMut<BucketRwCell<'tx>>,
    Option<RefMut<TxW<'tx>>>,
  ) {
    let (bucket, rw) = RefMut::map_split(self.cell.borrow_mut(), |c| (&mut c.1, &mut c.0));
    let (r, w) = RefMut::map_split(rw, |b| (&mut b.r, &mut b.w));
    (r, bucket, Some(w))
  }
}

impl<'tx> TxIAPI<'tx> for TxRwCell<'tx> {
  type BucketType = BucketRwCell<'tx>;

  fn non_physical_rollback(self) -> crate::Result<()> {
    todo!()
  }

  fn close(self) -> crate::Result<()> {
    todo!()
  }
}

impl<'tx> TxRwIAPI<'tx> for TxRwCell<'tx> {
  type CursorRwType = InnerCursor<'tx, Self, Self::BucketType>;

  fn freelist(self) -> RefMut<'tx, Freelist> {
    todo!()
  }

  fn allocate(self, count: usize) -> crate::Result<MutPage<'tx>> {
    todo!()
  }

  fn api_cursor_mut(self) -> Self::CursorRwType {
    let root_bucket = self.root_bucket();
    root_bucket.i_cursor()
  }

  fn api_create_bucket(self, name: &[u8]) -> crate::Result<Self::BucketType> {
    let root_bucket = self.root_bucket();
    root_bucket.api_create_bucket(name)
  }

  fn api_create_bucket_if_not_exist(self, name: &[u8]) -> crate::Result<Self::BucketType> {
    let root_bucket = self.root_bucket();
    root_bucket.api_create_bucket_if_not_exists(name)
  }

  fn api_delete_bucket(self, name: &[u8]) -> crate::Result<()> {
    let root_bucket = self.root_bucket();
    root_bucket.api_delete_bucket(name)
  }

  fn api_commit(self) -> crate::Result<()> {
    todo!("api_commit")
  }
}

pub struct TxImpl<'tx> {
  bump: Pin<Box<Bump>>,
  lock: Pin<Box<RwLockReadGuard<'tx, DBShared>>>,
  tx: Pin<Rc<TxCell<'tx>>>,
}

impl<'tx> TxImpl<'tx> {
  pub(crate) fn new(lock: RwLockReadGuard<'tx, DBShared>) -> TxImpl<'tx> {
    let bump = lock.records.lock().pop_read_bump();
    let meta = lock.backend.meta();
    let page_size = meta.page_size() as usize;
    let inline_bucket = meta.root();
    let mut uninit: MaybeUninit<TxImpl<'tx>> = MaybeUninit::uninit();
    let ptr = uninit.as_mut_ptr();
    unsafe {
      addr_of_mut!((*ptr).bump).write(Box::pin(bump));
      let bump = &(**addr_of!((*ptr).bump));
      addr_of_mut!((*ptr).lock).write(Box::pin(lock));
      let pager: &dyn Pager = &(**addr_of!((*ptr).lock));
      let tx = Rc::new_cyclic(|weak| {
        let r = TxR {
          b: bump,
          page_size,
          pager,
          meta,
          stats: Default::default(),
          is_rollback: false,
          p: Default::default(),
        };
        let bucket = BucketCell::new_in(bump, inline_bucket, weak.clone(), None);
        TxCell {
          cell: SCell::new_in((r, bucket), bump),
        }
      });
      addr_of_mut!((*ptr).tx).write(Pin::new(tx));
      uninit.assume_init()
    }
  }

  pub(crate) fn get_ref(&self) -> TxRef<'tx> {
    TxRef {
      tx: TxCell { cell: self.tx.cell },
    }
  }
}

impl<'tx> Drop for TxImpl<'tx> {
  fn drop(&mut self) {
    let tx_id = self.id();
    let stats = self.stats();
    let mut swap_bump = Bump::with_capacity(0);
    mem::swap(&mut swap_bump, &mut self.bump);
    let mut records = self.lock.records.lock();
    records.remove_tx(tx_id, stats, swap_bump);
  }
}

impl<'tx> TxApi<'tx> for TxImpl<'tx> {
  type CursorType = CursorImpl<'tx, InnerCursor<'tx, TxCell<'tx>, BucketCell<'tx>>>;
  type BucketType = BucketImpl<'tx>;

  fn id(&self) -> TxId {
    self.tx.api_id()
  }

  fn size(&self) -> u64 {
    self.tx.api_size()
  }

  fn writeable(&self) -> bool {
    false
  }

  fn cursor(&self) -> Self::CursorType {
    self.tx.api_cursor().into()
  }

  fn stats(&self) -> TxStats {
    self.tx.api_stats()
  }

  fn bucket(&self, name: &[u8]) -> Option<Self::BucketType> {
    self.tx.api_bucket(name).map(|b| b.into())
  }

  fn for_each<F: FnMut(&[u8], Self::BucketType)>(&self, f: F) -> crate::Result<()> {
    todo!()
  }

  fn rollback(self) -> crate::Result<()> {
    let _ = self.tx.rollback();

    Ok(())
  }

  fn page(&self, id: PgId) -> crate::Result<Option<PageInfo>> {
    self.tx.api_page(id)
  }
}

pub struct TxRef<'tx> {
  tx: TxCell<'tx>,
}

impl<'tx> TxApi<'tx> for TxRef<'tx> {
  type CursorType = CursorImpl<'tx, InnerCursor<'tx, TxCell<'tx>, BucketCell<'tx>>>;
  type BucketType = BucketImpl<'tx>;

  fn id(&self) -> TxId {
    self.tx.api_id()
  }

  fn size(&self) -> u64 {
    self.tx.api_size()
  }

  fn writeable(&self) -> bool {
    false
  }

  fn cursor(&self) -> Self::CursorType {
    self.tx.api_cursor().into()
  }

  fn stats(&self) -> TxStats {
    self.tx.api_stats()
  }

  fn bucket(&self, name: &[u8]) -> Option<Self::BucketType> {
    self.tx.api_bucket(name).map(|b| b.into())
  }

  fn for_each<F: FnMut(&[u8], Self::BucketType)>(&self, f: F) -> crate::Result<()> {
    todo!()
  }

  fn rollback(self) -> crate::Result<()> {
    self.tx.rollback()
  }

  fn page(&self, id: PgId) -> crate::Result<Option<PageInfo>> {
    self.tx.api_page(id)
  }
}

pub struct TxRwImpl<'tx> {
  bump: Pin<Box<Bump>>,
  lock: Pin<Box<RwLockWriteGuard<'tx, DBShared>>>,
  tx: Pin<Rc<TxRwCell<'tx>>>,
}

impl<'tx> TxRwImpl<'tx> {
  pub(crate) fn get_ref(&self) -> TxRwRef<'tx> {
    TxRwRef {
      tx: TxRwCell { cell: self.tx.cell },
    }
  }

  pub(crate) fn new(lock: RwLockWriteGuard<'tx, DBShared>) -> TxRwImpl<'tx> {
    let bump = lock.records.lock().pop_read_bump();
    let meta = lock.backend.meta();
    let page_size = meta.page_size() as usize;
    let inline_bucket = meta.root();
    let mut uninit: MaybeUninit<TxRwImpl<'tx>> = MaybeUninit::uninit();
    let ptr = uninit.as_mut_ptr();
    unsafe {
      addr_of_mut!((*ptr).bump).write(Box::pin(bump));
      let bump = &(**addr_of!((*ptr).bump));
      addr_of_mut!((*ptr).lock).write(Box::pin(lock));
      let pager: &dyn Pager = &(**addr_of!((*ptr).lock));
      let tx = Rc::new_cyclic(|weak| {
        let r = TxR {
          b: bump,
          page_size,
          pager,
          meta,
          stats: Default::default(),
          is_rollback: false,
          p: Default::default(),
        };
        let w = TxW {
          pages: HashMap::new_in(bump),
          commit_handlers: BVec::new_in(bump),
          p: Default::default(),
        };
        let bucket = BucketRwCell::new_in(bump, inline_bucket, weak.clone(), None);
        TxRwCell {
          cell: SCell::new_in((TxRW { r, w }, bucket), bump),
        }
      });
      addr_of_mut!((*ptr).tx).write(Pin::new(tx));
      uninit.assume_init()
    }
  }
}

impl<'tx> Drop for TxRwImpl<'tx> {
  fn drop(&mut self) {
    let tx_id = self.id();
    let stats = self.stats();
    let mut swap_bump = Bump::with_capacity(0);
    mem::swap(&mut swap_bump, &mut self.bump);
    let mut records = self.lock.records.lock();

    // TODO: Reload freelist
    records.remove_tx(tx_id, stats, swap_bump);
  }
}

impl<'tx> TxApi<'tx> for TxRwImpl<'tx> {
  type CursorType = CursorImpl<'tx, InnerCursor<'tx, TxRwCell<'tx>, BucketRwCell<'tx>>>;
  type BucketType = BucketRwImpl<'tx>;

  fn id(&self) -> TxId {
    self.tx.api_id()
  }

  fn size(&self) -> u64 {
    self.tx.api_size()
  }

  fn writeable(&self) -> bool {
    true
  }

  fn cursor(&self) -> Self::CursorType {
    self.tx.api_cursor().into()
  }

  fn stats(&self) -> TxStats {
    self.tx.api_stats()
  }

  fn bucket(&self, name: &[u8]) -> Option<Self::BucketType> {
    self.tx.api_bucket(name).map(|b| b.into())
  }

  fn for_each<F: FnMut(&[u8], Self::BucketType)>(&self, f: F) -> crate::Result<()> {
    //self.tx.api_for_each(f)
    // TODO: mismatching bucket types
    todo!()
  }

  fn rollback(self) -> crate::Result<()> {
    self.tx.rollback()
  }

  fn page(&self, id: PgId) -> crate::Result<Option<PageInfo>> {
    self.tx.api_page(id)
  }
}

impl<'tx> TxRwApi<'tx> for TxRwImpl<'tx> {
  type CursorRwType = CursorRwImpl<'tx, InnerCursor<'tx, TxRwCell<'tx>, BucketRwCell<'tx>>>;

  fn cursor_mut(&mut self) -> Self::CursorRwType {
    self.tx.api_cursor_mut().into()
  }

  fn create_bucket(&mut self, name: &[u8]) -> crate::Result<Self::BucketType> {
    self.tx.api_create_bucket(name).map(|b| b.into())
  }

  fn create_bucket_if_not_exists(&mut self, name: &[u8]) -> crate::Result<Self::BucketType> {
    self
      .tx
      .api_create_bucket_if_not_exist(name)
      .map(|b| b.into())
  }

  fn delete_bucket(&mut self, name: &[u8]) -> crate::Result<()> {
    self.tx.api_delete_bucket(name)
  }

  fn commit(self) -> crate::Result<()> {
    self.tx.api_commit()
  }
}

pub struct TxRwRef<'tx> {
  tx: TxRwCell<'tx>,
}

impl<'tx> TxApi<'tx> for TxRwRef<'tx> {
  type CursorType = CursorImpl<'tx, InnerCursor<'tx, TxRwCell<'tx>, BucketRwCell<'tx>>>;
  type BucketType = BucketRwImpl<'tx>;

  fn id(&self) -> TxId {
    self.tx.api_id()
  }

  fn size(&self) -> u64 {
    self.tx.api_size()
  }

  fn writeable(&self) -> bool {
    true
  }

  fn cursor(&self) -> Self::CursorType {
    self.tx.api_cursor().into()
  }

  fn stats(&self) -> TxStats {
    self.tx.api_stats()
  }

  fn bucket(&self, name: &[u8]) -> Option<Self::BucketType> {
    self.tx.api_bucket(name).map(|b| b.into())
  }

  fn for_each<F: FnMut(&[u8], Self::BucketType)>(&self, f: F) -> crate::Result<()> {
    todo!()
  }

  fn rollback(self) -> crate::Result<()> {
    self.tx.rollback()
  }

  fn page(&self, id: PgId) -> crate::Result<Option<PageInfo>> {
    self.tx.api_page(id)
  }
}

impl<'tx> TxRwApi<'tx> for TxRwRef<'tx> {
  type CursorRwType = CursorRwImpl<'tx, InnerCursor<'tx, TxRwCell<'tx>, BucketRwCell<'tx>>>;

  fn cursor_mut(&mut self) -> Self::CursorRwType {
    self.tx.api_cursor_mut().into()
  }

  fn create_bucket(&mut self, name: &[u8]) -> crate::Result<Self::BucketType> {
    self.tx.api_create_bucket(name).map(|b| b.into())
  }

  fn create_bucket_if_not_exists(&mut self, name: &[u8]) -> crate::Result<Self::BucketType> {
    self
      .tx
      .api_create_bucket_if_not_exist(name)
      .map(|b| b.into())
  }

  fn delete_bucket(&mut self, name: &[u8]) -> crate::Result<()> {
    self.tx.api_delete_bucket(name)
  }

  fn commit(self) -> crate::Result<()> {
    self.tx.api_commit()
  }
}
