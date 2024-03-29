use crate::arch::size::MAX_ALLOC_SIZE;
use crate::bucket::{
  BucketCell, BucketIApi, BucketImpl, BucketR, BucketRW, BucketRwCell, BucketRwIApi, BucketRwImpl,
  BucketW,
};
use crate::common::bump::PinBump;
use crate::common::cell::{Ref, RefCell, RefMut};
use crate::common::defaults::IGNORE_NO_SYNC;
use crate::common::lock::{LockGuard, PinLockGuard};
use crate::common::memory::BCell;
use crate::common::meta::{MappedMetaPage, Meta, MetaPage};
use crate::common::page::{CoerciblePage, MutPage, PageHeader, PageInfo, RefPage};
use crate::common::pool::SyncReusable;
use crate::common::self_owned::SelfOwned;
use crate::common::tree::{MappedBranchPage, TreePage};
use crate::common::{BVec, HashMap, PgId, SplitRef, TxId};
use crate::cursor::{CursorImpl, CursorRwIApi, CursorRwImpl, InnerCursor};
use crate::db::{AllocateResult, DbIApi, DbMutIApi, DbShared};
use crate::tx::check::TxICheck;
use crate::TxCheck;
use aliasable::boxed::AliasableBox;
use aligners::{alignment, AlignedBytes};
use bumpalo::Bump;
use parking_lot::{Mutex, RwLockReadGuard, RwLockUpgradableReadGuard};
use std::alloc::Layout;
use std::borrow::Cow;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::mem;
use std::mem::MaybeUninit;
use std::ops::{Deref, SubAssign};
use std::pin::Pin;
use std::ptr::{addr_of, addr_of_mut};
use std::slice::from_raw_parts_mut;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Read-only transaction API
pub trait TxApi<'tx>: TxCheck<'tx> {
  /// ID returns the transaction id.
  fn id(&self) -> TxId;

  /// Size returns current database size in bytes as seen by this transaction.
  fn size(&self) -> u64;

  /// Writable returns whether the transaction can perform write operations.
  fn writeable(&self) -> bool;

  /// Cursor creates a cursor associated with the root bucket.
  /// All items in the cursor will return None value because all root bucket keys point to buckets.
  fn cursor(&self) -> CursorImpl<'tx>;

  /// Stats retrieves a copy of the current transaction statistics.
  fn stats(&self) -> Arc<TxStats>;

  /// Bucket retrieves a bucket by name.
  /// Returns None if the bucket does not exist.
  fn bucket<T: AsRef<[u8]>>(&self, name: T) -> Option<BucketImpl<'tx>>;

  fn for_each<F: FnMut(&[u8], BucketImpl<'tx>) -> crate::Result<()>>(
    &self, f: F,
  ) -> crate::Result<()>;

  /// Rollback closes the transaction and ignores all previous updates.
  fn rollback(self) -> crate::Result<()>;

  /// Page returns page information for a given page number.
  /// This is only safe for concurrent use when used by a writable transaction.
  /// ```rust
  /// use bbolt_rs::{BucketApi, BucketRwApi, DB, DbApi, DbRwAPI, TxApi, TxRwRefApi};
  ///
  /// fn main() -> bbolt_rs::Result<()> {
  ///   let mut db = DB::new_mem()?;
  ///
  ///   db.update(|mut tx| {
  ///     let mut b = tx.create_bucket_if_not_exists("test")?;
  ///     b.put("key", "value")?;
  ///     Ok(())
  ///   })?;
  ///
  ///   let tx = db.begin()?;
  ///   let b = tx.bucket("test").unwrap();
  ///   let page_id = b.root();
  ///   let page_info = tx.page(page_id)?.unwrap();
  ///   println!("{:?}", page_info);
  ///
  ///   Ok(())
  /// }
  /// ```
  fn page(&self, id: PgId) -> crate::Result<Option<PageInfo>>;
}

/// RW transaction API
pub trait TxRwRefApi<'tx>: TxApi<'tx> {
  /// Cursor creates a mutable cursor associated with the root bucket.
  /// All items in the cursor will return None because all root bucket keys point to buckets.
  fn cursor_mut(&mut self) -> CursorRwImpl<'tx>;

  /// bucket_mut retrieves a mutable bucket by name.
  /// Returns None if the bucket does not exist.
  fn bucket_mut<T: AsRef<[u8]>>(&mut self, name: T) -> Option<BucketRwImpl<'tx>>;

  /// CreateBucket creates a new bucket.
  /// Returns an error if the bucket already exists, if the bucket name is blank, or if the bucket name is too long.
  fn create_bucket<T: AsRef<[u8]>>(&mut self, name: T) -> crate::Result<BucketRwImpl<'tx>>;

  /// CreateBucketIfNotExists creates a new bucket if it doesn't already exist.
  /// Returns an error if the bucket name is blank, or if the bucket name is too long.
  fn create_bucket_if_not_exists<T: AsRef<[u8]>>(
    &mut self, name: T,
  ) -> crate::Result<BucketRwImpl<'tx>>;

  /// DeleteBucket deletes a bucket.
  /// Returns an error if the bucket cannot be found or if the key represents a non-bucket value.
  fn delete_bucket<T: AsRef<[u8]>>(&mut self, name: T) -> crate::Result<()>;

  /// OnCommit adds a handler function to be executed after the transaction successfully commits.
  fn on_commit<F: FnMut() + 'tx>(&mut self, f: F);
}

/// RW transaction API + Commit
pub trait TxRwApi<'tx>: TxRwRefApi<'tx> {
  /// Commit writes all changes to disk and updates the meta page.
  /// Returns an error if a disk write error occurs
  fn commit(self) -> crate::Result<()>;
}

/// Stats for the transaction
#[derive(Default)]
pub struct TxStats {
  // Page statistics.
  //
  /// number of page allocations
  page_count: AtomicI64,
  /// total bytes allocated
  page_alloc: AtomicI64,

  // Cursor statistics.
  //
  /// number of cursors created
  cursor_count: AtomicI64,

  // Node statistics
  //
  /// number of node allocations
  node_count: AtomicI64,
  /// number of node dereferences
  node_deref: AtomicI64,

  // Rebalance statistics.
  //
  /// number of node rebalances
  rebalance: AtomicI64,
  /// total time spent rebalancing
  rebalance_time: Mutex<Duration>,

  // Split/Spill statistics.
  //
  /// number of nodes split
  split: AtomicI64,
  /// number of nodes spilled
  spill: AtomicI64,
  /// total time spent spilling
  spill_time: Mutex<Duration>,

  // Write statistics.
  //
  /// number of writes performed
  write: AtomicI64,
  /// total time spent writing to disk
  write_time: Mutex<Duration>,
}

impl TxStats {
  pub fn get_page_alloc(&self) -> i64 {
    self.page_alloc.load(Ordering::Acquire)
  }

  pub(crate) fn inc_page_alloc(&self, delta: i64) {
    self.page_alloc.fetch_add(delta, Ordering::AcqRel);
  }

  pub fn get_page_count(&self) -> i64 {
    self.page_count.load(Ordering::Acquire)
  }

  pub(crate) fn inc_page_count(&self, delta: i64) {
    self.page_count.fetch_add(delta, Ordering::AcqRel);
  }

  pub fn get_cursor_count(&self) -> i64 {
    self.cursor_count.load(Ordering::Acquire)
  }

  pub(crate) fn inc_cursor_count(&self, delta: i64) {
    self.cursor_count.fetch_add(delta, Ordering::AcqRel);
  }

  pub fn get_node_count(&self) -> i64 {
    self.node_count.load(Ordering::Acquire)
  }

  pub(crate) fn inc_node_count(&self, delta: i64) {
    self.node_count.fetch_add(delta, Ordering::AcqRel);
  }

  pub fn get_node_deref(&self) -> i64 {
    self.node_deref.load(Ordering::Acquire)
  }

  pub(crate) fn inc_node_deref(&self, delta: i64) {
    self.node_deref.fetch_add(delta, Ordering::AcqRel);
  }

  pub fn get_rebalance(&self) -> i64 {
    self.rebalance.load(Ordering::Acquire)
  }

  pub(crate) fn inc_rebalance(&self, delta: i64) {
    self.rebalance.fetch_add(delta, Ordering::AcqRel);
  }

  pub fn get_rebalance_time(&self) -> Duration {
    *self.rebalance_time.lock()
  }

  pub(crate) fn inc_rebalance_time(&self, delta: Duration) {
    *self.rebalance_time.lock() += delta;
  }

  pub fn get_split(&self) -> i64 {
    self.split.load(Ordering::Acquire)
  }

  pub(crate) fn inc_split(&self, delta: i64) {
    self.split.fetch_add(delta, Ordering::AcqRel);
  }

  pub fn get_spill(&self) -> i64 {
    self.spill.load(Ordering::Acquire)
  }

  pub(crate) fn inc_spill(&self, delta: i64) {
    self.spill.fetch_add(delta, Ordering::AcqRel);
  }

  pub fn get_spill_time(&self) -> Duration {
    *self.spill_time.lock()
  }

  pub(crate) fn inc_spill_time(&self, delta: Duration) {
    *self.spill_time.lock() += delta;
  }

  pub fn get_write(&self) -> i64 {
    self.write.load(Ordering::Acquire)
  }

  pub(crate) fn inc_write(&self, delta: i64) {
    self.write.fetch_add(delta, Ordering::AcqRel);
  }

  pub fn get_write_time(&self) -> Duration {
    *self.write_time.lock()
  }

  pub(crate) fn inc_write_time(&self, delta: Duration) {
    *self.write_time.lock() += delta;
  }

  pub(crate) fn add_assign(&self, rhs: &TxStats) {
    self.inc_page_count(rhs.get_page_count());
    self.inc_page_alloc(rhs.get_page_alloc());
    self.inc_cursor_count(rhs.get_cursor_count());
    self.inc_node_count(rhs.get_node_count());
    self.inc_node_deref(rhs.get_node_deref());
    self.inc_rebalance(rhs.get_rebalance());
    self.inc_rebalance_time(rhs.get_rebalance_time());
    self.inc_split(rhs.get_split());
    self.inc_spill(rhs.get_spill());
    self.inc_spill_time(rhs.get_spill_time());
    self.inc_write(rhs.get_write());
    self.inc_write_time(rhs.get_write_time());
  }

  pub(crate) fn add(&self, rhs: &TxStats) -> TxStats {
    let add = self.clone();
    add.add_assign(rhs);
    add
  }

  pub(crate) fn sub_assign(&self, rhs: &TxStats) {
    self.inc_page_count(-rhs.get_page_count());
    self.inc_page_alloc(-rhs.get_page_alloc());
    self.inc_cursor_count(-rhs.get_cursor_count());
    self.inc_node_count(-rhs.get_node_count());
    self.inc_node_deref(-rhs.get_node_deref());
    self.inc_rebalance(-rhs.get_rebalance());
    self
      .rebalance_time
      .lock()
      .sub_assign(rhs.get_rebalance_time());
    self.inc_split(-rhs.get_split());
    self.inc_spill(-rhs.get_spill());
    self.spill_time.lock().sub_assign(rhs.get_spill_time());
    self.inc_write(-rhs.get_write());
    self.write_time.lock().sub_assign(rhs.get_write_time());
  }

  pub(crate) fn sub(&self, rhs: &TxStats) -> TxStats {
    let sub = self.clone();
    sub.sub_assign(rhs);
    sub
  }
}

impl Clone for TxStats {
  fn clone(&self) -> Self {
    TxStats {
      page_count: self.get_page_count().into(),
      page_alloc: self.get_page_alloc().into(),
      cursor_count: self.get_cursor_count().into(),
      node_count: self.get_node_count().into(),
      node_deref: self.get_node_deref().into(),
      rebalance: self.get_rebalance().into(),
      rebalance_time: self.get_rebalance_time().into(),
      split: self.get_split().into(),
      spill: self.get_spill().into(),
      spill_time: self.get_spill_time().into(),
      write: self.get_write().into(),
      write_time: self.get_write_time().into(),
    }
  }
}

impl PartialEq for TxStats {
  fn eq(&self, other: &Self) -> bool {
    self.get_page_count() == other.get_page_count()
      && self.get_page_alloc() == other.get_page_alloc()
      && self.get_cursor_count() == other.get_cursor_count()
      && self.get_node_count() == other.get_node_count()
      && self.get_node_deref() == other.get_node_deref()
      && self.get_rebalance() == other.get_rebalance()
      && self.get_rebalance_time() == other.get_rebalance_time()
      && self.get_split() == other.get_split()
      && self.get_spill() == other.get_spill()
      && self.get_spill_time() == other.get_spill_time()
      && self.get_write() == other.get_write()
      && self.get_write_time() == other.get_write_time()
  }
}

impl Eq for TxStats {}

impl Debug for TxStats {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("TxStats")
      .field("page_count", &self.get_page_count())
      .field("page_alloc", &self.get_page_alloc())
      .field("cursor_count", &self.get_cursor_count())
      .field("node_count", &self.get_node_count())
      .field("node_deref", &self.get_node_deref())
      .field("rebalance", &self.get_rebalance())
      .field("rebalance_time", &self.get_rebalance_time())
      .field("split", &self.get_split())
      .field("spill", &self.get_spill())
      .field("spill_time", &self.get_spill_time())
      .field("write", &self.get_write())
      .field("write_time", &self.get_write_time())
      .finish()
  }
}

pub(crate) enum AnyPage<'a, 'tx: 'a> {
  Ref(RefPage<'tx>),
  Pending(Ref<'a, RefPage<'tx>>),
}

impl<'a, 'tx: 'a> Deref for AnyPage<'tx, 'a> {
  type Target = RefPage<'a>;

  fn deref(&self) -> &Self::Target {
    match self {
      AnyPage::Ref(r) => r,
      AnyPage::Pending(p) => p,
    }
  }
}

pub(crate) trait TxIApi<'tx>: SplitRef<TxR<'tx>, Self::BucketType, TxW<'tx>> {
  type BucketType: BucketIApi<'tx, Self>;

  fn bump(self) -> &'tx Bump {
    self.split_r().b
  }

  fn page_size(self) -> usize {
    self.split_r().page_size
  }

  fn meta<'a>(&'a self) -> Ref<'a, Meta>
  where
    'tx: 'a,
  {
    Ref::map(self.split_r(), |tx| &tx.meta)
  }

  fn mem_page(self, id: PgId) -> RefPage<'tx> {
    self.split_r().db.page(id)
  }

  fn any_page<'a>(&'a self, id: PgId) -> AnyPage<'a, 'tx> {
    if let Some(tx) = self.split_ow() {
      if tx.pages.contains_key(&id) {
        let page = Ref::map(tx, |t| t.pages.get(&id).unwrap().as_ref());
        page.fast_check(id);
        return AnyPage::Pending(page);
      }
    }
    let page = self.split_r().db.page(id);
    page.fast_check(id);
    AnyPage::Ref(page)
  }

  /// See [TxApi::id]
  fn api_id(self) -> TxId {
    self.split_r().meta.txid()
  }

  /// See [TxApi::size]
  fn api_size(self) -> u64 {
    let r = self.split_r();
    r.meta.pgid().0 * r.meta.page_size() as u64
  }

  /// See [TxApi::cursor]
  fn api_cursor(self) -> InnerCursor<'tx, Self, Self::BucketType> {
    let root_bucket = self.root_bucket();
    root_bucket.i_cursor()
  }

  /// See [TxApi::stats]
  fn api_stats(self) -> Arc<TxStats> {
    self.split_r().stats.as_ref().unwrap().clone()
  }

  fn root_bucket(self) -> Self::BucketType {
    self.split_bound()
  }

  /// See [TxApi::bucket]
  fn api_bucket(self, name: &[u8]) -> Option<Self::BucketType> {
    let root_bucket = self.root_bucket();
    root_bucket.api_bucket(name)
  }

  /// See [TxApi::for_each]
  fn api_for_each<F: FnMut(&[u8], BucketImpl<'tx>) -> crate::Result<()>>(
    &self, mut f: F,
  ) -> crate::Result<()> {
    let root_bucket = self.root_bucket();
    root_bucket.api_for_each_bucket(|k| {
      let bucket = root_bucket.api_bucket(k).unwrap();
      f(k, bucket.into_impl())?;
      Ok(())
    })
  }

  /// forEachPage iterates over every page within a given page and executes a function.
  fn for_each_page<F: FnMut(&RefPage<'tx>, usize, &mut BVec<PgId>)>(self, pg_id: PgId, f: &mut F) {
    let mut stack = BVec::with_capacity_in(10, self.bump());
    stack.push(pg_id);
    self.for_each_page_internal(&mut stack, f);
  }

  fn for_each_page_internal<F: FnMut(&RefPage<'tx>, usize, &mut BVec<PgId>)>(
    self, pgid_stack: &mut BVec<PgId>, f: &mut F,
  ) {
    let p = self.mem_page(*pgid_stack.last().unwrap());

    // Execute function.
    f(&p, pgid_stack.len() - 1, pgid_stack);

    // Recursively loop over children.
    if let Some(branch_page) = MappedBranchPage::coerce_ref(&p) {
      for elem in branch_page.elements() {
        pgid_stack.push(elem.pgid());
        self.for_each_page_internal(pgid_stack, f);
        pgid_stack.pop();
      }
    }
  }

  fn rollback(self) -> crate::Result<()> {
    self.split_r_mut().is_rollback = true;
    Ok(())
  }

  /// See [TxApi::page]
  fn api_page(&self, id: PgId) -> crate::Result<Option<PageInfo>> {
    let r = self.split_r();
    if id >= r.meta.pgid() {
      return Ok(None);
    }
    //TODO: Check if freelist loaded
    //WHEN: Freelists can be unloaded

    let p = r.db.page(id);
    let id = p.id;
    let count = p.count as u64;
    let overflow_count = p.overflow as u64;

    let t = if r.db.is_page_free(id) {
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
}

pub(crate) trait TxRwIApi<'tx>: TxIApi<'tx> + TxICheck<'tx> {
  type CursorRwType: CursorRwIApi<'tx>;
  fn freelist_free_page(self, txid: TxId, p: &PageHeader);

  fn root_bucket_mut(self) -> BucketRwCell<'tx>;

  fn allocate(
    self, count: usize,
  ) -> crate::Result<SelfOwned<AlignedBytes<alignment::Page>, MutPage<'tx>>>;

  fn queue_page(self, page: SelfOwned<AlignedBytes<alignment::Page>, MutPage<'tx>>);

  /// See [TxRwRefApi::cursor_mut]
  fn api_cursor_mut(self) -> Self::CursorRwType;

  /// See [TxRwRefApi::create_bucket]
  fn api_create_bucket(self, name: &[u8]) -> crate::Result<Self::BucketType>;

  /// See [TxRwRefApi::create_bucket_if_not_exists]
  fn api_create_bucket_if_not_exist(self, name: &[u8]) -> crate::Result<Self::BucketType>;

  /// See [TxRwRefApi::delete_bucket]
  fn api_delete_bucket(self, name: &[u8]) -> crate::Result<()>;

  fn write(self) -> crate::Result<()>;

  fn write_meta(self) -> crate::Result<()>;

  /// See [TxRwRefApi::on_commit]
  fn api_on_commit(self, f: Box<dyn FnOnce() + 'tx>);
}

pub struct TxR<'tx> {
  b: &'tx Bump,
  page_size: usize,
  db: &'tx LockGuard<'tx, DbShared>,
  pub(crate) stats: Option<Arc<TxStats>>,
  pub(crate) meta: Meta,
  is_rollback: bool,
  marker: PhantomData<&'tx u8>,
}

pub struct TxW<'tx> {
  pages: HashMap<'tx, PgId, SelfOwned<AlignedBytes<alignment::Page>, MutPage<'tx>>>,
  commit_handlers: BVec<'tx, Box<dyn FnOnce() + 'tx>>,
  no_sync: bool,
  marker: PhantomData<&'tx u8>,
}

pub struct TxRW<'tx> {
  pub(crate) r: TxR<'tx>,
  w: TxW<'tx>,
}

#[derive(Copy, Clone)]
pub struct TxCell<'tx> {
  pub(crate) cell: BCell<'tx, TxR<'tx>, BucketCell<'tx>>,
}

impl<'tx> SplitRef<TxR<'tx>, BucketCell<'tx>, TxW<'tx>> for TxCell<'tx> {
  fn split_r(&self) -> Ref<TxR<'tx>> {
    self.cell.borrow()
  }

  fn split_ref(&self) -> (Ref<TxR<'tx>>, Option<Ref<TxW<'tx>>>) {
    (self.cell.borrow(), None)
  }

  fn split_ow(&self) -> Option<Ref<TxW<'tx>>> {
    None
  }

  fn split_bound(&self) -> BucketCell<'tx> {
    self.cell.bound()
  }

  fn split_r_mut(&self) -> RefMut<TxR<'tx>> {
    self.cell.borrow_mut()
  }

  fn split_ow_mut(&self) -> Option<RefMut<TxW<'tx>>> {
    None
  }
}

impl<'tx> TxIApi<'tx> for TxCell<'tx> {
  type BucketType = BucketCell<'tx>;
}

#[derive(Copy, Clone)]
pub struct TxRwCell<'tx> {
  pub(crate) cell: BCell<'tx, TxRW<'tx>, BucketRwCell<'tx>>,
}

impl<'tx> SplitRef<TxR<'tx>, BucketRwCell<'tx>, TxW<'tx>> for TxRwCell<'tx> {
  fn split_r(&self) -> Ref<TxR<'tx>> {
    Ref::map(self.cell.borrow(), |c| &c.r)
  }

  fn split_ref(&self) -> (Ref<TxR<'tx>>, Option<Ref<TxW<'tx>>>) {
    let (r, w) = Ref::map_split(self.cell.borrow(), |b| (&b.r, &b.w));
    (r, Some(w))
  }

  fn split_ow(&self) -> Option<Ref<TxW<'tx>>> {
    Some(Ref::map(self.cell.borrow(), |c| &c.w))
  }

  fn split_bound(&self) -> BucketRwCell<'tx> {
    self.cell.bound()
  }

  fn split_r_mut(&self) -> RefMut<TxR<'tx>> {
    RefMut::map(self.cell.borrow_mut(), |c| &mut c.r)
  }

  fn split_ow_mut(&self) -> Option<RefMut<TxW<'tx>>> {
    Some(RefMut::map(self.cell.borrow_mut(), |c| &mut c.w))
  }
}

impl<'tx> TxIApi<'tx> for TxRwCell<'tx> {
  type BucketType = BucketRwCell<'tx>;
}

impl<'tx> TxRwIApi<'tx> for TxRwCell<'tx> {
  type CursorRwType = InnerCursor<'tx, Self, Self::BucketType>;

  fn freelist_free_page(self, txid: TxId, p: &PageHeader) {
    self.cell.borrow().r.db.free_page(txid, p)
  }

  fn root_bucket_mut(self) -> BucketRwCell<'tx> {
    self.split_bound()
  }

  fn allocate(
    self, count: usize,
  ) -> crate::Result<SelfOwned<AlignedBytes<alignment::Page>, MutPage<'tx>>> {
    let db = { self.cell.borrow().r.db };
    let page = match db.allocate(self, count as u64) {
      AllocateResult::Page(page) => page,
      AllocateResult::PageWithNewSize(page, min_size) => {
        db.get_mut().unwrap().mmap_to_new_size(min_size, self)?;
        page
      }
    };

    Ok(page)
  }

  fn queue_page(self, page: SelfOwned<AlignedBytes<alignment::Page>, MutPage<'tx>>) {
    let mut tx = self.cell.borrow_mut();
    if let Some(pending) = tx.w.pages.insert(page.id, page) {
      if pending.overflow == 0 {
        tx.r
          .db
          .get_mut()
          .unwrap()
          .repool_allocated(pending.into_owner());
      }
    }
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

  fn write(self) -> crate::Result<()> {
    let (pages, db, page_size, no_sync) = {
      let mut tx = self.cell.borrow_mut();
      let mut swap_pages = HashMap::with_capacity_in(0, tx.r.b);
      // Clear out page cache early.
      mem::swap(&mut swap_pages, &mut tx.w.pages);
      let mut pages = BVec::from_iter_in(swap_pages.into_iter().map(|(_, page)| page), tx.r.b);

      // Sort pages by id.
      pages.sort_by_key(|page| page.id);
      (pages, tx.r.db, tx.r.page_size, tx.w.no_sync)
    };

    let r = self.split_r();

    // Write pages to disk in order.
    for page in &pages {
      let mut rem = (page.overflow as usize + 1) * page_size;
      let mut offset = page.id.0 * page_size as u64;
      let mut written = 0;

      // Write out page in "max allocation" sized chunks.
      loop {
        let size = rem.min(MAX_ALLOC_SIZE.bytes() as usize - 1);
        let buf = &page.ref_owner()[written..size];

        let size = db.write_all_at(buf, offset)?;

        // Update statistics.
        r.stats.as_ref().unwrap().inc_write(1);

        rem -= size;
        if rem == 0 {
          break;
        }

        offset += size as u64;
        written += size;
      }
    }

    if !no_sync || IGNORE_NO_SYNC {
      db.fsync()?;
    }

    for page in pages.into_iter() {
      if page.overflow == 0 {
        db.repool_allocated(page.into_owner());
      }
    }
    Ok(())
  }

  fn write_meta(self) -> crate::Result<()> {
    let tx = self.cell.borrow();
    let page_size = tx.r.page_size;

    let layout = Layout::from_size_align(page_size, mem::align_of::<MetaPage>()).unwrap();
    let ptr = tx.r.b.alloc_layout(layout);

    let mut meta_page = unsafe { MappedMetaPage::new(ptr.as_ptr()) };
    tx.r.meta.write(&mut meta_page);

    let db = tx.r.db;
    let offset = meta_page.page.id.0 * page_size as u64;
    let buf = unsafe { from_raw_parts_mut(ptr.as_ptr(), page_size) };
    db.write_all_at(buf, offset)?;

    if !tx.w.no_sync || IGNORE_NO_SYNC {
      db.fsync()?;
    }

    tx.r.stats.as_ref().unwrap().inc_write(1);

    Ok(())
  }

  fn api_on_commit(self, f: Box<dyn FnOnce() + 'tx>) {
    self.cell.borrow_mut().w.commit_handlers.push(f);
  }
}

pub struct TxImpl<'tx> {
  bump: SyncReusable<Pin<Box<PinBump>>>,
  db: Pin<AliasableBox<PinLockGuard<'tx, DbShared>>>,
  pub(crate) tx: TxCell<'tx>,
}

impl<'tx> TxImpl<'tx> {
  pub(crate) fn new(
    bump: SyncReusable<Pin<Box<PinBump>>>, lock: RwLockReadGuard<'tx, DbShared>, meta: Meta,
  ) -> TxImpl<'tx> {
    let page_size = meta.page_size() as usize;
    let inline_bucket = meta.root();
    let mut uninit: MaybeUninit<TxImpl<'tx>> = MaybeUninit::uninit();
    let ptr = uninit.as_mut_ptr();
    unsafe {
      addr_of_mut!((*ptr).bump).write(bump);

      let bump = Pin::as_ref(&*addr_of!((*ptr).bump)).bump().get_ref();
      addr_of_mut!((*ptr).db).write(AliasableBox::from_unique_pin(Box::pin(PinLockGuard::new(
        lock,
      ))));
      let db = Pin::as_ref(&*addr_of!((*ptr).db)).guard().get_ref();
      let tx = {
        let r = TxR {
          b: bump,
          page_size,
          db,
          meta,
          stats: Some(Default::default()),
          is_rollback: false,
          marker: Default::default(),
        };

        let uninit_tx: MaybeUninit<(RefCell<TxR>, BucketCell<'tx>)> = MaybeUninit::uninit();
        let cell_tx = bump.alloc(uninit_tx);
        let cell_tx_ptr = cell_tx.as_ptr().cast_mut();
        let const_cell_ptr = cell_tx_ptr.cast_const();

        addr_of_mut!((*cell_tx_ptr).0).write(RefCell::new(r));
        addr_of_mut!((*cell_tx_ptr).1).write(BucketCell::new_in(
          bump,
          inline_bucket,
          TxCell {
            cell: BCell(const_cell_ptr, PhantomData),
          },
          None,
        ));
        TxCell {
          cell: BCell(cell_tx.assume_init_ref(), PhantomData),
        }
      };
      addr_of_mut!((*ptr).tx).write(tx);
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
    let stats = self.tx.cell.borrow_mut().stats.take().unwrap();
    Pin::as_ref(&self.db).guard().remove_tx(tx_id, stats);
  }
}

impl<'tx> TxApi<'tx> for TxImpl<'tx> {
  fn id(&self) -> TxId {
    self.tx.api_id()
  }

  fn size(&self) -> u64 {
    self.tx.api_size()
  }

  fn writeable(&self) -> bool {
    false
  }

  fn cursor(&self) -> CursorImpl<'tx> {
    self.tx.api_cursor().into()
  }

  fn stats(&self) -> Arc<TxStats> {
    self.tx.api_stats()
  }

  fn bucket<T: AsRef<[u8]>>(&self, name: T) -> Option<BucketImpl<'tx>> {
    self.tx.api_bucket(name.as_ref()).map(BucketImpl::from)
  }

  fn for_each<F: FnMut(&[u8], BucketImpl<'tx>) -> crate::Result<()>>(
    &self, f: F,
  ) -> crate::Result<()> {
    self.tx.api_for_each(f)
  }

  fn rollback(self) -> crate::Result<()> {
    self.tx.rollback()
  }

  fn page(&self, id: PgId) -> crate::Result<Option<PageInfo>> {
    self.tx.api_page(id)
  }
}

pub struct TxRef<'tx> {
  tx: TxCell<'tx>,
}

impl<'tx> TxApi<'tx> for TxRef<'tx> {
  fn id(&self) -> TxId {
    self.tx.api_id()
  }

  fn size(&self) -> u64 {
    self.tx.api_size()
  }

  fn writeable(&self) -> bool {
    false
  }

  fn cursor(&self) -> CursorImpl<'tx> {
    self.tx.api_cursor().into()
  }

  fn stats(&self) -> Arc<TxStats> {
    self.tx.api_stats()
  }

  fn bucket<T: AsRef<[u8]>>(&self, name: T) -> Option<BucketImpl<'tx>> {
    self.tx.api_bucket(name.as_ref()).map(BucketImpl::from)
  }

  fn for_each<F: FnMut(&[u8], BucketImpl<'tx>) -> crate::Result<()>>(
    &self, f: F,
  ) -> crate::Result<()> {
    self.tx.api_for_each(f)
  }

  fn rollback(self) -> crate::Result<()> {
    self.tx.rollback()
  }

  fn page(&self, id: PgId) -> crate::Result<Option<PageInfo>> {
    self.tx.api_page(id)
  }
}

pub struct TxRwImpl<'tx> {
  bump: SyncReusable<Pin<Box<PinBump>>>,
  db: Pin<AliasableBox<PinLockGuard<'tx, DbShared>>>,
  pub(crate) tx: TxRwCell<'tx>,
}

impl<'tx> TxRwImpl<'tx> {
  pub(crate) fn get_ref(&self) -> TxRwRef<'tx> {
    TxRwRef {
      tx: TxRwCell { cell: self.tx.cell },
    }
  }

  pub(crate) fn new(
    bump: SyncReusable<Pin<Box<PinBump>>>, lock: RwLockUpgradableReadGuard<'tx, DbShared>,
    meta: Meta,
  ) -> TxRwImpl<'tx> {
    let no_sync = lock.options.no_sync();
    let page_size = meta.page_size() as usize;
    let inline_bucket = meta.root();
    let mut uninit: MaybeUninit<TxRwImpl<'tx>> = MaybeUninit::uninit();
    let ptr = uninit.as_mut_ptr();
    unsafe {
      addr_of_mut!((*ptr).bump).write(bump);
      let bump = Pin::as_ref(&*addr_of!((*ptr).bump)).bump().get_ref();
      addr_of_mut!((*ptr).db).write(AliasableBox::from_unique_pin(Box::pin(PinLockGuard::new(
        lock,
      ))));

      let db = Pin::as_ref(&*addr_of!((*ptr).db)).guard().get_ref();
      let tx = {
        let tx_r = TxR {
          b: bump,
          page_size,
          db,
          meta,
          stats: Some(Default::default()),
          is_rollback: false,
          marker: Default::default(),
        };
        let tx_w = TxW {
          pages: HashMap::with_capacity_in(0, bump),
          commit_handlers: BVec::with_capacity_in(0, bump),
          no_sync,
          marker: Default::default(),
        };

        let bucket_r = BucketR::new(inline_bucket);
        let bucket_w = BucketW::new_in(bump);

        let uninit_tx: MaybeUninit<(RefCell<TxRW>, BucketRwCell<'tx>)> = MaybeUninit::uninit();
        let uninit_bucket: MaybeUninit<(RefCell<BucketRW<'tx>>, TxRwCell<'tx>)> =
          MaybeUninit::uninit();
        let cell_tx = bump.alloc(uninit_tx);
        let cell_tx_ptr = cell_tx.as_mut_ptr();
        let const_cell_tx_ptr = cell_tx_ptr.cast_const();
        let cell_bucket = bump.alloc(uninit_bucket);
        let cell_bucket_ptr = cell_bucket.as_mut_ptr();

        addr_of_mut!((*cell_tx_ptr).0).write(RefCell::new(TxRW { r: tx_r, w: tx_w }));
        addr_of_mut!((*cell_bucket_ptr).0).write(RefCell::new(BucketRW {
          r: bucket_r,
          w: bucket_w,
        }));
        addr_of_mut!((*cell_bucket_ptr).1).write(TxRwCell {
          cell: BCell(const_cell_tx_ptr, PhantomData),
        });
        addr_of_mut!((*cell_tx_ptr).1).write(BucketRwCell {
          cell: BCell(cell_bucket.assume_init_ref(), PhantomData),
        });
        TxRwCell {
          cell: BCell(cell_tx.assume_init_ref(), PhantomData),
        }
      };
      addr_of_mut!((*ptr).tx).write(tx);
      uninit.assume_init()
    }
  }

  fn commit_freelist(&mut self) -> crate::Result<()> {
    let allocated_page = Pin::as_ref(&self.db).guard().commit_freelist(self.tx)?;

    let freelist_page = match allocated_page {
      AllocateResult::Page(page) => page,
      AllocateResult::PageWithNewSize(page, min_size) => {
        Pin::as_ref(&self.db)
          .guard()
          .get_mut()
          .unwrap()
          .mmap_to_new_size(min_size, self.tx)?;
        page
      }
    };
    let pg_id = freelist_page.id;
    let mut tx = self.tx.cell.borrow_mut();
    tx.r.meta.set_free_list(pg_id);
    tx.w.pages.insert(pg_id, freelist_page);
    Ok(())
  }
}

impl<'tx> Drop for TxRwImpl<'tx> {
  fn drop(&mut self) {
    let stats = self.tx.cell.borrow_mut().r.stats.take().unwrap();
    Pin::as_ref(&self.db).guard().remove_rw_tx(stats);
  }
}

impl<'tx> TxApi<'tx> for TxRwImpl<'tx> {
  fn id(&self) -> TxId {
    self.tx.api_id()
  }

  fn size(&self) -> u64 {
    self.tx.api_size()
  }

  fn writeable(&self) -> bool {
    true
  }

  fn cursor(&self) -> CursorImpl<'tx> {
    self.tx.api_cursor().into()
  }

  fn stats(&self) -> Arc<TxStats> {
    self.tx.api_stats()
  }

  fn bucket<T: AsRef<[u8]>>(&self, name: T) -> Option<BucketImpl<'tx>> {
    self.tx.api_bucket(name.as_ref()).map(BucketImpl::from)
  }

  fn for_each<F: FnMut(&[u8], BucketImpl<'tx>) -> crate::Result<()>>(
    &self, f: F,
  ) -> crate::Result<()> {
    self.tx.api_for_each(f)
  }

  fn rollback(self) -> crate::Result<()> {
    self.tx.rollback()
  }

  fn page(&self, id: PgId) -> crate::Result<Option<PageInfo>> {
    self.tx.api_page(id)
  }
}

impl<'tx> TxRwRefApi<'tx> for TxRwImpl<'tx> {
  fn cursor_mut(&mut self) -> CursorRwImpl<'tx> {
    CursorRwImpl::new(self.tx.api_cursor_mut())
  }

  fn bucket_mut<T: AsRef<[u8]>>(&mut self, name: T) -> Option<BucketRwImpl<'tx>> {
    self.tx.api_bucket(name.as_ref()).map(BucketRwImpl::from)
  }

  fn create_bucket<T: AsRef<[u8]>>(&mut self, name: T) -> crate::Result<BucketRwImpl<'tx>> {
    self
      .tx
      .api_create_bucket(name.as_ref())
      .map(BucketRwImpl::from)
  }

  fn create_bucket_if_not_exists<T: AsRef<[u8]>>(
    &mut self, name: T,
  ) -> crate::Result<BucketRwImpl<'tx>> {
    self
      .tx
      .api_create_bucket_if_not_exist(name.as_ref())
      .map(BucketRwImpl::from)
  }

  fn delete_bucket<T: AsRef<[u8]>>(&mut self, name: T) -> crate::Result<()> {
    self.tx.api_delete_bucket(name.as_ref())
  }

  fn on_commit<F: FnOnce() + 'tx>(&mut self, f: F) {
    self.tx.api_on_commit(Box::new(f))
  }
}

impl<'tx> TxRwApi<'tx> for TxRwImpl<'tx> {
  fn commit(mut self) -> crate::Result<()> {
    if self.tx.split_r().is_rollback {
      return Ok(());
    }

    let tx_stats = self.tx.split_r().stats.as_ref().cloned().unwrap();
    let bump = self.tx.bump();

    let start_time = Instant::now();
    self.tx.root_bucket().rebalance();
    if tx_stats.get_rebalance() > 0 {
      tx_stats.inc_rebalance_time(start_time.elapsed());
    }
    let opgid = self.tx.meta().pgid();
    let start_time = Instant::now();
    match self.tx.root_bucket().spill(bump) {
      Ok(_) => {
        tx_stats.inc_spill_time(start_time.elapsed());
      }
      Err(e) => {
        let _ = self.rollback();
        return Err(e);
      }
    }
    {
      let new_bucket = self.tx.cell.bound().split_r().bucket_header;
      let mut tx = self.tx.cell.borrow_mut();
      tx.r.meta.set_root(new_bucket);

      //TODO: implement pgidNoFreeList
      let freelist_pg = tx.r.db.page(tx.r.meta.free_list());
      let tx_id = tx.r.meta.txid();
      Pin::as_ref(&self.db).guard().free_page(tx_id, &freelist_pg);
    }
    // TODO: implement noFreelistSync

    match self.commit_freelist() {
      Ok(_) => {}
      Err(e) => {
        let _ = self.rollback();
        return Err(e);
      }
    }

    let new_pgid = self.tx.meta().pgid();
    let page_size = self.tx.meta().page_size();
    {
      let tx = self.tx.cell.borrow();
      for page in tx.w.pages.values() {
        assert!(page.id.0 > 1, "Invalid page id");
      }
    }
    if new_pgid > opgid {
      Pin::as_ref(&self.db)
        .guard()
        .grow((new_pgid.0 + 1) * page_size as u64)?;
    }
    let start_time = Instant::now();
    match self.tx.write() {
      Ok(_) => {}
      Err(e) => {
        let _ = self.tx.rollback();
        return Err(e);
      }
    };

    #[cfg(feature = "strict")]
    {
      let errors = self.tx.check();
      if !errors.is_empty() {
        panic!("check fail: {}", errors.join("\n"))
      }
    }

    match self.tx.write_meta() {
      Ok(_) => {
        tx_stats.inc_write_time(start_time.elapsed());
      }
      Err(e) => {
        let _ = self.rollback();
        return Err(e);
      }
    }

    let mut tx = self.tx.cell.borrow_mut();
    let mut commit_handlers = BVec::with_capacity_in(0, tx.r.b);
    mem::swap(&mut commit_handlers, &mut tx.w.commit_handlers);
    for f in commit_handlers.into_iter() {
      f();
    }
    Ok(())
  }
}

pub struct TxRwRef<'tx> {
  pub(crate) tx: TxRwCell<'tx>,
}

impl<'tx> TxApi<'tx> for TxRwRef<'tx> {
  fn id(&self) -> TxId {
    self.tx.api_id()
  }

  fn size(&self) -> u64 {
    self.tx.api_size()
  }

  fn writeable(&self) -> bool {
    true
  }

  fn cursor(&self) -> CursorImpl<'tx> {
    self.tx.api_cursor().into()
  }

  fn stats(&self) -> Arc<TxStats> {
    self.tx.api_stats()
  }

  fn bucket<T: AsRef<[u8]>>(&self, name: T) -> Option<BucketImpl<'tx>> {
    self.tx.api_bucket(name.as_ref()).map(BucketImpl::from)
  }

  fn for_each<F: FnMut(&[u8], BucketImpl<'tx>) -> crate::Result<()>>(
    &self, f: F,
  ) -> crate::Result<()> {
    self.tx.api_for_each(f)
  }

  fn rollback(self) -> crate::Result<()> {
    self.tx.rollback()
  }

  fn page(&self, id: PgId) -> crate::Result<Option<PageInfo>> {
    self.tx.api_page(id)
  }
}

impl<'tx> TxRwRefApi<'tx> for TxRwRef<'tx> {
  fn cursor_mut(&mut self) -> CursorRwImpl<'tx> {
    CursorRwImpl::new(self.tx.api_cursor_mut())
  }

  fn bucket_mut<T: AsRef<[u8]>>(&mut self, name: T) -> Option<BucketRwImpl<'tx>> {
    self.tx.api_bucket(name.as_ref()).map(BucketRwImpl::from)
  }

  fn create_bucket<T: AsRef<[u8]>>(&mut self, name: T) -> crate::Result<BucketRwImpl<'tx>> {
    self
      .tx
      .api_create_bucket(name.as_ref())
      .map(BucketRwImpl::from)
  }

  fn create_bucket_if_not_exists<T: AsRef<[u8]>>(
    &mut self, name: T,
  ) -> crate::Result<BucketRwImpl<'tx>> {
    self
      .tx
      .api_create_bucket_if_not_exist(name.as_ref())
      .map(BucketRwImpl::from)
  }

  fn delete_bucket<T: AsRef<[u8]>>(&mut self, name: T) -> crate::Result<()> {
    self.tx.api_delete_bucket(name.as_ref())
  }

  fn on_commit<F: FnOnce() + 'tx>(&mut self, f: F) {
    self.tx.api_on_commit(Box::new(f))
  }
}

pub(crate) mod check {
  use crate::bucket::BucketIApi;
  use crate::common::page::{CoerciblePage, RefPage};
  use crate::common::tree::{MappedBranchPage, MappedLeafPage, TreePage};
  use crate::common::{BVec, HashMap, HashSet, PgId, ZERO_PGID};
  use crate::db::DbIApi;
  use crate::tx::{TxCell, TxIApi, TxImpl, TxRef, TxRwCell, TxRwIApi, TxRwImpl, TxRwRef};

  pub(crate) trait UnsealTx<'tx> {
    fn unseal(&self) -> impl TxIApi<'tx> + TxICheck<'tx>;
  }

  pub(crate) trait UnsealRwTx<'tx>: UnsealTx<'tx> {
    fn unseal_rw(&self) -> impl TxRwIApi<'tx>;
  }

  impl<'tx> UnsealTx<'tx> for TxImpl<'tx> {
    fn unseal(&self) -> impl TxIApi<'tx> + TxICheck<'tx> {
      TxCell { cell: self.tx.cell }
    }
  }

  impl<'tx> UnsealTx<'tx> for TxRef<'tx> {
    fn unseal(&self) -> impl TxIApi<'tx> + TxICheck<'tx> {
      TxCell { cell: self.tx.cell }
    }
  }

  impl<'tx> UnsealTx<'tx> for TxRwImpl<'tx> {
    fn unseal(&self) -> impl TxIApi<'tx> + TxICheck<'tx> {
      TxRwCell { cell: self.tx.cell }
    }
  }

  impl<'tx> UnsealTx<'tx> for TxRwRef<'tx> {
    fn unseal(&self) -> impl TxIApi<'tx> + TxICheck<'tx> {
      self.tx
    }
  }

  impl<'tx> UnsealRwTx<'tx> for TxRwImpl<'tx> {
    fn unseal_rw(&self) -> impl TxRwIApi<'tx> {
      TxRwCell { cell: self.tx.cell }
    }
  }

  impl<'tx> UnsealRwTx<'tx> for TxRwRef<'tx> {
    fn unseal_rw(&self) -> impl TxRwIApi<'tx> {
      self.tx
    }
  }

  /// Check performs several consistency checks on the database for this transaction.
  /// An error is returned if any inconsistency is found.
  ///
  /// It can be safely run concurrently on a writable transaction. However, this
  /// incurs a high cost for large databases and databases with a lot of subbuckets
  /// because of caching. This overhead can be removed if running on a read-only
  /// transaction, however, it is not safe to execute other writer transactions at
  /// the same time.
  pub trait TxCheck<'tx> {
    fn check(&self) -> Vec<String>;
  }

  impl<'tx, T> TxCheck<'tx> for T
  where
    T: UnsealTx<'tx>,
  {
    fn check(&self) -> Vec<String> {
      let tx = self.unseal();
      tx.check()
    }
  }

  pub(crate) trait TxICheck<'tx>: TxIApi<'tx> {
    fn check(self) -> Vec<String> {
      let mut errors = Vec::new();
      let bump = self.bump();
      let db = self.split_r().db;
      let freelist_count = db.freelist_count();
      let high_water = self.meta().pgid();
      // TODO: ReadOnly mode handling

      // Check if any pages are double freed.
      let mut freed = HashSet::new_in(bump);
      let mut all = BVec::with_capacity_in(freelist_count as usize, bump);
      for _ in 0..freelist_count {
        all.push(ZERO_PGID);
      }
      db.freelist_copyall(&mut all);
      for id in &all {
        if freed.contains(id) {
          errors.push(format!("page {}: already freed", id));
        } else {
          freed.insert(*id);
        }
      }

      // Track every reachable page.
      let mut reachable = HashMap::new_in(bump);
      reachable.insert(PgId(0), self.mem_page(PgId(0))); //meta 0
      reachable.insert(PgId(1), self.mem_page(PgId(1))); // meta 1
      let freelist_pgid = self.meta().free_list();
      for i in 0..=self.mem_page(freelist_pgid).overflow {
        let pg_id = freelist_pgid + i as u64;
        reachable.insert(pg_id, self.mem_page(freelist_pgid));
      }

      // Recursively check buckets.
      self.check_bucket(self.split_bound(), &mut reachable, &mut freed, &mut errors);

      // Ensure all pages below high water mark are either reachable or freed.
      for i in 0..high_water.0 {
        let pg_id = PgId(i);
        if !reachable.contains_key(&pg_id) && !freed.contains(&pg_id) {
          errors.push(format!("page {}: unreachable unfreed", pg_id));
        }
      }

      errors
    }

    fn check_bucket(
      &self, bucket: Self::BucketType, reachable: &mut HashMap<PgId, RefPage<'tx>>,
      freed: &mut HashSet<PgId>, errors: &mut Vec<String>,
    ) {
      // ignore inline buckets
      if bucket.root() == ZERO_PGID {
        return;
      }

      self.for_each_page(bucket.root(), &mut |p, _, pgid_stack| {
        if p.id > self.meta().pgid() {
          errors.push(format!(
            "page {}: out of bounds: {} (stack: {:?})",
            p.id,
            self.meta().pgid(),
            pgid_stack
          ));
        }
        for i in 0..=p.overflow {
          let id = p.id + i as u64;
          if reachable.contains_key(&id) {
            errors.push(format!(
              "page {}: multiple references (stack: {:?})",
              id, pgid_stack
            ));
          }
          reachable.insert(id, *p);
        }

        if freed.contains(&p.id) {
          errors.push(format!("page {}: reachable freed", p.id));
        } else if !p.is_branch() && !p.is_leaf() {
          errors.push(format!(
            "page {}: invalid type: {} (stack: {:?})",
            p.id,
            p.page_type(),
            pgid_stack
          ));
        }
      });

      self.recursively_check_pages(bucket.root(), errors);

      bucket
        .api_for_each_bucket(|key| {
          let child = bucket.api_bucket(key).unwrap();
          self.check_bucket(child, reachable, freed, errors);
          Ok(())
        })
        .unwrap();
    }

    fn recursively_check_pages(self, pg_id: PgId, errors: &mut Vec<String>) {
      let bump = self.bump();
      let mut pgid_stack = BVec::new_in(bump);
      self.recursively_check_pages_internal(pg_id, &[], &[], &mut pgid_stack, errors);
    }

    fn recursively_check_pages_internal(
      self, pg_id: PgId, min_key_closed: &[u8], max_key_open: &[u8], pageid_stack: &mut BVec<PgId>,
      errors: &mut Vec<String>,
    ) -> &'tx [u8] {
      let p = self.mem_page(pg_id);
      pageid_stack.push(pg_id);
      let mut max_key_in_subtree = [].as_slice();
      if let Some(branch_page) = MappedBranchPage::coerce_ref(&p) {
        let mut running_min = min_key_closed;
        let elements_len = branch_page.elements().len();
        for (i, (pg_id, key)) in branch_page
          .elements()
          .iter()
          .map(|e| {
            (e.pgid(), unsafe {
              e.key(branch_page.page_ptr().cast_const())
            })
          })
          .enumerate()
        {
          self.verify_key_order(
            pg_id,
            "branch",
            i,
            key,
            running_min,
            max_key_open,
            pageid_stack,
            errors,
          );
          let mut max_key = max_key_open;
          if i < elements_len - 1 {
            max_key = branch_page.get_elem(i as u16 + 1).unwrap().key();
          }
          max_key_in_subtree =
            self.recursively_check_pages_internal(pg_id, key, max_key, pageid_stack, errors);
          running_min = max_key_in_subtree;
        }
        pageid_stack.pop();
        return max_key_in_subtree;
      } else if let Some(leaf_page) = MappedLeafPage::coerce_ref(&p) {
        let mut running_min = min_key_closed;
        for (i, key) in leaf_page
          .elements()
          .iter()
          .map(|e| unsafe { e.key(leaf_page.page_ptr().cast_const()) })
          .enumerate()
        {
          self.verify_key_order(
            pg_id,
            "leaf",
            i,
            key,
            running_min,
            max_key_open,
            pageid_stack,
            errors,
          );
          running_min = key;
        }
        if p.count > 0 {
          pageid_stack.pop();
          return leaf_page.get_elem(p.count - 1).unwrap().key();
        }
      } else {
        errors.push(format!("unexpected page type for pgId: {}", pg_id));
      }
      pageid_stack.pop();
      &[]
    }

    /***
     * verifyKeyOrder checks whether an entry with given #index on pgId (pageType: "branch|leaf") that has given "key",
     * is within range determined by (previousKey..maxKeyOpen) and reports found violations to the channel (ch).
     */
    fn verify_key_order(
      self, pg_id: PgId, page_type: &str, index: usize, key: &[u8], previous_key: &[u8],
      max_key_open: &[u8], pageid_stack: &mut BVec<PgId>, errors: &mut Vec<String>,
    ) {
      if index == 0 && !previous_key.is_empty() && previous_key > key {
        errors.push(format!("the first key[{}]={:02X?} on {} page({}) needs to be >= the key in the ancestor ({:02X?}). Stack: {:?}", index, key, page_type, pg_id, previous_key, pageid_stack));
      }
      if index > 0 {
        if previous_key > key {
          errors.push(format!("key[{}]=(hex){:02X?} on {} page({}) needs to be > (found <) than previous element (hex){:02X?}. Stack: {:?}", index, key, page_type, pg_id, previous_key, pageid_stack));
        } else if previous_key == key {
          errors.push(format!("key[{}]=(hex){:02X?} on {} page({}) needs to be > (found =) than previous element (hex){:02X?}. Stack: {:?}", index, key, page_type, pg_id, previous_key, pageid_stack));
        }
      }
      if !max_key_open.is_empty() && key >= max_key_open {
        errors.push(format!("key[{}]=(hex){:02X?} on {} page({}) needs to be < than key of the next element in ancestor (hex){:02X?}. Pages stack: {:?}", index, key, page_type, pg_id, previous_key, pageid_stack));
      }
    }
  }

  impl<'tx> TxICheck<'tx> for TxRwCell<'tx> {}
  impl<'tx> TxICheck<'tx> for TxCell<'tx> {}
}

#[cfg(test)]
mod test {
  use crate::common::cell::RefCell;
  use crate::common::defaults::DEFAULT_PAGE_SIZE;
  use crate::test_support::TestDb;
  use crate::tx::check::TxCheck;
  use crate::tx::{TxRwApi, TxStats};
  use crate::{
    BucketApi, BucketRwApi, CursorApi, DBOptions, DbApi, DbRwAPI, Error, TxApi, TxImpl, TxRwRefApi,
    DB,
  };
  use anyhow::anyhow;
  use std::time::Duration;

  #[test]
  fn test_tx_check_read_only() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let mut b = tx.create_bucket("widgets")?;
      b.put("foo", "bar")?;
      Ok(())
    })?;
    let close_db = db.db.clone();
    close_db.close();

    let file = db.tmp_file.as_ref().unwrap();
    let ro = DB::open_ro(file.as_ref());
    let ro_db = ro.unwrap();
    let tx = ro_db.begin()?;
    let errors = tx.check();
    assert!(errors.is_empty(), "{:?}", errors);

    Ok(())
  }

  #[test]
  #[ignore]
  fn test_tx_commit_err_tx_closed() {
    todo!("not possible")
  }

  #[test]
  #[ignore]
  fn test_tx_rollback_err_tx_closed() {
    todo!("not possible")
  }

  #[test]
  #[ignore]
  fn test_tx_commit_err_tx_not_writable() {
    todo!("not possible")
  }

  #[test]
  fn test_tx_cursor() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      tx.create_bucket("widgets")?;
      tx.create_bucket("woojits")?;
      let mut c = tx.cursor();
      assert_eq!(Some(("widgets".as_bytes(), None)), c.first());
      assert_eq!(Some(("woojits".as_bytes(), None)), c.next());
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  #[ignore]
  fn test_tx_create_bucket_err_tx_not_writable() {
    todo!("not possible")
  }

  #[test]
  #[ignore]
  fn test_tx_create_bucket_err_tx_closed() {
    todo!("not possible")
  }

  #[test]
  fn test_tx_bucket() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      tx.create_bucket("widgets")?;
      assert!(tx.bucket("widgets").is_some(), "expected bucket");
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  fn test_tx_get_not_found() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let mut b = tx.create_bucket("widgets")?;
      b.put("foo", "bar")?;
      assert_eq!(None, b.get("no_such_key"), "expected None");
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  fn test_tx_create_bucket() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      tx.create_bucket(b"widgets")?;
      Ok(())
    })?;
    db.view(|tx| {
      let bucket = tx.bucket(b"widgets");
      assert!(bucket.is_some(), "expected bucket");
      Ok(())
    })
  }

  #[test]
  fn test_tx_create_bucket_if_not_exists() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      tx.create_bucket_if_not_exists("widgets")?;
      tx.create_bucket_if_not_exists("widgets")?;
      Ok(())
    })?;
    db.view(|tx| {
      assert!(tx.bucket("widgets").is_some());
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  fn test_tx_create_bucket_if_not_exists_err_bucket_name_required() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      assert_eq!(
        Some(Error::BucketNameRequired),
        tx.create_bucket_if_not_exists("").err()
      );
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  fn test_tx_create_bucket_err_bucket_exists() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      tx.create_bucket("widgets")?;
      Ok(())
    })?;
    db.update(|mut tx| {
      assert_eq!(Some(Error::BucketExists), tx.create_bucket("widgets").err());
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  fn test_tx_create_bucket_err_bucket_name_required() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      assert_eq!(Some(Error::BucketNameRequired), tx.create_bucket("").err());
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  fn test_tx_delete_bucket() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let mut b = tx.create_bucket("widgets")?;
      b.put("foo", "bar")?;
      Ok(())
    })?;
    db.update(|mut tx| {
      tx.delete_bucket("widgets")?;
      assert!(tx.bucket("widgets").is_none());
      Ok(())
    })?;
    db.update(|mut tx| {
      let b = tx.create_bucket("widgets")?;
      assert!(b.get("widgets").is_none());
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  #[ignore]
  fn test_tx_delete_bucket_err_tx_closed() {
    todo!("not possible")
  }

  #[test]
  #[ignore]
  fn test_tx_delete_bucket_read_only() {
    todo!("not possible")
  }

  #[test]
  fn test_tx_delete_bucket_not_found() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      assert_eq!(
        Some(Error::BucketNotFound),
        tx.delete_bucket("widgets").err()
      );
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  fn test_tx_for_each_no_error() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let mut b = tx.create_bucket("widgets")?;
      b.put("foo", "bar")?;
      tx.for_each(|_, _| Ok(()))?;
      Ok(())
    })?;
    Ok(())
  }

  #[test]
  fn test_tx_for_each_with_error() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    let result = db.update(|mut tx| {
      let mut b = tx.create_bucket("widgets")?;
      b.put("foo", "bar")?;
      tx.for_each(|_, _| Err(Error::Other(anyhow!("marker"))))?;
      Ok(())
    });
    let e = result.map_err(|e| e.to_string()).err().unwrap();
    assert_eq!("marker", e);
    Ok(())
  }

  #[test]
  fn test_tx_on_commit() -> crate::Result<()> {
    let x = RefCell::new(0u64);
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      tx.on_commit(|| {
        *x.borrow_mut() += 1;
      });
      tx.on_commit(|| {
        *x.borrow_mut() += 2;
      });
      let mut b = tx.create_bucket("widgets")?;
      b.put("foo", "bar")?;
      Ok(())
    })?;
    assert_eq!(3, *x.borrow());
    Ok(())
  }

  #[test]
  fn test_tx_on_commit_rollback() -> crate::Result<()> {
    let x = RefCell::new(0u64);
    let mut db = TestDb::new()?;
    let _ = db.update(|mut tx| {
      tx.on_commit(|| {
        *x.borrow_mut() += 1;
      });
      tx.on_commit(|| {
        *x.borrow_mut() += 2;
      });
      tx.create_bucket("widgets")?;
      Err(Error::Other(anyhow!("rollback")))
    });
    assert_eq!(0, *x.borrow());
    Ok(())
  }

  #[test]
  #[ignore]
  fn test_tx_copy_file() {
    todo!()
  }

  #[test]
  #[ignore]
  fn test_tx_copy_file_error_meta() {
    todo!()
  }

  #[test]
  #[ignore]
  fn test_tx_copy_file_error_normal() {
    todo!()
  }

  #[test]
  fn test_tx_rollback() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    let mut tx = db.begin_rw_tx()?;
    tx.create_bucket("mybucket")?;
    tx.commit()?;
    let mut tx = db.begin_rw_tx()?;
    let mut b = tx.bucket_mut("mybucket").unwrap();
    b.put("k", "v")?;
    tx.rollback()?;
    let tx = db.begin_tx()?;
    let b = tx.bucket("mybucket").unwrap();
    assert_eq!(None, b.get("k"));
    tx.rollback()?;
    todo!("noSyncFreelist");
    Ok(())
  }

  #[test]
  fn test_tx_release_range() -> crate::Result<()> {
    // Set initial mmap size well beyond the limit we will hit in this
    // test, since we are testing with long running read transactions
    // and will deadlock if db.grow is triggered.
    let initial_mmap_size = DEFAULT_PAGE_SIZE.bytes() as u64 * 100;
    let db_options = DBOptions::builder()
      .initial_mmap_size(initial_mmap_size)
      .build();
    let db = TestDb::with_options(db_options)?;
    let bucket = "bucket";

    let mut put_db = db.clone_db();
    let mut put = move |key, value| {
      put_db
        .update(|mut tx| {
          let mut b = tx.create_bucket_if_not_exists(bucket)?;
          b.put(key, value)?;
          Ok(())
        })
        .unwrap();
    };

    let mut del_db = db.clone_db();
    let mut del = move |key| {
      del_db
        .update(|mut tx| {
          let mut b = tx.create_bucket_if_not_exists(bucket)?;
          b.delete(key)?;
          Ok(())
        })
        .unwrap();
    };

    let open_read_tx = || db.begin_tx().unwrap();

    let check_with_read_tx = |tx: &TxImpl, key, want_value| {
      let value = tx.bucket(bucket).unwrap().get(key);
      assert_eq!(want_value, value);
    };

    let rollback = |tx: TxImpl| {
      tx.rollback().unwrap();
    };

    put("k1", "v1");
    let rtx1 = open_read_tx();
    put("k2", "v2");
    let hold1 = open_read_tx();
    put("k3", "v3");
    let hold2 = open_read_tx();
    del("k3");
    let rtx2 = open_read_tx();
    del("k1");
    let hold3 = open_read_tx();
    del("k2");
    let hold4 = open_read_tx();
    put("k4", "v4");
    let hold5 = open_read_tx();

    // Close the read transactions we established to hold a portion of the pages in pending state.
    rollback(hold1);
    rollback(hold2);
    rollback(hold3);
    rollback(hold4);
    rollback(hold5);

    // Execute a write transaction to trigger a releaseRange operation in the db
    // that will free multiple ranges between the remaining open read transactions, now that the
    // holds have been rolled back.
    put("k4", "v4");

    // Check that all long running reads still read correct values.
    check_with_read_tx(&rtx1, "k1", Some("v1".as_bytes()));
    check_with_read_tx(&rtx2, "k2", Some("v2".as_bytes()));
    rollback(rtx1);
    rollback(rtx2);

    // Check that the final state is correct.
    let rtx7 = open_read_tx();
    check_with_read_tx(&rtx7, "k1", None);
    check_with_read_tx(&rtx7, "k2", None);
    check_with_read_tx(&rtx7, "k3", None);
    check_with_read_tx(&rtx7, "k4", Some("v4".as_bytes()));
    Ok(())
  }

  #[test]
  #[ignore]
  fn example_tx_rollback() {
    todo!()
  }

  #[test]
  #[ignore]
  fn example_tx_copy_file() {
    todo!()
  }

  #[test]
  fn test_tx_stats_get_and_inc_atomically() {
    let stats = TxStats::default();

    stats.inc_page_count(1);
    assert_eq!(1, stats.get_page_count());

    stats.inc_page_alloc(2);
    assert_eq!(2, stats.get_page_alloc());

    stats.inc_cursor_count(3);
    assert_eq!(3, stats.get_cursor_count());

    stats.inc_node_count(100);
    assert_eq!(100, stats.get_node_count());

    stats.inc_node_deref(101);
    assert_eq!(101, stats.get_node_deref());

    stats.inc_rebalance(1000);
    assert_eq!(1000, stats.get_rebalance());

    stats.inc_rebalance_time(Duration::from_secs(1001));
    assert_eq!(1001, stats.get_rebalance_time().as_secs());

    stats.inc_split(10000);
    assert_eq!(10000, stats.get_split());

    stats.inc_spill(10001);
    assert_eq!(10001, stats.get_spill());

    stats.inc_spill_time(Duration::from_secs(10001));
    assert_eq!(10001, stats.get_spill_time().as_secs());

    stats.inc_write(100_000);
    assert_eq!(100_000, stats.get_write());

    stats.inc_write_time(Duration::from_secs(100_001));
    assert_eq!(100_001, stats.get_write_time().as_secs());

    let expected_stats = TxStats {
      page_count: 1.into(),
      page_alloc: 2.into(),
      cursor_count: 3.into(),
      node_count: 100.into(),
      node_deref: 101.into(),
      rebalance: 1000.into(),
      rebalance_time: Duration::from_secs(1001).into(),
      split: 10000.into(),
      spill: 10001.into(),
      spill_time: Duration::from_secs(10001).into(),
      write: 100_000.into(),
      write_time: Duration::from_secs(100_001).into(),
    };

    assert_eq!(expected_stats, stats);
  }

  #[test]
  fn test_tx_stats_sub() {
    let stats_a = TxStats {
      page_count: 1.into(),
      page_alloc: 2.into(),
      cursor_count: 3.into(),
      node_count: 100.into(),
      node_deref: 101.into(),
      rebalance: 1000.into(),
      rebalance_time: Duration::from_secs(1001).into(),
      split: 10000.into(),
      spill: 10001.into(),
      spill_time: Duration::from_secs(10001).into(),
      write: 100_000.into(),
      write_time: Duration::from_secs(100_001).into(),
    };

    let stats_b = TxStats {
      page_count: 2.into(),
      page_alloc: 3.into(),
      cursor_count: 4.into(),
      node_count: 101.into(),
      node_deref: 102.into(),
      rebalance: 1001.into(),
      rebalance_time: Duration::from_secs(1002).into(),
      split: 11001.into(),
      spill: 11002.into(),
      spill_time: Duration::from_secs(11002).into(),
      write: 110_001.into(),
      write_time: Duration::from_secs(110_010).into(),
    };

    let diff = stats_b.sub(&stats_a);
    let expected_stats = TxStats {
      page_count: 1.into(),
      page_alloc: 1.into(),
      cursor_count: 1.into(),
      node_count: 1.into(),
      node_deref: 1.into(),
      rebalance: 1.into(),
      rebalance_time: Duration::from_secs(1).into(),
      split: 1001.into(),
      spill: 1001.into(),
      spill_time: Duration::from_secs(1001).into(),
      write: 10001.into(),
      write_time: Duration::from_secs(10009).into(),
    };

    assert_eq!(expected_stats, diff);
  }

  #[test]
  #[ignore]
  fn test_tx_truncate_before_write() {
    todo!()
  }

  #[test]
  fn test_tx_stats_add() {
    let stats_a = TxStats {
      page_count: 1.into(),
      page_alloc: 2.into(),
      cursor_count: 3.into(),
      node_count: 100.into(),
      node_deref: 101.into(),
      rebalance: 1000.into(),
      rebalance_time: Duration::from_secs(1001).into(),
      split: 10000.into(),
      spill: 10001.into(),
      spill_time: Duration::from_secs(10001).into(),
      write: 100_000.into(),
      write_time: Duration::from_secs(100_001).into(),
    };

    let stats_b = TxStats {
      page_count: 2.into(),
      page_alloc: 3.into(),
      cursor_count: 4.into(),
      node_count: 101.into(),
      node_deref: 102.into(),
      rebalance: 1001.into(),
      rebalance_time: Duration::from_secs(1002).into(),
      split: 11001.into(),
      spill: 11002.into(),
      spill_time: Duration::from_secs(11002).into(),
      write: 110_001.into(),
      write_time: Duration::from_secs(110_010).into(),
    };

    let add = stats_b.add(&stats_a);
    let expected_stats = TxStats {
      page_count: 3.into(),
      page_alloc: 5.into(),
      cursor_count: 7.into(),
      node_count: 201.into(),
      node_deref: 203.into(),
      rebalance: 2001.into(),
      rebalance_time: Duration::from_secs(2003).into(),
      split: 21001.into(),
      spill: 21003.into(),
      spill_time: Duration::from_secs(21003).into(),
      write: 210001.into(),
      write_time: Duration::from_secs(210011).into(),
    };

    assert_eq!(expected_stats, add);
  }
}
