use crate::arch::size::MAX_ALLOC_SIZE;
use crate::bucket::{BucketCell, BucketIApi, BucketImpl, BucketRwCell, BucketRwIApi, BucketRwImpl};
use crate::common::memory::BCell;
use crate::common::meta::{MappedMetaPage, Meta, MetaPage};
use crate::common::page::{CoerciblePage, MutPage, Page, PageInfo, RefPage};
use crate::common::pool::SyncReusable;
use crate::common::self_owned::SelfOwned;
use crate::common::tree::{MappedBranchPage, TreePage};
use crate::common::{BVec, HashMap, PgId, SplitRef, TxId};
use crate::cursor::{CursorImpl, CursorRwIApi, CursorRwImpl, InnerCursor};
use crate::db::{DbIApi, DbRwIApi, DbShared};
use crate::tx::check::{TxICheck, UnsealRwTx, UnsealTx};
use crate::{BucketApi, BucketRwApi, CursorApi, CursorRwApi, LockGuard, PinBump, PinLockGuard};
use aliasable::boxed::AliasableBox;
use aligners::{alignment, AlignedBytes};
use bumpalo::Bump;
use parking_lot::{RwLockReadGuard, RwLockUpgradableReadGuard};
use std::alloc::Layout;
use std::borrow::Cow;
use std::cell::{Ref, RefMut};
use std::marker::{PhantomData};
use std::mem;
use std::mem::MaybeUninit;
use std::ops::{AddAssign, Deref, Sub};
use std::pin::Pin;
use std::ptr::{addr_of, addr_of_mut};
use std::rc::Rc;
use std::slice::from_raw_parts_mut;
use std::time::{Duration, Instant};

pub trait TxApi<'tx> {
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
  fn cursor(&self) -> impl CursorApi<'tx>;

  /// Stats retrieves a copy of the current transaction statistics.
  fn stats(&self) -> TxStats;

  /// Bucket retrieves a bucket by name.
  /// Returns nil if the bucket does not exist.
  /// The bucket instance is only valid for the lifetime of the transaction.
  fn bucket<T: AsRef<[u8]>>(&self, name: T) -> Option<impl BucketApi<'tx>>;

  fn for_each<F: FnMut(&[u8], &dyn BucketApi<'tx>)>(&self, f: F) -> crate::Result<()>;

  /// Rollback closes the transaction and ignores all previous updates. Read-only
  /// transactions must be rolled back and not committed.
  fn rollback(self) -> crate::Result<()>;

  /// Page returns page information for a given page number.
  /// This is only safe for concurrent use when used by a writable transaction.
  fn page(&self, id: PgId) -> crate::Result<Option<PageInfo>>;
}

pub trait TxRwApi<'tx>: TxApi<'tx> + UnsealRwTx<'tx> {
  /// Cursor creates a cursor associated with the root bucket.
  /// All items in the cursor will return a nil value because all root bucket keys point to buckets.
  /// The cursor is only valid as long as the transaction is open.
  /// Do not use a cursor after the transaction is closed.
  fn cursor_mut(&mut self) -> impl CursorRwApi<'tx>;

  fn bucket_mut<T: AsRef<[u8]>>(&mut self, name: T) -> Option<impl BucketRwApi<'tx>>;

  /// CreateBucket creates a new bucket.
  /// Returns an error if the bucket already exists, if the bucket name is blank, or if the bucket name is too long.
  /// The bucket instance is only valid for the lifetime of the transaction.
  fn create_bucket<T: AsRef<[u8]>>(&mut self, name: T) -> crate::Result<impl BucketRwApi<'tx>>;

  /// CreateBucketIfNotExists creates a new bucket if it doesn't already exist.
  /// Returns an error if the bucket name is blank, or if the bucket name is too long.
  /// The bucket instance is only valid for the lifetime of the transaction.
  fn create_bucket_if_not_exists<T: AsRef<[u8]>>(
    &mut self, name: T,
  ) -> crate::Result<impl BucketRwApi<'tx>>;

  /// DeleteBucket deletes a bucket.
  /// Returns an error if the bucket cannot be found or if the key represents a non-bucket value.
  fn delete_bucket<T: AsRef<[u8]>>(&mut self, name: T) -> crate::Result<()>;

  /// Commit writes all changes to disk and updates the meta page.
  /// Returns an error if a disk write error occurs, or if Commit is
  /// called on a read-only transaction.
  fn commit(self) -> crate::Result<()>;
}

// TODO: Add functions to simplify access
#[derive(Copy, Clone, Default)]
pub struct TxStats {
  // Page statistics.
  //
  /// number of page allocations
  pub(crate) page_count: i64,
  /// total bytes allocated
  pub(crate) page_alloc: i64,

  // Cursor statistics.
  //
  /// number of cursors created
  pub(crate) cursor_count: i64,

  // Node statistics
  //
  /// number of node allocations
  pub(crate) node_count: i64,
  /// number of node dereferences
  pub(crate) node_deref: i64,

  // Rebalance statistics.
  //
  /// number of node rebalances
  pub(crate) rebalance: i64,
  /// total time spent rebalancing
  pub(crate) rebalance_time: Duration,

  // Split/Spill statistics.
  //
  /// number of nodes split
  pub(crate) split: i64,
  /// number of nodes spilled
  pub(crate) spill: i64,
  /// total time spent spilling
  pub(crate) spill_time: Duration,

  // Write statistics.
  //
  /// number of writes performed
  pub(crate) write: i64,
  /// total time spent writing to disk
  pub(crate) write_time: Duration,
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
        return AnyPage::Pending(Ref::map(tx, |t| t.pages.get(&id).unwrap().as_ref()));
      }
    }
    AnyPage::Ref(self.split_r().db.page(id))
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

  /// See [TxApi::writeable]
  fn api_writeable(self) -> bool {
    self.split_ow().is_some()
  }

  /// See [TxApi::cursor]
  fn api_cursor(self) -> InnerCursor<'tx, Self, Self::BucketType> {
    let root_bucket = self.root_bucket();
    root_bucket.i_cursor()
  }

  /// See [TxApi::stats]
  fn api_stats(self) -> TxStats {
    self.split_r().stats
  }

  fn mut_stats<'a>(&'a self) -> RefMut<'a, TxStats>
  where
    'tx: 'a,
  {
    RefMut::map(self.split_r_mut(), |r| &mut r.stats)
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
  fn api_for_each<F: FnMut(&[u8], Self::BucketType)>(&self, mut f: F) -> crate::Result<()> {
    let root_bucket = self.root_bucket();
    // TODO: Are we calling the right function?
    root_bucket.api_for_each_bucket(|k| {
      let bucket = root_bucket.api_bucket(k).unwrap();
      f(k, bucket);
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

  /// See [TxApi::rollback]
  fn api_rollback(self) -> crate::Result<()> {
    todo!()
  }

  fn non_physical_rollback(self) -> crate::Result<()>;

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

  fn close(self) -> crate::Result<()>;
}

pub(crate) trait TxRwIApi<'tx>: TxIApi<'tx> + TxICheck<'tx> {
  type CursorRwType: CursorRwIApi<'tx>;
  fn freelist_free_page(self, txid: TxId, p: &Page);

  fn root_bucket_mut(self) -> BucketRwCell<'tx>;

  fn allocate(
    self, count: usize,
  ) -> crate::Result<SelfOwned<AlignedBytes<alignment::Page>, MutPage<'tx>>>;

  fn queue_page(self, page: SelfOwned<AlignedBytes<alignment::Page>, MutPage<'tx>>);

  /// See [TxRwApi::cursor_mut]
  fn api_cursor_mut(self) -> Self::CursorRwType;

  /// See [TxRwApi::create_bucket]
  fn api_create_bucket(self, name: &[u8]) -> crate::Result<Self::BucketType>;

  /// See [TxRwApi::create_bucket_if_not_exists]
  fn api_create_bucket_if_not_exist(self, name: &[u8]) -> crate::Result<Self::BucketType>;

  /// See [TxRwApi::delete_bucket]
  fn api_delete_bucket(self, name: &[u8]) -> crate::Result<()>;

  fn write(self) -> crate::Result<()>;

  fn write_meta(self) -> crate::Result<()>;
}

pub struct TxR<'tx> {
  b: &'tx Bump,
  page_size: usize,
  db: &'tx LockGuard<'tx, DbShared>,
  pub(crate) stats: TxStats,
  pub(crate) meta: Meta,
  is_rollback: bool,
  p: PhantomData<&'tx u8>,
}

pub struct TxW<'tx> {
  pages: HashMap<'tx, PgId, SelfOwned<AlignedBytes<alignment::Page>, MutPage<'tx>>>,
  commit_handlers: BVec<'tx, Box<dyn Fn()>>,
  p: PhantomData<&'tx u8>,
}

pub struct TxRW<'tx> {
  r: TxR<'tx>,
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

  fn split_r_ow(&self) -> (Ref<TxR<'tx>>, Option<Ref<TxW<'tx>>>) {
    (self.cell.borrow(), None)
  }

  fn split_ow(&self) -> Option<Ref<TxW<'tx>>> {
    None
  }

  fn split_bound(&self) -> BucketCell<'tx> {
    self.cell.bound()
  }

  fn split_ref(&self) -> (Ref<TxR<'tx>>, BucketCell<'tx>, Option<Ref<TxW<'tx>>>) {
    (self.cell.borrow(), self.cell.bound(), None)
  }

  fn split_r_mut(&self) -> RefMut<TxR<'tx>> {
    self.cell.borrow_mut()
  }

  fn split_r_ow_mut(&self) -> (RefMut<TxR<'tx>>, Option<RefMut<TxW<'tx>>>) {
    (self.cell.borrow_mut(), None)
  }

  fn split_ow_mut(&self) -> Option<RefMut<TxW<'tx>>> {
    None
  }

  fn split_ref_mut(&self) -> (RefMut<TxR<'tx>>, BucketCell<'tx>, Option<RefMut<TxW<'tx>>>) {
    (self.cell.borrow_mut(), self.cell.bound(), None)
  }
}

impl<'tx> TxIApi<'tx> for TxCell<'tx> {
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
  pub(crate) cell: BCell<'tx, TxRW<'tx>, BucketRwCell<'tx>>,
}

impl<'tx> SplitRef<TxR<'tx>, BucketRwCell<'tx>, TxW<'tx>> for TxRwCell<'tx> {
  fn split_r(&self) -> Ref<TxR<'tx>> {
    Ref::map(self.cell.borrow(), |c| &c.r)
  }

  fn split_r_ow(&self) -> (Ref<TxR<'tx>>, Option<Ref<TxW<'tx>>>) {
    let (r, w) = Ref::map_split(self.cell.borrow(), |b| (&b.r, &b.w));
    (r, Some(w))
  }

  fn split_ow(&self) -> Option<Ref<TxW<'tx>>> {
    Some(Ref::map(self.cell.borrow(), |c| &c.w))
  }

  fn split_bound(&self) -> BucketRwCell<'tx> {
    self.cell.bound()
  }

  fn split_ref(&self) -> (Ref<TxR<'tx>>, BucketRwCell<'tx>, Option<Ref<TxW<'tx>>>) {
    let (r, w) = Ref::map_split(self.cell.borrow(), |b| (&b.r, &b.w));
    (r, self.cell.bound(), Some(w))
  }

  fn split_r_mut(&self) -> RefMut<TxR<'tx>> {
    RefMut::map(self.cell.borrow_mut(), |c| &mut c.r)
  }

  fn split_r_ow_mut(&self) -> (RefMut<TxR<'tx>>, Option<RefMut<TxW<'tx>>>) {
    let (r, w) = RefMut::map_split(self.cell.borrow_mut(), |b| (&mut b.r, &mut b.w));
    (r, Some(w))
  }

  fn split_ow_mut(&self) -> Option<RefMut<TxW<'tx>>> {
    Some(RefMut::map(self.cell.borrow_mut(), |c| &mut c.w))
  }

  fn split_ref_mut(
    &self,
  ) -> (
    RefMut<TxR<'tx>>,
    BucketRwCell<'tx>,
    Option<RefMut<TxW<'tx>>>,
  ) {
    let (r, w) = RefMut::map_split(self.cell.borrow_mut(), |b| (&mut b.r, &mut b.w));
    (r, self.cell.bound(), Some(w))
  }
}

impl<'tx> TxIApi<'tx> for TxRwCell<'tx> {
  type BucketType = BucketRwCell<'tx>;

  fn non_physical_rollback(self) -> crate::Result<()> {
    todo!()
  }

  fn close(self) -> crate::Result<()> {
    todo!()
  }
}

impl<'tx> TxRwIApi<'tx> for TxRwCell<'tx> {
  type CursorRwType = InnerCursor<'tx, Self, Self::BucketType>;

  fn freelist_free_page(self, txid: TxId, p: &Page) {
    self
      .cell
      .borrow()
      .r
      .db
      .get_mut()
      .unwrap()
      .free_page(txid, p)
  }

  fn root_bucket_mut(self) -> BucketRwCell<'tx> {
    self.split_bound()
  }

  fn allocate(
    self, count: usize,
  ) -> crate::Result<SelfOwned<AlignedBytes<alignment::Page>, MutPage<'tx>>> {
    let mut db = { self.cell.borrow().r.db.get_mut().unwrap() };
    let page = db.allocate(self, count as u64)?;

    let mut tx = self.cell.borrow_mut();
    tx.r.stats.page_count += 1;
    tx.r.stats.page_alloc += (count * tx.r.page_size) as i64;
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
    let (pages, mut db, page_size) = {
      let mut tx = self.cell.borrow_mut();
      let mut swap_pages = HashMap::with_capacity_in(0, tx.r.b);
      // Clear out page cache early.
      mem::swap(&mut swap_pages, &mut tx.w.pages);
      let mut pages = BVec::from_iter_in(swap_pages.into_iter().map(|(_, page)| page), tx.r.b);

      // Sort pages by id.
      pages.sort_by_key(|page| page.id);
      (pages, tx.r.db.get_mut().unwrap(), tx.r.page_size)
    };

    let mut stats = self.mut_stats();

    // Write pages to disk in order.
    for page in &pages {
      let mut rem = (page.overflow as usize + 1) * page_size;
      let mut offset = page.id.0 * page_size as u64;
      let mut written = 0;

      // Write out page in "max allocation" sized chunks.
      loop {
        let size = rem.min(MAX_ALLOC_SIZE.bytes() as usize - 1);
        let buf = &page.ref_owner()[written..size];

        let size = db.write_at(buf, offset)?;

        // Update statistics.
        stats.write += 1;

        rem -= size;
        if rem == 0 {
          break;
        }

        offset += size as u64;
        written += size;
      }
    }

    // TODO: skip fsync?
    db.fsync()?;

    for page in pages.into_iter() {
      if page.overflow == 0 {
        db.repool_allocated(page.into_owner());
      }
    }
    Ok(())
  }

  fn write_meta(self) -> crate::Result<()> {
    let mut r = self.split_r_mut();
    let page_size = r.page_size;

    let layout = Layout::from_size_align(page_size, mem::align_of::<MetaPage>()).unwrap();
    let ptr = r.b.alloc_layout(layout);

    let mut meta_page = unsafe { MappedMetaPage::new(ptr.as_ptr()) };
    r.meta.write(&mut meta_page);

    let mut db = r.db.get_mut().unwrap();
    let offset = meta_page.page.id.0 * page_size as u64;
    let buf = unsafe { from_raw_parts_mut(ptr.as_ptr(), page_size) };
    db.write_at(buf, offset)?;

    //TODO: Ignore sync
    db.fsync()?;

    r.stats.write += 1;

    Ok(())
  }
}

pub struct TxImpl<'tx> {
  bump: SyncReusable<Pin<Box<PinBump>>>,
  db: Pin<AliasableBox<PinLockGuard<'tx, DbShared>>>,
  pub(crate) tx: Rc<TxCell<'tx>>,
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
      let tx = Rc::new_cyclic(|weak| {
        let r = TxR {
          b: bump,
          page_size,
          db,
          meta,
          stats: Default::default(),
          is_rollback: false,
          p: Default::default(),
        };
        let bucket = BucketCell::new_in(bump, inline_bucket, weak.clone(), None);
        TxCell {
          cell: BCell::new_in(r, bucket, bump),
        }
      });
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
    let stats = self.stats();
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

  fn cursor(&self) -> impl CursorApi<'tx> {
    CursorImpl::new(self.tx.api_cursor())
  }

  fn stats(&self) -> TxStats {
    self.tx.api_stats()
  }

  fn bucket<T: AsRef<[u8]>>(&self, name: T) -> Option<impl BucketApi<'tx>> {
    self.tx.api_bucket(name.as_ref()).map(BucketImpl::from)
  }

  fn for_each<F: FnMut(&[u8], &dyn BucketApi<'tx>)>(&self, f: F) -> crate::Result<()> {
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
  fn id(&self) -> TxId {
    self.tx.api_id()
  }

  fn size(&self) -> u64 {
    self.tx.api_size()
  }

  fn writeable(&self) -> bool {
    false
  }

  fn cursor(&self) -> impl CursorApi<'tx> {
    CursorImpl::new(self.tx.api_cursor())
  }

  fn stats(&self) -> TxStats {
    self.tx.api_stats()
  }

  fn bucket<T: AsRef<[u8]>>(&self, name: T) -> Option<impl BucketApi<'tx>> {
    self.tx.api_bucket(name.as_ref()).map(BucketImpl::from)
  }

  fn for_each<F: FnMut(&[u8], &dyn BucketApi<'tx>)>(&self, f: F) -> crate::Result<()> {
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
  bump: SyncReusable<Pin<Box<PinBump>>>,
  db: Pin<AliasableBox<PinLockGuard<'tx, DbShared>>>,
  pub(crate) tx: Rc<TxRwCell<'tx>>,
}

impl<'tx> TxRwImpl<'tx> {
  pub(crate) fn get_ref(&self) -> TxRwRef<'tx> {
    TxRwRef {
      tx: TxRwCell { cell: self.tx.cell },
    }
  }

  pub(crate) fn new(
    bump: SyncReusable<Pin<Box<PinBump>>>, lock: RwLockUpgradableReadGuard<'tx, DbShared>,
    mut meta: Meta,
  ) -> TxRwImpl<'tx> {
    meta.set_txid(meta.txid() + 1);
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
      let tx = Rc::new_cyclic(|weak| {
        let r = TxR {
          b: bump,
          page_size,
          db,
          meta,
          stats: Default::default(),
          is_rollback: false,
          p: Default::default(),
        };
        let w = TxW {
          pages: HashMap::with_capacity_in(0, bump),
          commit_handlers: BVec::with_capacity_in(0, bump),
          p: Default::default(),
        };
        let bucket = BucketRwCell::new_in(bump, inline_bucket, weak.clone(), None);
        TxRwCell {
          cell: BCell::new_in(TxRW { r, w }, bucket, bump),
        }
      });
      addr_of_mut!((*ptr).tx).write(tx);
      uninit.assume_init()
    }
  }

  fn commit_freelist(&mut self) -> crate::Result<()> {
    let freelist_page = Pin::as_ref(&self.db)
      .guard()
      .get_mut()
      .unwrap()
      .commit_freelist(*self.tx)?;
    let pg_id = freelist_page.id;
    let mut tx = self.tx.cell.borrow_mut();
    tx.r.meta.set_free_list(pg_id);
    if let Some(old_page) = tx.w.pages.insert(pg_id, freelist_page) {
      if old_page.overflow == 0 {
        Pin::as_ref(&self.db)
          .guard()
          .get_mut()
          .unwrap()
          .repool_allocated(old_page.into_owner());
      }
    }
    Ok(())
  }
}

impl<'tx> Drop for TxRwImpl<'tx> {
  fn drop(&mut self) {
    let stats = self.stats();
    Pin::as_ref(&self.db)
      .guard()
      .get_mut()
      .unwrap()
      .remove_rw_tx(stats);
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

  fn cursor(&self) -> impl CursorApi<'tx> {
    CursorImpl::new(self.tx.api_cursor())
  }

  fn stats(&self) -> TxStats {
    self.tx.api_stats()
  }

  fn bucket<T: AsRef<[u8]>>(&self, name: T) -> Option<impl BucketApi<'tx>> {
    self.tx.api_bucket(name.as_ref()).map(BucketRwImpl::from)
  }

  fn for_each<F: FnMut(&[u8], &dyn BucketApi<'tx>)>(&self, f: F) -> crate::Result<()> {
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
  fn cursor_mut(&mut self) -> impl CursorRwApi<'tx> {
    CursorRwImpl::new(self.tx.api_cursor_mut())
  }

  fn bucket_mut<T: AsRef<[u8]>>(&mut self, name: T) -> Option<impl BucketRwApi<'tx>> {
    self.tx.api_bucket(name.as_ref()).map(BucketRwImpl::from)
  }

  fn create_bucket<T: AsRef<[u8]>>(&mut self, name: T) -> crate::Result<impl BucketRwApi<'tx>> {
    self
      .tx
      .api_create_bucket(name.as_ref())
      .map(BucketRwImpl::from)
  }

  fn create_bucket_if_not_exists<T: AsRef<[u8]>>(
    &mut self, name: T,
  ) -> crate::Result<impl BucketRwApi<'tx>> {
    self
      .tx
      .api_create_bucket_if_not_exist(name.as_ref())
      .map(BucketRwImpl::from)
  }

  fn delete_bucket<T: AsRef<[u8]>>(&mut self, name: T) -> crate::Result<()> {
    self.tx.api_delete_bucket(name.as_ref())
  }

  fn commit(mut self) -> crate::Result<()> {
    {
      Pin::as_ref(&self.db)
        .guard()
        .get_mut()
        .unwrap()
        .free_pages();
    }

    let start_time = Instant::now();
    self.tx.root_bucket().rebalance();
    {
      let mut stats = self.tx.mut_stats();
      if stats.rebalance > 0 {
        stats.rebalance_time += start_time.elapsed();
      }
    }
    let opgid = self.tx.meta().pgid();
    let bump = self.tx.bump();
    let start_time = Instant::now();
    match self.tx.root_bucket().spill(bump) {
      Ok(_) => {
        let mut stats = self.tx.mut_stats();
        stats.spill_time += start_time.elapsed();
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
      Pin::as_ref(&self.db)
        .guard()
        .get_mut()
        .unwrap()
        .free_page(tx_id, &freelist_pg);
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
        .get_mut()
        .unwrap()
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

    // TODO: Add strict mode

    match self.tx.write_meta() {
      Ok(_) => {
        let mut stats = self.tx.mut_stats();
        stats.write_time += start_time.elapsed();
      }
      Err(e) => {
        let _ = self.rollback();
        return Err(e);
      }
    }

    let mut tx = self.tx.cell.borrow_mut();
    for f in &tx.w.commit_handlers {
      f();
    }
    tx.w.commit_handlers.clear();
    let bytes = tx.r.b.allocated_bytes();
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

  fn cursor(&self) -> impl CursorApi<'tx> {
    CursorImpl::new(self.tx.api_cursor())
  }

  fn stats(&self) -> TxStats {
    self.tx.api_stats()
  }

  fn bucket<T: AsRef<[u8]>>(&self, name: T) -> Option<impl BucketApi<'tx>> {
    self.tx.api_bucket(name.as_ref()).map(BucketRwImpl::from)
  }

  fn for_each<F: FnMut(&[u8], &dyn BucketApi<'tx>)>(&self, f: F) -> crate::Result<()> {
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
  fn cursor_mut(&mut self) -> impl CursorRwApi<'tx> {
    CursorRwImpl::new(self.tx.api_cursor_mut())
  }

  fn bucket_mut<T: AsRef<[u8]>>(&mut self, name: T) -> Option<impl BucketRwApi<'tx>> {
    self.tx.api_bucket(name.as_ref()).map(BucketRwImpl::from)
  }

  fn create_bucket<T: AsRef<[u8]>>(&mut self, name: T) -> crate::Result<impl BucketRwApi<'tx>> {
    self
      .tx
      .api_create_bucket(name.as_ref())
      .map(BucketRwImpl::from)
  }

  fn create_bucket_if_not_exists<T: AsRef<[u8]>>(
    &mut self, name: T,
  ) -> crate::Result<impl BucketRwApi<'tx>> {
    self
      .tx
      .api_create_bucket_if_not_exist(name.as_ref())
      .map(BucketRwImpl::from)
  }

  fn delete_bucket<T: AsRef<[u8]>>(&mut self, name: T) -> crate::Result<()> {
    self.tx.api_delete_bucket(name.as_ref())
  }

  fn commit(self) -> crate::Result<()> {
    todo!()
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

    fn unseal(&self) -> impl TxIApi<'tx> + TxICheck<'tx>{
      TxCell { cell: self.tx.cell }
    }
  }

  impl<'tx> UnsealTx<'tx> for TxRwImpl<'tx> {
    fn unseal(&self) -> impl TxIApi<'tx> + TxICheck<'tx>{
      TxRwCell { cell: self.tx.cell }
    }
  }

  impl<'tx> UnsealTx<'tx> for TxRwRef<'tx> {
    fn unseal(&self) -> impl TxIApi<'tx> + TxICheck<'tx>{
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
  pub trait TxCheck<'tx>: UnsealTx<'tx> {
    fn check(&self) -> Vec<String> {
      let i_tx = self.unseal();
      i_tx.check()
    }
  }

  impl<'tx, T> TxCheck<'tx> for T where T: UnsealTx<'tx> {}

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
      for i in 0..freelist_count {
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
  use crate::test_support::TestDb;
  use crate::{DbApi, DbRwAPI, TxApi, TxRwApi};
  use bumpalo::Bump;

  #[test]
  fn test_tx_check_read_only() {
    todo!()
  }

  #[test]
  fn test_tx_commit_err_tx_closed() {
    todo!()
  }

  #[test]
  fn test_tx_rollback_err_tx_closed() {
    todo!()
  }

  #[test]
  fn test_tx_commit_err_tx_not_writable() {
    todo!()
  }

  #[test]
  fn test_tx_cursor() {
    todo!()
  }

  #[test]
  fn test_tx_create_bucket_err_tx_not_writable() {
    todo!()
  }

  #[test]
  fn test_tx_create_bucket_err_tx_closed() {
    todo!()
  }

  #[test]
  fn test_tx_bucket() {
    todo!()
  }

  #[test]
  fn test_tx_get_not_found() {
    todo!()
  }

  #[test]
  fn test_tx_create_bucket() -> crate::Result<()> {
    let mut db = TestDb::new()?;
    db.update(|mut tx| {
      let bucket = tx.create_bucket(b"widgets")?;
      Ok(())
    })?;
    db.view(|tx| {
      let bucket = tx.bucket(b"widgets");
      assert!(bucket.is_some(), "expected bucket");
      Ok(())
    })
  }

  #[test]
  fn test_tx_create_bucket_if_not_exists() {
    todo!()
  }

  #[test]
  fn test_tx_create_bucket_if_not_exists_err_bucket_name_required() {
    todo!()
  }

  #[test]
  fn test_tx_create_bucket_err_bucket_exists() {
    todo!()
  }

  #[test]
  fn test_tx_create_bucket_err_bucket_name_required() {
    todo!()
  }

  #[test]
  fn test_tx_delete_bucket() {
    todo!()
  }

  #[test]
  fn test_tx_delete_bucket_err_tx_closed() {
    todo!()
  }

  #[test]
  fn test_tx_delete_bucket_read_only() {
    todo!()
  }

  #[test]
  fn test_tx_delete_bucket_not_found() {
    todo!()
  }

  #[test]
  fn test_tx_for_each_no_error() {
    todo!()
  }

  #[test]
  fn test_tx_for_each_with_error() {
    todo!()
  }

  #[test]
  fn test_tx_on_commit() {
    todo!()
  }

  #[test]
  fn test_tx_on_commit_rollback() {
    todo!()
  }

  #[test]
  fn test_tx_copy_file() {
    todo!()
  }

  #[test]
  fn test_tx_copy_file_error_meta() {
    todo!()
  }

  #[test]
  fn test_tx_copy_file_error_normal() {
    todo!()
  }

  #[test]
  fn test_tx_rollback() {
    todo!()
  }

  #[test]
  fn test_tx_release_range() {
    todo!()
  }

  #[test]
  fn example_tx_rollback() {
    todo!()
  }

  #[test]
  fn example_tx_copy_file() {
    todo!()
  }

  #[test]
  fn test_tx_stats_get_and_inc_atomically() {
    todo!()
  }

  #[test]
  fn test_tx_stats_sub() {
    todo!()
  }

  #[test]
  fn test_tx_truncate_before_write() {
    todo!()
  }

  #[test]
  fn test_tx_stats_add() {
    todo!()
  }
}
