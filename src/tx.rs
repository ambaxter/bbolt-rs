use crate::bucket::{BucketCell, BucketIAPI, BucketRW, BucketRwCell};
use crate::common::defaults::DEFAULT_PAGE_SIZE;
use crate::common::memory::SCell;
use crate::common::meta::Meta;
use crate::common::page::{MutPage, PageInfo, RefPage};
use crate::common::selfowned::SelfOwned;
use crate::common::{BVec, HashMap, PgId, SplitRef, TxId};
use crate::cursor::{CursorRwIAPI, InnerCursor};
use crate::db::DBShared;
use crate::freelist::Freelist;
use crate::node::NodeRwCell;
use crate::{BucketApi, CursorApi, CursorRwApi};
use bumpalo::Bump;
use parking_lot::{RwLockReadGuard, RwLockWriteGuard};
use std::borrow::Cow;
use std::cell;
use std::cell::{Ref, RefCell, RefMut};
use std::marker::{PhantomData, PhantomPinned};
use std::mem::MaybeUninit;
use std::ops::{AddAssign, Deref, DerefMut, Sub};
use std::pin::Pin;
use std::ptr::{addr_of, addr_of_mut};
use std::rc::Rc;
use std::time::Duration;

pub trait TxApi<'tx>: {
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
  fn page(id: PgId) -> crate::Result<PageInfo>;
}

pub trait TxMutApi<'tx>: TxApi<'tx> {
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

#[derive(Copy, Clone)]
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
    todo!()
  }

  fn page_mut(self, id: PgId) -> MutPage<'tx> {
    todo!()
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
    let(_, root_bucket, _) = self.split_ref();
    root_bucket.i_cursor()
  }

  fn api_stats(self) -> TxStats {
    todo!()
  }

  fn api_bucket(self, name: &[u8]) -> Option<Self::BucketType> {
    let(_, root_bucket, _) = self.split_ref();
    root_bucket.api_bucket(name)
  }

  fn api_for_each<F: FnMut(&[u8], Self::BucketType)>(&self, mut f: F) -> crate::Result<()> {
    let(_, root_bucket, _) = self.split_ref();
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
    todo!()
  }

  fn api_page(id: PgId) -> crate::Result<PageInfo> {
    todo!()
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
  pub fn page<'tx, T: TxIAPI<'tx>>(cell: &T, id: PgId) -> RefPage<'tx> {
    todo!()
  }

  pub(crate) fn for_each_page<'tx, T: TxIAPI<'tx>, F: FnMut(&RefPage, usize, &[PgId])>(
    cell: &T, root: PgId, f: F,
  ) {
    todo!()
  }
}

struct TxOwned<D: Deref<Target = DBShared> + Unpin> {
  b: Bump,
  db: RefCell<D>,
}

struct DBHider<'tx, D: Deref<Target = DBShared> + 'tx + Unpin> {
  db: &'tx RefCell<D>,
}

trait DBAccess {
  fn get(&self) -> Ref<DBShared>;
  fn get_mut(&self) -> Option<RefMut<DBShared>>;
}

impl<'tx> DBAccess for DBHider<'tx, RwLockReadGuard<'tx, DBShared>> {
  fn get(&self) -> Ref<DBShared> {
    Ref::map(self.db.borrow(), |r| r.deref())
  }

  fn get_mut(&self) -> Option<RefMut<DBShared>> {
    None
  }
}

impl<'tx> DBAccess for DBHider<'tx, RwLockWriteGuard<'tx, DBShared>> {
  fn get(&self) -> Ref<DBShared> {
    Ref::map(self.db.borrow(), |r| r.deref())
  }

  fn get_mut(&self) -> Option<RefMut<DBShared>> {
    Some(RefMut::map(self.db.borrow_mut(), |w| w.deref_mut()))
  }
}

pub struct TxR<'tx> {
  b: &'tx Bump,
  page_size: usize,

  db_ref: &'tx dyn DBAccess,
  is_managed: bool,
  is_rollback: bool,
  meta: Meta,
  p: PhantomData<&'tx u8>,
}

pub struct TxW<'tx> {
  pages: HashMap<'tx, PgId, MutPage<'tx>>,
  //TODO: We leak memory when this drops. Need special handling here
  commit_handlers: BVec<'tx, Box<dyn FnMut()>>,
  is_commit: bool,
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
    todo!()
  }

  fn api_create_bucket(self, name: &[u8]) -> crate::Result<Self::BucketType> {
    todo!()
  }

  fn api_create_bucket_if_not_exist(self, name: &[u8]) -> crate::Result<Self::BucketType> {
    todo!()
  }

  fn api_delete_bucket(self, name: &[u8]) -> crate::Result<()> {
    todo!()
  }

  fn api_commit(self) -> crate::Result<()> {
    todo!()
  }
}

struct TxSelfRef<'tx, T: TxIAPI<'tx>> {
  h: Pin<Box<dyn DBAccess + 'tx>>,
  tx: Pin<Rc<T>>,
  marker: PhantomPinned,
}

impl<'tx, T: TxIAPI<'tx>> TxSelfRef<'tx, T> {
  fn new_tx(o: &'tx mut TxOwned<RwLockReadGuard<'tx, DBShared>>) -> TxSelfRef<'tx, TxCell<'tx>> {
    let bump = &o.b;
    let meta = o.db.borrow().backend.meta();
    let page_size = meta.page_size() as usize;
    let inline_bucket = meta.root();
    let mut uninit: MaybeUninit<TxSelfRef<'tx, TxCell<'tx>>> = MaybeUninit::uninit();
    let ptr = uninit.as_mut_ptr();
    unsafe {
      addr_of_mut!((*ptr).h).write(Box::pin(DBHider { db: &o.db }));
      let db = &(**(addr_of_mut!((*ptr).h)));
      let tx = Rc::new_cyclic(|weak_tx| {
        let r = TxR {
          b: &o.b,
          page_size,
          db_ref: db,
          is_managed: false,
          is_rollback: false,
          meta,
          p: Default::default(),
        };
        let bucket = BucketCell::new_in(bump, inline_bucket, weak_tx.clone(), None);
        TxCell {
          cell: SCell::new_in((r, bucket), bump),
        }
      });

      addr_of_mut!((*ptr).tx).write(Pin::new(tx));
      addr_of_mut!((*ptr).marker).write(PhantomPinned);
      uninit.assume_init()
    }
  }

  fn new_tx_mut(
    o: &'tx mut TxOwned<RwLockWriteGuard<'tx, DBShared>>,
  ) -> TxSelfRef<'tx, TxRwCell<'tx>> {
    let bump = &o.b;
    let meta = o.db.borrow().backend.meta();
    let page_size = meta.page_size() as usize;
    let inline_bucket = meta.root();
    let mut uninit: MaybeUninit<TxSelfRef<'tx, TxRwCell<'tx>>> = MaybeUninit::uninit();
    let ptr = uninit.as_mut_ptr();
    unsafe {
      addr_of_mut!((*ptr).h).write(Box::pin(DBHider { db: &o.db }));
      let db = &(**(addr_of_mut!((*ptr).h)));
      let tx = Rc::new_cyclic(|weak_tx| {
        let rw = TxRW {
          r: TxR {
            b: &o.b,
            page_size,
            db_ref: db,
            is_managed: false,
            is_rollback: false,
            meta,
            p: Default::default(),
          },
          w: TxW {
            pages: HashMap::with_capacity_in(0, &o.b),
            commit_handlers: BVec::with_capacity_in(0, &o.b),
            is_commit: false,
            p: Default::default(),
          },
        };
        let bucket = BucketRwCell::new_in(bump, inline_bucket, weak_tx.clone(), None);
        TxRwCell {
          cell: SCell::new_in((rw, bucket), bump),
        }
      });
      addr_of_mut!((*ptr).tx).write(Pin::new(tx));
      addr_of_mut!((*ptr).marker).write(PhantomPinned);
      uninit.assume_init()
    }
  }
}

struct TxHolder<'tx, D: Deref<Target = DBShared> + Unpin, T: TxIAPI<'tx>> {
  s: SelfOwned<TxOwned<D>, TxSelfRef<'tx, T>>,
}

impl<'tx, D: Deref<Target = DBShared> + Unpin, T: TxIAPI<'tx>> TxHolder<'tx, D, T> {
  fn new_tx(
    lock: RwLockReadGuard<'tx, DBShared>,
  ) -> TxHolder<'tx, RwLockReadGuard<'tx, DBShared>, TxCell<'tx>> {
    let bump = Bump::new();
    let tx_owned = TxOwned {
      b: bump,
      db: RefCell::new(lock),
    };
    TxHolder {
      s: SelfOwned::new_with_map(tx_owned, |t| TxSelfRef::<TxCell<'tx>>::new_tx(t)),
    }
  }

  fn new_rwtx(
    lock: RwLockWriteGuard<'tx, DBShared>,
  ) -> TxHolder<'tx, RwLockWriteGuard<'tx, DBShared>, TxRwCell<'tx>> {
    let bump = Bump::new();
    let tx_owned = TxOwned {
      b: bump,
      db: RefCell::new(lock),
    };
    TxHolder {
      s: SelfOwned::new_with_map(tx_owned, |t| TxSelfRef::<TxRwCell<'tx>>::new_tx_mut(t)),
    }
  }
}

pub struct TxImpl {}

pub struct TxRwImpl {}

pub struct TxRef<'tx> {
  tx: TxCell<'tx>
}

pub struct TxRwRef<'tx> {
  tx: TxRwCell<'tx>
}
