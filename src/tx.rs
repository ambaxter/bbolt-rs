use crate::bucket::{Bucket, BucketIAPI, BucketMut};
use crate::common::defaults::DEFAULT_PAGE_SIZE;
use crate::common::memory::SCell;
use crate::common::meta::Meta;
use crate::common::page::{MutPage, PageInfo, RefPage};
use crate::common::selfowned::SelfOwned;
use crate::common::{BVec, HashMap, PgId, SplitRef, TxId};
use crate::db::DBShared;
use crate::freelist::Freelist;
use crate::node::NodeMut;
use crate::{BucketAPI, CursorAPI, CursorMutAPI};
use bumpalo::Bump;
use parking_lot::{RwLockReadGuard, RwLockWriteGuard};
use std::borrow::Cow;
use std::cell;
use std::cell::{Ref, RefCell, RefMut};
use std::marker::{PhantomData, PhantomPinned};
use std::mem::MaybeUninit;
use std::ops::{AddAssign, Deref, DerefMut, Sub};
use std::pin::Pin;
use std::ptr::addr_of_mut;
use std::time::Duration;

pub trait TxAPI<'tx>: 'tx {
  type CursorType: CursorAPI<'tx>;
  type BucketType: BucketAPI<'tx>;

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

pub trait TxMutAPI<'tx>: TxAPI<'tx> {
  type CursorMutType: CursorMutAPI<'tx>;

  /// Cursor creates a cursor associated with the root bucket.
  /// All items in the cursor will return a nil value because all root bucket keys point to buckets.
  /// The cursor is only valid as long as the transaction is open.
  /// Do not use a cursor after the transaction is closed.
  fn cursor_mut(&mut self) -> Self::CursorMutType;

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
  fn delete_bucket(name: &[u8]) -> crate::Result<()>;

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
pub(crate) trait TxIAPI<'tx>: SplitRef<TxR<'tx>, TxW<'tx>> + 'tx {
  fn bump(&self) -> &'tx Bump;

  fn page_size(&self) -> usize;

  fn meta(&self) -> Ref<Meta>;

  fn page(&self, id: PgId) -> RefPage<'tx> {
    todo!()
  }

  fn txid(&self) -> TxId;
}

pub trait TxMutIAPI<'tx> {
  fn freelist(&self) -> RefMut<Freelist>;

  fn allocate(&self, count: usize) -> crate::Result<MutPage<'tx>>;
}

pub(crate) struct TxImpl {}

impl TxImpl {
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
  managed: bool,
  meta: Meta,
  p: PhantomData<&'tx u8>,
}

pub struct TxW<'tx> {
  pages: HashMap<'tx, PgId, RefPage<'tx>>,
  commit_handlers: BVec<'tx, Box<dyn FnMut()>>,
  p: PhantomData<&'tx u8>,
}

pub struct TxRW<'tx> {
  r: TxR<'tx>,
  w: TxW<'tx>,
}

#[derive(Copy, Clone)]
pub struct Tx<'tx> {
  cell: SCell<'tx, TxR<'tx>>,
}

impl<'tx> SplitRef<TxR<'tx>, TxW<'tx>> for Tx<'tx> {
  fn split_ref(&self) -> (Ref<TxR<'tx>>, Option<Ref<TxW<'tx>>>) {
    (RefCell::borrow(&*self.cell), None)
  }

  fn split_ref_mut(&self) -> (RefMut<TxR<'tx>>, Option<RefMut<TxW<'tx>>>) {
    (self.cell.borrow_mut(), None)
  }
}

impl<'tx> TxIAPI<'tx> for Tx<'tx> {
  #[inline(always)]
  fn bump(&self) -> &'tx Bump {
    self.cell.borrow().b
  }

  #[inline(always)]
  fn page_size(&self) -> usize {
    RefCell::borrow(&self.cell).page_size
  }

  fn meta(&self) -> Ref<Meta> {
    Ref::map(RefCell::borrow(&self.cell), |tx| &tx.meta)
  }

  fn txid(&self) -> TxId {
    RefCell::borrow(&self.cell).meta.txid()
  }
}

#[derive(Copy, Clone)]
pub struct TxMut<'tx> {
  cell: SCell<'tx, TxRW<'tx>>,
}

impl<'tx> SplitRef<TxR<'tx>, TxW<'tx>> for TxMut<'tx> {
  fn split_ref(&self) -> (Ref<TxR<'tx>>, Option<Ref<TxW<'tx>>>) {
    let (r, w) = Ref::map_split(RefCell::borrow(&*self.cell), |b| (&b.r, &b.w));
    (r, Some(w))
  }

  fn split_ref_mut(&self) -> (RefMut<TxR<'tx>>, Option<RefMut<TxW<'tx>>>) {
    let (r, w) = RefMut::map_split(self.cell.borrow_mut(), |b| (&mut b.r, &mut b.w));
    (r, Some(w))
  }
}

impl<'tx> TxIAPI<'tx> for TxMut<'tx> {
  #[inline(always)]
  fn bump(&self) -> &'tx Bump {
    self.cell.borrow().r.b
  }

  #[inline(always)]
  fn page_size(&self) -> usize {
    self.cell.borrow().r.page_size
  }

  fn meta(&self) -> Ref<Meta> {
    Ref::map(self.cell.borrow(), |tx| &tx.r.meta)
  }

  fn txid(&self) -> TxId {
    self.cell.borrow().r.meta.txid()
  }
}

impl<'tx> TxMutIAPI<'tx> for TxMut<'tx> {
  fn freelist(&self) -> RefMut<Freelist> {
    todo!()
  }

  fn allocate(&self, count: usize) -> crate::Result<MutPage<'tx>> {
    todo!()
  }
}

struct TxSelfRef<'tx, T: TxIAPI<'tx>> {
  h: Pin<Box<dyn DBAccess + 'tx>>,
  tx: T,
  marker: PhantomPinned,
}

impl<'tx, T: TxIAPI<'tx>> TxSelfRef<'tx, T> {
  fn new_tx(o: &'tx mut TxOwned<RwLockReadGuard<'tx, DBShared>>) -> TxSelfRef<'tx, Tx<'tx>> {
    let page_size = o.db.borrow().backend.meta().page_size() as usize;
    let mut uninit: MaybeUninit<TxSelfRef<'tx, Tx<'tx>>> = MaybeUninit::uninit();
    let ptr = uninit.as_mut_ptr();
    unsafe {
      addr_of_mut!((*ptr).h).write(Box::pin(DBHider { db: &o.db }));
      let db = &(**(addr_of_mut!((*ptr).h)));
      let tx = Tx {
        cell: SCell::new_in(
          TxR {
            b: &o.b,
            page_size,
            db_ref: db,
            managed: false,
            meta: db.get().backend.meta(),
            p: Default::default(),
          },
          &o.b,
        ),
      };
      addr_of_mut!((*ptr).tx).write(tx);
      addr_of_mut!((*ptr).marker).write(PhantomPinned);
      uninit.assume_init()
    }
  }

  fn new_tx_mut(
    o: &'tx mut TxOwned<RwLockWriteGuard<'tx, DBShared>>,
  ) -> TxSelfRef<'tx, TxMut<'tx>> {
    let page_size = o.db.borrow().backend.meta().page_size() as usize;
    let mut uninit: MaybeUninit<TxSelfRef<'tx, TxMut<'tx>>> = MaybeUninit::uninit();
    let ptr = uninit.as_mut_ptr();
    unsafe {
      addr_of_mut!((*ptr).h).write(Box::pin(DBHider { db: &o.db }));
      let db = &(**(addr_of_mut!((*ptr).h)));
      let tx = TxMut {
        cell: SCell::new_in(
          TxRW {
            r: TxR {
              b: &o.b,
              page_size,
              db_ref: db,
              managed: false,
              meta: db.get().backend.meta(),
              p: Default::default(),
            },
            w: TxW {
              pages: HashMap::with_capacity_in(0, &o.b),
              commit_handlers: BVec::with_capacity_in(0, &o.b),
              p: Default::default(),
            },
          },
          &o.b,
        ),
      };
      addr_of_mut!((*ptr).tx).write(tx);
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
  ) -> TxHolder<'tx, RwLockReadGuard<'tx, DBShared>, Tx<'tx>> {
    let bump = Bump::new();
    let tx_owned = TxOwned {
      b: bump,
      db: RefCell::new(lock),
    };
    TxHolder {
      s: SelfOwned::new_with_map(tx_owned, |t| TxSelfRef::<Tx<'tx>>::new_tx(t)),
    }
  }

  fn new_rwtx(
    lock: RwLockWriteGuard<'tx, DBShared>,
  ) -> TxHolder<'tx, RwLockWriteGuard<'tx, DBShared>, TxMut<'tx>> {
    let bump = Bump::new();
    let tx_owned = TxOwned {
      b: bump,
      db: RefCell::new(lock),
    };
    TxHolder {
      s: SelfOwned::new_with_map(tx_owned, |t| TxSelfRef::<TxMut<'tx>>::new_tx_mut(t)),
    }
  }
}
