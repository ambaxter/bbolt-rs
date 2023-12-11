use crate::bucket::{Bucket, BucketIAPI, BucketMut};
use crate::common::defaults::DEFAULT_PAGE_SIZE;
use crate::common::memory::SCell;
use crate::common::meta::Meta;
use crate::common::page::{MutPage, RefPage};
use crate::common::selfowned::SelfOwned;
use crate::common::{BVec, HashMap, IRef, PgId, TxId};
use crate::db::DBShared;
use crate::freelist::Freelist;
use crate::node::NodeMut;
use bumpalo::Bump;
use parking_lot::{RwLockReadGuard, RwLockWriteGuard};
use std::borrow::Borrow;
use std::cell;
use std::cell::{Ref, RefCell, RefMut};
use std::marker::{PhantomData, PhantomPinned};
use std::mem::MaybeUninit;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::ptr::addr_of_mut;

pub(crate) trait TxIAPI<'tx>: IRef<TxR<'tx>, TxW<'tx>> + 'tx {
  fn bump(&self) -> &'tx Bump;

  fn page_size(&self) -> usize;

  fn meta(&self) -> &Meta;

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

pub trait TxAPI<'tx>: Copy + Clone + 'tx {
  fn writeable(&self) -> bool;
}

pub trait TxMutAPI<'tx>: TxAPI<'tx> {}

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

impl<'tx> IRef<TxR<'tx>, TxW<'tx>> for Tx<'tx> {
  fn borrow_iref(&self) -> (Ref<TxR<'tx>>, Option<Ref<TxW<'tx>>>) {
    (RefCell::borrow(&*self.cell), None)
  }

  fn borrow_mut_iref(&self) -> (RefMut<TxR<'tx>>, Option<RefMut<TxW<'tx>>>) {
    (self.cell.borrow_mut(), None)
  }
}

impl<'tx> TxIAPI<'tx> for Tx<'tx> {
  #[inline(always)]
  fn bump(&self) -> &'tx Bump {
    todo!()
  }

  #[inline(always)]
  fn page_size(&self) -> usize {
    DEFAULT_PAGE_SIZE.bytes() as usize
  }

  fn meta(&self) -> &Meta {
    todo!()
  }

  fn txid(&self) -> TxId {
    todo!()
  }
}

impl<'tx> TxAPI<'tx> for Tx<'tx> {
  fn writeable(&self) -> bool {
    false
  }
}

#[derive(Copy, Clone)]
pub struct TxMut<'tx> {
  cell: SCell<'tx, TxRW<'tx>>,
}

impl<'tx> IRef<TxR<'tx>, TxW<'tx>> for TxMut<'tx> {
  fn borrow_iref(&self) -> (Ref<TxR<'tx>>, Option<Ref<TxW<'tx>>>) {
    let (r, w) = Ref::map_split(RefCell::borrow(&*self.cell), |b| (&b.r, &b.w));
    (r, Some(w))
  }

  fn borrow_mut_iref(&self) -> (RefMut<TxR<'tx>>, Option<RefMut<TxW<'tx>>>) {
    let (r, w) = RefMut::map_split(self.cell.borrow_mut(), |b| (&mut b.r, &mut b.w));
    (r, Some(w))
  }
}

impl<'tx> TxIAPI<'tx> for TxMut<'tx> {
  #[inline(always)]
  fn bump(&self) -> &'tx Bump {
    todo!()
  }

  #[inline(always)]
  fn page_size(&self) -> usize {
    DEFAULT_PAGE_SIZE.bytes() as usize
  }

  fn meta(&self) -> &Meta {
    todo!()
  }

  fn txid(&self) -> TxId {
    todo!()
  }
}

impl<'tx> TxAPI<'tx> for TxMut<'tx> {
  fn writeable(&self) -> bool {
    true
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

impl<'tx> TxMutAPI<'tx> for TxMut<'tx> {}

struct TxSelfRef<'tx, T: TxIAPI<'tx>> {
  h: Pin<Box<dyn DBAccess + 'tx>>,
  tx: T,
  marker: PhantomPinned,
}

impl<'tx, T: TxIAPI<'tx>> TxSelfRef<'tx, T> {
  fn new_tx(o: &'tx mut TxOwned<RwLockReadGuard<'tx, DBShared>>) -> TxSelfRef<'tx, Tx<'tx>> {
    let mut uninit: MaybeUninit<TxSelfRef<'tx, Tx<'tx>>> = MaybeUninit::uninit();
    let ptr = uninit.as_mut_ptr();
    unsafe {
      addr_of_mut!((*ptr).h).write(Box::pin(DBHider { db: &o.db }));
      let db = &(**(addr_of_mut!((*ptr).h)));
      let tx = Tx {
        cell: SCell::new_in(
          TxR {
            b: &o.b,
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
