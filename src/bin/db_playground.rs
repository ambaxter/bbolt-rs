use aliasable::boxed::AliasableBox;
use aligners::AlignedBytes;
use bbolt_rs::{BumpPool, PinBump, UpgradableGuard};
use parking_lot::{RwLock, RwLockReadGuard, RwLockUpgradableReadGuard, RwLockWriteGuard};
use pin_project::pin_project;
use std::cell::{RefCell, RefMut};
use std::ops::{Deref, DerefMut};
use std::pin::{pin, Pin};
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::{mem, thread};

#[derive(Default)]
struct DbBackend {
  alloc_pool: Vec<AlignedBytes<aligners::alignment::Page>>,
}

unsafe impl Sync for DbBackend {}
unsafe impl Send for DbBackend {}

#[derive(Default, Clone)]
struct Meta {}

#[derive(Default, Clone)]
struct DbStats {}

struct TxResources {
  backend: DbBackend,
}

#[derive(Clone)]
struct Db {
  bump_pool: BumpPool,
  meta: Arc<Mutex<Meta>>,
  backend: Arc<RwLock<TxResources>>,
}

impl Db {
  fn new() -> Db {
    Db {
      bump_pool: Default::default(),
      meta: Mutex::new(Meta {}).into(),
      backend: RwLock::new(TxResources {
        backend: Default::default(),
      })
      .into(),
    }
  }

  fn r(&self) -> Tx {
    let meta_lock = self.meta.lock().unwrap();
    let lock = self.backend.read();
    Tx {
      bump: self.bump_pool.pop(),
      meta: meta_lock.clone(),
      db: AliasableBox::from_unique_pin(Box::pin(PinDbGuard::new(lock))),
      tx_stats: TxStats {},
    }
  }

  fn w(&mut self) -> Tx {
    let lock = self.backend.upgradable_read();
    let meta_lock = self.meta.lock().unwrap();
    Tx {
      bump: self.bump_pool.pop(),
      meta: meta_lock.clone(),
      db: AliasableBox::from_unique_pin(Box::pin(PinDbGuard::new(lock))),
      tx_stats: TxStats {},
    }
  }
}

#[derive(Default, Clone)]
struct TxStats {}

enum DbGuard<'a, T> {
  R(RwLockReadGuard<'a, T>),
  U(RefCell<UpgradableGuard<'a, T>>),
}

impl<'a, T> DbGuard<'a, T> {
  fn is_write(&self) -> bool {
    if let DbGuard::U(g) = self {
      g.borrow().is_write()
    } else {
      false
    }
  }

  fn get_mut(&self) -> Option<RefMut<T>> {
    match self {
      DbGuard::R(_) => None,
      DbGuard::U(g) => Some(RefMut::map(g.borrow_mut(), |l| l.deref_mut())),
    }
  }
}

impl<'a, T> From<RwLockReadGuard<'a, T>> for DbGuard<'a, T> {
  fn from(value: RwLockReadGuard<'a, T>) -> Self {
    DbGuard::R(value)
  }
}

impl<'a, T> From<RwLockUpgradableReadGuard<'a, T>> for DbGuard<'a, T> {
  fn from(value: RwLockUpgradableReadGuard<'a, T>) -> Self {
    DbGuard::U(RefCell::new(UpgradableGuard::UR(value)))
  }
}

impl<'a, T> From<RwLockWriteGuard<'a, T>> for DbGuard<'a, T> {
  fn from(value: RwLockWriteGuard<'a, T>) -> Self {
    DbGuard::U(RefCell::new(UpgradableGuard::W(value)))
  }
}

// The empty derive prevents RustRover from having issues
#[derive()]
#[pin_project(!Unpin)]
struct PinDbGuard<'a, T> {
  #[pin]
  guard: DbGuard<'a, T>,
}

impl<'a, T> PinDbGuard<'a, T> {
  fn new<L: Into<DbGuard<'a, T>>>(lock: L) -> PinDbGuard<'a, T> {
    PinDbGuard { guard: lock.into() }
  }
}

struct Tx<'a> {
  bump: Pin<Box<PinBump>>,
  meta: Meta,
  db: Pin<AliasableBox<PinDbGuard<'a, TxResources>>>,
  tx_stats: TxStats,
}

fn send_test<T: Send>(_s: &T) {}

fn sync_test<T: Sync + Send>(_s: &T) {}

fn main() -> bbolt_rs::Result<()> {
  let pool = BumpPool::default();
  sync_test(&pool);
  let bump = pool.pop();
  send_test(&bump);
  pool.push(bump);

  let mut db = Db::new();
  sync_test(&db);
  send_test(&db);

  {
    let mut v = vec![];
    for _ in 0..50 {
      v.push(db.r());
    }
  }
  {
    let w = db.w();
    w.db.guard.get_mut();
  }

  let w = db.w();

  Ok(())
}
