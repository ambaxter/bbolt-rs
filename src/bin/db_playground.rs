use aliasable::boxed::AliasableBox;
use aligners::AlignedBytes;
use bbolt_rs::{PinBump, PinLockGuard, SyncPool, SyncReusable};
use parking_lot::RwLock;
use std::pin::Pin;
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
  bump_pool: Arc<SyncPool<Pin<Box<PinBump>>>>,
  meta: Arc<Mutex<Meta>>,
  backend: Arc<RwLock<TxResources>>,
}

impl Db {
  fn new() -> Db {
    Db {
      bump_pool: SyncPool::pin_default(),
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
      bump: self.bump_pool.pull(),
      meta: meta_lock.clone(),
      db: AliasableBox::from_unique_pin(Box::pin(PinLockGuard::new(lock))),
      tx_stats: TxStats {},
    }
  }

  fn w(&mut self) -> Tx {
    let lock = self.backend.upgradable_read();
    let meta_lock = self.meta.lock().unwrap();
    Tx {
      bump: self.bump_pool.pull(),
      meta: meta_lock.clone(),
      db: AliasableBox::from_unique_pin(Box::pin(PinLockGuard::new(lock))),
      tx_stats: TxStats {},
    }
  }
}

#[derive(Default, Clone)]
struct TxStats {}

struct Tx<'a> {
  bump: SyncReusable<Pin<Box<PinBump>>>,
  meta: Meta,
  db: Pin<AliasableBox<PinLockGuard<'a, TxResources>>>,
  tx_stats: TxStats,
}

fn send_test<T: Send>(_s: &T) {}

fn sync_test<T: Sync + Send>(_s: &T) {}

fn main() -> bbolt_rs::Result<()> {
  let pool: Arc<SyncPool<Pin<Box<PinBump>>>> = SyncPool::pin_default();
  sync_test(&pool);
  let bump = pool.pull();
  send_test(&bump);
  drop(bump);

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
    Pin::as_ref(&w.db).guard().get_mut();
  }

  let w = db.w();

  Ok(())
}
