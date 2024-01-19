use parking_lot::{RwLockUpgradableReadGuard, RwLockWriteGuard};
use std::mem;
use std::ops::{Deref, DerefMut};

pub enum UpgradableGuard<'a, T> {
  Upgrading,
  UR(RwLockUpgradableReadGuard<'a, T>),
  W(RwLockWriteGuard<'a, T>),
}

impl<'a, T> UpgradableGuard<'a, T> {
  pub fn is_write(&self) -> bool {
    matches!(self, UpgradableGuard::W(_))
  }

  pub fn upgrade(&mut self) {
    let guard = mem::replace(self, UpgradableGuard::Upgrading);
    *self = match guard {
      UpgradableGuard::UR(g) => UpgradableGuard::W(RwLockUpgradableReadGuard::upgrade(g)),
      w @ UpgradableGuard::W(_) => w,
      _ => panic!("In Upgrading State"),
    };
  }
}

impl<'a, T> Deref for UpgradableGuard<'a, T> {
  type Target = T;

  fn deref(&self) -> &Self::Target {
    match self {
      UpgradableGuard::UR(g) => g,
      UpgradableGuard::W(g) => g,
      _ => panic!("Upgrading"),
    }
  }
}

impl<'a, T> DerefMut for UpgradableGuard<'a, T> {
  fn deref_mut(&mut self) -> &mut Self::Target {
    if !self.is_write() {
      self.upgrade();
    }
    match self {
      UpgradableGuard::W(g) => g,
      _ => panic!("Unexpected state"),
    }
  }
}
