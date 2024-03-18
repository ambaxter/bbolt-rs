use crate::common::cell::{RefCell, RefMut};
use parking_lot::{RwLockReadGuard, RwLockUpgradableReadGuard, RwLockWriteGuard};
use pin_project::pin_project;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;

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

pub enum LockGuard<'a, T> {
  R(RwLockReadGuard<'a, T>),
  U(RefCell<UpgradableGuard<'a, T>>),
}

impl<'a, T> LockGuard<'a, T> {
  pub fn get_mut(&self) -> Option<RefMut<T>> {
    match self {
      LockGuard::R(_) => None,
      LockGuard::U(cell) => Some(RefMut::map(cell.borrow_mut(), |guard| guard.deref_mut())),
    }
  }
}

impl<'a, T> From<RwLockReadGuard<'a, T>> for LockGuard<'a, T> {
  fn from(value: RwLockReadGuard<'a, T>) -> Self {
    LockGuard::R(value)
  }
}

impl<'a, T> From<RwLockUpgradableReadGuard<'a, T>> for LockGuard<'a, T> {
  fn from(value: RwLockUpgradableReadGuard<'a, T>) -> Self {
    LockGuard::U(RefCell::new(UpgradableGuard::UR(value)))
  }
}

impl<'a, T> From<RwLockWriteGuard<'a, T>> for LockGuard<'a, T> {
  fn from(value: RwLockWriteGuard<'a, T>) -> Self {
    LockGuard::U(RefCell::new(UpgradableGuard::W(value)))
  }
}

// The empty derive prevents RustRover from having issues
#[derive()]
#[pin_project(!Unpin)]
pub struct PinLockGuard<'a, T> {
  #[pin]
  guard: LockGuard<'a, T>,
}

impl<'a, T> PinLockGuard<'a, T> {
  pub fn new<L: Into<LockGuard<'a, T>>>(lock: L) -> PinLockGuard<'a, T> {
    PinLockGuard { guard: lock.into() }
  }

  pub fn guard(self: Pin<&Self>) -> Pin<&LockGuard<'a, T>> {
    self.project_ref().guard
  }
}
