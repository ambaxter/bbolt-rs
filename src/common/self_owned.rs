use std::marker::PhantomPinned;
use std::mem::MaybeUninit;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::ptr::{addr_of, addr_of_mut};

// Modified version of https://github.com/noamtashma/owning-ref-rs
// It's note quite as fully featured, but is minimally exactly what I need
pub struct SelfOwned<O: Unpin, T> {
  owner: Pin<Box<O>>,
  marker: PhantomPinned,
  referenced: T,
}

impl<O: Unpin, T> SelfOwned<O, T> {
  pub fn new_with_map<'a, F>(owner: O, f: F) -> SelfOwned<O, T>
  where
    F: FnOnce(&'a mut O) -> T,
    O: 'a,
    T: 'a,
  {
    let mut uninit: MaybeUninit<SelfOwned<O, T>> = MaybeUninit::uninit();
    let ptr = uninit.as_mut_ptr();
    unsafe {
      addr_of_mut!((*ptr).owner).write(Box::pin(owner));
      addr_of_mut!((*ptr).marker).write(PhantomPinned);
      let b = (*(addr_of_mut!((*ptr).owner))).deref_mut();
      addr_of_mut!((*ptr).referenced).write(f(b));
      uninit.assume_init()
    }
  }

  pub fn ref_owner(&self) -> &O { & self.owner }
  pub fn into_owner(self) -> O {
    *Pin::into_inner(self.owner)
  }
}

impl<O: Unpin, T> Deref for SelfOwned<O, T> {
  type Target = T;

  fn deref(&self) -> &Self::Target {
    &self.referenced
  }
}

impl<O: Unpin, T> DerefMut for SelfOwned<O, T> {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.referenced
  }
}
