use std::cell::UnsafeCell;
use std::cmp::Ordering;
use std::fmt;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;

#[cfg(not(feature = "allgasnobrakes"))]
pub use std::cell::{Ref, RefCell, RefMut};

#[cfg(feature = "allgasnobrakes")]
pub use {UnsafeRef as Ref, UnsafeRefCell as RefCell, UnsafeRefMut as RefMut};

pub struct UnsafeRefCell<T: ?Sized> {
  value: UnsafeCell<T>,
}

impl<T> UnsafeRefCell<T> {
  #[inline]
  pub const fn new(value: T) -> UnsafeRefCell<T> {
    UnsafeRefCell {
      value: UnsafeCell::new(value),
    }
  }
}

impl<T: ?Sized> UnsafeRefCell<T> {
  #[inline]
  pub fn borrow(&self) -> UnsafeRef<'_, T> {
    UnsafeRef {
      value: NonNull::new(self.value.get()).unwrap(),
      marker: PhantomData,
    }
  }

  #[inline]
  pub fn borrow_mut(&self) -> UnsafeRefMut<'_, T> {
    UnsafeRefMut {
      value: NonNull::new(self.value.get()).unwrap(),
      marker: PhantomData,
    }
  }
}

unsafe impl<T: ?Sized> Send for UnsafeRefCell<T> where T: Send {}

impl<T: Clone> Clone for UnsafeRefCell<T> {
  #[inline]
  fn clone(&self) -> Self {
    UnsafeRefCell::new(self.borrow().clone())
  }
}

impl<T: Default> Default for UnsafeRefCell<T> {
  #[inline]
  fn default() -> Self {
    UnsafeRefCell::new(Default::default())
  }
}

impl<T: ?Sized + PartialEq> PartialEq for UnsafeRefCell<T> {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    *self.borrow() == *other.borrow()
  }
}

impl<T: ?Sized + Eq> Eq for UnsafeRefCell<T> {}

impl<T: ?Sized + PartialOrd> PartialOrd for UnsafeRefCell<T> {
  #[inline]
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    self.borrow().partial_cmp(&*other.borrow())
  }

  #[inline]
  fn lt(&self, other: &Self) -> bool {
    *self.borrow() < *other.borrow()
  }

  #[inline]
  fn le(&self, other: &Self) -> bool {
    *self.borrow() <= *other.borrow()
  }

  #[inline]
  fn gt(&self, other: &Self) -> bool {
    *self.borrow() > *other.borrow()
  }

  #[inline]
  fn ge(&self, other: &Self) -> bool {
    *self.borrow() >= *other.borrow()
  }
}

impl<T: ?Sized + Ord> Ord for UnsafeRefCell<T> {
  #[inline]
  fn cmp(&self, other: &Self) -> Ordering {
    self.borrow().cmp(&*other.borrow())
  }
}

impl<T> From<T> for UnsafeRefCell<T> {
  fn from(value: T) -> Self {
    UnsafeRefCell::new(value)
  }
}

pub struct UnsafeRef<'b, T: ?Sized> {
  value: NonNull<T>,
  marker: PhantomData<&'b T>,
}

impl<T: ?Sized> Deref for UnsafeRef<'_, T> {
  type Target = T;

  fn deref(&self) -> &Self::Target {
    unsafe { self.value.as_ref() }
  }
}

impl<'b, T: ?Sized> UnsafeRef<'b, T> {
  #[inline]
  pub fn map<U: ?Sized, F>(orig: UnsafeRef<'b, T>, f: F) -> UnsafeRef<'b, U>
  where
    F: FnOnce(&T) -> &U,
  {
    UnsafeRef {
      value: NonNull::from(f(&*orig)),
      marker: PhantomData,
    }
  }

  #[inline]
  pub fn map_split<U: ?Sized, V: ?Sized, F>(
    orig: UnsafeRef<'b, T>, f: F,
  ) -> (UnsafeRef<'b, U>, UnsafeRef<'b, V>)
  where
    F: FnOnce(&T) -> (&U, &V),
  {
    let (a, b) = f(&*orig);
    (
      UnsafeRef {
        value: NonNull::from(a),
        marker: PhantomData,
      },
      UnsafeRef {
        value: NonNull::from(b),
        marker: PhantomData,
      },
    )
  }
}

impl<T: ?Sized + fmt::Display> fmt::Display for UnsafeRef<'_, T> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    (**self).fmt(f)
  }
}
pub struct UnsafeRefMut<'b, T: ?Sized> {
  value: NonNull<T>,
  marker: PhantomData<&'b mut T>,
}

impl<T: ?Sized> Deref for UnsafeRefMut<'_, T> {
  type Target = T;

  #[inline]
  fn deref(&self) -> &Self::Target {
    unsafe { self.value.as_ref() }
  }
}

impl<T: ?Sized> DerefMut for UnsafeRefMut<'_, T> {
  #[inline]
  fn deref_mut(&mut self) -> &mut Self::Target {
    unsafe { self.value.as_mut() }
  }
}

impl<'b, T: ?Sized> UnsafeRefMut<'b, T> {
  #[inline]
  pub fn map<U: ?Sized, F>(mut orig: UnsafeRefMut<'b, T>, f: F) -> UnsafeRefMut<'b, U>
  where
    F: FnOnce(&mut T) -> &mut U,
  {
    let value = NonNull::from(f(&mut *orig));
    UnsafeRefMut {
      value,
      marker: PhantomData,
    }
  }

  #[inline]
  pub fn map_split<U: ?Sized, V: ?Sized, F>(
    mut orig: UnsafeRefMut<'b, T>, f: F,
  ) -> (UnsafeRefMut<'b, U>, UnsafeRefMut<'b, V>)
  where
    F: FnOnce(&mut T) -> (&mut U, &mut V),
  {
    let (a, b) = f(&mut *orig);
    (
      UnsafeRefMut {
        value: NonNull::from(a),
        marker: PhantomData,
      },
      UnsafeRefMut {
        value: NonNull::from(b),
        marker: PhantomData,
      },
    )
  }
}

impl<T: ?Sized + fmt::Display> fmt::Display for UnsafeRefMut<'_, T> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    (**self).fmt(f)
  }
}
