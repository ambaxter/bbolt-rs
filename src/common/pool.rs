use parking_lot::Mutex;
use std::mem::{forget, ManuallyDrop};
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::Arc;

// SyncPool and RcPool are modified forms of the excellent https://github.com/CJP10/object-pool
pub struct SyncPool<T> {
  objects: Mutex<Vec<T>>,
  init: Arc<dyn Fn() -> T>,
  reset: Arc<dyn Fn(&mut T)>,
}

impl<T> SyncPool<T> {
  pub fn new<I, R>(init: I, reset: R) -> Arc<SyncPool<T>>
  where
    I: Fn() -> T + 'static,
    R: Fn(&mut T) + 'static,
  {
    let init = Arc::new(init);
    let reset = Arc::new(reset);
    let objects = Vec::new();

    Arc::new(SyncPool {
      objects: Mutex::new(objects),
      init,
      reset,
    })
  }

  pub fn with_capacity<I: Fn() -> T + 'static, R: Fn(&mut T) + 'static>(
    cap: usize, init: I, reset: R,
  ) -> Arc<SyncPool<T>> {
    let init = Arc::new(init);
    let reset = Arc::new(reset);
    let mut objects = Vec::with_capacity(cap);
    for _ in 0..cap {
      objects.push(init());
    }
    Arc::new(SyncPool {
      objects: Mutex::new(objects),
      init,
      reset,
    })
  }

  pub fn len(&self) -> usize {
    self.objects.lock().len()
  }

  pub fn is_empty(&self) -> bool {
    self.objects.lock().is_empty()
  }

  pub fn attach(&self, mut t: T) {
    (self.reset)(&mut t);
    self.objects.lock().push(t)
  }

  pub fn pull(self: &Arc<Self>) -> SyncReusable<T> {
    let object = self.objects.lock().pop().unwrap_or_else(&*self.init);
    SyncReusable::new(self.clone(), object)
  }

  pub fn clear(&self) {
    self.objects.lock().clear();
  }
}

impl<T> SyncPool<T>
where
  T: Default,
{
  pub fn pin_default() -> Arc<SyncPool<Pin<Box<T>>>> {
    Arc::new(SyncPool {
      objects: Mutex::new(Vec::new()),
      init: Arc::new(|| Box::pin(Default::default())),
      reset: Arc::new(|_| {}),
    })
  }
}

impl<T> Default for SyncPool<T>
where
  T: Default,
{
  fn default() -> Self {
    SyncPool {
      objects: Mutex::new(Vec::new()),
      init: Arc::new(|| Default::default()),
      reset: Arc::new(|_| {}),
    }
  }
}
unsafe impl<T> Send for SyncPool<T> {}
unsafe impl<T> Sync for SyncPool<T> {}

pub struct SyncReusable<T> {
  pool: ManuallyDrop<Arc<SyncPool<T>>>,
  data: ManuallyDrop<T>,
}

impl<T> SyncReusable<T> {
  pub fn new(pool: Arc<SyncPool<T>>, t: T) -> SyncReusable<T> {
    SyncReusable {
      pool: ManuallyDrop::new(pool),
      data: ManuallyDrop::new(t),
    }
  }

  pub fn detach(mut self) -> T {
    let (pool, object) = unsafe { self.take() };
    drop(pool);
    forget(self);
    object
  }

  unsafe fn take(&mut self) -> (Arc<SyncPool<T>>, T) {
    (
      ManuallyDrop::take(&mut self.pool),
      ManuallyDrop::take(&mut self.data),
    )
  }
}

impl<T> Deref for SyncReusable<T> {
  type Target = T;

  fn deref(&self) -> &Self::Target {
    &self.data
  }
}

impl<T> DerefMut for SyncReusable<T> {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.data
  }
}

impl<T> Drop for SyncReusable<T> {
  fn drop(&mut self) {
    let (pool, object) = unsafe { self.take() };
    pool.attach(object);
  }
}

#[cfg(test)]
mod tests {
  use crate::common::pool::SyncPool;
  use std::rc::Rc;
  use std::sync::Arc;

  #[test]
  fn test_arc() {
    let pool: Arc<SyncPool<Vec<u8>>> = SyncPool::new(Default::default, |_| {});
    assert_eq!(0, pool.len());
    let mut object = pool.pull().detach();
    assert_eq!(0, pool.len());
    object.push(8);
    pool.attach(object);

    assert_eq!(1, pool.len());
  }
}
