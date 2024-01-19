use bumpalo::Bump;
use pin_project::pin_project;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

#[derive(Default)]
#[pin_project(!Unpin)]
pub struct PinBump {
  #[pin]
  bump: Bump,
}

impl PinBump {
  pub fn reset(self: Pin<&mut Self>) {
    let mut this = self.project();
    this.bump.reset();
  }

  pub fn bump(self: Pin<&Self>) -> Pin<&Bump> {
    self.project_ref().bump
  }
}

#[derive(Default, Clone)]
pub struct BumpPool {
  pool: Arc<Mutex<Vec<Pin<Box<PinBump>>>>>,
}

impl BumpPool {
  pub fn pop(&self) -> Pin<Box<PinBump>> {
    self
      .pool
      .lock()
      .unwrap()
      .pop()
      .unwrap_or_else(|| Box::pin(PinBump::default()))
  }

  pub fn push(&self, mut bump: Pin<Box<PinBump>>) {
    Pin::as_mut(&mut bump).reset();
    self.pool.lock().unwrap().push(bump);
  }
}
