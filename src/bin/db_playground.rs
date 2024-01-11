use bumpalo::Bump;
use pin_project::pin_project;
use std::pin::{pin, Pin};
use std::sync::Mutex;

#[derive(Default)]
#[pin_project(!Unpin)]
struct PinBump {
  #[pin]
  bump: Bump,
}

impl PinBump {
  fn reset(mut self: Pin<&mut Self>) {
    let mut this = self.as_mut().project();
    this.bump.reset();
  }
}

#[derive(Default)]
struct BumpPool {
  pool: Mutex<Vec<Pin<Box<PinBump>>>>,
}

impl BumpPool {
  fn pop(&self) -> Pin<Box<PinBump>> {
    self
      .pool
      .lock()
      .unwrap()
      .pop()
      .unwrap_or_else(|| Box::pin(PinBump::default()))
  }

  fn push(&self, mut bump: Pin<Box<PinBump>>) {
    Pin::as_mut(&mut bump).reset();
    self.pool.lock().unwrap().push(bump);
  }
}

fn main() -> bbolt_rs::Result<()> {
  let pool = BumpPool::default();

  let bump = pool.pop();

  let p = bump.as_ref();

  Ok(())
}
