use bumpalo::Bump;
use pin_project::pin_project;
use std::pin::Pin;

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
