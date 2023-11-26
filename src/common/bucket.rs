use crate::common::PgId;
use bytemuck::{Pod, Zeroable};
use getset::{CopyGetters, Setters};

#[repr(C)]
#[derive(Debug, Default, Copy, Clone, CopyGetters, Setters, Zeroable, Pod)]
#[getset(get_copy = "pub", set = "pub")]
pub struct InBucket {
  root: PgId,
  sequence: u64,
}

impl InBucket {
  pub fn new(root: PgId, sequence: u64) -> InBucket {
    InBucket { root, sequence }
  }

  pub fn inc_sequence(&mut self) {
    self.sequence += 1;
  }
}

impl From<InBucket> for String {
  fn from(value: InBucket) -> Self {
    format!("<pgid={},seq={}>", value.root, value.sequence)
  }
}
