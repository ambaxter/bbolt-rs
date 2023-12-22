use crate::common::PgId;
use bytemuck::{Pod, Zeroable};
use getset::{CopyGetters, Setters};
use std::mem;

pub(crate) const IN_BUCKET_SIZE: usize = mem::size_of::<InBucket>();

/// `InBucket` represents the on-file representation of a bucket.
/// This is stored as the "value" of a bucket key. If the bucket is small enough,
/// then its root page can be stored inline in the "value", after the bucket
/// header. In the case of inline buckets, the "root" will be 0.
#[repr(C)]
#[derive(Debug, Default, Copy, Clone, CopyGetters, Setters, Zeroable, Pod)]
#[getset(get_copy = "pub", set = "pub")]
pub struct InBucket {
  /// page id of the bucket's root-level page
  root: PgId,
  /// monotonically incrementing, used by NextSequence()
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
