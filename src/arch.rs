#[cfg(target_pointer_width = "32")]
pub mod size {
  use size::consts::{GIBIBYTE, MEBIBYTE};
  use size::Size;

  pub const MAX_MAP_SIZE: Size = Size::from_const(2 * GIBIBYTE);
  pub const MAX_ALLOC_SIZE: Size = Size::from_const(256 * MEBIBYTE);
}

#[cfg(target_pointer_width = "64")]
pub mod size {
  use size::consts::{GIBIBYTE, TEBIBYTE};
  use size::Size;

  pub const MAX_MAP_SIZE: Size = Size::from_const(256 * TEBIBYTE);
  pub const MAX_ALLOC_SIZE: Size = Size::from_const(2 * GIBIBYTE);
}
