use crate::common::PgId;
use once_cell::sync::Lazy;
use size;
use size::consts::{GiB, MiB};
use size::Size;
use std::time::Duration;

pub const MAX_MMAP_STEP: Size = Size::from_const(1 * GiB);

pub const VERSION: u32 = 2;

// Chosen from https://nedbatchelder.com/text/hexwords.html
// as we are using the Go BBolt project code as a scaffold
pub const MAGIC: u32 = 0x5caff01d;

pub const PGID_NO_FREE_LIST: PgId = PgId(0xffffffffffffffff);

pub const IGNORE_NO_SYNC: bool = cfg!(openbsd);

pub const DEFAULT_MAX_BATCH_SIZE: u32 = 1000;
pub const DEFAULT_MAX_BATCH_DELAY: Duration = Duration::from_millis(10);
pub const DEFAULT_ALLOC_SIZE: Size = Size::from_const(16 * MiB);

pub static DEFAULT_PAGE_SIZE: Lazy<Size> = Lazy::new(|| Size::from_bytes(page_size::get()));
