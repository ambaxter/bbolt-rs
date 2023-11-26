use std::io;

fn new_error<S: AsRef<str>>(s: S) -> io::Error {
  io::Error::new(io::ErrorKind::Other, s.as_ref())
}

pub const INVALID: fn() -> io::Error = || new_error("invalid database");
pub const VERSION_MISMATCH: fn() -> io::Error = || new_error("version mismatch");
pub const CHECKSUM: fn() -> io::Error = || new_error("version mismatch");
