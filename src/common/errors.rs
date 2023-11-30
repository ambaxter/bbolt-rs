use std::io;

fn new_error<S: AsRef<str>>(s: S) -> io::Error {
  io::Error::other(s.as_ref())
}

pub const INVALID: fn() -> io::Error = || new_error("invalid database");
pub const VERSION_MISMATCH: fn() -> io::Error = || new_error("version mismatch");
pub const CHECKSUM: fn() -> io::Error = || new_error("version mismatch");

pub const INCOMPATIBLE_VALUE: fn() -> io::Error = || new_error("incompatible value");

pub const BUCKET_NAME_REQUIRED: fn() -> io::Error = || new_error("bucket name required");

pub const BUCKET_EXISTS: fn() -> io::Error = || new_error("bucket already exists");

pub const BUCKET_NOT_FOUND: fn() -> io::Error = || new_error("bucket not found");
