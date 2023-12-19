#![cfg_attr(debug_assertions, allow(dead_code, unused_imports))]

use bbolt_rs::DB;

fn main() -> bbolt_rs::Result<()> {
  println!("Hello, world!");
  let db = DB::new("test.db")?;
  println!("Goodbye, world!");
  Ok(())
}
