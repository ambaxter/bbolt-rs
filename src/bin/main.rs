#![cfg_attr(debug_assertions, allow(dead_code, unused_imports))]

use bbolt_rs::{DbApi, DbRwAPI, DB, TxApi};

fn main() -> bbolt_rs::Result<()> {
  println!("Hello, world!");
  let mut db = DB::new("test.db")?;
  db.view(|tx| {
    if let Some(b) = tx.bucket("test".as_bytes()) {
      println!("Ok");
    } else {
      println!("Not found");
    }
    Ok(())
  })?;
  db.update(|tx| {
    Ok(())
  })?;
  println!("Goodbye, world!");
  Ok(())
}
