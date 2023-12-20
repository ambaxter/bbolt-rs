#![cfg_attr(debug_assertions, allow(dead_code, unused_imports))]

use bbolt_rs::{DbApi, DbRwAPI, TxApi, TxRwApi, DB};

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
  db.update(|mut tx| {
    if let Ok(b) = tx.create_bucket_if_not_exists("test".as_bytes()) {
      println!("found bucket");
    }
    Ok(())
  })?;
  println!("Goodbye, world!");
  Ok(())
}
