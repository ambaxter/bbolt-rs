use bbolt_rs::{BucketRwApi, DbApi, DbRwAPI, TxCheck, TxRwApi, DB};
use std::time::Instant;

fn main() -> bbolt_rs::Result<()> {
  println!("Hello, world!");

  let mut db = DB::open("test.db")?;
  for _ in 0..5 {
    widgets(&mut db)?;
    db.clone().close();
  }
  db.begin().unwrap().check();
  let db2 = db.clone();
  db.close();
  //let _ = db2.begin()?;
  println!("Goodbye, world!");
  Ok(())
}

fn widgets(db: &mut DB) -> bbolt_rs::Result<()> {
  let n = 400000u32;
  let batch_n = 200000u32;

  let v = [0u8; 500];
  let total = Instant::now();
  for i in (0..n).step_by(batch_n as usize) {
    let update = Instant::now();
    db.update(|mut tx| {
      let mut b = tx.create_bucket_if_not_exists("widgets")?;
      for j in 1..batch_n {
        b.put((i + j).to_be_bytes().as_slice(), v)?;
      }
      Ok(())
    })?;
    println!("Updated from {} in {:?}ms", i, update.elapsed().as_millis());
  }
  println!("Updated total in {:?}ms", total.elapsed().as_millis());

  let check = Instant::now();
  db.update(|tx| {
    let errors = tx.check();
    if !errors.is_empty() {
      for error in &errors[0..10.min(errors.len())] {
        eprintln!("{}", error);
      }
      panic!()
    }
    Ok(())
  })?;
  println!("Checked in {:?}s", check.elapsed().as_secs_f32());
  Ok(())
}
