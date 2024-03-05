use clap::Parser;
use bbolt_rs::*;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Spill {
  path: String
}

fn main() -> Result<()> {
  let spill = Spill::parse();
  let db = DB::open_ro(&spill.path)?;
  db.view(|tx| {
    tx.for_each(|key, bucket| {display_bucket(0, key, &bucket)})
  })
}

fn display_bucket(depth: usize, key: & [u8], bucket: &BucketImpl) -> Result<()> {
  let mut d = "  ".repeat(depth);
  println!("{}bucket: {} ", d, String::from_utf8_lossy(key));
  d.push_str("  ");
  bucket.for_each(|k, v| {
    println!("{}k: {:?}, len: {:?} ", d, k, v.map(|v| v.len()));
    Ok(())
  })?;
  bucket.for_each_bucket(|bk| {
    let b = bucket.bucket(bk).unwrap();
    display_bucket(depth + 1, bk, &b)?;
    Ok(())
  })?;

  Ok(())
}