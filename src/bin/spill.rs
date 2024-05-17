use bbolt_rs::*;
use clap::Parser;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Spill {
  path: String,
}

fn main() -> Result<()> {
  let spill = Spill::parse();
  let db = Bolt::open_ro(&spill.path)?;
  db.view(|tx| {
    for (key, bucket) in tx.iter_buckets() {
      display_bucket(0, key, &bucket)?;
    }
    Ok(())
  })
}

fn display_bucket(depth: usize, key: &[u8], bucket: &BucketImpl) -> Result<()> {
  let mut d = "  ".repeat(depth);
  println!("{}bucket: {} ", d, String::from_utf8_lossy(key));
  d.push_str("  ");
  for (k, v) in bucket.iter_entries() {
    println!("{}k: {:?}, len: {:?} ", d, k, v.len());
  }
  for (bk, b) in bucket.iter_buckets() {
    display_bucket(depth + 1, bk, &b)?;
  }
  Ok(())
}
