use std::cell::RefCell;
use std::fmt::{Display, Formatter};
use std::rc::Rc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use byteorder::{BigEndian, ByteOrder};
use clap::{Parser, ValueEnum};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{RngCore, SeedableRng};
use tempfile::Builder;

use bbolt_rs::{
  Bolt, BoltOptions, BucketApi, BucketRwApi, DbApi, DbRwAPI, Error, TxApi, TxRwRefApi,
};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Bench {
  #[arg(short, long, default_value_t = WriteMode::Seq)]
  write_mode: WriteMode,
  #[arg(short, long, default_value_t = ReadMode::Seq)]
  read_mode: ReadMode,
  #[arg(short, long, default_value_t = 1000)]
  count: u64,
  #[arg(short, long, default_value_t = 0)]
  batch_size: u64,
  #[arg(short, long, default_value_t = 8)]
  key_size: usize,
  #[arg(short, long, default_value_t = 32)]
  value_size: usize,
  #[arg(short, long, default_value_t = 0.5f64)]
  fill_percent: f64,
  #[arg(short, long)]
  mem_backend: bool,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum WriteMode {
  /// Sequential Write
  Seq,
  /// Random Write
  Rnd,
  /// Sequential Nested
  SeqNest,
  /// Random Nested
  RndNest,
}

impl Display for WriteMode {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    let str = match self {
      WriteMode::Seq => "seq",
      WriteMode::Rnd => "rnd",
      WriteMode::SeqNest => "seq-nest",
      WriteMode::RndNest => "rnd-nest",
    };
    f.write_str(str)
  }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum ReadMode {
  /// Sequential Read
  Seq,
  /// Random Read
  Rnd,
}

impl Display for ReadMode {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    let str = match self {
      ReadMode::Seq => "seq",
      ReadMode::Rnd => "rnd",
    };
    f.write_str(str)
  }
}

#[derive(Debug, Copy, Clone, Default)]
struct BenchResults {
  ops: u64,
  duration: Duration,
}

impl BenchResults {
  fn op_duration(&self) -> Duration {
    if self.ops == 0 {
      Duration::from_secs(0)
    } else {
      Duration::from_secs_f64(self.duration.as_secs_f64() / self.ops as f64)
    }
  }

  fn ops_per_second(&self) -> u64 {
    let op = self.op_duration();
    if op.is_zero() {
      0
    } else {
      (1.0f64 / op.as_secs_f64()) as u64
    }
  }
}

impl Display for BenchResults {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    f.write_fmt(format_args!(
      "{:?}\t({:?}/op)\t({} op/sec)",
      self.duration,
      self.op_duration(),
      self.ops_per_second()
    ))
  }
}

struct NestedKey {
  bucket: Rc<[u8]>,
  key: Box<[u8]>,
}

static BENCH_BUCKET_NAME: &str = "bench";

fn main() -> bbolt_rs::Result<()> {
  let mut bench = Bench::parse();

  if bench.batch_size == 0 {
    bench.batch_size = bench.count;
  } else if bench.count % bench.batch_size != 0 {
    return Err(Error::Other(anyhow!(
      "number of iterations must be divisible by the batch size"
    )));
  }
  let (_tmp_file, mut db) = if bench.mem_backend {
    (None, BoltOptions::default().open_mem()?)
  } else {
    let tmp_file = Builder::new()
      .prefix("bbolt-rs-")
      .suffix(".db")
      .tempfile()?;

    let path = tmp_file.path().to_path_buf();
    (Some(tmp_file), BoltOptions::default().open(path)?)
  };

  let mut rng = StdRng::from_entropy();
  let mut write_results = BenchResults::default();
  let mut n_keys = run_writes(&mut db, &bench, &mut write_results, &mut rng)?;
  if let Some(keys) = n_keys.as_mut() {
    keys.shuffle(&mut rng);
  }

  let mut read_results = BenchResults::default();
  run_reads(&mut db, &bench, &mut read_results, &n_keys)?;
  println!("# Write\t{}", write_results);
  println!("# Read\t{}", read_results);
  Ok(())
}

fn run_reads(
  db: &mut Bolt, options: &Bench, read_results: &mut BenchResults, n_keys: &Option<Vec<NestedKey>>,
) -> bbolt_rs::Result<()> {
  let start = Instant::now();
  let ops = match (options.read_mode, options.write_mode) {
    (ReadMode::Seq, WriteMode::RndNest | WriteMode::SeqNest) => {
      run_reads_sequential_nested(db, options)?
    }
    (ReadMode::Rnd, WriteMode::RndNest | WriteMode::SeqNest) => {
      let keys = n_keys.as_ref().unwrap();
      run_reads_random_nested(db, options, keys)?
    }
    (ReadMode::Seq, _) => run_reads_sequential(db, options)?,
    (ReadMode::Rnd, _) => {
      let keys = n_keys.as_ref().unwrap();
      run_reads_random(db, options, keys)?
    }
  };

  read_results.ops = ops;
  read_results.duration = start.elapsed();
  Ok(())
}

fn run_writes(
  db: &mut Bolt, options: &Bench, write_results: &mut BenchResults, rng: &mut StdRng,
) -> bbolt_rs::Result<Option<Vec<NestedKey>>> {
  let start = Instant::now();
  let n_keys = match options.write_mode {
    WriteMode::Seq => {
      let mut i = 0u32;
      run_write_with_sources(db, options, move || {
        i += 1;
        i
      })?
    }
    WriteMode::Rnd => run_write_with_sources(db, options, || rng.next_u32())?,
    WriteMode::SeqNest => {
      let mut i = 0u32;
      run_write_nested_with_sources(db, options, move || {
        i += 1;
        i
      })?
    }
    WriteMode::RndNest => run_write_nested_with_sources(db, options, || rng.next_u32())?,
  };

  write_results.ops = options.count;
  write_results.duration = start.elapsed();
  Ok(n_keys)
}

fn run_write_with_sources<F>(
  db: &mut Bolt, options: &Bench, mut key_source: F,
) -> bbolt_rs::Result<Option<Vec<NestedKey>>>
where
  F: FnMut() -> u32,
{
  let mut n_keys = if options.read_mode == ReadMode::Rnd {
    Some(Vec::with_capacity(options.count as usize))
  } else {
    None
  };

  for _ in (0..options.count).step_by(options.batch_size as usize) {
    db.update(|mut tx| {
      let mut b = tx.create_bucket_if_not_exists(BENCH_BUCKET_NAME)?;
      let bucket = Rc::new([]);
      b.set_fill_percent(options.fill_percent);
      for _ in 0..options.batch_size {
        let mut key = vec![0u8; options.key_size];
        let value = vec![0u8; options.value_size];
        let k = key_source();
        BigEndian::write_u32(&mut key, k);
        b.put(&key, &value)?;
        if let Some(keys) = n_keys.as_mut() {
          keys.push(NestedKey {
            bucket: bucket.clone(),
            key: key.into(),
          })
        }
      }
      Ok(())
    })?
  }
  Ok(n_keys)
}

fn run_write_nested_with_sources<F>(
  db: &mut Bolt, options: &Bench, mut key_source: F,
) -> bbolt_rs::Result<Option<Vec<NestedKey>>>
where
  F: FnMut() -> u32,
{
  let mut n_keys = if options.read_mode == ReadMode::Rnd {
    Some(Vec::with_capacity(options.count as usize))
  } else {
    None
  };

  for _ in (0..options.count).step_by(options.batch_size as usize) {
    db.update(|mut tx| {
      let mut top = tx.create_bucket_if_not_exists(BENCH_BUCKET_NAME)?;
      top.set_fill_percent(options.fill_percent);
      let name = key_source().to_be_bytes();
      let bucket: Rc<[u8]> = Rc::from(name);
      let mut b = top.create_bucket_if_not_exists(&name)?;
      b.set_fill_percent(options.fill_percent);
      for _ in 0..options.batch_size {
        let mut key = vec![0u8; options.key_size];
        let value = vec![0u8; options.value_size];
        let k = key_source();
        BigEndian::write_u32(&mut key, k);
        b.put(&key, &value)?;
        if let Some(keys) = n_keys.as_mut() {
          keys.push(NestedKey {
            bucket: bucket.clone(),
            key: key.into(),
          })
        }
      }
      Ok(())
    })?
  }
  Ok(n_keys)
}

fn run_reads_sequential(db: &Bolt, options: &Bench) -> bbolt_rs::Result<u64> {
  let results = RefCell::new(0u64);
  db.view(|tx| {
    let mut result = results.borrow_mut();
    let t = Instant::now();
    loop {
      let mut count = 0;
      let b = tx.bucket(BENCH_BUCKET_NAME).unwrap();
      for (_k, _v) in b.iter_entries() {
        count += 1;
      }

      if options.write_mode == WriteMode::Seq && count != options.count {
        return Err(Error::Other(anyhow!(
          "read seq: iter mismatch: expected {}, got {}",
          options.count,
          count
        )));
      }
      *result += count;

      if t.elapsed() > Duration::from_secs(1) {
        break;
      }
    }
    Ok(())
  })?;
  Ok(results.take())
}

fn run_reads_random(db: &Bolt, options: &Bench, keys: &[NestedKey]) -> bbolt_rs::Result<u64> {
  let results = RefCell::new(0u64);
  db.view(|tx| {
    let mut result = results.borrow_mut();
    let t = Instant::now();
    loop {
      let mut count = 0;
      let b = tx.bucket(BENCH_BUCKET_NAME).unwrap();
      for entry in keys.iter() {
        let v = b.get(&entry.key);
        v.ok_or_else(|| anyhow!("invalid value"))?;
        count += 1;
      }
      if options.write_mode == WriteMode::Seq && count != options.count {
        return Err(Error::Other(anyhow!(
          "read seq: iter mismatch: expected {}, got {}",
          options.count,
          count
        )));
      }
      *result += count;

      if t.elapsed() > Duration::from_secs(1) {
        break;
      }
    }
    Ok(())
  })?;
  Ok(results.take())
}

fn run_reads_sequential_nested(db: &Bolt, options: &Bench) -> bbolt_rs::Result<u64> {
  let results = RefCell::new(0u64);
  db.view(|tx| {
    let mut result = results.borrow_mut();
    let t = Instant::now();
    loop {
      let top = tx.bucket(BENCH_BUCKET_NAME).unwrap();
      let mut count = 0u64;
      for (_k, b) in top.iter_buckets() {
        for (_k, _v) in b.iter_entries() {
          count += 1;
        }
      }

      if options.write_mode == WriteMode::Seq && count != options.count {
        return Err(Error::Other(anyhow!(
          "read seq: iter mismatch: expected {}, got {}",
          options.count,
          count
        )));
      }
      *result += count;

      if t.elapsed() > Duration::from_secs(1) {
        break;
      }
    }
    Ok(())
  })?;
  Ok(results.take())
}

fn run_reads_random_nested(
  db: &Bolt, options: &Bench, keys: &[NestedKey],
) -> bbolt_rs::Result<u64> {
  let results = RefCell::new(0u64);
  db.view(|tx| {
    let mut result = results.borrow_mut();
    let t = Instant::now();
    loop {
      let top = tx.bucket(BENCH_BUCKET_NAME).unwrap();
      let mut count = 0u64;
      for entry in keys.iter() {
        let b = top.bucket(&entry.bucket).unwrap();
        let v = b.get(&entry.key);
        v.ok_or_else(|| anyhow!("invalid value"))?;
        count += 1;
      }

      if options.write_mode == WriteMode::Seq && count != options.count {
        return Err(Error::Other(anyhow!(
          "read seq: iter mismatch: expected {}, got {}",
          options.count,
          count
        )));
      }
      *result += count;

      if t.elapsed() > Duration::from_secs(1) {
        break;
      }
    }
    Ok(())
  })?;
  Ok(results.take())
}
