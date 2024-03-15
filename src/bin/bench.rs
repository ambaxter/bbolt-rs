use anyhow::anyhow;
use bbolt_rs::{
  BucketApi, BucketRwApi, CursorApi, DBOptions, DbApi, DbRwAPI, Error, TxApi, TxRwRefApi, DB,
};
use byteorder::{BigEndian, ByteOrder};
use clap::{Parser, ValueEnum};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{RngCore, SeedableRng};
use std::cell::RefCell;
use std::fmt::{Display, Formatter};
use std::rc::Rc;
use std::time::{Duration, Instant};
use tempfile::Builder;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Bench {
  #[arg(short, long, default_value_t = WriteMode::Seq)]
  write_mode: WriteMode,
  #[arg(short, long, default_value_t = ReadMode::Seq)]
  read_mode: ReadMode,
  #[arg(short, long, default_value_t = 1000)]
  count: u32,
  #[arg(short, long, default_value_t = 0)]
  batch_size: u32,
  #[arg(short, long, default_value_t = 8)]
  key_size: usize,
  #[arg(short, long, default_value_t = 32)]
  value_size: usize,
  #[arg(short, long, default_value_t = 0.5f64)]
  fill_percent: f64,
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
  ops: u32,
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
  let tmp_file = Builder::new()
    .prefix("bbolt-rs-")
    .suffix(".db")
    .tempfile()?;

  let mut db = DBOptions::default().open(tmp_file.path())?;
  let write_results = run_writes(&mut db, &bench)?;
  let read_results = run_reads(&mut db, &bench)?;
  eprintln!("# Write\t{}", write_results);
  eprintln!("# Read\t{}", read_results);
  Ok(())
}

fn run_reads(db: &mut DB, options: &Bench) -> bbolt_rs::Result<BenchResults> {
  let start = Instant::now();
  let ops = match (options.read_mode, options.write_mode) {
    (ReadMode::Seq, WriteMode::RndNest | WriteMode::SeqNest) => {
      run_reads_sequential_nested(db, options)?
    }
    (ReadMode::Rnd, WriteMode::RndNest | WriteMode::SeqNest) => {
      let mut rng = StdRng::from_entropy();
      let mut keys = collect_nested_keys(db)?;
      run_reads_random_nested(db, options, &mut rng, &mut keys)?
    }
    (ReadMode::Seq, _) => run_reads_sequential(db, options)?,
    (ReadMode::Rnd, _) => {
      let mut rng = StdRng::from_entropy();
      let mut keys = collect_keys(db)?;
      run_reads_random(db, options, &mut rng, &mut keys)?
    }
  };
  Ok(BenchResults {
    ops,
    duration: start.elapsed(),
  })
}

fn run_writes(db: &mut DB, options: &Bench) -> bbolt_rs::Result<BenchResults> {
  let start = Instant::now();
  let ops = match options.write_mode {
    WriteMode::Seq => {
      let mut i = 0u32;
      run_write_with_sources(db, options, || {
        i += 1;
        i
      })?
    }
    WriteMode::Rnd => {
      let mut rng = StdRng::from_entropy();
      run_write_with_sources(db, options, || rng.next_u32())?
    }
    WriteMode::SeqNest => {
      let mut i = 0u32;
      run_write_nested_with_sources(db, options, || {
        i += 1;
        i
      })?
    }
    WriteMode::RndNest => {
      let mut rng = StdRng::from_entropy();
      run_write_nested_with_sources(db, options, || rng.next_u32())?
    }
  };

  Ok(BenchResults {
    ops,
    duration: start.elapsed(),
  })
}

fn run_write_with_sources<F>(
  db: &mut DB, options: &Bench, mut key_source: F,
) -> bbolt_rs::Result<u32>
where
  F: FnMut() -> u32,
{
  for _ in (0..options.count).step_by(options.batch_size as usize) {
    db.update(|mut tx| {
      let mut b = tx.create_bucket_if_not_exists(BENCH_BUCKET_NAME)?;
      b.set_fill_percent(options.fill_percent);
      for _ in 0..options.batch_size {
        let mut key = vec![0u8; options.key_size];
        let value = vec![0u8; options.value_size];
        let k = key_source();
        BigEndian::write_u32(&mut key, k);
        b.put(&key, &value)?;
      }
      Ok(())
    })?
  }
  Ok(options.count)
}

fn run_write_nested_with_sources<F>(
  db: &mut DB, options: &Bench, mut key_source: F,
) -> bbolt_rs::Result<u32>
where
  F: FnMut() -> u32,
{
  for i in (0..options.count).step_by(options.batch_size as usize) {
    db.update(|mut tx| {
      let mut top = tx.create_bucket_if_not_exists(BENCH_BUCKET_NAME)?;
      top.set_fill_percent(options.fill_percent);
      let name = key_source().to_be_bytes();
      let mut b = top.create_bucket_if_not_exists(&name)?;
      b.set_fill_percent(options.fill_percent);
      for j in 0..options.batch_size {
        let mut key = vec![0u8; options.key_size];
        let value = vec![0u8; options.value_size];
        let k = key_source();
        BigEndian::write_u32(&mut key, k);
        b.put(&key, &value)?;
      }
      Ok(())
    })?
  }
  Ok(options.count)
}

fn collect_keys(db: &DB) -> bbolt_rs::Result<Vec<Box<[u8]>>> {
  let results = RefCell::new(Vec::new());
  db.view(|tx| {
    let mut r = results.borrow_mut();
    let mut c = tx.bucket(BENCH_BUCKET_NAME).unwrap().cursor();
    let mut pos = c.first();
    while let Some((k, _)) = pos {
      r.push(k.into());
      pos = c.next();
    }
    Ok(())
  })?;
  Ok(results.take())
}

fn run_reads_sequential(db: &DB, options: &Bench) -> bbolt_rs::Result<u32> {
  let results = RefCell::new(0u32);
  db.view(|tx| {
    let mut result = results.borrow_mut();
    let t = Instant::now();
    loop {
      let mut count = 0;
      let mut c = tx.bucket(BENCH_BUCKET_NAME).unwrap().cursor();
      let mut pos = c.first();
      while let Some((_, v)) = pos {
        v.ok_or_else(|| anyhow!("invalid value"))?;
        count += 1;
        pos = c.next();
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

fn run_reads_random(
  db: &DB, options: &Bench, rng: &mut StdRng, keys: &mut [Box<[u8]>],
) -> bbolt_rs::Result<u32> {
  keys.shuffle(rng);
  let results = RefCell::new(0u32);
  db.view(|tx| {
    let mut result = results.borrow_mut();
    let t = Instant::now();
    loop {
      let mut count = 0;
      let b = tx.bucket(BENCH_BUCKET_NAME).unwrap();
      for k in keys.iter() {
        let v = b.get(k);
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

fn collect_nested_keys(db: &DB) -> bbolt_rs::Result<Vec<(Rc<[u8]>, Box<[u8]>)>> {
  let results = RefCell::new(Vec::new());
  db.view(|tx| {
    let mut r = results.borrow_mut();
    let top = tx.bucket(BENCH_BUCKET_NAME).unwrap();
    top.for_each_bucket(|name| {
      let bk: Rc<[u8]> = name.into();
      let mut c = top.bucket(name).unwrap().cursor();
      let mut pos = c.first();
      while let Some((k, _)) = pos {
        pos = c.next();
        r.push((bk.clone(), k.into()));
      }
      Ok(())
    })?;
    Ok(())
  })?;
  Ok(results.take())
}

fn run_reads_sequential_nested(db: &DB, options: &Bench) -> bbolt_rs::Result<u32> {
  let results = RefCell::new(0u32);
  db.view(|tx| {
    let mut result = results.borrow_mut();
    let t = Instant::now();
    loop {
      let top = tx.bucket(BENCH_BUCKET_NAME).unwrap();
      let mut count = 0;
      top.for_each_bucket(|name| {
        let mut c = top.bucket(name).unwrap().cursor();
        let mut pos = c.first();
        while let Some((_, v)) = pos {
          v.ok_or_else(|| anyhow!("invalid value"))?;
          count += 1;
          pos = c.next();
        }
        Ok(())
      })?;

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
  db: &DB, options: &Bench, rng: &mut StdRng, keys: &mut [(Rc<[u8]>, Box<[u8]>)],
) -> bbolt_rs::Result<u32> {
  keys.shuffle(rng);
  let results = RefCell::new(0u32);
  db.view(|tx| {
    let mut result = results.borrow_mut();
    let t = Instant::now();
    loop {
      let top = tx.bucket(BENCH_BUCKET_NAME).unwrap();
      let mut count = 0;
      for (bucket_name, key) in keys.iter() {
        let b = top.bucket(bucket_name).unwrap();
        let v = b.get(key);
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
