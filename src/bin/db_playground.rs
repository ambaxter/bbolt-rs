use bbolt_rs::{DBOptions, PgId};
use memmap2::{MmapOptions, MmapRaw};
//use qcell::TLCellOwner;
use std::cell::RefCell;
use std::fs;
use std::io::{Seek, SeekFrom, Write};
use std::ops::{Range, RangeInclusive};

struct TXM {}
struct NodeM {}
struct BucketM {}
/*
std::thread_local! {
    static TX_OWNER: std::cell::RefCell<TLCellOwner<TXM>> = std::cell::RefCell::new(TLCellOwner::new());
    static NODE_OWNER: std::cell::RefCell<TLCellOwner<NodeM>> = std::cell::RefCell::new(TLCellOwner::new());
    static BUCKET_OWNER: std::cell::RefCell<TLCellOwner<BucketM>> = std::cell::RefCell::new(TLCellOwner::new());
}

  fn tx_owner<'tx>() -> &'tx RefCell<TLCellOwner<TXM>> {
    TX_OWNER.with(|cell| cell)
  }

  fn node_owner<'tx>() -> &'tx RefCell<TLCellOwner<NodeM>> {
    NODE_OWNER.with(|cell| cell)
  }

  fn bucket_owner<'tx>() -> &'tx RefCell<TLCellOwner<BucketM>> {
    BUCKET_OWNER.with(|cell| cell)
  }*/

fn main() -> bbolt_rs::Result<()> {
  /*  let cell = TX_OWNER.with(|cell| cell);
  let owner = TLCellOwner::<TXM>::new();*/
  Ok(())
}
