use crate::common::page::meta::{MappedMetaPage, MetaPage};
use crate::common::page::{CoerciblePage, MutPage};
use crate::common::SplitRef;
use crate::iter::{DbWalker, ValueBucketSeq};
use crate::tx::{TxCell, TxIApi};
use crate::{Bolt, BucketRwApi, DbApi, DbPath, PgId, TxRwApi, TxRwRefApi};
use aligners::{alignment, AlignedBytes};
use std::fs::OpenOptions;
use std::ops::Deref;
use std::path::Path;

#[cfg(test)]
fn copy<P: AsRef<Path>>(cell: TxCell, path: P) {
  let mut f = OpenOptions::new()
    .write(true)
    .truncate(true)
    .open(path)
    .unwrap();
  let page_size = cell.page_size();
  let meta = *cell.meta();
  let db_path = cell.split_r().db;
  let mut bytes = AlignedBytes::<alignment::Page>::new_zeroed(page_size);
  {
    let mut page = MutPage::new(bytes.as_mut_ptr());
    let meta_page = MappedMetaPage::mut_into(&mut page);
    meta_page.page.set_meta();
    meta_page.meta = meta;

    meta_page.page.id = PgId(0);
    let checksum = meta_page.meta.checksum();
    meta_page.meta.set_checksum(checksum);
  }
  {
    let mut page = MutPage::new(bytes.as_mut_ptr());
    let meta_page = MappedMetaPage::coerce_mut(&mut page).unwrap();

    meta_page.page.id = PgId(1);
    let txid = meta_page.meta.txid();
    meta_page.meta.set_txid(txid - 1);
    let checksum = meta_page.meta.checksum();
    meta_page.meta.set_checksum(checksum);
  }
}

pub fn compact(src: &Bolt, dst: &mut Bolt, tx_max_size: u64) -> crate::Result<()> {
  if dst.path() != &DbPath::Memory {
    assert_ne!(src.path(), dst.path());
  }
  let mut size = 0u64;
  let mut dst_tx = dst.begin_rw_tx()?;

  let src_tx = src.begin_tx()?;
  let mut walker = DbWalker::new(src_tx.tx.api_cursor());
  while let Some((k, v)) = walker.next() {
    let sz = (k.len() + v.len()) as u64;
    if size + sz > tx_max_size && tx_max_size != 0 {
      dst_tx.commit()?;
      dst_tx = dst.begin_rw_tx()?;
      size = 0;
    }
    size += sz;
    let path = walker.path();
    let mut b = dst_tx.bucket_mut_path(path).unwrap();
    b.set_fill_percent(1.0);
    match v {
      ValueBucketSeq::Value(v) => {
        b.put(k, v)?;
      }
      ValueBucketSeq::BucketSeq(seq) => {
        let mut b = b.create_bucket(k)?;
        b.set_sequence(seq)?;
      }
    }
  }
  Ok(())
}
