use crate::common::page::CoerciblePage;
use crate::common::self_owned::SelfOwned;
use crate::tx::check::{TxCheck, UnsealRwTx, UnsealTx};
use crate::{DbApi, TxApi, TxRwApi, DB};
use aligners::{alignment, AlignedBytes};
use std::ops::{Deref, DerefMut};
use tempfile::{Builder, NamedTempFile};

pub(crate) fn mapped_page<T: CoerciblePage + Sized>(
  bytes: usize,
) -> SelfOwned<AlignedBytes<alignment::Page>, T> {
  SelfOwned::new_with_map(
    AlignedBytes::<alignment::Page>::new_zeroed(bytes),
    |aligned_bytes| T::own(aligned_bytes.as_mut_ptr()),
  )
}

pub(crate) struct TestDb {
  pub(crate) tmp_file: Option<NamedTempFile>,
  pub(crate) db: DB,
}

impl Deref for TestDb {
  type Target = DB;

  fn deref(&self) -> &Self::Target {
    &self.db
  }
}

impl DerefMut for TestDb {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.db
  }
}

impl TestDb {
  pub(crate) fn new_tmp() -> crate::Result<TestDb> {
    let tmp_file = Builder::new()
      .prefix("bbolt-rs-")
      .suffix(".db")
      .tempfile()?;
    let db = DB::open(tmp_file.path())?;
    Ok(TestDb {
      tmp_file: Some(tmp_file),
      db,
    })
  }

  pub(crate) fn new() -> crate::Result<TestDb> {
    let db = DB::new_mem()?;
    Ok(TestDb { tmp_file: None, db })
  }

  pub(crate) fn must_check(&mut self) {
    if let Ok(tx) = self.db.begin() {
      let errors = tx.check();
      if !errors.is_empty() {
        for error in errors {
          eprintln!("{}", error);
        }
        panic!()
      }
    }
  }

  pub(crate) fn begin_unseal(&self) -> crate::Result<impl TxApi + UnsealTx> {
    self.db.begin_tx()
  }

  pub(crate) fn begin_rw_unseal(&mut self) -> crate::Result<impl TxRwApi + UnsealRwTx> {
    self.db.begin_rw_tx()
  }
}

impl Drop for TestDb {
  fn drop(&mut self) {
    self.must_check()
  }
}
