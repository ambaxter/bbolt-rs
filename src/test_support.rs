use crate::common::page::CoerciblePage;
use crate::common::self_owned::SelfOwned;
use crate::tx::check::{TxCheck, UnsealRwTx, UnsealTx};
use crate::{Bolt, BoltOptions, DbApi, TxApi, TxRwRefApi};
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
  pub(crate) db: Bolt,
}

impl Deref for TestDb {
  type Target = Bolt;

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
  pub(crate) fn new() -> crate::Result<TestDb> {
    if cfg!(miri) {
      Self::new_mem(BoltOptions::default())
    } else {
      Self::new_tmp(BoltOptions::default())
    }
  }

  pub(crate) fn with_options(options: BoltOptions) -> crate::Result<TestDb> {
    if cfg!(miri) {
      Self::new_mem(options)
    } else {
      Self::new_tmp(options)
    }
  }

  pub(crate) fn new_tmp(options: BoltOptions) -> crate::Result<TestDb> {
    let tmp_file = Builder::new()
      .prefix("bbolt-rs-")
      .suffix(".db")
      .tempfile()?;
    let db = options.open(tmp_file.path())?;

    Ok(TestDb {
      tmp_file: Some(tmp_file),
      db,
    })
  }

  pub(crate) fn new_mem(options: BoltOptions) -> crate::Result<TestDb> {
    let db = options.new_mem()?;
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

  pub(crate) fn clone_db(&self) -> Bolt {
    self.db.clone()
  }

  pub(crate) fn begin_unseal(&self) -> crate::Result<impl TxApi + UnsealTx> {
    self.db.begin_tx()
  }

  pub(crate) fn begin_rw_unseal(&mut self) -> crate::Result<impl TxRwRefApi + UnsealRwTx> {
    self.db.begin_rw_tx()
  }
}

impl Drop for TestDb {
  fn drop(&mut self) {
    self.must_check()
  }
}
