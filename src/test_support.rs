use crate::common::page::CoerciblePage;
use crate::common::self_owned::SelfOwned;
use crate::tx::check::{TxCheck, UnsealRwTx, UnsealTx};
use crate::{
  Bolt, BoltOptions, BucketApi, BucketRwApi, CursorApi, DbApi, Error, TxApi, TxRwApi, TxRwRefApi,
};
use aligners::{alignment, AlignedBytes};
use fake::faker::lorem::en::Sentence;
use fake::locales::EN;
use fake::Faker;
use fake::{Dummy, Fake};
use itertools::Itertools;
use std::fmt::Debug;
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

pub(crate) fn temp_file() -> crate::Result<NamedTempFile> {
  let temp_file = Builder::new()
    .prefix("bbolt-rs-")
    .suffix(".db")
    .tempfile()?;

  Ok(temp_file)
}

pub(crate) struct TestDb {
  pub(crate) tmp_file: Option<NamedTempFile>,
  pub(crate) db: Option<Bolt>,
  options: BoltOptions,
}

impl Deref for TestDb {
  type Target = Bolt;

  fn deref(&self) -> &Self::Target {
    self.db.as_ref().unwrap()
  }
}

impl DerefMut for TestDb {
  fn deref_mut(&mut self) -> &mut Self::Target {
    self.db.as_mut().unwrap()
  }
}

impl TestDb {
  pub(crate) fn new() -> crate::Result<TestDb> {
    Self::with_options(BoltOptions::default())
  }

  pub(crate) fn with_options(options: BoltOptions) -> crate::Result<TestDb> {
    if cfg!(any(miri, feature = "test-mem-backend")) {
      Self::new_mem(options)
    } else {
      Self::new_tmp(options)
    }
  }

  pub(crate) fn new_tmp(options: BoltOptions) -> crate::Result<TestDb> {
    let tmp_file = temp_file()?;
    let db = options.clone().open(tmp_file.path())?;

    Ok(TestDb {
      tmp_file: Some(tmp_file),
      db: Some(db),
      options,
    })
  }

  pub(crate) fn new_mem(options: BoltOptions) -> crate::Result<TestDb> {
    let db = options.clone().open_mem()?;
    Ok(TestDb {
      tmp_file: None,
      db: Some(db),
      options,
    })
  }

  pub(crate) fn must_check(&mut self) {
    if let Some(Ok(tx)) = self.db.as_ref().map(|db| db.begin()) {
      let errors = tx.check();
      if !errors.is_empty() {
        for error in errors {
          eprintln!("{}", error);
        }
        panic!()
      }
    }
  }

  #[cfg(not(any(miri, feature = "test-mem-backend")))]
  pub(crate) fn must_close(&mut self) {
    let db = self.db.take().unwrap();
    db.close();
  }

  #[cfg(not(any(miri, feature = "test-mem-backend")))]
  pub(crate) fn reopen(&mut self) -> crate::Result<()> {
    assert!(
      self.tmp_file.is_some(),
      "Reopen only supported on file based databases"
    );
    assert!(self.db.is_none(), "Please call close before must_reopen");
    let options = self.options.clone();
    match options.open(self.tmp_file.as_ref().unwrap().path()) {
      Ok(db) => self.db = Some(db),
      Err(err) => return Err(err),
    }
    Ok(())
  }

  #[cfg(not(any(miri, feature = "test-mem-backend")))]
  pub(crate) fn must_reopen(&mut self) {
    self.reopen().expect("Unable to reopen db")
  }

  pub(crate) fn clone_db(&self) -> Bolt {
    self.db.as_ref().unwrap().clone()
  }

  pub(crate) fn begin_unseal(&self) -> crate::Result<impl TxApi + UnsealTx> {
    self.db.as_ref().unwrap().begin_tx()
  }

  pub(crate) fn begin_rw_unseal(&mut self) -> crate::Result<impl TxRwRefApi + UnsealRwTx> {
    self.db.as_mut().unwrap().begin_rw_tx()
  }
}

impl Drop for TestDb {
  fn drop(&mut self) {
    self.must_check()
  }
}

#[derive(Debug, Dummy)]
pub struct DummyEntry {
  #[dummy(faker = "(Faker, 1..1024)")]
  pub key: Vec<u8>,
  #[dummy(faker = "(Faker, 1..1024)")]
  pub value: Vec<u8>,
}

#[derive(Debug, Dummy)]
pub struct DummyVec {
  #[dummy(faker = "(Faker, 1..1000)")]
  pub values: Vec<DummyEntry>,
}

pub(crate) fn quick_check<D, F>(count: u64, mut f: F)
where
  D: Dummy<Faker> + Debug,
  F: FnMut(&mut D) -> bool,
{
  for i in 0..count {
    let mut d = Faker.fake();
    let r = f(&mut d);
    if !r {
      panic!("Iteration {} failed. {:?}", i + 1, d);
    }
  }
}
