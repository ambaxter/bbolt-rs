use crate::common::meta::MappedMetaPage;
use crate::common::selfowned::SelfOwned;
use std::mem;
use std::ops::{Deref, DerefMut};
//use crate::freelist::MappedFreeListPage;
use crate::common::page::{CoerciblePage, MutPage};
use crate::common::tree::{MappedBranchPage, MappedLeafPage};
use crate::tx::{TxRwCell, TxRwImpl};
use crate::DB;
use aligners::{alignment, AlignedBytes};
use tempfile::{tempfile, Builder, NamedTempFile};

pub(crate) fn mapped_page<T: CoerciblePage + Sized>(
  bytes: usize,
) -> SelfOwned<AlignedBytes<alignment::Page>, T> {
  SelfOwned::new_with_map(
    AlignedBytes::<alignment::Page>::new_zeroed(bytes),
    |aligned_bytes| T::own(aligned_bytes.as_mut_ptr()),
  )
}

pub(crate) trait Unseal {
  type Unsealed;

  fn unseal(&self) -> Self::Unsealed;
}

impl<'tx> Unseal for TxRwImpl<'tx> {
  type Unsealed = TxRwCell<'tx>;

  fn unseal(&self) -> Self::Unsealed {
    TxRwCell { cell: self.tx.cell }
  }
}

pub(crate) struct TestDb {
  tmp_file: NamedTempFile,
  db: DB,
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
  pub(crate) fn new() -> crate::Result<TestDb> {
    let tmp_file = Builder::new()
      .prefix("bbolt-rs-")
      .suffix(".db")
      .tempfile()?;
    let db = DB::new(tmp_file.path())?;
    Ok(TestDb { tmp_file, db })
  }
}
