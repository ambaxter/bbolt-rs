use crate::common::memory::SCell;
use crate::common::page::RefPage;
use crate::common::selfowned::SelfOwned;
use crate::common::{IRef, IRefMut, PgId};
use bumpalo::Bump;
use std::cell;
use std::cell::{Ref, RefMut};
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

pub trait TxAPI<'tx>: IRef<TxR<'tx>> + Copy + Clone {
  fn writeable(&self) -> bool;

  fn page(&self, id: PgId) -> RefPage<'tx>;
}

pub trait TxMutAPI<'tx>: TxAPI<'tx> + IRefMut<TxRW<'tx>> {}

pub struct TxR<'tx> {
  bump: &'tx Bump,
  p: PhantomData<&'tx u8>,
}

pub struct TxW<'tx> {
  p: PhantomData<&'tx u8>,
}

pub struct TxRW<'tx> {
  r: TxR<'tx>,
  w: TxW<'tx>,
}

#[derive(Copy, Clone)]
pub struct Tx<'tx> {
  cell: SCell<'tx, TxR<'tx>>,
}

impl<'tx> IRef<TxR<'tx>> for Tx<'tx> {
  fn borrow_iref(&self) -> Ref<TxR<'tx>> {
    self.cell.borrow()
  }

  fn borrow_mut_iref(&self) -> RefMut<TxR<'tx>> {
    self.cell.borrow_mut()
  }
}

impl<'tx> TxAPI<'tx> for Tx<'tx> {
  fn writeable(&self) -> bool {
    false
  }

  fn page(&self, id: PgId) -> RefPage<'tx> {
    todo!()
  }
}

#[derive(Copy, Clone)]
pub struct TxMut<'tx> {
  cell: SCell<'tx, TxRW<'tx>>,
}

impl<'tx> IRef<TxR<'tx>> for TxMut<'tx> {
  fn borrow_iref(&self) -> Ref<TxR<'tx>> {
    Ref::map(self.cell.borrow(), |tx| &tx.r)
  }

  fn borrow_mut_iref(&self) -> RefMut<TxR<'tx>> {
    RefMut::map(self.cell.borrow_mut(), |tx| &mut tx.r)
  }
}

impl<'tx> TxAPI<'tx> for TxMut<'tx> {
  fn writeable(&self) -> bool {
    true
  }

  fn page(&self, id: PgId) -> RefPage<'tx> {
    todo!()
  }
}

impl<'tx> IRefMut<TxRW<'tx>> for TxMut<'tx> {
  fn borrow_iref_mut(&self) -> Ref<TxRW<'tx>> {
    self.cell.borrow()
  }

  fn borrow_mut_iref_mut(&self) -> RefMut<TxRW<'tx>> {
    self.cell.borrow_mut()
  }
}

impl<'tx> TxMutAPI<'tx> for TxMut<'tx> {}
