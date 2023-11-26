use std::cell;
use std::cell::{Ref, RefMut};
use crate::common::memory::SCell;
use crate::common::selfowned::SelfOwned;
use bumpalo::Bump;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use crate::common::{CRef, CRefMut};


pub trait TTx<'tx>: CRef<TxR<'tx>> + Copy + Clone {
  fn writeable(&self) -> bool;
}

pub trait TTxMut<'tx>: TTx<'tx> + CRefMut<TxRW<'tx>> {}

pub struct TxR<'tx> {
  bump: &'tx Bump,
  p: PhantomData<&'tx u8>,
}

pub struct TxW<'tx> {
  p: PhantomData<&'tx u8>
}

pub struct TxRW<'tx> {
  r: TxR<'tx>,
  w: TxW<'tx>
}

#[derive(Copy, Clone)]
pub struct Tx<'tx> {
  cell: SCell<'tx, TxR<'tx>>,
}

impl<'tx> CRef<TxR<'tx>> for Tx<'tx> {
  fn as_cref(&self) -> Ref<TxR<'tx>> {
    self.cell.borrow()
  }

  fn as_cref_mut(&self) -> RefMut<TxR<'tx>> {
    self.cell.borrow_mut()
  }
}

impl<'tx> TTx<'tx> for Tx<'tx> {
  fn writeable(&self) -> bool {
    false
  }
}

#[derive(Copy, Clone)]
pub struct TxMut<'tx> {
  cell: SCell<'tx, TxRW<'tx>>,
}

impl<'tx> CRef<TxR<'tx>> for TxMut<'tx> {
  fn as_cref(&self) -> Ref<TxR<'tx>> {
    Ref::map(self.cell.borrow(), |tx | &tx.r)
  }

  fn as_cref_mut(&self) -> RefMut<TxR<'tx>> {
    RefMut::map(self.cell.borrow_mut(), |tx | &mut tx.r)
  }
}

impl<'tx> TTx<'tx> for TxMut<'tx> {
  fn writeable(&self) -> bool {
    true
  }
}

impl<'tx> CRefMut<TxRW<'tx>> for TxMut<'tx> {
  fn as_mut_cref(&self) -> Ref<TxRW<'tx>> {
    self.cell.borrow()
  }

  fn as_mut_cref_mut(&self) -> RefMut<TxRW<'tx>> {
    self.cell.borrow_mut()
  }
}

impl<'tx> TTxMut<'tx> for TxMut<'tx> {}
