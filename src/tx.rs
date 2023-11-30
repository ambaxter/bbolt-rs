use crate::bucket::{Bucket, BucketIAPI, BucketIRef, BucketMut};
use crate::common::memory::SCell;
use crate::common::page::RefPage;
use crate::common::selfowned::SelfOwned;
use crate::common::{IRef, PgId};
use crate::node::NodeMut;
use bumpalo::Bump;
use std::cell;
use std::cell::{Ref, RefMut};
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

pub(crate) trait TxIAPI<'tx> {
  type BucketType: BucketIRef<'tx>;
}

pub trait TxIRef<'tx>: TxIAPI<'tx> + IRef<TxR<'tx>, TxW<'tx>> {}

pub(crate) struct TxImpl {}

impl TxImpl {
  pub fn page<'tx, T: TxIRef<'tx>>(cell: T, id: PgId) -> RefPage<'tx> {
    todo!()
  }

  pub(crate) fn bump<'tx, T: TxIRef<'tx>>(cell: T) -> &'tx Bump {
    todo!()
  }
}

pub trait TxAPI<'tx>: Copy + Clone {
  fn writeable(&self) -> bool;

  fn page(&self, id: PgId) -> RefPage<'tx>;
}

pub trait TxMutAPI<'tx>: TxAPI<'tx> {}

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

impl<'tx> IRef<TxR<'tx>, TxW<'tx>> for Tx<'tx> {
  fn borrow_iref(&self) -> (Ref<TxR<'tx>>, Option<Ref<TxW<'tx>>>) {
    (self.cell.borrow(), None)
  }

  fn borrow_mut_iref(&self) -> (RefMut<TxR<'tx>>, Option<RefMut<TxW<'tx>>>) {
    (self.cell.borrow_mut(), None)
  }
}

impl<'tx> TxIAPI<'tx> for Tx<'tx> {
  type BucketType = Bucket<'tx>;
}

impl<'tx> TxIRef<'tx> for Tx<'tx> {}

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

impl<'tx> IRef<TxR<'tx>, TxW<'tx>> for TxMut<'tx> {
  fn borrow_iref(&self) -> (Ref<TxR<'tx>>, Option<Ref<TxW<'tx>>>) {
    let (r, w) = Ref::map_split(self.cell.borrow(), |b| (&b.r, &b.w));
    (r, Some(w))
  }

  fn borrow_mut_iref(&self) -> (RefMut<TxR<'tx>>, Option<RefMut<TxW<'tx>>>) {
    let (r, w) = RefMut::map_split(self.cell.borrow_mut(), |b| (&mut b.r, &mut b.w));
    (r, Some(w))
  }
}

impl<'tx> TxIAPI<'tx> for TxMut<'tx> {
  type BucketType = BucketMut<'tx>;
}

impl<'tx> TxIRef<'tx> for TxMut<'tx> {}

impl<'tx> TxAPI<'tx> for TxMut<'tx> {
  fn writeable(&self) -> bool {
    true
  }

  fn page(&self, id: PgId) -> RefPage<'tx> {
    todo!()
  }
}

impl<'tx> TxMutAPI<'tx> for TxMut<'tx> {}
