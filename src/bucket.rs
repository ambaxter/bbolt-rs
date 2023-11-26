use crate::common::bucket::InBucket;
use crate::common::memory::SCell;
use crate::common::page::RefPage;
use crate::common::{HashMap, PgId, CRef};
use crate::cursor::{Cursor, CursorMut, ElemRef, TCursor};
use crate::node::{NodeR, Node, NodeMut};
use crate::tx::{TxR, TTx, Tx, TxMut};
use bumpalo::Bump;
use either::Either;
use std::cell::{Ref, RefCell, RefMut};
use std::io;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

const DEFAULT_FILL_PERCENT: f64 = 0.5;

pub struct BucketStats {}

pub(crate) trait TCBucket<'tx> {
  type NodeType: CRef<NodeR<'tx>>;
  type TxType: TTx<'tx>;

  fn page_node(&self, pgid: PgId) -> Either<RefPage<'tx>, Self::NodeType>;
}

pub trait TBucket<'tx> : TCBucket<'tx>{

  fn root(&self) -> PgId;

  fn writeable(&self) -> bool;

  fn cursor(&self) -> Cursor<'tx>;

  fn bucket(&self, name: &[u8]) -> Self;

  fn get(&self, key: &[u8]) -> &'tx [u8];

  fn sequence(&self) -> u64;

  fn for_each<F: Fn(&[u8]) -> io::Result<()>>(&self, f: F) -> io::Result<()>;

  fn for_each_bucket<F: Fn(&[u8]) -> io::Result<()>>(&self, f: F) -> io::Result<()>;

  fn status(&self) -> BucketStats;
}

pub trait TBucketMut<'tx>: TBucket<'tx> + Sized {
  fn create_bucket(&mut self, key: &[u8]) -> io::Result<Self>;

  fn create_bucket_if_not_exists(&mut self, key: &[u8]) -> io::Result<Self>;

  fn cursor_mut(&self) -> CursorMut<'tx>;

  fn delete_bucket(&mut self, key: &[u8]) -> io::Result<()>;

  fn put(&mut self, key: &[u8], data: &[u8]) -> io::Result<()>;

  fn delete(&mut self, key: &[u8]) -> io::Result<()>;

  fn set_sequence(&mut self, v: u64) -> io::Result<()>;

  fn next_sequence(&mut self) -> io::Result<u64>;

  fn for_each_mut<F: Fn(&[u8]) -> io::Result<()>>(&mut self, f: F) -> io::Result<()>;

  fn for_each_bucket_mut<F: Fn(&[u8]) -> io::Result<()>>(&mut self, f: F) -> io::Result<()>;
}

pub struct BucketR<'tx, T: TTx<'tx> + CRef<TxR<'tx>>> {
  pub(crate) inline_bucket: InBucket,
  pub(crate) tx: T,
  pub(crate) inline_page: Option<RefPage<'tx>>,
  p: PhantomData<&'tx u8>,
}

impl<'tx, T: TTx<'tx>> BucketR<'tx, T> {
  pub fn new(tx: T, in_bucket: InBucket) -> BucketR<'tx, T> {
    BucketR {
      inline_bucket: in_bucket,
      tx,
      inline_page: None,
      p: Default::default(),
    }
  }

}

pub struct BucketW<'tx> {
  root_node: Option<NodeMut<'tx>>,
  buckets: HashMap<'tx, &'tx str, BucketMut<'tx>>,
  nodes: HashMap<'tx, PgId, NodeMut<'tx>>,
  fill_percent: f64,
}

impl<'tx> BucketW<'tx> {
  pub fn new_in(bump: &'tx Bump) -> BucketW<'tx> {
    BucketW {
      root_node: None,
      buckets: HashMap::new_in(bump),
      nodes: HashMap::new_in(bump),
      fill_percent: DEFAULT_FILL_PERCENT,
    }
  }
}

pub struct BucketRW<'tx> {
  r: BucketR<'tx, TxMut<'tx>>,
  w: BucketW<'tx>
}

impl<'tx> BucketRW<'tx> {
  pub fn new_in(bump: &'tx Bump, tx: TxMut<'tx>, in_bucket: InBucket) -> BucketRW<'tx> {
    BucketRW {
      r: BucketR::new(tx, in_bucket),
      w: BucketW::new_in(bump)
    }
  }
}

#[derive(Copy, Clone)]
pub struct Bucket<'tx> {
  cell: SCell<'tx, BucketR<'tx, Tx<'tx>>>,
}

impl<'tx> CRef<BucketR<'tx, Tx<'tx>>> for Bucket<'tx> {
  fn as_cref(&self) -> Ref<BucketR<'tx, Tx<'tx>>> {
    self.cell.borrow()
  }

  fn as_cref_mut(&self) -> RefMut<BucketR<'tx, Tx<'tx>>> {
    self.cell.borrow_mut()
  }
}

impl<'tx> TCBucket<'tx> for Bucket<'tx> {
  type NodeType = Node<'tx>;
  type TxType = Tx<'tx>;

  fn page_node(&self, pgid: PgId) -> Either<RefPage<'tx>, Self::NodeType> {
    self.root() == 0.into();
    todo!()
  }
}

impl<'tx> TBucket<'tx> for Bucket<'tx> {

  fn root(&self) -> PgId {
    todo!()
  }

  fn writeable(&self) -> bool {
    todo!()
  }

  fn cursor(&self) -> Cursor<'tx> {
    todo!()
  }

  fn bucket(&self, name: &[u8]) -> Self {
    todo!()
  }

  fn get(&self, key: &[u8]) -> &'tx [u8] {
    todo!()
  }

  fn sequence(&self) -> u64 {
    todo!()
  }

  fn for_each<F: Fn(&[u8]) -> io::Result<()>>(&self, f: F) -> io::Result<()> {
    todo!()
  }

  fn for_each_bucket<F: Fn(&[u8]) -> io::Result<()>>(&self, f: F) -> io::Result<()> {
    todo!()
  }

  fn status(&self) -> BucketStats {
    todo!()
  }
}

#[derive(Copy, Clone)]
pub struct BucketMut<'tx> {
  cell: SCell<'tx, BucketRW<'tx>>,
}

impl<'tx> CRef<BucketR<'tx, TxMut<'tx>>> for BucketMut<'tx> {
  fn as_cref(&self) -> Ref<BucketR<'tx, TxMut<'tx>>> {
    Ref::map(self.cell.borrow(), | b | &b.r)
  }

  fn as_cref_mut(&self) -> RefMut<BucketR<'tx, TxMut<'tx>>> {
    RefMut::map(self.cell.borrow_mut(), | b | &mut b.r)
  }
}

impl<'tx> TCBucket<'tx> for BucketMut<'tx> {
  type NodeType = NodeMut<'tx>;
  type TxType = TxMut<'tx>;

  fn page_node(&self, pgid: PgId) -> Either<RefPage<'tx>, Self::NodeType> {
    todo!()
  }
}

impl<'tx> TBucket<'tx> for BucketMut<'tx> {


  fn root(&self) -> PgId {
    todo!()
  }

  fn writeable(&self) -> bool {
    todo!()
  }

  fn cursor(&self) -> Cursor<'tx> {
    todo!()
  }

  fn bucket(&self, name: &[u8]) -> Self {
    todo!()
  }

  fn get(&self, key: &[u8]) -> &'tx [u8] {
    todo!()
  }

  fn sequence(&self) -> u64 {
    todo!()
  }

  fn for_each<F: Fn(&[u8]) -> io::Result<()>>(&self, f: F) -> io::Result<()> {
    todo!()
  }

  fn for_each_bucket<F: Fn(&[u8]) -> io::Result<()>>(&self, f: F) -> io::Result<()> {
    todo!()
  }

  fn status(&self) -> BucketStats {
    todo!()
  }
}
impl<'tx> TBucketMut<'tx> for BucketMut<'tx> {
  fn create_bucket(&mut self, key: &[u8]) -> io::Result<Self> {
    todo!()
  }

  fn create_bucket_if_not_exists(&mut self, key: &[u8]) -> io::Result<Self> {
    todo!()
  }

  fn cursor_mut(&self) -> CursorMut<'tx> {
    todo!()
  }

  fn delete_bucket(&mut self, key: &[u8]) -> io::Result<()> {
    todo!()
  }

  fn put(&mut self, key: &[u8], data: &[u8]) -> io::Result<()> {
    todo!()
  }

  fn delete(&mut self, key: &[u8]) -> io::Result<()> {
    todo!()
  }

  fn set_sequence(&mut self, v: u64) -> io::Result<()> {
    todo!()
  }

  fn next_sequence(&mut self) -> io::Result<u64> {
    todo!()
  }

  fn for_each_mut<F: Fn(&[u8]) -> io::Result<()>>(&mut self, f: F) -> io::Result<()> {
    todo!()
  }

  fn for_each_bucket_mut<F: Fn(&[u8]) -> io::Result<()>>(&mut self, f: F) -> io::Result<()> {
    todo!()
  }
}
