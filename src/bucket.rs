use crate::common::bucket::InBucket;
use crate::common::memory::SCell;
use crate::common::page::RefPage;
use crate::common::{HashMap, IRef, PgId, ZERO_PGID};
use crate::cursor::{Cursor, CursorAPI, CursorMut, ElemRef};
use crate::node::{Node, NodeMut, NodeR};
use crate::tx::{Tx, TxAPI, TxMut, TxR};
use bumpalo::Bump;
use either::Either;
use std::cell::{Ref, RefCell, RefMut};
use std::io;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

const DEFAULT_FILL_PERCENT: f64 = 0.5;

pub struct BucketStats {}

pub(crate) trait BucketIAPI<'tx> {
  type NodeType: IRef<NodeR<'tx>>;
  type TxType: TxAPI<'tx>;
}

pub trait BucketAPI<'tx>: BucketIAPI<'tx> {
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

pub trait BucketMutAPI<'tx>: BucketAPI<'tx> + Sized {
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

pub struct BucketR<'tx, T: TxAPI<'tx> + IRef<TxR<'tx>>> {
  pub(crate) inline_bucket: InBucket,
  pub(crate) tx: T,
  pub(crate) inline_page: Option<RefPage<'tx>>,
  p: PhantomData<&'tx u8>,
}

impl<'tx, T: TxAPI<'tx>> BucketR<'tx, T> {
  pub fn new(tx: T, in_bucket: InBucket) -> BucketR<'tx, T> {
    BucketR {
      inline_bucket: in_bucket,
      tx,
      inline_page: None,
      p: Default::default(),
    }
  }

  /// pageNode returns the in-memory node, if it exists.
  /// Otherwise, returns the underlying page.
  pub(crate) fn page_node<B: BucketAPI<'tx>>(
    &self, id: PgId, w: Option<&BucketP<'tx, B>>,
  ) -> Either<RefPage<'tx>, B::NodeType> {
    // Inline buckets have a fake page embedded in their value so treat them
    // differently. We'll return the rootNode (if available) or the fake page.
    if self.inline_bucket.root() == ZERO_PGID {
      if id != ZERO_PGID {
        panic!("inline bucket non-zero page access(2): {} != 0", id)
      }
      if let Some(wb) = w {
        return if let Some(root_node) = wb.root_node {
          Either::Right(root_node)
        } else {
          Either::Left(self.inline_page.unwrap())
        };
      }
    }
    // Check the node cache for non-inline buckets.
    if let Some(wb) = w {
      if let Some(node) = wb.nodes.get(&id) {
        return Either::Right(*node);
      }
    }
    // Finally lookup the page from the transaction if no node is materialized.
    Either::Left(self.tx.page(id))
  }
}

pub struct BucketP<'tx, B: BucketAPI<'tx>> {
  root_node: Option<B::NodeType>,
  buckets: HashMap<'tx, &'tx str, B>,
  nodes: HashMap<'tx, PgId, B::NodeType>,
  fill_percent: f64,
}

impl<'tx, B: BucketAPI<'tx>> BucketP<'tx, B> {
  pub fn new_in(bump: &'tx Bump) -> BucketP<'tx, B> {
    BucketP {
      root_node: None,
      buckets: HashMap::new_in(bump),
      nodes: HashMap::new_in(bump),
      fill_percent: DEFAULT_FILL_PERCENT,
    }
  }
}

pub type BucketPR<'tx> = BucketP<'tx, Bucket<'tx>>;

pub type BucketW<'tx> = BucketP<'tx, BucketMut<'tx>>;

pub struct BucketRW<'tx> {
  r: BucketR<'tx, TxMut<'tx>>,
  w: BucketW<'tx>,
}

impl<'tx> BucketRW<'tx> {
  pub fn new_in(bump: &'tx Bump, tx: TxMut<'tx>, in_bucket: InBucket) -> BucketRW<'tx> {
    BucketRW {
      r: BucketR::new(tx, in_bucket),
      w: BucketW::new_in(bump),
    }
  }
}

#[derive(Copy, Clone)]
pub struct Bucket<'tx> {
  cell: SCell<'tx, BucketR<'tx, Tx<'tx>>>,
}

impl<'tx> IRef<BucketR<'tx, Tx<'tx>>> for Bucket<'tx> {
  fn borrow_iref(&self) -> Ref<BucketR<'tx, Tx<'tx>>> {
    self.cell.borrow()
  }

  fn borrow_mut_iref(&self) -> RefMut<BucketR<'tx, Tx<'tx>>> {
    self.cell.borrow_mut()
  }
}

impl<'tx> BucketIAPI<'tx> for Bucket<'tx> {
  type NodeType = Node<'tx>;
  type TxType = Tx<'tx>;
}

impl<'tx> BucketAPI<'tx> for Bucket<'tx> {
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

impl<'tx> IRef<BucketR<'tx, TxMut<'tx>>> for BucketMut<'tx> {
  fn borrow_iref(&self) -> Ref<BucketR<'tx, TxMut<'tx>>> {
    Ref::map(self.cell.borrow(), |b| &b.r)
  }

  fn borrow_mut_iref(&self) -> RefMut<BucketR<'tx, TxMut<'tx>>> {
    RefMut::map(self.cell.borrow_mut(), |b| &mut b.r)
  }
}

impl<'tx> BucketIAPI<'tx> for BucketMut<'tx> {
  type NodeType = NodeMut<'tx>;
  type TxType = TxMut<'tx>;
}

impl<'tx> BucketAPI<'tx> for BucketMut<'tx> {
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
impl<'tx> BucketMutAPI<'tx> for BucketMut<'tx> {
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
