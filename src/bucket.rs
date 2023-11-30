use crate::common::bucket::{InBucket, IN_BUCKET_SIZE};
use crate::common::errors::{
  BUCKET_EXISTS, BUCKET_NAME_REQUIRED, BUCKET_NOT_FOUND, INCOMPATIBLE_VALUE,
};
use crate::common::memory::{IsAligned, SCell};
use crate::common::meta::MetaPage;
use crate::common::page::{Page, RefPage, BUCKET_LEAF_FLAG};
use crate::common::{BVec, HashMap, IRef, PgId, ZERO_PGID};
use crate::cursor::{Cursor, CursorAPI, CursorIAPI, CursorMut, CursorMutIAPI, ElemRef, ICursor};
use crate::node::{NodeImpl, NodeMut, NodeW};
use crate::tx::{Tx, TxAPI, TxIRef, TxImpl, TxMut, TxR, TxW};
use bumpalo::Bump;
use bytemuck::{Pod, Zeroable};
use either::Either;
use std::alloc::Layout;
use std::cell::{Ref, RefCell, RefMut};
use std::error::Error;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::ptr::slice_from_raw_parts_mut;
use std::{io, mem};

const DEFAULT_FILL_PERCENT: f64 = 0.5;

const INLINE_PAGE_ALIGNMENT: usize = mem::align_of::<InlinePage>();
const INLINE_PAGE_SIZE: usize = mem::size_of::<InlinePage>();

#[repr(C)]
#[derive(Copy, Clone, Default, Pod, Zeroable)]
struct InlinePage {
  header: InBucket,
  page: Page,
}

pub struct BucketStats {}

pub(crate) trait BucketIAPI<'tx> {
  type TxType: TxIRef<'tx>;
  type BucketType: BucketIRef<'tx>;

  fn new(
    bump: &'tx Bump, bucket_header: InBucket, tx: Self::TxType, inline_page: Option<RefPage<'tx>>,
  ) -> Self::BucketType;
}

pub trait BucketIRef<'tx>:
  BucketIAPI<'tx> + IRef<BucketR<'tx, Self::TxType>, BucketP<'tx, Self::BucketType>>
{
}

pub(crate) struct BucketImpl {}

impl BucketImpl {
  fn free(cell: BucketMut) {
    todo!()
  }
}

impl BucketImpl {
  pub(crate) fn api_tx<'tx, B: BucketIRef<'tx>>(cell: B) -> B::TxType {
    cell.borrow_iref().0.tx
  }

  pub(crate) fn root<'tx, B: BucketIRef<'tx>>(cell: B) -> PgId {
    cell.borrow_iref().0.bucket_header.root()
  }

  pub(crate) fn i_cursor<'tx, B: BucketIRef<'tx>>(cell: B, bump: &'tx Bump) -> ICursor<'tx, B> {
    ICursor::new(cell, bump)
  }

  pub(crate) fn api_bucket<'tx, B: BucketIRef<'tx>>(cell: B, name: &[u8]) -> Option<B::BucketType> {
    let bump = {
      let tx = {
        let b = cell.borrow_iref();
        if let Some(w) = b.1 {
          if let Some(child) = w.buckets.get(name) {
            return Some(*child);
          }
        }
        b.0.tx
      };
      TxImpl::bump(tx)
    };

    let mut c = BucketImpl::i_cursor(cell, bump);
    let (k, v, flags) = c.i_seek(name)?;
    if !(name == k) || (flags & BUCKET_LEAF_FLAG) == 0 {
      return None;
    }

    let child = BucketImpl::open_bucket(cell, bump, v);
    if let Some(mut w) = cell.borrow_mut_iref().1 {
      let name = bump.alloc_slice_copy(name);
      w.buckets.insert(name, child);
    }

    Some(child)
  }

  fn open_bucket<'tx, B: BucketIRef<'tx>>(
    cell: B, bump: &'tx Bump, mut value: &'tx [u8],
  ) -> <B as BucketIAPI<'tx>>::BucketType {
    // 3 goals

    // Unaligned access requires a copy to be made.
    //TODO: use std is_aligned_to when it comes out
    if !IsAligned::is_aligned_to::<InlinePage>(value.as_ptr()) {
      // TODO: Shove this into a centralized function somewhere
      let layout = Layout::from_size_align(value.len(), INLINE_PAGE_ALIGNMENT).unwrap();
      let new_value = unsafe {
        let mut new_value = bump.alloc_layout(layout);
        let new_value_ptr = new_value.as_mut() as *mut u8;
        &mut *slice_from_raw_parts_mut(new_value_ptr, value.len())
      };
      new_value.copy_from_slice(value);
      value = new_value;
    }
    let bucket_header = *bytemuck::from_bytes::<InBucket>(value);
    let ref_page = if bucket_header.root() == ZERO_PGID {
      assert!(
        value.len() >= INLINE_PAGE_SIZE,
        "subbucket value not large enough. Expected at least {} bytes. Was {}",
        INLINE_PAGE_SIZE,
        value.len()
      );
      unsafe {
        let ref_page_ptr = value.as_ptr().add(IN_BUCKET_SIZE);
        Some(RefPage::new(ref_page_ptr))
      }
    } else {
      None
    };
    let tx = cell.borrow_iref().0.tx;
    B::new(bump, bucket_header, tx, ref_page)
  }

  pub(crate) fn api_create_bucket<'tx>(
    cell: BucketMut<'tx>, key: &[u8],
  ) -> io::Result<BucketMut<'tx>> {
    if key.is_empty() {
      return Err(BUCKET_NAME_REQUIRED());
    }

    let tx = cell.borrow_iref().0.tx;
    let bump = TxImpl::bump(tx);
    let mut c = BucketImpl::i_cursor(cell, bump);

    if let Some((k, _, flags)) = c.i_seek(key) {
      if k == key {
        if flags & BUCKET_LEAF_FLAG != 0 {
          return Err(BUCKET_EXISTS());
        }
        return Err(INCOMPATIBLE_VALUE());
      }
    }

    let mut inline_page = InlinePage::default();
    inline_page.page.set_leaf();
    let layout = Layout::from_size_align(INLINE_PAGE_SIZE, INLINE_PAGE_ALIGNMENT).unwrap();
    let value = unsafe {
      let mut data = bump.alloc_layout(layout);
      &mut *slice_from_raw_parts_mut(data.as_mut() as *mut u8, INLINE_PAGE_SIZE)
    };
    value.copy_from_slice(bytemuck::bytes_of(&inline_page));
    let key = bump.alloc_slice_clone(key) as &[u8];

    NodeImpl::put(c.node(), key, key, value, ZERO_PGID, BUCKET_LEAF_FLAG);

    cell.borrow_mut_iref().0.inline_page = None;

    return Ok(BucketImpl::api_bucket(cell, key).unwrap());
  }

  pub(crate) fn api_create_bucket_if_not_exists<'tx>(
    cell: BucketMut<'tx>, key: &[u8],
  ) -> io::Result<BucketMut<'tx>> {
    match BucketImpl::api_create_bucket(cell, key) {
      Ok(child) => Ok(child),
      Err(error) => {
        let e = error.get_ref().unwrap();
        // TODO: Rework this logic so we don't have to rely on errors
        // TODO: Also rework the errors API so we can check this easier
        // Yes, I know it's deprecated. Sue me
        if e.description() == "bucket already exists" {
          return Ok(BucketImpl::api_bucket(cell, key).unwrap());
        } else {
          return Err(error);
        }
      }
    }
  }

  pub(crate) fn api_delete_bucket<'tx>(cell: BucketMut<'tx>, key: &[u8]) -> io::Result<()> {
    let tx = cell.borrow_iref().0.tx;
    let bump = TxImpl::bump(tx);
    let mut c = BucketImpl::i_cursor(cell, bump);

    let (k, _, flags) = c.i_seek(key).unwrap();
    if key != k {
      return Err(BUCKET_NOT_FOUND());
    } else if flags & BUCKET_LEAF_FLAG != 0 {
      return Err(INCOMPATIBLE_VALUE());
    }

    let child = BucketImpl::api_bucket(cell, key).unwrap();
    BucketImpl::for_each_bucket(child, |k| {
      match BucketImpl::api_delete_bucket(cell, k) {
        Ok(_) => Ok(()),
        // TODO: Ideally we want to chain errors here
        Err(e) => Err(io::Error::other(format!("delete bucket: {}", e))),
      }
    })?;

    if let Some(mut w) = cell.borrow_mut_iref().1 {
      w.buckets.remove(key);
    }

    if let Some(mut w) = child.borrow_mut_iref().1 {
      w.nodes.clear();
      w.root_node = None;
    }
    BucketImpl::free(child);

    NodeImpl::del(c.node(), key);
    Ok(())
  }

  fn for_each_bucket<'tx, B: BucketIRef<'tx>, F: Fn(&[u8]) -> io::Result<()>>(
    cell: B, func: F,
  ) -> io::Result<()> {
    todo!()
  }

  pub(crate) fn node<'tx>(
    cell: BucketMut<'tx>, pgid: PgId, parent: Option<NodeMut<'tx>>,
  ) -> NodeMut<'tx> {
    todo!()
  }

  pub fn page_node<'tx, B: BucketIRef<'tx>>(
    cell: B, id: PgId,
  ) -> Either<RefPage<'tx>, NodeMut<'tx>> {
    let tx = {
      let (r, w) = cell.borrow_iref();
      // Inline buckets have a fake page embedded in their value so treat them
      // differently. We'll return the rootNode (if available) or the fake page.
      if r.bucket_header.root() == ZERO_PGID {
        if id != ZERO_PGID {
          panic!("inline bucket non-zero page access(2): {} != 0", id)
        }
        if let Some(root_node) = &w.map(|wb| wb.root_node).flatten() {
          return Either::Right(*root_node);
        } else {
          return Either::Left(r.inline_page.unwrap());
        }
      }
      // Check the node cache for non-inline buckets.
      if let Some(wb) = &w {
        if let Some(node) = wb.nodes.get(&id) {
          return Either::Right(*node);
        }
      }
      r.tx
    };
    Either::Left(TxImpl::page(tx, id))
  }
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

pub struct BucketR<'tx, T: TxIRef<'tx>> {
  pub(crate) bucket_header: InBucket,
  pub(crate) tx: T,
  pub(crate) inline_page: Option<RefPage<'tx>>,
  p: PhantomData<&'tx u8>,
}

impl<'tx, T: TxIRef<'tx>> BucketR<'tx, T> {
  pub fn new(tx: T, in_bucket: InBucket) -> BucketR<'tx, T> {
    BucketR {
      bucket_header: in_bucket,
      tx,
      inline_page: None,
      p: Default::default(),
    }
  }
}

pub struct BucketP<'tx, B: BucketIRef<'tx>> {
  root_node: Option<NodeMut<'tx>>,
  buckets: HashMap<'tx, &'tx [u8], B>,
  nodes: HashMap<'tx, PgId, NodeMut<'tx>>,
  fill_percent: f64,
}

impl<'tx, B: BucketIRef<'tx>> BucketP<'tx, B> {
  pub fn new_in(bump: &'tx Bump) -> BucketP<'tx, B> {
    BucketP {
      root_node: None,
      buckets: HashMap::new_in(bump),
      nodes: HashMap::new_in(bump),
      fill_percent: DEFAULT_FILL_PERCENT,
    }
  }
}

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

impl<'tx> BucketIAPI<'tx> for Bucket<'tx> {
  type TxType = Tx<'tx>;
  type BucketType = Bucket<'tx>;

  fn new(
    bump: &'tx Bump, bucket_header: InBucket, tx: Self::TxType, inline_page: Option<RefPage<'tx>>,
  ) -> Self::BucketType {
    let r = BucketR {
      bucket_header,
      tx,
      inline_page,
      p: Default::default(),
    };

    Bucket {
      cell: SCell::new_in(r, bump),
    }
  }
}

impl<'tx> IRef<BucketR<'tx, Tx<'tx>>, BucketP<'tx, Bucket<'tx>>> for Bucket<'tx> {
  fn borrow_iref(
    &self,
  ) -> (
    Ref<BucketR<'tx, Tx<'tx>>>,
    Option<Ref<BucketP<'tx, Bucket<'tx>>>>,
  ) {
    (self.cell.borrow(), None)
  }

  fn borrow_mut_iref(
    &self,
  ) -> (
    RefMut<BucketR<'tx, Tx<'tx>>>,
    Option<RefMut<BucketP<'tx, Bucket<'tx>>>>,
  ) {
    (self.cell.borrow_mut(), None)
  }
}

impl<'tx> BucketIRef<'tx> for Bucket<'tx> {}

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

impl<'tx> IRef<BucketR<'tx, TxMut<'tx>>, BucketW<'tx>> for BucketMut<'tx> {
  fn borrow_iref(&self) -> (Ref<BucketR<'tx, TxMut<'tx>>>, Option<Ref<BucketW<'tx>>>) {
    let (r, w) = Ref::map_split(self.cell.borrow(), |b| (&b.r, &b.w));
    (r, Some(w))
  }

  fn borrow_mut_iref(
    &self,
  ) -> (
    RefMut<BucketR<'tx, TxMut<'tx>>>,
    Option<RefMut<BucketW<'tx>>>,
  ) {
    let (r, w) = RefMut::map_split(self.cell.borrow_mut(), |b| (&mut b.r, &mut b.w));
    (r, Some(w))
  }
}

impl<'tx> BucketIAPI<'tx> for BucketMut<'tx> {
  type TxType = TxMut<'tx>;
  type BucketType = BucketMut<'tx>;

  fn new(
    bump: &'tx Bump, bucket_header: InBucket, tx: Self::TxType, inline_page: Option<RefPage<'tx>>,
  ) -> Self::BucketType {
    let r = BucketR {
      bucket_header,
      tx,
      inline_page,
      p: Default::default(),
    };

    let w = BucketW::new_in(bump);

    BucketMut {
      cell: SCell::new_in(BucketRW { r, w }, bump),
    }
  }
}

impl<'tx> BucketIRef<'tx> for BucketMut<'tx> {}

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
