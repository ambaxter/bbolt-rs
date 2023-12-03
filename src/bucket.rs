use crate::common::bucket::{InBucket, IN_BUCKET_SIZE};
use crate::common::memory::{IsAligned, SCell};
use crate::common::meta::MetaPage;
use crate::common::page::{CoerciblePage, Page, RefPage, BUCKET_LEAF_FLAG, PAGE_HEADER_SIZE};
use crate::common::tree::{MappedBranchPage, TreePage, LEAF_PAGE_ELEMENT_SIZE};
use crate::common::{BVec, HashMap, IRef, PgId, ZERO_PGID};
use crate::cursor::{Cursor, CursorAPI, CursorIAPI, CursorMut, CursorMutIAPI, ElemRef, ICursor};
use crate::node::{NodeMut, NodeW};
use crate::tx::{Tx, TxAPI, TxIAPI, TxImpl, TxMut, TxMutIAPI, TxR, TxW};
use crate::Error::{
  BucketExists, BucketNameRequired, BucketNotFound, IncompatibleValue, KeyRequired, KeyTooLarge,
  ValueTooLarge,
};
use crate::{CursorMutAPI, Error};
use bumpalo::Bump;
use bytemuck::{Pod, Zeroable};
use either::Either;
use std::alloc::Layout;
use std::cell::{Ref, RefCell, RefMut};
use std::marker::PhantomData;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::ptr::slice_from_raw_parts_mut;

const DEFAULT_FILL_PERCENT: f64 = 0.5;
const MAX_KEY_SIZE: u32 = 32768;
const MAX_VALUE_SIZE: u32 = (1 << 31) - 2;
const INLINE_PAGE_ALIGNMENT: usize = mem::align_of::<InlinePage>();
const INLINE_PAGE_SIZE: usize = mem::size_of::<InlinePage>();

#[repr(C)]
#[derive(Copy, Clone, Default, Pod, Zeroable)]
struct InlinePage {
  header: InBucket,
  page: Page,
}

pub struct BucketStats {}

pub(crate) trait BucketIAPI<'tx, T: TxIAPI<'tx>>:
  IRef<BucketR<'tx>, BucketP<'tx, T, Self>> + 'tx
{
  fn new(bucket_header: InBucket, tx: &'tx T, inline_page: Option<RefPage<'tx>>) -> Self;

  fn is_writeable(&self) -> bool;

  fn api_tx(self) -> &'tx T;

  fn root(self) -> PgId {
    self.borrow_iref().0.bucket_header.root()
  }

  fn i_cursor(self) -> ICursor<'tx, T, Self> {
    ICursor::new(self, self.api_tx().bump())
  }

  fn api_bucket(self, name: &[u8]) -> Option<Self> {
    if self.is_writeable() {
      let b = self.borrow_iref();
      if let Some(w) = b.1 {
        if let Some(child) = w.buckets.get(name) {
          return Some(*child);
        }
      }
    }
    let mut c = self.i_cursor();
    let (k, v, flags) = c.i_seek(name)?;
    if !(name == k) || (flags & BUCKET_LEAF_FLAG) == 0 {
      return None;
    }

    let child = self.open_bucket(v);
    if let Some(mut w) = self.borrow_mut_iref().1 {
      let bump = self.api_tx().bump();
      let name = bump.alloc_slice_copy(name);
      w.buckets.insert(name, child);
    }

    Some(child)
  }

  fn open_bucket(self, mut value: &[u8]) -> Self {
    // Unaligned access requires a copy to be made.
    //TODO: use std is_aligned_to when it comes out
    if !IsAligned::is_aligned_to::<InlinePage>(value.as_ptr()) {
      // TODO: Shove this into a centralized function somewhere
      let layout = Layout::from_size_align(value.len(), INLINE_PAGE_ALIGNMENT).unwrap();
      let bump = self.api_tx().bump();
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
    Self::new(bucket_header, self.api_tx(), ref_page)
  }

  fn api_get(self, key: &[u8]) -> Option<&'tx [u8]> {
    let (k, v, flags) = self.i_cursor().i_seek(key).unwrap();
    if (flags & BUCKET_LEAF_FLAG) != 0 {
      return None;
    }
    if key != k {
      return None;
    }
    Some(v)
  }

  fn api_for_each<F: Fn(&[u8]) -> crate::Result<()>>(self, f: F) -> crate::Result<()> {
    let mut c = self.i_cursor();
    let mut inode = c.i_first();
    while let Some((k, _, flags)) = inode {
      f(k)?;
      inode = c.i_next();
    }
    Ok(())
  }

  fn api_for_each_bucket<F: FnMut(&[u8]) -> crate::Result<()>>(
    self, mut f: F,
  ) -> crate::Result<()> {
    let mut c = self.i_cursor();
    let mut inode = c.i_first();
    while let Some((k, _, flags)) = inode {
      if flags & BUCKET_LEAF_FLAG != 0 {
        f(k)?;
      }
      inode = c.i_next();
    }
    Ok(())
  }

  fn for_each_page<F: FnMut(&RefPage, usize, &[PgId]) + Copy>(self, mut f: F) {
    let root = {
      let (r, _) = self.borrow_iref();
      let root = r.bucket_header.root();
      if let Some(page) = &r.inline_page {
        f(page, 0, &[root]);
        return;
      }
      root
    };

    TxImpl::for_each_page(self.api_tx(), root, f);
  }

  fn for_each_page_node<F: FnMut(&Either<RefPage, NodeMut<'tx>>, usize) + Copy>(self, mut f: F) {
    let root = {
      let (r, _) = self.borrow_iref();
      if let Some(page) = &r.inline_page {
        f(&Either::Left(*page), 0);
        return;
      }
      r.bucket_header.root()
    };
    self._for_each_page_node(root, 0, f);
  }

  fn _for_each_page_node<F: FnMut(&Either<RefPage, NodeMut<'tx>>, usize) + Copy>(
    self, root: PgId, depth: usize, mut f: F,
  ) {
    let pn = self.page_node(root);
    f(&pn, depth);
    match &pn {
      Either::Left(page) => {
        if let Some(branch_page) = MappedBranchPage::coerce_ref(page) {
          branch_page.elements().iter().for_each(|elem| {
            self._for_each_page_node(elem.pgid(), depth + 1, f);
          });
        }
      }
      Either::Right(node) => {
        let bump = self.api_tx().bump();
        // To keep with our rules we much copy the inode pgids to temporary storage first
        // This should be unnecessary, but working first *then* optimize
        let v = {
          let node_borrow = node.cell.borrow();
          let mut v = BVec::with_capacity_in(node_borrow.inodes.len(), bump);
          let ids = node_borrow.inodes.iter().map(|inode| inode.pgid());
          v.extend(ids);
          v
        };
        v.into_iter()
          .for_each(|pgid| self._for_each_page_node(pgid, depth + 1, f));
      }
    }
  }

  fn page_node(self, id: PgId) -> Either<RefPage<'tx>, NodeMut<'tx>> {
    let (r, w) = self.borrow_iref();
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

    if self.is_writeable() {
      // Check the node cache for non-inline buckets.
      if let Some(wb) = &w {
        if let Some(node) = wb.nodes.get(&id) {
          return Either::Right(*node);
        }
      }
    }
    Either::Left(TxImpl::page(self.api_tx(), id))
  }

  fn api_sequence(self) -> u64 {
    self.borrow_iref().0.bucket_header.sequence()
  }

  fn max_inline_bucket_size(&self) -> usize {
    self.api_tx().page_size() / 4
  }
}

pub(crate) trait BucketMutIAPI<'tx>: BucketIAPI<'tx, TxMut<'tx>> {
  fn api_create_bucket(self, key: &[u8]) -> crate::Result<Self>;
  fn api_create_bucket_if_not_exists(self, key: &[u8]) -> crate::Result<Self>;
  fn api_delete_bucket(self, key: &[u8]) -> crate::Result<()>;

  fn api_put(self, key: &[u8], value: &[u8]) -> crate::Result<()>;

  fn api_delete(self, key: &[u8]) -> crate::Result<()>;

  fn api_set_sequence(cell: BucketMut<'tx>, v: u64) -> crate::Result<()>;

  fn api_next_sequence(cell: BucketMut<'tx>) -> crate::Result<u64>;

  fn free(self);

  fn spill(self, bump: &'tx Bump) -> crate::Result<()>;

  fn inlineable(self) -> bool;

  fn own_in(self);

  fn node(self, pgid: PgId, parent: Option<NodeMut<'tx>>) -> NodeMut<'tx>;
}

pub trait BucketAPI<'tx, T: TxIAPI<'tx>>: BucketIAPI<'tx, T> {
  fn root(&self) -> PgId;

  fn writeable(&self) -> bool;

  fn cursor(&self) -> ICursor<'tx, T, Self>;

  fn bucket(&self, name: &[u8]) -> Self;

  fn get(&self, key: &[u8]) -> &'tx [u8];

  fn sequence(&self) -> u64;

  fn for_each<F: Fn(&[u8]) -> crate::Result<()>>(&self, f: F) -> crate::Result<()>;

  fn for_each_bucket<F: Fn(&[u8]) -> crate::Result<()>>(&self, f: F) -> crate::Result<()>;

  fn status(&self) -> BucketStats;
}

pub trait BucketMutAPI<'tx>: BucketAPI<'tx, TxMut<'tx>> {
  fn create_bucket(&mut self, key: &[u8]) -> crate::Result<Self>;

  fn create_bucket_if_not_exists(&mut self, key: &[u8]) -> crate::Result<Self>;

  fn cursor_mut(&self) -> CursorMut<'tx>;

  fn delete_bucket(&mut self, key: &[u8]) -> crate::Result<()>;

  fn put(&mut self, key: &[u8], data: &[u8]) -> crate::Result<()>;

  fn delete(&mut self, key: &[u8]) -> crate::Result<()>;

  fn set_sequence(&mut self, v: u64) -> crate::Result<()>;

  fn next_sequence(&mut self) -> crate::Result<u64>;
}

pub struct BucketR<'tx> {
  pub(crate) bucket_header: InBucket,
  pub(crate) inline_page: Option<RefPage<'tx>>,
  p: PhantomData<&'tx u8>,
}

impl<'tx> BucketR<'tx> {
  pub fn new(in_bucket: InBucket) -> BucketR<'tx> {
    BucketR {
      bucket_header: in_bucket,
      inline_page: None,
      p: Default::default(),
    }
  }
}

pub struct BucketP<'tx, T: TxIAPI<'tx>, B: BucketIAPI<'tx, T>> {
  root_node: Option<NodeMut<'tx>>,
  buckets: HashMap<'tx, &'tx [u8], B>,
  nodes: HashMap<'tx, PgId, NodeMut<'tx>>,
  fill_percent: f64,
  phantom_t: PhantomData<T>,
}

impl<'tx, T: TxIAPI<'tx>, B: BucketIAPI<'tx, T>> BucketP<'tx, T, B> {
  pub fn new_in(bump: &'tx Bump) -> BucketP<'tx, T, B> {
    BucketP {
      root_node: None,
      buckets: HashMap::new_in(bump),
      nodes: HashMap::new_in(bump),
      fill_percent: DEFAULT_FILL_PERCENT,
      phantom_t: PhantomData,
    }
  }
}

pub type BucketW<'tx> = BucketP<'tx, TxMut<'tx>, BucketMut<'tx>>;

pub struct BucketRW<'tx> {
  r: BucketR<'tx>,
  w: BucketW<'tx>,
}

impl<'tx> BucketRW<'tx> {
  pub fn new_in(bump: &'tx Bump, in_bucket: InBucket) -> BucketRW<'tx> {
    BucketRW {
      r: BucketR::new(in_bucket),
      w: BucketW::new_in(bump),
    }
  }
}

#[derive(Copy, Clone)]
pub struct Bucket<'tx> {
  tx: &'tx Tx<'tx>,
  cell: SCell<'tx, BucketR<'tx>>,
}

impl<'tx> BucketIAPI<'tx, Tx<'tx>> for Bucket<'tx> {
  fn new(bucket_header: InBucket, tx: &'tx Tx<'tx>, inline_page: Option<RefPage<'tx>>) -> Self {
    let r = BucketR {
      bucket_header,
      inline_page,
      p: Default::default(),
    };

    Bucket {
      tx,
      cell: SCell::new_in(r, tx.bump()),
    }
  }

  #[inline(always)]
  fn is_writeable(&self) -> bool {
    false
  }

  #[inline(always)]
  fn api_tx(self) -> &'tx Tx<'tx> {
    &self.tx
  }
}

impl<'tx> IRef<BucketR<'tx>, BucketP<'tx, Tx<'tx>, Bucket<'tx>>> for Bucket<'tx> {
  fn borrow_iref(
    &self,
  ) -> (
    Ref<BucketR<'tx>>,
    Option<Ref<BucketP<'tx, Tx<'tx>, Bucket<'tx>>>>,
  ) {
    (self.cell.borrow(), None)
  }

  fn borrow_mut_iref(
    &self,
  ) -> (
    RefMut<BucketR<'tx>>,
    Option<RefMut<BucketP<'tx, Tx<'tx>, Bucket<'tx>>>>,
  ) {
    (self.cell.borrow_mut(), None)
  }
}

impl<'tx> BucketAPI<'tx, Tx<'tx>> for Bucket<'tx> {
  fn root(&self) -> PgId {
    todo!()
  }

  fn writeable(&self) -> bool {
    todo!()
  }

  fn cursor(&self) -> Cursor<'tx, Tx<'tx>> {
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

  fn for_each<F: Fn(&[u8]) -> crate::Result<()>>(&self, f: F) -> crate::Result<()> {
    todo!()
  }

  fn for_each_bucket<F: Fn(&[u8]) -> crate::Result<()>>(&self, f: F) -> crate::Result<()> {
    todo!()
  }

  fn status(&self) -> BucketStats {
    todo!()
  }
}

#[derive(Copy, Clone)]
pub struct BucketMut<'tx> {
  tx: &'tx TxMut<'tx>,
  cell: SCell<'tx, BucketRW<'tx>>,
}

impl<'tx> IRef<BucketR<'tx>, BucketW<'tx>> for BucketMut<'tx> {
  fn borrow_iref(&self) -> (Ref<BucketR<'tx>>, Option<Ref<BucketW<'tx>>>) {
    let (r, w) = Ref::map_split(self.cell.borrow(), |b| (&b.r, &b.w));
    (r, Some(w))
  }

  fn borrow_mut_iref(&self) -> (RefMut<BucketR<'tx>>, Option<RefMut<BucketW<'tx>>>) {
    let (r, w) = RefMut::map_split(self.cell.borrow_mut(), |b| (&mut b.r, &mut b.w));
    (r, Some(w))
  }
}

impl<'tx> BucketIAPI<'tx, TxMut<'tx>> for BucketMut<'tx> {
  fn new(bucket_header: InBucket, tx: &'tx TxMut<'tx>, inline_page: Option<RefPage<'tx>>) -> Self {
    let r = BucketR {
      bucket_header,
      inline_page,
      p: Default::default(),
    };

    let bump = tx.bump();
    let w = BucketW::new_in(bump);

    BucketMut {
      tx,
      cell: SCell::new_in(BucketRW { r, w }, bump),
    }
  }

  #[inline(always)]
  fn is_writeable(&self) -> bool {
    true
  }

  fn api_tx(self) -> &'tx TxMut<'tx> {
    &self.tx
  }
}

impl<'tx> BucketMutIAPI<'tx> for BucketMut<'tx> {
  fn api_create_bucket(self, key: &[u8]) -> crate::Result<Self> {
    if key.is_empty() {
      return Err(BucketNameRequired);
    }
    let mut c = self.i_cursor();

    if let Some((k, _, flags)) = c.i_seek(key) {
      if k == key {
        if flags & BUCKET_LEAF_FLAG != 0 {
          return Err(BucketExists);
        }
        return Err(IncompatibleValue);
      }
    }

    let mut inline_page = InlinePage::default();
    inline_page.page.set_leaf();
    let layout = Layout::from_size_align(INLINE_PAGE_SIZE, INLINE_PAGE_ALIGNMENT).unwrap();
    let bump = self.api_tx().bump();
    let value = unsafe {
      let mut data = bump.alloc_layout(layout);
      &mut *slice_from_raw_parts_mut(data.as_mut() as *mut u8, INLINE_PAGE_SIZE)
    };
    value.copy_from_slice(bytemuck::bytes_of(&inline_page));
    let key = bump.alloc_slice_clone(key) as &[u8];

    c.node().put(key, key, value, ZERO_PGID, BUCKET_LEAF_FLAG);

    self.borrow_mut_iref().0.inline_page = None;

    return Ok(self.api_bucket(key).unwrap());
  }

  fn api_create_bucket_if_not_exists(self, key: &[u8]) -> crate::Result<Self> {
    match self.api_create_bucket(key) {
      Ok(child) => Ok(child),
      Err(error) => {
        if error == BucketExists {
          return Ok(self.api_bucket(key).unwrap());
        } else {
          return Err(error);
        }
      }
    }
  }

  fn api_delete_bucket(self, key: &[u8]) -> crate::Result<()> {
    let mut c = self.i_cursor();

    let (k, _, flags) = c.i_seek(key).unwrap();
    if key != k {
      return Err(BucketNotFound);
    } else if flags & BUCKET_LEAF_FLAG != 0 {
      return Err(IncompatibleValue);
    }

    let child = self.api_bucket(key).unwrap();
    child.api_for_each_bucket(|k| {
      match self.api_delete_bucket(k) {
        Ok(_) => Ok(()),
        // TODO: Ideally we want to properly chain errors here
        Err(e) => Err(Error::Other(e.into())),
      }
    })?;

    if let Some(mut w) = self.borrow_mut_iref().1 {
      w.buckets.remove(key);
    }

    if let Some(mut w) = child.borrow_mut_iref().1 {
      w.nodes.clear();
      w.root_node = None;
    }
    child.free();

    c.node().del(key);
    Ok(())
  }

  fn api_put(self, key: &[u8], value: &[u8]) -> crate::Result<()> {
    if key.is_empty() {
      return Err(KeyRequired);
    } else if key.len() > MAX_KEY_SIZE as usize {
      return Err(KeyTooLarge);
    } else if value.len() > MAX_VALUE_SIZE as usize {
      return Err(ValueTooLarge);
    }
    let mut c = self.i_cursor();
    let (k, _, flags) = c.i_seek(key).unwrap();

    if (flags & BUCKET_LEAF_FLAG) != 0 || key != k {
      return Err(IncompatibleValue);
    }
    let bump = self.api_tx().bump();
    let key = &*bump.alloc_slice_clone(key);
    let value = &*bump.alloc_slice_clone(value);
    c.node().put(key, key, value, ZERO_PGID, 0);
    Ok(())
  }

  fn api_delete(self, key: &[u8]) -> crate::Result<()> {
    let mut c = self.i_cursor();
    let (k, _, flags) = c.i_seek(key).unwrap();

    if key != k {
      return Ok(());
    }

    if flags & BUCKET_LEAF_FLAG != 0 {
      return Err(IncompatibleValue);
    }

    c.node().del(key);

    Ok(())
  }

  fn api_set_sequence(cell: BucketMut<'tx>, v: u64) -> crate::Result<()> {
    // TODO: Since this is repeated a bunch, let materialize root in a single function
    let mut materialize_root = None;
    if let (r, Some(w)) = cell.borrow_iref() {
      materialize_root = match w.root_node {
        None => Some(r.bucket_header.root()),
        Some(_) => None,
      }
    }

    materialize_root.and_then(|root| Some(Self::node(cell, root, None)));

    cell.borrow_mut_iref().0.bucket_header.set_sequence(v);
    Ok(())
  }

  fn api_next_sequence(cell: BucketMut<'tx>) -> crate::Result<u64> {
    // TODO: Since this is repeated a bunch, let materialize root in a single function
    let mut materialize_root = None;
    if let (r, Some(w)) = cell.borrow_iref() {
      materialize_root = match w.root_node {
        None => Some(r.bucket_header.root()),
        Some(_) => None,
      }
    }
    materialize_root.and_then(|root| Some(Self::node(cell, root, None)));

    let mut r = cell.borrow_mut_iref().0;
    r.bucket_header.inc_sequence();
    Ok(r.bucket_header.sequence())
  }

  fn free(self) {
    if self.borrow_iref().0.bucket_header.root() == ZERO_PGID {
      return;
    }

    let txid = self.api_tx().meta().txid();

    self.for_each_page_node(|pn, depth| match pn {
      Either::Left(page) => self.api_tx().freelist().free(txid, page),
      Either::Right(node) => node.free(),
    });
  }

  fn spill(self, bump: &'tx Bump) -> crate::Result<()> {
    // To keep with our rules we much copy the bucket entries to temporary storage first
    // This should be unnecessary, but working first *then* optimize
    let v = {
      let bucket_mut = self.borrow_iref();
      let w = bucket_mut.1.unwrap();
      let mut v = BVec::with_capacity_in(w.buckets.len(), bump);
      // v.extend() would be more idiomatic, but I'm too tired atm to figure out why
      // it's not working
      w.buckets.iter().for_each(|(k, b)| {
        v.push((*k, *b));
      });
      v
    };

    for (name, child) in v.into_iter() {}

    Ok(())
  }

  /// inlineable returns true if a bucket is small enough to be written inline
  /// and if it contains no subbuckets. Otherwise returns false.
  fn inlineable(self) -> bool {
    let b = self.borrow_iref();
    let w = b.1.unwrap();

    // Bucket must only contain a single leaf node.
    let n = match w.root_node {
      None => return false,
      Some(n) => n,
    };
    let node_ref = n.cell.borrow();
    if node_ref.is_leaf {
      return false;
    }

    // Bucket is not inlineable if it contains subbuckets or if it goes beyond
    // our threshold for inline bucket size.
    let mut size = PAGE_HEADER_SIZE;
    for inode in &node_ref.inodes {
      size += LEAF_PAGE_ELEMENT_SIZE + inode.key().len() + inode.value().len();

      if inode.flags() & BUCKET_LEAF_FLAG != 0 {
        return false;
      } else if size > self.max_inline_bucket_size() {
        return false;
      }
    }

    true
  }

  fn own_in(self) {
    let bump = self.api_tx().bump();
    let (root, children) = {
      let (r, w) = self.borrow_iref();
      let wb = w.unwrap();
      let mut children: BVec<BucketMut<'tx>> = BVec::with_capacity_in(wb.buckets.len(), bump);
      children.extend(wb.buckets.values());
      (wb.root_node, children)
    };

    if let Some(node) = root {
      node.root().own_in(bump);
    }

    for child in children.into_iter() {
      child.own_in();
    }
  }

  fn node(self, pgid: PgId, parent: Option<NodeMut<'tx>>) -> NodeMut<'tx> {
    let inline_page = {
      let (r, w) = self.borrow_mut_iref();
      let wb = w.unwrap();

      if let Some(n) = wb.nodes.get(&pgid) {
        return *n;
      }
      r.inline_page
    };

    let page = match inline_page {
      None => self.api_tx().page(pgid),
      Some(page) => page,
    };

    let n = NodeMut::read_in(self, parent, &page);
    let (r, w) = self.borrow_mut_iref();
    let mut wb = w.unwrap();
    wb.nodes.insert(pgid, n);
    n
  }
}

impl<'tx> BucketAPI<'tx, TxMut<'tx>> for BucketMut<'tx> {
  fn root(&self) -> PgId {
    todo!()
  }

  fn writeable(&self) -> bool {
    todo!()
  }

  fn cursor(&self) -> ICursor<'tx, TxMut<'tx>, Self> {
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

  fn for_each<F: Fn(&[u8]) -> crate::Result<()>>(&self, f: F) -> crate::Result<()> {
    todo!()
  }

  fn for_each_bucket<F: Fn(&[u8]) -> crate::Result<()>>(&self, f: F) -> crate::Result<()> {
    todo!()
  }

  fn status(&self) -> BucketStats {
    todo!()
  }
}
