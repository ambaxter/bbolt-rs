use crate::common::page::Page;
use crate::common::page::{CoerciblePage, FREE_LIST_PAGE_FLAG, PAGE_HEADER_SIZE};
use crate::common::utility::is_sorted;
use crate::common::{PgId, TxId};
use bytemuck::{bytes_of, Contiguous};
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use std::iter::{repeat, zip};
use std::marker::PhantomData;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::slice::{from_raw_parts, from_raw_parts_mut};

pub struct MappedFreeListPage {
  bytes: *mut u8,
  phantom: PhantomData<[u8]>,
}

impl MappedFreeListPage {
  pub unsafe fn new(bytes: *mut u8) -> MappedFreeListPage {
    MappedFreeListPage {
      bytes,
      phantom: PhantomData,
    }
  }

  pub fn init(bytes: *mut u8) -> MappedFreeListPage {
    let mut page = unsafe { Self::new(bytes) };
    page.set_free_list();
    page
  }
}

impl Deref for MappedFreeListPage {
  type Target = Page;

  fn deref(&self) -> &Self::Target {
    unsafe { &*(self.bytes as *const Page) }
  }
}

impl DerefMut for MappedFreeListPage {
  fn deref_mut(&mut self) -> &mut Self::Target {
    unsafe { &mut *(self.bytes as *mut Page) }
  }
}

impl CoerciblePage for MappedFreeListPage {
  #[inline]
  fn page_flag() -> u16 {
    FREE_LIST_PAGE_FLAG
  }

  fn own(bytes: *mut u8) -> Self {
    let mut page = unsafe { Self::new(bytes) };
    page.set_free_list();
    page
  }
}

impl MappedFreeListPage {
  pub(crate) fn page_count(&self) -> (u32, u32) {
    let mut idx = 0;
    let count = {
      if self.count == u16::MAX {
        idx = 1;
        let page_ptr = self.bytes;
        let count = unsafe {
          let count_ptr = page_ptr.add(PAGE_HEADER_SIZE).cast::<PgId>();
          *count_ptr
        };
        let count_64 = u64::from(count);
        if count_64 > u32::MAX_VALUE as u64 {
          panic!("leading element count {} overflows u32", count);
        }
        count_64 as u32
      } else {
        self.count as u32
      }
    };
    (idx, count)
  }

  pub(crate) fn page_ids(&self) -> &[PgId] {
    let (idx, count) = self.page_count();
    let page_ptr = self.bytes;
    unsafe {
      let data_ptr = page_ptr
        .add(PAGE_HEADER_SIZE)
        .add(mem::size_of::<PgId>() * idx as usize)
        .cast::<PgId>();
      from_raw_parts(data_ptr, count as usize)
    }
  }

  pub(crate) fn page_ids_mut(&mut self, count: u64) -> &mut [PgId] {
    let page_ptr = self.bytes;
    let data_ptr = unsafe { page_ptr.add(PAGE_HEADER_SIZE) };
    assert_eq!(
      data_ptr as usize % mem::align_of::<PgId>(),
      0,
      "Unaligned pointer: {:?}",
      data_ptr
    );

    if count == 0 {
      self.count = 0;
      &mut []
    } else if count < u16::MAX as u64 {
      self.count = count as u16;
      unsafe { from_raw_parts_mut(data_ptr as *mut PgId, count as usize) }
    } else {
      let data = unsafe { from_raw_parts_mut(data_ptr as *mut PgId, count as usize + 1) };
      data[0] = count.into();
      data.split_at_mut(1).1
    }
  }
}

#[derive(Debug)]
pub struct Freelist {
  ids: Vec<PgId>,
  allocs: HashMap<PgId, TxId>,
  pub pending: HashMap<TxId, TxPending>,
  cache: HashSet<PgId>,
  free_maps: HashMap<u64, HashSet<PgId>>,
  forward_map: HashMap<PgId, u64>,
  backward_map: HashMap<PgId, u64>,
}

#[derive(Clone, Debug)]
pub struct TxPending {
  pub ids: Vec<PgId>,
  pub alloc_tx: Vec<TxId>,
  pub last_release_begin: TxId,
}

impl<'tx> TxPending {
  pub(crate) fn new() -> TxPending {
    TxPending {
      ids: Vec::new(),
      alloc_tx: Vec::new(),
      last_release_begin: Default::default(),
    }
  }
}

pub(crate) fn merge_pids(dst: &mut [PgId], a: &[PgId], b: &[PgId]) {
  for (i, &id) in [a, b].into_iter().kmerge().enumerate() {
    dst[i] = id;
  }
}

impl Freelist {
  pub(crate) fn new() -> Self {
    Freelist {
      ids: Vec::new(),
      allocs: HashMap::new(),
      pending: HashMap::new(),
      cache: HashSet::new(),
      free_maps: HashMap::new(),
      forward_map: HashMap::new(),
      backward_map: HashMap::new(),
    }
  }

  /// returns count of free pages
  pub(crate) fn free_count(&self) -> u64 {
    self.forward_map.values().sum()
  }

  /// pending_count returns count of pending pages
  pub(crate) fn pending_count(&self) -> u64 {
    self.pending.values().map(|txp| txp.ids.len() as u64).sum()
  }

  /// count returns count of pages on the freelist
  pub(crate) fn count(&self) -> u64 {
    self.free_count() + self.pending_count()
  }

  /// returns the sorted free page ids
  pub(crate) fn free_page_ids(&self) -> Vec<PgId> {
    let count = self.free_count();
    let mut m = Vec::with_capacity(count as usize);
    if count > 0 {
      for (&start, &size) in &self.forward_map {
        for i in 0..size {
          m.push(start + i)
        }
      }
      m.sort();
    }
    m
  }

  /// copy_all copies a list of all free ids and all pending ids in one sorted list.
  pub(crate) fn copy_all(&self, dst: &mut [PgId]) {
    let mut pending_ids = Vec::with_capacity(self.pending_count() as usize);
    self
      .pending
      .values()
      .map(|txp| &txp.ids)
      .for_each(|ids| pending_ids.extend_from_slice(ids));
    pending_ids.sort();
    let free_page_ids = self.free_page_ids();
    merge_pids(dst, &free_page_ids, &pending_ids);
  }

  pub(crate) fn del_span(&mut self, start: PgId, size: u64) {
    self.forward_map.remove(&start);
    self.backward_map.remove(&(start + (size - 1)));
    let rm_free_entry = {
      if let Some(pids) = self.free_maps.get_mut(&size) {
        pids.remove(&start);
        pids.is_empty()
      } else {
        false
      }
    };
    if rm_free_entry {
      self.free_maps.remove(&size);
    }
  }

  pub(crate) fn add_span(&mut self, start: PgId, size: u64) {
    self.backward_map.insert(start - 1 + size, size);
    self.forward_map.insert(start, size);
    self
      .free_maps
      .entry(size)
      .or_insert_with(|| HashSet::new())
      .insert(start);
  }

  pub(crate) fn allocate(&mut self, txid: TxId, n: u64) -> Option<PgId> {
    if n == 0 {
      return None;
    }
    if let Some(pgid) = self
      .free_maps
      .get(&n)
      .iter()
      .flat_map(|set| set.iter())
      .copied()
      .next()
    {
      self.del_span(pgid, n);
      self.allocs.insert(pgid, txid);
      for i in 0..n {
        self.cache.remove(&(pgid + i));
      }
      return Some(pgid);
    }
    if let Some((size, pgid)) = self
      .free_maps
      .iter()
      .filter(|(&size, _)| size > n)
      .flat_map(|(&size, pgids)| zip(repeat(size), pgids.iter().copied()))
      .next()
    {
      self.del_span(pgid, size);
      self.allocs.insert(pgid, txid);
      let remain = size - n;

      // add remain span
      self.add_span(pgid + n, remain);
      for i in 0..n {
        self.cache.remove(&(pgid + i));
      }
      return Some(pgid);
    }
    None
  }

  /// free releases a page and its overflow for a given transaction id.
  /// If the page is already free then a panic will occur.
  pub(crate) fn free(&mut self, txid: TxId, p: &Page) {
    assert!(u64::from(p.id) > 1, "Cannot free page 0 or 1: {}", p.id);

    // Free page and all its overflow pages.
    let txp = self.pending.entry(txid).or_insert_with(|| TxPending::new());
    let alloc_tx_id = {
      if let Some(allocs_tx_id) = self.allocs.remove(&p.id) {
        allocs_tx_id
      } else if p.is_free_list() {
        // Freelist is always allocated by prior tx.
        txid - 1
      } else {
        0.into()
      }
    };

    for id in p.id.0..=p.id.0 + p.overflow as u64 {
      let pgid_i = PgId(id);
      // Verify that page is not already free.
      if self.cache.contains(&pgid_i) {
        panic!("Page {} already freed", pgid_i);
      }
      // Add to the freelist and cache.
      txp.ids.push(pgid_i);
      txp.alloc_tx.push(alloc_tx_id);
      self.cache.insert(pgid_i);
    }
  }

  /// merges pid to the existing free spans, try to merge it backward and forward
  pub(crate) fn merge_with_existing_span(&mut self, pid: PgId) {
    let prev = pid - 1;
    let next = pid + 1;

    let mut new_start = pid;
    let mut new_size = 1;

    if let Some(pre_size) = self.backward_map.get(&prev).cloned() {
      let start = prev + 1 - pre_size;
      self.del_span(start, pre_size);
      new_start -= pre_size;
      new_size += pre_size;
    }

    if let Some(next_size) = self.forward_map.get(&next).cloned() {
      self.del_span(next, next_size);
      new_size += next_size;
    }

    self.add_span(new_start, new_size);
  }

  /// try to merge list of pages(represented by pgids) with existing spans
  pub(crate) fn merge_spans(&mut self, pgids: Vec<PgId>) {
    for pgid in pgids {
      self.merge_with_existing_span(pgid);
    }
  }

  /// release moves all page ids for a transaction id (or older) to the freelist.
  pub(crate) fn release(&mut self, txid: TxId) {
    let mut m = Vec::with_capacity(0);
    self.pending.retain(|&tid, txp| {
      if tid <= txid {
        m.extend_from_slice(&txp.ids);
        false
      } else {
        true
      }
    });
    self.merge_spans(m);
  }

  /// releaseRange moves pending pages allocated within an extent \[begin,end\] to the free list.
  pub(crate) fn release_range(&mut self, begin: TxId, end: TxId) {
    if begin > end {
      return;
    }
    let mut m = Vec::new();
    self.pending.retain(|&tid, txp| {
      if tid < begin || tid > end {
        return true;
      }
      // Don't recompute freed pages if ranges haven't updated.
      if txp.last_release_begin == begin {
        return true;
      }
      let mut i = 0;
      while i < txp.ids.len() {
        if let Some(&atx) = txp.alloc_tx.get(i) {
          if atx < begin || atx > end {
            i += 1;
            continue;
          }
          let txid = txp.ids.swap_remove(i);
          m.push(txid);
          txp.alloc_tx.swap_remove(i);
        }
      }
      txp.last_release_begin = begin;
      !txp.ids.is_empty()
    });
    self.merge_spans(m);
  }

  /// rollback removes the pages from a given pending tx.
  pub(crate) fn rollback(&mut self, txid: TxId) {
    // Remove page ids from cache.
    if let Some(txp) = self.pending.remove(&txid) {
      let mut m = Vec::new();
      for (i, &pgid) in txp.ids.iter().enumerate() {
        self.cache.remove(&pgid);
        if let Some(&tx) = txp.alloc_tx.get(i) {
          if tx != txid {
            // Pending free aborted; restore page back to alloc list.
            self.allocs.insert(pgid, tx);
          } else {
            // Freed page was allocated by this txn; OK to throw away.
            m.push(pgid);
          }
        }
      }
      // Remove pages from pending list and mark as free if allocated by txid.
      self.merge_spans(m);
    }
  }

  /// freed returns whether a given page is in the free list.
  pub(crate) fn freed(&self, pgid: PgId) -> bool {
    self.cache.contains(&pgid)
  }

  pub(crate) fn reindex(&mut self) {
    let ids = self.free_page_ids();
    let pending_count: usize = self
      .pending
      .values()
      .map(|tx_pending| tx_pending.ids.len())
      .sum();
    self.cache.clear();
    self.cache.reserve(ids.len() + pending_count);
    for pgid in ids.into_iter() {
      self.cache.insert(pgid);
    }
    for tx_pending in self.pending.values() {
      for &pgid in &tx_pending.ids {
        self.cache.insert(pgid);
      }
    }
  }

  /// read initializes the freelist from a freelist page.
  pub(crate) fn read(&mut self, page: &MappedFreeListPage) {
    // Copy the list of page ids from the freelist.
    self.ids.clear();
    let data = page.page_ids();
    self.read_ids(data);
  }

  /// write writes the page ids onto a freelist page. All free and pending ids are
  /// saved to disk since in the event of a program crash, all pending ids will
  /// become free.
  pub(crate) fn write(&self, page: &mut MappedFreeListPage) {
    // Combine the old free pgids and pgids waiting on an open transaction.
    let data = page.page_ids_mut(self.count());
    if data.len() > 0 {
      self.copy_all(data);
    }
  }

  pub(crate) fn read_ids(&mut self, ids: &[PgId]) {
    self.init(ids);
    self.reindex();
  }

  pub(crate) fn init(&mut self, ids: &[PgId]) {
    if ids.is_empty() {
      return;
    }
    let mut size: u64 = 1;
    let mut start = ids[0];
    if !is_sorted(ids) {
      panic!("pgids not sorted");
    }
    self.free_maps.clear();
    self.forward_map.clear();
    self.backward_map.clear();

    for i in 1..ids.len() {
      if ids[i] == ids[i - 1] + 1 {
        size += 1;
      } else {
        self.add_span(start, size);
        size = 1;
        start = ids[i];
      }
    }

    if size != 0 && u64::from(start) != 0 {
      self.add_span(start, size);
    }
  }

  //TODO: reload
  pub(crate) fn reload(&mut self, header: &Page) {
    todo!()
  }

  /// size returns the size of the page after serialization.
  pub(crate) fn size(&self) -> u64 {
    let mut n = self.count();
    if n >= 0xFFFF {
      n += 1;
    }

    PAGE_HEADER_SIZE as u64 + (mem::size_of::<PgId>() as u64 * n)
  }
}

#[cfg(test)]
mod tests {
  use crate::common::page::Page;
  use crate::common::{PgId, TxId};
  use crate::freelist::{Freelist, MappedFreeListPage, TxPending};
  use crate::test_support::mapped_page;
  use aligners::{alignment, AlignedBytes};
  use bumpalo::Bump;
  use itertools::Itertools;
  use std::collections::{HashMap, HashSet};
  use std::default::Default;
  use std::hash::Hash;

  const fn pg(id: u64) -> PgId {
    PgId(id)
  }

  const fn tx(id: u64) -> TxId {
    TxId(id)
  }

  fn hashset(ids: &[u64]) -> HashSet<PgId> {
    let mut set = HashSet::new();
    set.extend(ids.iter().map(|i| pg(*i)));
    set
  }

  #[test]
  // Ensure that a page is added to a transaction's freelist.
  fn freelist_free() {
    let mut f = Freelist::new();
    let p = Page {
      id: pg(12),
      ..Default::default()
    };
    f.free(tx(100), &p);
    assert_eq!(&[12], &f.pending.get(&tx(100)).unwrap().ids.as_slice())
  }

  #[test]
  // Ensure that a page and its overflow is added to a transaction's freelist.
  fn freelist_free_overflow() {
    let mut f = Freelist::new();
    let p = Page {
      id: pg(12),
      overflow: 3,
      ..Default::default()
    };
    f.free(tx(100), &p);
    assert_eq!(
      &[12, 13, 14, 15],
      &f.pending.get(&tx(100)).unwrap().ids.as_slice()
    )
  }

  #[test]
  // Ensure that a transaction's free pages can be released.
  fn freelist_release() {
    let mut f = Freelist::new();
    f.free(
      tx(100),
      &Page {
        id: pg(12),
        overflow: 1,
        ..Default::default()
      },
    );
    f.free(
      tx(100),
      &Page {
        id: pg(9),
        ..Default::default()
      },
    );
    f.free(
      tx(102),
      &Page {
        id: pg(39),
        ..Default::default()
      },
    );
    f.release(tx(100));
    f.release(tx(101));
    assert_eq!(&[9, 12, 13], f.free_page_ids().as_slice());
    f.release(tx(102));
    assert_eq!(&[9, 12, 13, 39], f.free_page_ids().as_slice());
  }

  #[test]
  fn freelist_release_range() {
    #[derive(Debug, Copy, Clone, Default)]
    struct TRange {
      b: TxId,
      e: TxId,
    }

    impl TRange {
      const fn new(b: u64, e: u64) -> TRange {
        TRange {
          b: TxId(b),
          e: TxId(e),
        }
      }
    }

    #[derive(Debug, Copy, Clone, Default)]
    struct TPage {
      id: PgId,
      n: u64,
      alloc_txn: TxId,
      free_txn: TxId,
    }

    impl TPage {
      const fn new(id: u64, n: u64, alloc_txn: u64, free_txn: u64) -> TPage {
        TPage {
          id: PgId(id),
          n,
          alloc_txn: TxId(alloc_txn),
          free_txn: TxId(free_txn),
        }
      }
    }

    #[derive(Debug, Clone, Default)]
    struct ReleaseRangeTest {
      title: &'static str,
      pages_in: Vec<TPage>,
      release_ranges: Vec<TRange>,
      want_free: Vec<PgId>,
    }

    let release_range_tests = [
      ReleaseRangeTest {
        title: "Single pending in range",
        pages_in: vec![TPage::new(3, 1, 100, 200)],
        release_ranges: vec![TRange::new(1, 300)],
        want_free: vec![pg(3)],
      },
      ReleaseRangeTest {
        title: "Single pending with minimum end range",
        pages_in: vec![TPage::new(3, 1, 100, 200)],
        release_ranges: vec![TRange::new(1, 200)],
        want_free: vec![pg(3)],
      },
      ReleaseRangeTest {
        title: "Single pending outsize minimum end range",
        pages_in: vec![TPage::new(3, 1, 100, 200)],
        release_ranges: vec![TRange::new(1, 199)],
        want_free: vec![],
      },
      ReleaseRangeTest {
        title: "Single pending with minimum begin range",
        pages_in: vec![TPage::new(3, 1, 100, 200)],
        release_ranges: vec![TRange::new(100, 200)],
        want_free: vec![pg(3)],
      },
      ReleaseRangeTest {
        title: "Single pending in minimum range",
        pages_in: vec![TPage::new(3, 1, 199, 200)],
        release_ranges: vec![TRange::new(199, 200)],
        want_free: vec![pg(3)],
      },
      ReleaseRangeTest {
        title: "Single pending and read transaction at 199",
        pages_in: vec![TPage::new(3, 1, 199, 200)],
        release_ranges: vec![TRange::new(100, 198), TRange::new(200, 300)],
        want_free: vec![],
      },
      ReleaseRangeTest {
        title: "Adjacent pending and read transactions at 199, 200",
        pages_in: vec![TPage::new(3, 1, 199, 200), TPage::new(4, 1, 200, 201)],
        release_ranges: vec![
          TRange::new(100, 198),
          TRange::new(200, 199),
          TRange::new(201, 300),
        ],
        want_free: vec![],
      },
      ReleaseRangeTest {
        title: "Out of order ranges",
        pages_in: vec![TPage::new(3, 1, 199, 200), TPage::new(4, 1, 200, 201)],
        release_ranges: vec![
          TRange::new(201, 199),
          TRange::new(201, 200),
          TRange::new(200, 200),
        ],
        want_free: vec![],
      },
      ReleaseRangeTest {
        title: "Multiple pending, read transaction at 150",
        pages_in: vec![
          TPage::new(3, 1, 100, 200),
          TPage::new(4, 1, 100, 125),
          TPage::new(5, 1, 125, 150),
          TPage::new(6, 1, 125, 175),
          TPage::new(7, 2, 150, 175),
          TPage::new(9, 2, 175, 200),
        ],
        release_ranges: vec![TRange::new(50, 149), TRange::new(151, 300)],
        want_free: vec![pg(4), pg(9), pg(10)],
      },
    ];
    let bump = Bump::new();

    for c in release_range_tests.iter() {
      let mut f = Freelist::new();
      let ids: Vec<PgId> = c
        .pages_in
        .iter()
        .flat_map(|p| u64::from(p.id)..(u64::from(p.id) + p.n))
        .map(|id| pg(id))
        .collect();
      f.read_ids(&ids);

      for p in &c.pages_in {
        f.allocate(p.alloc_txn, p.n);
      }
      for p in &c.pages_in {
        f.free(
          p.free_txn,
          &Page {
            id: p.id,
            overflow: p.n as u32 - 1,
            ..Default::default()
          },
        );
      }
      for r in &c.release_ranges {
        f.release_range(r.b, r.e);
      }

      assert_eq!(&c.want_free, &f.free_page_ids(), "name {};", c.title);
    }
  }

  #[test]
  fn freelist_allocate() {
    let mut f = Freelist::new();
    f.read_ids(
      &[3, 4, 5, 6, 7, 9, 12, 13, 18]
        .iter()
        .cloned()
        .map(pg)
        .collect_vec(),
    );

    f.allocate(tx(1), 3);
    assert_eq!(6, f.free_count());

    f.allocate(tx(1), 2);
    assert_eq!(4, f.free_count());

    f.allocate(tx(1), 1);
    assert_eq!(3, f.free_count());

    f.allocate(tx(1), 0);
    assert_eq!(3, f.free_count());
  }

  #[test]
  fn freelist_read() {
    let mut mapped_page = mapped_page::<MappedFreeListPage>(4096);
    mapped_page.set_free_list();
    mapped_page
      .page_ids_mut(2)
      .copy_from_slice(&[pg(23), pg(50)]);
    let mut f = Freelist::new();
    f.read(&mapped_page);
    assert_eq!(&[23, 50], f.free_page_ids().as_slice());
  }

  #[test]
  fn freelist_write() {
    let mut mapped_page = mapped_page::<MappedFreeListPage>(4096);
    let mut f = Freelist::new();
    f.read_ids(&[pg(12), pg(39)]);
    f.pending
      .entry(tx(100))
      .or_insert_with(|| TxPending::new())
      .ids
      .extend_from_slice(&[pg(28), pg(11)]);
    f.pending
      .entry(tx(101))
      .or_insert_with(|| TxPending::new())
      .ids
      .extend_from_slice(&[pg(3)]);

    f.write(&mut mapped_page);

    let mut f2 = Freelist::new();
    f2.read(&mapped_page);
    assert_eq!(&[3, 11, 12, 28, 39], f2.free_page_ids().as_slice());
  }

  #[test]
  fn freelist_read_ids_and_free_page_ids() {
    let mut f = Freelist::new();
    let exp = [3, 4, 5, 6, 7, 9, 12, 13, 18]
      .iter()
      .cloned()
      .map(pg)
      .collect_vec();
    f.read_ids(&exp);

    assert_eq!(exp, f.free_page_ids().as_slice());

    let mut f2 = Freelist::new();
    let exp2 = &[];
    f2.read_ids(exp2);
    assert_eq!(exp2, f2.free_page_ids().as_slice());
  }
  /*
  // TODO: merge with exist
  #[test]
  fn freelist_merge_with_exist() {
    let bm1: HashSet<PgId> = hash! {1};
    let bm2: HashSet<PgId> = hash! {5};

    struct MergeTest {
      name: &'static str,
      ids: &'static [PgId],
      pgid: PgId,
      want: &'static [PgId],
      want_forward_map: HashMap<PgId, u64>,
      want_backward_map: HashMap<PgId, u64>,
      want_free_map: HashMap<u64, HashSet<PgId>>,
    }

    let tests = [
      MergeTest {
        name: "test1",
        ids: &[1, 2, 4, 5, 6],
        pgid: 3,
        want: &[1, 2, 3, 4, 5, 6],
        want_forward_map: hash! { 1 => 6},
        want_backward_map: hash! { 6 => 6},
        want_free_map: hash! {6 => bm1.clone()},
      },
      MergeTest {
        name: "test2",
        ids: &[1, 2, 5, 6],
        pgid: 3,
        want: &[1, 2, 3, 5, 6],
        want_forward_map: hash! { 1 => 3, 5 => 2},
        want_backward_map: hash! { 6 => 2, 3 => 3},
        want_free_map: hash! {3 => bm1.clone(), 2 => bm2.clone()},
      },
      MergeTest {
        name: "test3",
        ids: &[1, 2],
        pgid: 3,
        want: &[1, 2, 3],
        want_forward_map: hash! { 1 => 3},
        want_backward_map: hash! { 3 => 3},
        want_free_map: hash! {3 => bm1.clone() },
      },
      MergeTest {
        name: "test4",
        ids: &[2, 3],
        pgid: 1,
        want: &[1, 2, 3],
        want_forward_map: hash! { 1 => 3},
        want_backward_map: hash! { 3 => 3},
        want_free_map: hash! {3 => bm1.clone() },
      },
    ];

    for tt in &tests {
      let mut f = Freelist::new();
      f.read_ids(tt.ids);

      f.merge_with_existing_span(tt.pgid);
      assert_eq!(tt.want, f.free_page_ids().as_slice(), "name {};", tt.name);

      assert_eq!(tt.want_forward_map, f.forward_map, "name {};", tt.name,);

      assert_eq!(tt.want_backward_map, f.backward_map, "name {};", tt.name);

      assert_eq!(tt.want_free_map, f.free_maps, "name {};", tt.name);
    }
  }*/
}
