use crate::common::inode::INode;
use crate::common::memory::PhantomUnsend;
use crate::common::page::tree::TreePage;
use crate::common::page::{CoerciblePage, PageHeader, PAGE_HEADER_SIZE};
use crate::common::ZERO_PGID;
use crate::PgId;
use bytemuck::{Pod, Zeroable};
use getset::{CopyGetters, Setters};
use std::marker::PhantomData;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::slice::from_raw_parts;

pub const BRANCH_PAGE_ELEMENT_SIZE: usize = mem::size_of::<BranchPageElement>();

pub const BRANCH_PAGE_FLAG: u16 = 0x01;

pub struct MappedBranchPage {
  bytes: *mut u8,
  phantom: PhantomData<[u8]>,
}

impl MappedBranchPage {
  pub unsafe fn new(bytes: *mut u8) -> MappedBranchPage {
    MappedBranchPage {
      bytes,
      phantom: PhantomData,
    }
  }
}

impl Deref for MappedBranchPage {
  type Target = PageHeader;

  fn deref(&self) -> &Self::Target {
    unsafe { &*(self.bytes as *const PageHeader) }
  }
}

impl DerefMut for MappedBranchPage {
  fn deref_mut(&mut self) -> &mut Self::Target {
    unsafe { &mut *(self.bytes as *mut PageHeader) }
  }
}

impl CoerciblePage for MappedBranchPage {
  #[inline]
  fn page_flag() -> u16 {
    BRANCH_PAGE_FLAG
  }

  fn own(bytes: *mut u8) -> MappedBranchPage {
    let mut page = unsafe { Self::new(bytes) };
    page.set_branch();
    page
  }
}

///`BranchPageElement` represents the on-file layout of a branch page's element
///
/// `branchPageElement` in Go BBolt
#[repr(C)]
#[derive(Debug, Copy, Clone, Pod, Zeroable, CopyGetters, Setters)]
#[getset(get_copy = "pub", set = "pub")]
pub struct BranchPageElement {
  /// The distance from this element's pointer to its key location
  pos: u32,
  /// Key length
  key_size: u32,
  /// Page ID of this branch
  pgid: PgId,
  /// To implement !Send
  unsend: PhantomUnsend,
}

impl BranchPageElement {
  pub(crate) unsafe fn key(&self, page_ptr: *const u8) -> &[u8] {
    let elem_ptr = self as *const BranchPageElement as *const u8;
    let dist = usize::try_from(elem_ptr.offset_from(page_ptr)).unwrap_unchecked();
    let key_ptr = page_ptr.add(dist + self.pos as usize);
    from_raw_parts(key_ptr, self.key_size as usize)
  }
}

#[derive(Debug)]
pub struct BranchElementRef<'tx> {
  elem: &'tx BranchPageElement,
  key_ref: &'tx [u8],
  unsend: PhantomUnsend,
}

impl<'tx> BranchElementRef<'tx> {
  pub fn key(&self) -> &'tx [u8] {
    self.key_ref
  }
}

impl<'tx> Deref for BranchElementRef<'tx> {
  type Target = BranchPageElement;

  fn deref(&self) -> &Self::Target {
    self.elem
  }
}

impl<'tx> TreePage<'tx> for MappedBranchPage {
  type Elem = BranchPageElement;
  type ElemRef = BranchElementRef<'tx>;

  #[inline]
  unsafe fn page_ptr(&self) -> *mut u8 {
    self.bytes
  }

  #[inline]
  fn page_element_size(&self) -> usize {
    BRANCH_PAGE_ELEMENT_SIZE
  }

  fn get_elem(&self, i: u16) -> Option<Self::ElemRef> {
    if i >= self.count {
      None
    } else {
      unsafe {
        let elem_ptr = self
          .bytes
          .add(PAGE_HEADER_SIZE)
          .add(self.page_element_size() * i as usize);
        let elem = &*(elem_ptr as *const BranchPageElement);
        let key_ptr = elem_ptr.add(elem.pos as usize);
        let key_ref = from_raw_parts(key_ptr, elem.key_size as usize);
        Some(BranchElementRef {
          elem,
          key_ref,
          unsend: Default::default(),
        })
      }
    }
  }

  fn write_element(element: &mut Self::Elem, pos: u32, node: &INode) {
    assert_ne!(ZERO_PGID, node.pgid());
    element.set_pos(pos);
    element.set_pgid(node.pgid());
    element.set_key_size(node.key().len() as u32);
  }
}
