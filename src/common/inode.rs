use crate::common::memory::{CodSlice, PhantomUnsend};
use crate::common::page::{CoerciblePage, MutPage, RefPage};
use crate::common::tree::{
  BranchElementRef, LeafElementRef, MappedBranchPage, MappedLeafPage, TreePage,
};
use crate::common::PgId;
use bumpalo::collections::Vec as BVec;
use bumpalo::Bump;
use std::marker::PhantomData;
use std::ops::Deref;

#[derive(Debug)]
pub struct INode<'tx> {
  flags: u32,
  pgid: PgId,
  key: CodSlice<'tx, u8>,
  value: CodSlice<'tx, u8>,
  unsend: PhantomUnsend,
}

impl<'tx> INode<'tx> {
  pub fn default_in(bump: &'tx Bump) -> INode<'tx> {
    INode {
      flags: 0,
      pgid: 0.into(),
      key: CodSlice::default_in(bump),
      value: CodSlice::default_in(bump),
      unsend: PhantomData,
    }
  }

  pub fn new_mapped_in(flags: u32, pgid: PgId, key: &'tx [u8], value: &'tx [u8]) -> INode<'tx> {
    INode {
      flags,
      pgid,
      key: CodSlice::Mapped(key),
      value: CodSlice::Mapped(value),
      unsend: PhantomData,
    }
  }

  pub fn new_owned_in<R: AsRef<[u8]>>(
    flags: u32, pgid: PgId, key: R, value: R, bump: &'tx Bump,
  ) -> INode<'tx> {
    INode {
      flags,
      pgid,
      key: CodSlice::Owned(bump.alloc_slice_copy(&key.as_ref())),
      value: CodSlice::Owned(bump.alloc_slice_copy(&value.as_ref())),
      unsend: PhantomData,
    }
  }

  pub fn from_leaf_in(elem: LeafElementRef<'tx>) -> INode<'tx> {
    assert!(!elem.key().is_empty(), "read: zero-length inode key");
    let inode = INode::new_mapped_in(elem.flags(), 0.into(), elem.key(), elem.value());

    inode
  }

  pub fn from_branch_in(elem: BranchElementRef<'tx>) -> INode<'tx> {
    assert!(!elem.key().is_empty(), "read: zero-length inode key");
    let inode = INode::new_mapped_in(0, elem.pgid(), elem.key(), &[]);
    inode
  }

  // WARNING! MAYBE UNSOUND/UNSAFE
  //TODO: I am not sure why I need to transmute. If deref() provides a 'tx lifetime, why is
  // Rust convinced is actually has a lifetime of 'a?
  #[inline]
  pub fn key<'a>(&'a self) -> &'tx [u8] {
    unsafe { std::mem::transmute(self.key.deref()) }
  }

  #[inline]
  pub fn value<'a>(&'a self) -> &'tx [u8] {
    unsafe { std::mem::transmute(self.value.deref()) }
  }

  #[inline]
  pub fn flags(&self) -> u32 {
    self.flags
  }

  #[inline]
  pub fn pgid(&self) -> PgId {
    self.pgid
  }

  pub fn read_inodes_in(inodes: &mut BVec<'tx, INode<'tx>>, page: &RefPage<'tx>) {
    if let Some(leaf_page) = MappedLeafPage::coerce_ref(page) {
      let i = leaf_page.iter().map(|elem| INode::from_leaf_in(elem));
      inodes.extend(i);
    } else if let Some(branch_page) = MappedBranchPage::coerce_ref(page) {
      let i = branch_page.iter().map(|elem| INode::from_branch_in(elem));
      inodes.extend(i);
    }
  }

  pub fn write_inodes(inodes: &[INode<'tx>], page: &mut MutPage<'tx>) -> u32 {
    if let Some(leaf_page) = MappedLeafPage::coerce_mut(page) {
      leaf_page.write_elements(inodes)
    } else if let Some(branch_page) = MappedBranchPage::coerce_mut(page) {
      branch_page.write_elements(inodes)
    } else {
      panic!(
        "INodes::write_inodes - Unexpected page type: {:?}",
        page.flags
      );
    }
  }
}
