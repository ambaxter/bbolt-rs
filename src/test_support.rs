use crate::common::meta::MappedMetaPage;
use crate::common::selfowned::SelfOwned;
use std::mem;
//use crate::freelist::MappedFreeListPage;
use crate::common::page::{CoerciblePage, MutPage};
use crate::common::tree::{MappedBranchPage, MappedLeafPage};
use aligners::{alignment, AlignedBytes};

pub(crate) fn mapped_page<T: CoerciblePage + Sized>(
  bytes: usize,
) -> SelfOwned<AlignedBytes<alignment::Page>, T> {
  SelfOwned::new_with_map(
    AlignedBytes::<alignment::Page>::new_zeroed(bytes),
    |aligned_bytes| T::own(aligned_bytes.as_mut_ptr()),
  )
}

/*pub(crate) fn mapped_free_list() -> POwned<AlignedBytes<alignment::Page>, MappedFreeListPage<4096>>
{
  POwned::new_with_map(AlignedBytes::<alignment::Page>::new_zeroed(4096), |bytes| {
    MappedFreeListPage::init(bytes.as_mut_ptr())
  })
}
*/
