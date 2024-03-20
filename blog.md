```
 % ./target/release/bench -w seq -r seq
# Write 64.945474ms     (64.945µs/op)   (15397 op/sec)
# Read  1.000046609s    (24ns/op)       (41666666 op/sec)
```

With those two lines the last few months of labor bear fruit. I reimplemented the BBolt database in Rust. It is the culmination of my journey where I learned how to build a database, how to design database architecture in Rust, and how to use API design to remove footguns.

# Impetus

The project started last year when I wanted to learn how ETCD and the Raft algorithm worked which then lead me to learning how the BBolt database worked. Following in the software engineering tradition what started as curiosity turned into an all-consuming case of yak shaving. 

# Goals and Standards
* Built in stable Rust
* Library API as close to the original as possible
* On-file representation exactly matches original code 
* Manage transaction memory without Rc
* Unsafe behavior validated by Miri

# BBolt Under a Microscope
Under the covers BBolt is a minimalistic B+ tree database designed to store Buckets. Buckets may store key/value pairs in bytes and sub-Buckets. Sub-buckets may be stored inline inside a parent bucket or on its own page.  

The data types used to represent the BBolt database are relatively straightforward. 

```rust
/// The Page ID. Page address = `PgId` * meta.page_size
#[repr(C)]
pub struct PgId(u64);

/// The Transaction ID. Monotonic and incremented every commit
#[repr(C)]
pub struct TxId(u64);

/// `PageHeader` represents the on-file layout of a page header. 
/// 
/// `page` in Go BBolt
#[repr(C)]
pub struct PageHeader {
  /// This Page's ID
  id: PgId,
  /// Page's type. Branch(0x01), Leaf(0x02), Meta(0x04), or FreeList(0x10)
  flags: u16,
  /// Defines the number of items in the Branch, Leaf, and Freelist pages
  count: u16,
  /// How many additional meta.page_size pages are included in this page
  overflow: u32,
}

/// `BucketHeader` represents the on-file layout of a bucket header.
/// This is stored as the "value" of a bucket key. If the bucket is small enough,
/// then its root page can be stored inline in the "value", after the bucket
/// header. In the case of inline buckets, the "root" will be 0.
/// 
/// `bucket` in Go BBolt
#[repr(C)]
pub struct BucketHeader {
  /// Page ID of the bucket's root-level page
  root: PgId,
  /// monotonically incrementing, used by NextSequence()
  sequence: u64,
}

/// `Meta` represents the on-file layout of a database's metadata
/// 
/// `meta` in Go BBolt
#[repr(C)]
pub struct Meta {
  /// Uniquely ID for BBolt databases 
  magic: u32,
  /// Database version number
  version: u32,
  /// Database page size where page address = [PgId] * meta.page_size
  page_size: u32,
  flags: u32,
  /// Root bucket header
  root: BucketHeader,
  /// FreeList page location
  free_list: PgId,
  /// The end of the database where EOF = meta.pgid * meta.page_size
  pgid: PgId,
  /// Current transaction ID
  txid: TxId,
  /// Checksum of the previous Meta fields using the 64-bit version of the Fowler-Noll-Vo hash function
  checksum: u64,
}

/// `LeafPageElement` represents the on-file layout of a leaf page's element
/// 
/// `leafPageElement` in Go BBolt 
#[repr(C)]
pub struct LeafPageElement {
  /// Additional flag for each element. If leaf is a Bucket then 0x01 set
  flags: u32,
  /// The distance from this element's pointer to its key/value location
  pos: u32,
  /// Key length
  key_size: u32,
  /// Value length
  value_size: u32,
}

///`BranchPageElement` represents the on-file layout of a branch page's element
/// 
/// `branchPageElement` in Go BBolt
#[repr(C)]
pub struct BranchPageElement {
  /// The distance from this element's pointer to its key location
  pos: u32,
  /// Key length
  key_size: u32,
  /// Page ID of this branch's
  pgid: PgId,
}

```

The 4 database page types are also straightforward. 

Page Layouts
```
      Meta Page
      ┌──────┐
      │ Page │
      │Header│
      ├──────┤
      │ Meta │
      └──────┘


   Freelist Page
      ┌──────┐
      │ Page │
      │Header│
      ├──────┤     First element will be the FreeList
      │PgId 0│◀────   count if PageHeader.count ==
      ├──────┤                  U16::MAX
      │PgId 1│
      ├──────┤
      │PgId 2│
      ├──────┤
      │ .... │
      └──────┘

 Leaf Element Page
      ┌──────┐
      │ Page │
      │Header│
      ├──────┤
      │Elem 0│
      ├──────┤
      │Elem 1│       key ptr =
      ├──────┤  elem ptr + elem.pos
      │Elem 2│   ┌─── ┌───┐
      ├──────┤   │    │ k │
      │ .... │   │    │ e │
      ├──────┤───┘    │ y │
      │K/V 0 │        │ v │
      ├──────┤───┐    │ a │
      │K/V 1 │   │    │ l │
      ├──────┤   │    │ u │
      │K/V 2 │   │    │ e │
      ├──────┤   └─── └───┘
      │ .... │
      └──────┘

Branch Element Page
      ┌──────┐
      │ Page │
      │Header│
      ├──────┤
      │Elem 0│
      ├──────┤
      │Elem 1│
      ├──────┤
      │Elem 2│
      ├──────┤
      │ .... │
      ├──────┤           key ptr =
      │Key 0 │◀──── elem ptr + elem.pos
      ├──────┤
      │Key 1 │
      ├──────┤
      │Key 2 │
      ├──────┤
      │ .... │
      └──────┘

Created with Monodraw
```

# Foundations of Reading Mmapped Files
[memmap2](https://crates.io/crates/memmap2)
[Mmap](https://en.wikipedia.org/wiki/Mmap) [CreateFileMappingW](https://learn.microsoft.com/en-us/windows/win32/api/memoryapi/nf-memoryapi-createfilemappingw)

## Reading a Page

## Reading a Meta Page

```go
// meta returns a pointer to the metadata section of the page.
func (p *page) meta() *meta {
	return (*meta)(unsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p)))
}
```

## Reading INodes

```go
// leafPageElement retrieves the leaf node by index
func (p *page) leafPageElement(index uint16) *leafPageElement {
	return (*leafPageElement)(unsafeIndex(unsafe.Pointer(p), unsafe.Sizeof(*p),
		leafPageElementSize, int(index)))
}
```

```go
// key returns a byte slice of the node key.
func (n *leafPageElement) key() []byte {
	i := int(n.pos)
	j := i + int(n.ksize)
	return unsafeByteSlice(unsafe.Pointer(n), 0, i, j)
}
```

# Removing Footguns With API Design

```go
// Commit writes all changes to disk and updates the meta page.
// Returns an error if a disk write error occurs, or if Commit is
// called on a read-only transaction.
func (tx *Tx) Commit() error {
	_assert(!tx.managed, "managed tx commit not allowed")
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	}
```

```go
// CreateBucket creates a new bucket at the given key and returns the new bucket.
// Returns an error if the key already exists, if the bucket name is blank, or if the bucket name is too long.
// The bucket instance is only valid for the lifetime of the transaction.
func (b *Bucket) CreateBucket(key []byte) (*Bucket, error) {
	if b.tx.db == nil {
		return nil, ErrTxClosed
	} else if !b.tx.writable {
		return nil, ErrTxNotWritable
```

# Lifetimes: Costs and Benefits

# Future Improvements