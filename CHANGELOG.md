# Upcoming

## Added

## Breaking Changes

## Changes

# 1.3.10

## Added
* Added Reverse Iterators

## Breaking Changes

## Changes
* Up to date with BBolt 1.3.10
* Testing includes QuickCheck analog
* Corrects Leaf node being treated as a branch node in certain cases

# 1.3.9
## Added
* Add support for aarch64 (e.g. Apple M1)
* Added util::compact
* Added Iterators

## Breaking Changes
* Some APIs had lifetimes that were accidentally valid for the whole transaction

## Changed
* Deprecated *foreach functions
* Use Relaxed Atomic ordering for everything except the final get
* Lots of internal cleanup

# 1.3.8
* Initial release
