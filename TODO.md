bbolt-rs
=====

# TODO

## Before Release 1.38
- [ ] Most of the db tests
- [ ] Node leaf tests

## General
- [ ] Quick Check analog
- [ ] Logging capabilities
- [ ] Replace NodeW.inodes with BTreeMap because woof - it does not scale. (Note: Shared CodSlice key with RefCell)
- [ ] blag post
- [ ] Power failure testing
- [ ] Write failure testing (especially freelist reloading)

## Open Questions
- [ ] Why do we need so much memory on large commits? Almost 3x the Go version
- [ ] Can we squeeze performance by moving the leaf keys all next to each other?

# To Refactor

## src/tx.rs
- [ ] Move tx stats in db.allocate to tx.allocate

## src/nodes.rs
- [ ] We have at least 3 different functions to write inodes
- [ ] Rebalance could be a lot better

## src/db.rs
- [ ] Rework page size determination functions

## src/bucket.rs
- [ ] Centralize memory aligned bump allocation (BucketIApi.open_bucket)
- [ ] Proper error chaining (BucketIApi.api_delete_bucket)
- [ ] Bucket tests: test_bucket_get_capacity - allow editing values?

## src/common/page.rs
- [ ] CoerciblePage API needs cleanup and renaming
- [ ] Make setting overflow unsafe

## src/common/memory.rs
- [ ] Retire RWSlice
- [ ] Use std IsAligned whenever it comes out

# To Complete

## src/tx.rs
- [ ] Copy file
- [ ] test_tx_check_read_only
- [ ] test_tx_copy_file
- [ ] test_tx_copy_file_error_meta
- [ ] test_tx_copy_file_error_normal
- [ ] example_tx_copy_file
- [ ] test_tx_truncate_before_write

## src/nodes.rs
- [ ] test_node_read_leaf_page
- [ ] test_node_write_leaf_page

## src/db.rs
- [ ] Rework page size determination functions
- [ ] Log IO Errors
- [ ] Save a special bump just for the RW?
- [ ] test_open
- [ ] test_open_multiple_goroutines
- [ ] test_open_err_path_required
- [ ] test_open_err_not_exists
- [ ] test_open_err_invalid
- [ ] test_open_err_version_mismatch
- [ ] test_open_err_checksum
- [ ] test_open_read_page_size_from_meta1_os
- [ ] test_open_read_page_size_from_meta1_given
- [ ] test_open_size
- [ ] test_open_size_large
- [ ] test_open_check
- [ ] test_open_meta_init_write_error
- [ ] test_open_file_too_small
- [ ] test_db_open_initial_mmap_size
- [ ] test_db_open_read_only
- [ ] test_open_big_page
- [ ] test_open_recover_free_list
- [ ] test_db_begin_err_database_not_open
- [ ] test_db_begin_rw
- [ ] test_db_concurrent_write_to
- [ ] test_db_begin_rw_closed
- [ ] test_db_close_pending_tx_rw
- [ ] test_db_close_pending_tx_ro
- [ ] test_db_update
- [ ] test_db_update_closed
- [ ] test_db_update_manual_commit
- [ ] test_db_update_manual_rollback
- [ ] test_db_view_manual_commit
- [ ] test_db_update_panic
- [ ] test_db_view_error
- [ ] test_db_view_panic
- [ ] test_db_consistency
- [ ] test_db_batch_panic
- [ ] test_db_batch_full
- [ ] test_db_batch_time
- [ ] test_dbunmap
- [ ] benchmark_dbbatch_automatic
- [ ] benchmark_dbbatch_single
- [ ] benchmark_dbbatch_manual10x100

## src/freelist.rs
- [ ] Freelist.reload
- [ ] freelist_merge_with_exist

## src/cursor.rs
- [ ] Internal api call for getting the root page id and its page_node
- [ ] Return cursor's bucket
- [ ] test_cursor_quick_check
- [ ] test_cursor_quick_check_reverse
- [ ] test_cursor_quick_check_buckets_only
- [ ] test_cursor_quick_check_buckets_only_reverse

## src/bucket.rs
- [ ] test_bucket_put_single
- [ ] test_bucket_put_multiple
- [ ] test_bucket_delete_quick

# Notstarted
- [ ] tx_stats_test.go
- [ ] simulation_test.go
- [ ] simulation_no_freelist_sync_test.go
- [ ] quick_test.go
- [ ] manydbs_test.go
- [ ] db_whitebox_test.go
- [ ] compact.go
- [ ] allocate_test.go
- [ ] tests/failpoint/db_failpoint_test.go
- [ ] internal/tests/tx_check_test.go
- [ ] internal/surgeon/surgeon_test.go
- [ ] internal/surgeon/surgeon.go
- [ ] internal/surgeon/xray_test.go
- [ ] internal/surgeon/xray.go
- [ ] internal/guts_cli/guts_cli.go
- [ ] internal/btesting/btesting.go
- [ ] cmd/bbolt/main_test.go
- [ ] cmd/bbolt/main.go
- [ ] cmd/bbolt/page_command.go
- [ ] cmd/bbolt/surgery_command_test.go
- [ ] cmd/bbolt/surgery_command.go