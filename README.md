bbolt-rs
=====

bbolt-rs is a work in progress implementation of the [etcd-io/bbolt](https://github.com/etcd-io/bbolt) database in Rust. It successfully reads and commits, but it has many rough edges.

I'm rather pleased with my work as I believe the public API I've created has substantially fewer footguns than the Go code has. The database can't be dropped until all sessions are dropped. No resources from the transaction can escape the transaction. You can't deadlock the database by opening up a RW transaction and then opening up a R transaction right afterwards.

It also is interesting that, despite the increased memory usage, this code is about 40% faster in a synthetic large transaction than the equivalent Go code. Further benchmarking is postponed until the database is fully feature complete and we can have a proper duel.

Lastly, I must express my eternal gratitude to the bbolt developers who have created such a simple and easy to understand project to learn from.

Features:
* Arena memory allocation per transaction
* Explicitly designed to prevent transaction dependant resources from escaping the transaction.
* RwLock based transactions
* File backed database
* Memory backed database for Miri to use
* Simple and straightforward public APIs


## TODO

### General
- [ ] Read-only DB access with optional freelist loading
- [ ] Update layout to match current bbolt directory structure
- [ ] Quick Check analog
- [ ] Logging capabilities
- [x] Follow Clippy guidance
- [x] Remove commented out trace
- [ ] Optional fsync
- [ ] Optional mlock
- [ ] Strict mode
- [ ] impl trait for public apis?
- [ ] blag post

### Open Questions
- [ ] How do we properly pin memory for the transactions?
- [ ] How do we properly set up sync and send for the database?
- [ ] Why do I need to transmute the key deref in key(&'a self) -> &'tx [u8] in inode.rs?
- [ ] Why do we leak memory when dropping TxRwImpl?
- [ ] Why do we need so much memory on large commits? Almost 3x the Go version
- [ ] Can we squeeze performance by moving the leaf keys all next to each other?

## To Refactor

### src/tx.rs
- [ ] API allows commit for TxRwRef

### src/nodes.rs
- [ ] We have at least 3 different functions to write inodes
- [ ] Double check inodes aren't used after they're spilled
- [ ] Rebalance could be a lot better

### src/db.rs
- [ ] Rework page size determination functions
- [ ] Access freelist easier

### src/bucket.rs
- [ ] Centralize memory aligned bump allocation (BucketIApi.open_bucket)
- [ ] Proper error chaining (BucketIApi.api_delete_bucket)
- [ ] Bucket tests: test_bucket_get_capacity - allow editing values?

### src/common/mod.rs
- [ ] Remove unnecessary functions for SplitRef

### src/common/page.rs
- [ ] CoerciblePage API needs cleanup and renaming
- [ ] Make setting overflow unsafe

### src/common/memory.rs
- [ ] Retire RWSlice
- [ ] Use std IsAligned whenever it comes out

## To Complete

### src/tx.rs
- [ ] TxStats getter/setters
- [ ] TxIAPI Double check api_for_each -> api_for_each_bucket call 
- [ ] TxIAPI.\*rollback\*
- [ ] TxApi.for_each
- [ ] TxCheck for TxImpl, TxRef
- [ ] Copy file
- [ ] test_tx_check_read_only
- [ ] test_tx_commit_err_tx_closed
- [ ] test_tx_rollback_err_tx_closed
- [ ] test_tx_commit_err_tx_not_writable
- [ ] test_tx_cursor
- [ ] test_tx_create_bucket_err_tx_not_writable
- [ ] test_tx_create_bucket_err_tx_closed
- [ ] test_tx_bucket
- [ ] test_tx_get_not_found
- [ ] test_tx_create_bucket_if_not_exists
- [ ] test_tx_create_bucket_if_not_exists_err_bucket_name_required
- [ ] test_tx_create_bucket_err_bucket_exists
- [ ] test_tx_create_bucket_err_bucket_name_required
- [ ] test_tx_delete_bucket
- [ ] test_tx_delete_bucket_err_tx_closed
- [ ] test_tx_delete_bucket_read_only
- [ ] test_tx_delete_bucket_not_found
- [ ] test_tx_for_each_no_error
- [ ] test_tx_for_each_with_error
- [ ] test_tx_on_commit
- [ ] test_tx_on_commit_rollback
- [ ] test_tx_copy_file
- [ ] test_tx_copy_file_error_meta
- [ ] test_tx_copy_file_error_normal
- [ ] test_tx_rollback
- [ ] test_tx_release_range
- [ ] example_tx_rollback
- [ ] example_tx_copy_file
- [ ] test_tx_stats_get_and_inc_atomically
- [ ] test_tx_stats_sub
- [ ] test_tx_truncate_before_write
- [ ] test_tx_stats_add

### src/nodes.rs
- [ ] test_node_read_leaf_page
- [ ] test_node_write_leaf_page

### src/db.rs
- [ ] Rework page size determination functions
- [ ] Log IO Errors
- [ ] Save a special bump just for the RW?
- [ ] DbApi.close (?)
- [ ] DbApi.batch
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
- [ ] test_db_stats
- [ ] test_db_consistency
- [ ] test_dbstats_sub
- [ ] test_db_batch
- [ ] test_db_batch_panic
- [ ] test_db_batch_full
- [ ] test_db_batch_time
- [ ] test_dbunmap
- [ ] example_db_update
- [ ] example_db_view
- [ ] example_db_begin
- [ ] benchmark_dbbatch_automatic
- [ ] benchmark_dbbatch_single
- [ ] benchmark_dbbatch_manual10x100

### src/freelist.rs
- [ ] Freelist.reload
- [ ] freelist_merge_with_exist

### src/cursor.rs
- [ ] Internal api call for getting the root page id and its page_node
- [ ] Return cursor's bucket
- [ ] test_cursor_quick_check
- [ ] test_cursor_quick_check_reverse
- [ ] test_cursor_quick_check_buckets_only
- [ ] test_cursor_quick_check_buckets_only_reverse
- [ ] example_cursor
- [ ] example_cursor_reverse

### src/bucket.rs
- [ ] BucketApi.stats
- [ ] BucketApi.set_sequence
- [ ] BucketApi.next_sequence
- [ ] test_bucket_delete_freelist_overflow
- [ ] test_bucket_delete_non_existing
- [ ] test_bucket_nested
- [ ] test_bucket_delete_bucket
- [ ] test_bucket_delete_read_only
- [ ] test_bucket_delete_closed
- [ ] test_bucket_delete_bucket_nested
- [ ] test_bucket_delete_bucket_nested2
- [ ] test_bucket_delete_bucket_large
- [ ] test_bucket_bucket_incompatible_value
- [ ] test_bucket_create_bucket_incompatible_value
- [ ] test_bucket_delete_bucket_incompatible_value
- [ ] test_bucket_sequence
- [ ] test_bucket_next_sequence
- [ ] test_bucket_next_sequence_persist
- [ ] test_bucket_next_sequence_read_only
- [ ] test_bucket_next_sequence_closed
- [ ] test_bucket_for_each
- [ ] test_bucket_for_each_bucket
- [ ] test_bucket_for_each_bucket_no_buckets
- [ ] test_bucket_for_each_short_circuit
- [ ] test_bucket_for_each_closed
- [ ] test_bucket_put_empty_key
- [ ] test_bucket_put_key_too_large
- [ ] test_bucket_put_value_too_large
- [ ] test_bucket_stats
- [ ] test_bucket_stats_random_fill
- [ ] test_bucket_stats_small
- [ ] test_bucket_stats_empty_bucket
- [ ] test_bucket_stats_nested
- [ ] test_bucket_stats_large
- [ ] test_bucket_put_single
- [ ] test_bucket_put_multiple
- [ ] test_bucket_delete_quick
- [ ] example_bucket_put
- [ ] example_bucket_delete
- [ ] example_bucket_for_each

## Notstarted
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