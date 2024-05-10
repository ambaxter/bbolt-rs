bbolt-rs
=====

bbolt-rs is an implementation of the [etcd-io/bbolt](https://github.com/etcd-io/bbolt) database in Rust.
It successfully reads and commits, but it has some limitations. 

The current version matches Bolt v1.3.8.

I'm rather pleased with my work as I believe the public API I've created has substantially fewer footguns than the Go code has.
* The database can't be dropped until all sessions are dropped.
* No resources from the transaction can escape the transaction.
* You can't deadlock the database by opening up a RW transaction and then opening up a R transaction right afterwards.

It also is interesting that, despite the increased memory usage, this code is about 40% faster in a synthetic large transaction than the equivalent Go code.
Further benchmarking is postponed until the database is fully feature complete and we can have a proper duel (in progress!).

Lastly, I must express my eternal gratitude to the bbolt developers who have created such a simple and easy to understand project to learn from.

Features:
* Arena memory allocation per transaction
* Explicitly designed to prevent transaction dependant resources from escaping the transaction.
* RwLock based transactions
* File backed database
* Memory backed database
* Miri tested to prevent memory errors in unsafe blocks
* Simple and straightforward public APIs

[Check out the documentation!!](https://docs.rs/bbolt-rs/1.3.8/bbolt_rs/)

Currently not supported:
* Tx.copy
* Compact
* Most of the main application
* A variety of DB Options including 
  * no freelist sync
  * file open timeout
* Panic handling during bench
