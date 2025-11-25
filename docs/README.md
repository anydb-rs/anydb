# anydb

A collection of high-performance embedded database crates for Rust, focused on efficient storage and retrieval of fixed-size data types.

## Crates

- **[`rawdb`](../crates/rawdb/)** - Low-level single-file storage engine with filesystem-like API and automatic space reclamation
- **[`vecdb`](../crates/vecdb/)** - High-performance mutable persistent vectors with compression support, rollback, sparse deletions, and computation methods
- **[`vecdb_derive`](../crates/vecdb_derive/)** - Derive macros for `Bytes` and `Pco` traits to enable custom types in vecdb
- **[`vecdb_bench`](../crates/vecdb_bench/)** - Benchmarking suite comparing vecdb against fjall, redb, lmdb, and rocksdb

## Use Cases

**Choose rawdb when:**
- You need a simple, low-level storage abstraction
- You want filesystem-like named regions in a single file
- You need automatic space reclamation via hole punching
- You want zero-copy mmap access to your data

**Choose vecdb when:**
- You need persistent Vec-like collections on disk
- You have append-heavy or append-mostly workloads
- You want compression for numeric or general data
- You need rollback without full snapshots
- You want sparse deletions without reindexing
- You need high sequential or random read performance
- You want computed vectors from other vectors (stored on disk or computed on-the-fly)

## Resources

- [Changelog](CHANGELOG.md)
- [License](LICENSE.md)
- [TODO](TODO.md)
