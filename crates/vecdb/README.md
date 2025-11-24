# vecdb

High-performance mutable persistent vectors built on [`rawdb`](../rawdb/README.md).

It features:

- `Vec` based API: push, update, truncate, delete by index
- Multiple variants: `BytesVec`, `ZeroCopyVec`, `PcoVec`, `LZ4Vec`, `ZstdVec`, `EagerVec`, `LazyVec`, ...
- Rollback via stamped change deltas
- Sparse deletions with holes
- Thread-safe with concurrent reads
- Blazing fast ([benchmark](../vecdb_bench/README.md))
- Persistence only on `flush`

It is not:

- A key-value store (consider [`fjall`](https://crates.io/crates/fjall) or [`redb`](https://crates.io/crates/redb))
- Suited for variable-sized types (`String`, `Vec<T>`, etc.)

## Install

```bash
cargo add vecdb
```

## Usage

```rust
use vecdb::{AnyStoredVec, Importable, Database, GenericStoredVec, BytesVec, Result, Version};

fn main() -> Result<()> {
    // create
    let temp_dir = tempfile::TempDir::new()?;
    let db = Database::open(temp_dir.path())?;
    let mut vec: BytesVec<usize, u64> = BytesVec::import(&db, "vec", Version::ONE)?;

    // push
    for i in 0..1_000_000 {
        vec.push(i);
    }

    // flush
    vec.flush()?;
    db.flush()?;

    // read (sequential)
    let mut sum = 0u64;
    for value in vec.iter()? {
        sum = sum.wrapping_add(value);
    }

    // read (random)
    let indices: Vec<usize> = vec![500, 1000, 10];
    let reader = vec.create_reader();
    for i in indices {
        if let Ok(value) = vec.read_at(i, &reader) {
            sum = sum.wrapping_add(value);
        }
    }

    Ok(())
}
```

## Constraints

Data must be fixed-size types: numbers, fixed arrays, structs with `#[repr(C)]`.

Compression via Pcodec works for numeric types only.

## When to use it

- Need to store `Vec`s on disk
- Append-only or append-mostly workloads
- Need very high read speeds
- Space-efficient storage for numeric data
- Sparse deletions without reindexing
- Rollback without full snapshots
