# vecdb_derive

Derive macros for [`vecdb`](../vecdb/) compression support.

Automatically implements compression traits for custom wrapper types, enabling them to work with `PcoVec`.

## Install

```bash
cargo add vecdb --features derive
```

## Usage

```rust
use vecdb::PcoVecValue;

#[derive(PcoVecValue)]
struct Timestamp(u64);

// Now works with PcoVec
let mut vec: PcoVec<usize, Timestamp> = ...;
vec.push(Timestamp(12345));
```

## `#[derive(PcoVecValue)]`

Implements `PcoVecValue` for single-field tuple structs. The wrapper inherits compression characteristics from the inner type.

**Requirements:**
- Must be a tuple struct with exactly one field
- Inner type must implement `PcoVecValue`
