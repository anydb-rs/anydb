#![doc = include_str!("../README.md")]
// #![doc = "\n## Examples\n"]
// #![doc = "\n### Raw\n\n```rust"]
// #![doc = include_str!("../examples/raw.rs")]
// #![doc = "```\n"]
// #![doc = "\n### Compressed\n\n```rust"]
// #![doc = include_str!("../examples/compressed.rs")]
// #![doc = "```"]

pub use rawdb::{Database, Error as RawDBError, PAGE_SIZE, Reader, likely, unlikely};

#[cfg(feature = "derive")]
pub use vecdb_derive::{Bytes, Pco};

mod error;
mod exit;
mod iterators;
mod lookback;
mod stamp;
mod traits;
mod variants;
mod version;

use variants::*;

pub use error::*;
pub use exit::*;
pub use iterators::*;
pub use stamp::*;
pub use traits::*;
pub use variants::*;
pub use version::*;

const ONE_KIB: usize = 1024;
const BUFFER_SIZE: usize = 512 * ONE_KIB;
const SIZE_OF_U64: usize = std::mem::size_of::<u64>();
