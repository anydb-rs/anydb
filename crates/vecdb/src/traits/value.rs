use std::fmt::Debug;

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

#[cfg(feature = "serde")]
pub trait Serialize: serde::Serialize {}
#[cfg(feature = "serde")]
impl<T: serde::Serialize> Serialize for T {}

#[cfg(not(feature = "serde"))]
pub trait Serialize {}
#[cfg(not(feature = "serde"))]
impl<T> Serialize for T {}

pub trait VecValue
where
    Self: Sized
        + Debug
        + Clone
        + FromBytes
        + IntoBytes
        + Immutable
        + KnownLayout
        + Serialize
        + Send
        + Sync
        + 'static,
{
}

impl<T> VecValue for T where
    T: Sized
        + Debug
        + Clone
        + FromBytes
        + IntoBytes
        + Immutable
        + KnownLayout
        + Serialize
        + Send
        + Sync
        + 'static
{
}
