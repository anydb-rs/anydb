use super::AnyCollectableVec;

/// Type-erased trait for serializable vectors.
pub trait AnySerializableVec: AnyCollectableVec {
    /// Write JSON array to output buffer
    #[cfg(feature = "serde")]
    fn write_json(
        &self,
        from: Option<usize>,
        to: Option<usize>,
        buf: &mut Vec<u8>,
    ) -> crate::Result<()>;

    /// Write single JSON value to output buffer (first value in range)
    #[cfg(feature = "serde")]
    fn write_json_value(&self, from: Option<usize>, buf: &mut Vec<u8>) -> crate::Result<()>;
}

#[cfg(feature = "serde")]
impl<V> AnySerializableVec for V
where
    V: crate::TypedVec,
    V: crate::CollectableVec<V::I, V::T>,
    V::T: serde::Serialize,
{
    fn write_json(
        &self,
        from: Option<usize>,
        to: Option<usize>,
        buf: &mut Vec<u8>,
    ) -> crate::Result<()> {
        let len = self.len();
        let from_idx = from.unwrap_or(0);
        let to_idx = to.unwrap_or(len).min(len);

        let vec: Vec<V::T> = self.iter_range(Some(from_idx), Some(to_idx)).collect();

        #[cfg(feature = "sonic-rs")]
        sonic_rs::to_writer(buf, &vec)?;
        #[cfg(all(feature = "serde_json", not(feature = "sonic-rs")))]
        serde_json::to_writer(buf, &vec)?;

        Ok(())
    }

    fn write_json_value(&self, from: Option<usize>, buf: &mut Vec<u8>) -> crate::Result<()> {
        let from_idx = from.unwrap_or(0);

        if let Some(value) = self.iter_range(Some(from_idx), Some(from_idx + 1)).next() {
            #[cfg(feature = "sonic-rs")]
            sonic_rs::to_writer(buf, &value)?;
            #[cfg(all(feature = "serde_json", not(feature = "sonic-rs")))]
            serde_json::to_writer(buf, &value)?;
        }

        Ok(())
    }
}
