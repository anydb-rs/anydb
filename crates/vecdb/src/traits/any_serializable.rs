use super::AnyReadableVec;

/// Type-erased trait for serializable vectors.
pub trait AnySerializableVec: AnyReadableVec {
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
    V: crate::ReadableVec<V::I, V::T>,
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

        buf.push(b'[');
        let mut first = true;
        self.for_each_range_dyn_at(from_idx, to_idx, &mut |value: V::T| {
            if !first {
                buf.push(b',');
            }
            first = false;
            #[cfg(feature = "sonic-rs")]
            sonic_rs::to_writer(&mut *buf, &value).expect("json serialization failed");
            #[cfg(all(feature = "serde_json", not(feature = "sonic-rs")))]
            serde_json::to_writer(&mut *buf, &value).expect("json serialization failed");
        });
        buf.push(b']');

        Ok(())
    }

    fn write_json_value(&self, from: Option<usize>, buf: &mut Vec<u8>) -> crate::Result<()> {
        let idx = from.unwrap_or(0);
        if let Some(value) = self.collect_one_at(idx) {
            #[cfg(feature = "sonic-rs")]
            sonic_rs::to_writer(buf, &value)?;
            #[cfg(all(feature = "serde_json", not(feature = "sonic-rs")))]
            serde_json::to_writer(buf, &value)?;
        }

        Ok(())
    }
}
