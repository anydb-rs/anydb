use super::DeltaOp;

/// Delta change: `source[h] - source[start]` via f64, allowing cross-type (unsigned → signed).
#[derive(Clone, Copy)]
pub struct DeltaChange;

impl<S, C> DeltaOp<S, C> for DeltaChange
where
    S: Into<f64>,
    C: From<f64>,
{
    #[inline]
    fn combine(current: S, ago: S, _count: usize) -> C {
        C::from(Into::<f64>::into(current) - Into::<f64>::into(ago))
    }
}
