use super::DeltaOp;

/// Delta rate (growth): `(source[h] - source[start]) / source[start]` via f64.
#[derive(Clone, Copy)]
pub struct DeltaRate;

impl<S, B> DeltaOp<S, B> for DeltaRate
where
    S: Into<f64>,
    B: From<f64>,
{
    #[inline]
    fn combine(current: S, ago: S, _count: usize) -> B {
        let current_f: f64 = current.into();
        let ago_f: f64 = ago.into();
        if ago_f == 0.0 {
            B::from(0.0)
        } else {
            B::from((current_f - ago_f) / ago_f)
        }
    }
}
