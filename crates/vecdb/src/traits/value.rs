use std::fmt::Debug;

pub trait VecValue
where
    Self: Sized + Debug + Clone + Send + Sync + 'static,
{
}

impl<T> VecValue for T where T: Sized + Debug + Clone + Send + Sync + 'static {}
