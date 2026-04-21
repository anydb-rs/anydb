use super::WithPrev;

impl<T: Default> Default for WithPrev<T> {
    fn default() -> Self {
        Self {
            current: T::default(),
            previous: T::default(),
        }
    }
}
