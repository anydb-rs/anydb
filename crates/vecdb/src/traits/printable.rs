use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

/// Extracts the short type name from a full type path and caches it.
pub fn short_type_name<T: 'static>() -> &'static str {
    static CACHE: OnceLock<Mutex<HashMap<&'static str, &'static str>>> = OnceLock::new();

    let full: &'static str = std::any::type_name::<T>();

    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = cache.lock().unwrap();

    if let Some(&short) = guard.get(full) {
        return short;
    }

    let short_owned = full.rsplit("::").next().unwrap_or(full).to_string();
    let short: &'static str = Box::leak(short_owned.into_boxed_str());
    guard.insert(full, short);
    short
}

/// Provides string representations of index types for display and region naming.
pub trait PrintableIndex {
    /// Returns the canonical string name for this index type.
    fn to_string() -> &'static str;

    /// Returns all accepted string representations for this index type.
    /// Used for parsing and type identification.
    fn to_possible_strings() -> &'static [&'static str];
}

impl PrintableIndex for usize {
    fn to_string() -> &'static str {
        "usize"
    }

    fn to_possible_strings() -> &'static [&'static str] {
        &["usize"]
    }
}
