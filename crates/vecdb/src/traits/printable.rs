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
