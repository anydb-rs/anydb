use rawdb::Database;

use crate::{Format, Version};

/// Options for importing or creating stored vectors.
#[derive(Debug, Clone, Copy)]
pub struct ImportOptions<'a> {
    /// Database to store the vector in.
    pub db: &'a Database,
    /// Name of the vector.
    pub name: &'a str,
    /// Version for tracking data schema compatibility.
    pub version: Version,
    /// Storage format for the vector.
    pub format: Format,
    /// Number of stamped change files to keep for rollback support (0 to disable).
    pub saved_stamped_changes: u16,
}

impl<'a> ImportOptions<'a> {
    pub fn new(db: &'a Database, name: &'a str, version: Version, format: Format) -> Self {
        Self {
            db,
            name,
            version,
            format,
            saved_stamped_changes: 0,
        }
    }

    pub fn with_saved_stamped_changes(mut self, num: u16) -> Self {
        self.saved_stamped_changes = num;
        self
    }

    pub fn with_format(mut self, format: Format) -> Self {
        self.format = format;
        self
    }
}

impl<'a> From<(&'a Database, &'a str, Version, Format)> for ImportOptions<'a> {
    fn from((db, name, version, format): (&'a Database, &'a str, Version, Format)) -> Self {
        Self::new(db, name, version, format)
    }
}
