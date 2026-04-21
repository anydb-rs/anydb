use rawdb::Database;

use crate::Version;

use super::ImportOptions;

impl<'a> From<(&'a Database, &'a str, Version)> for ImportOptions<'a> {
    fn from((db, name, version): (&'a Database, &'a str, Version)) -> Self {
        Self::new(db, name, version)
    }
}
