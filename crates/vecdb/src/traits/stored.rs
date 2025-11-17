use std::path::PathBuf;

use rawdb::{Database, Region};

use crate::{AnyVec, Exit, Result, Stamp, variants::Header};

/// Trait for stored vectors that persist data to disk (as opposed to lazy computed vectors).
pub trait AnyStoredVec: AnyVec {
    fn db_path(&self) -> PathBuf;

    fn region(&self) -> &Region;

    fn header(&self) -> &Header;

    fn mut_header(&mut self) -> &mut Header;

    /// Number of stamped change files to keep for rollback support.
    fn saved_stamped_changes(&self) -> u16;

    #[doc(hidden)]
    fn write(&mut self) -> Result<()>;

    #[doc(hidden)]
    fn db(&self) -> Database;

    #[inline]
    fn flush(&mut self) -> Result<()> {
        self.write()?;
        self.region().flush()?; // OS only syncs dirty pages anyway
        Ok(())
    }

    /// Flushes while holding the exit lock to ensure consistency during shutdown.
    #[inline]
    fn safe_flush(&mut self, exit: &Exit) -> Result<()> {
        // info!("safe flush {}", self.name());
        let _lock = exit.lock();
        self.flush()?;
        Ok(())
    }

    /// The actual length stored on disk.
    fn real_stored_len(&self) -> usize;
    /// The effective stored length (may differ from real_stored_len during truncation).
    fn stored_len(&self) -> usize;

    fn update_stamp(&mut self, stamp: Stamp) {
        self.mut_header().update_stamp(stamp);
    }

    fn stamp(&self) -> Stamp {
        self.header().stamp()
    }

    #[inline]
    fn stamped_flush(&mut self, stamp: Stamp) -> Result<()> {
        self.update_stamp(stamp);
        self.write()
    }

    fn serialize_changes(&self) -> Result<Vec<u8>>;
}
