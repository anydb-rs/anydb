use std::path::PathBuf;

use rawdb::{Database, Region};

use crate::{AnyVec, Exit, Header, Result, Stamp};

/// Trait for stored vectors that persist data to disk (as opposed to lazy computed vectors).
pub trait AnyStoredVec: AnyVec {
    fn db_path(&self) -> PathBuf;

    fn region(&self) -> &Region;

    fn header(&self) -> &Header;

    fn mut_header(&mut self) -> &mut Header;

    /// Number of stamped change files to keep for rollback support.
    fn saved_stamped_changes(&self) -> u16;

    /// Writes pending changes to storage.
    /// Returns `Ok(true)` if data was written, `Ok(false)` if nothing to write.
    #[doc(hidden)]
    fn write(&mut self) -> Result<bool>;

    #[doc(hidden)]
    fn db(&self) -> Database;

    #[inline]
    fn flush(&mut self) -> Result<()> {
        if self.write()? {
            self.region().flush()?;
        }
        Ok(())
    }

    /// Flushes while holding the exit lock to ensure consistency during shutdown.
    #[inline]
    fn safe_flush(&mut self, exit: &Exit) -> Result<()> {
        let _lock = exit.lock();
        self.flush()?;
        Ok(())
    }

    /// Writes to mmap without fsync, holding the exit lock.
    /// Data is visible to readers immediately but not durable until sync.
    /// Use this for performance when durability can be deferred.
    #[inline]
    fn safe_write(&mut self, exit: &Exit) -> Result<()> {
        let _lock = exit.lock();
        self.write()?;
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
    fn stamped_write(&mut self, stamp: Stamp) -> Result<()> {
        self.update_stamp(stamp);
        self.write()?;
        Ok(())
    }

    fn serialize_changes(&self) -> Result<Vec<u8>>;

    /// Removes this vector's region from the database.
    fn remove(self) -> Result<()>;
}
