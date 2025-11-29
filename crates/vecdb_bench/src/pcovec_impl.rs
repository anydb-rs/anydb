use anyhow::Result;
use rayon::prelude::*;
use std::path::Path;
use vecdb::{AnyStoredVec, Database, GenericStoredVec, ImportableVec, PcoVec, Version};

use crate::database::DatabaseBenchmark;

pub struct PcoVecBench {
    db: Database,
    vec: PcoVec<usize, u64>,
}

impl DatabaseBenchmark for PcoVecBench {
    fn name() -> &'static str {
        "pcovec"
    }

    fn create(path: &Path) -> Result<Self> {
        let db = Database::open(path)?;
        let options = (&db, "bench", Version::TWO).into();
        let vec: PcoVec<usize, u64> = PcoVec::forced_import_with(options)?;
        Ok(Self { db, vec })
    }

    fn open(path: &Path) -> Result<Self> {
        let db = Database::open(path)?;
        let options = (&db, "bench", Version::TWO).into();
        let vec: PcoVec<usize, u64> = PcoVec::forced_import_with(options)?;
        Ok(Self { db, vec })
    }

    fn write_sequential(&mut self, count: u64) -> Result<()> {
        for i in 0..count {
            self.vec.push(i);
        }
        Ok(())
    }

    fn read_sequential(&self) -> Result<u64> {
        let mut sum = 0u64;

        for value in self.vec.clean_iter()? {
            sum = sum.wrapping_add(value);
        }

        Ok(sum)
    }

    fn read_random(&self, indices: &[u64]) -> Result<u64> {
        let mut sum = 0u64;
        let reader = self.vec.create_reader();

        for &idx in indices {
            if let Ok(value) = self.vec.read_at(idx as usize, &reader) {
                sum = sum.wrapping_add(value);
            }
        }

        Ok(sum)
    }

    fn read_random_rayon(&self, indices: &[u64]) -> Result<u64> {
        let reader = self.vec.create_reader();
        let sum = indices
            .par_iter()
            .map(|&idx| self.vec.read_at(idx as usize, &reader).unwrap_or_default())
            .reduce(|| 0, |a, b| a.wrapping_add(b));

        Ok(sum)
    }

    fn flush(&mut self) -> Result<()> {
        self.vec.write()?;
        self.db.flush()?;
        Ok(())
    }

    fn disk_size(path: &Path) -> Result<u64> {
        let mut total = 0u64;
        if path.exists() {
            for entry in std::fs::read_dir(path)? {
                let entry = entry?;
                if entry.file_type()?.is_file() {
                    total += entry.metadata()?.len();
                }
            }
        }
        Ok(total)
    }
}
