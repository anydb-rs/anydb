use anyhow::Result;
use rayon::prelude::*;
use std::path::Path;
use vecdb::{AnyStoredVec, BytesVec, Database, GenericStoredVec, ImportableVec, Version};

use crate::database::DatabaseBenchmark;

pub struct BytesVecBench {
    db: Database,
    vec: BytesVec<usize, u64>,
}

impl DatabaseBenchmark for BytesVecBench {
    fn name() -> &'static str {
        "bytesvec"
    }

    fn create(path: &Path) -> Result<Self> {
        Self::open(path)
    }

    fn open(path: &Path) -> Result<Self> {
        let db = Database::open(path)?;
        let vec: BytesVec<usize, u64> = BytesVec::import(&db, "bench", Version::TWO)?;
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
        let view = self.vec.view();
        for &idx in indices {
            sum = sum.wrapping_add(view.get(idx as usize));
        }
        Ok(sum)
    }

    fn read_random_rayon(&self, indices: &[u64]) -> Result<u64> {
        let view = self.vec.view();
        let sum = indices
            .par_iter()
            .map(|&idx| view.get(idx as usize))
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
