use std::path::Path;

use anyhow::Result;
use fjall2::{
    Config, PartitionCreateOptions, PersistMode, TransactionalKeyspace,
    TransactionalPartitionHandle,
};
use rayon::prelude::*;

use crate::database::DatabaseBenchmark;

pub struct Fjall2Bench {
    keyspace: TransactionalKeyspace,
    partition: TransactionalPartitionHandle,
}

impl DatabaseBenchmark for Fjall2Bench {
    fn name() -> &'static str {
        "fjall2"
    }

    fn create(path: &Path) -> Result<Self> {
        Self::open(path)
    }

    fn open(path: &Path) -> Result<Self> {
        let keyspace = Config::new(path).open_transactional()?;
        let partition = keyspace.open_partition("bench", PartitionCreateOptions::default())?;
        Ok(Self {
            keyspace,
            partition,
        })
    }

    fn write_sequential(&mut self, count: u64) -> Result<()> {
        (0..count).try_for_each(|i| {
            let b = i.to_be_bytes();
            self.partition.insert(b, b)
        })?;

        Ok(())
    }

    fn read_sequential(&self) -> Result<u64> {
        let mut sum = 0u64;

        for item in self.keyspace.read_tx().iter(&self.partition) {
            let (_, value) = item?;
            let val = u64::from_be_bytes(value.as_ref().try_into()?);
            sum = sum.wrapping_add(val);
        }

        Ok(sum)
    }

    fn read_random(&self, indices: &[u64]) -> Result<u64> {
        let mut sum = 0u64;

        for &idx in indices {
            let key = idx.to_be_bytes();
            if let Some(value) = self.partition.get(key)? {
                let val = u64::from_be_bytes(value.as_ref().try_into()?);
                sum = sum.wrapping_add(val);
            }
        }

        Ok(sum)
    }

    fn read_random_rayon(&self, indices: &[u64]) -> Result<u64> {
        let partition = &self.partition;
        let sum = indices
            .par_iter()
            .map(|&idx| {
                let key = idx.to_be_bytes();
                if let Some(value) = partition.get(key).ok().flatten()
                    && let Ok(val_bytes) = TryInto::<[u8; 8]>::try_into(value.as_ref())
                {
                    u64::from_be_bytes(val_bytes)
                } else {
                    0
                }
            })
            .reduce(|| 0, |a, b| a.wrapping_add(b));

        Ok(sum)
    }

    fn flush(&mut self) -> Result<()> {
        self.keyspace.persist(PersistMode::SyncAll)?;
        Ok(())
    }

    fn disk_size(path: &Path) -> Result<u64> {
        let mut total = 0u64;
        if path.exists() {
            for entry in walkdir::WalkDir::new(path) {
                let entry = entry?;
                if entry.file_type().is_file() {
                    total += entry.metadata()?.len();
                }
            }
        }
        Ok(total)
    }
}
