use std::{cell::RefCell, path::Path};

use anyhow::Result;
use rayon::prelude::*;
use redb::{Builder, Database, ReadableDatabase, ReadableTable, TableDefinition};

use crate::database::DatabaseBenchmark;

const TABLE: TableDefinition<u64, u64> = TableDefinition::new("bench");

pub struct RedbBench {
    db: Database,
}

impl DatabaseBenchmark for RedbBench {
    fn name() -> &'static str {
        "redb"
    }

    fn create(path: &Path) -> Result<Self> {
        Self::open(path)
    }

    fn open(path: &Path) -> Result<Self> {
        let db = Builder::new()
            .set_cache_size(1024 * 1024 * 1024)
            .create(path.join("bench.redb"))?;
        Ok(Self { db })
    }

    fn write_sequential(&mut self, count: u64) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(TABLE)?;
            for i in 0..count {
                table.insert(i, i)?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    fn read_sequential(&self) -> Result<u64> {
        let mut sum = 0u64;
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(TABLE)?;

        for item in table.iter()? {
            let (_, value) = item?;
            sum = sum.wrapping_add(value.value());
        }

        Ok(sum)
    }

    fn read_random(&self, indices: &[u64]) -> Result<u64> {
        let mut sum = 0u64;
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(TABLE)?;

        for &idx in indices {
            if let Some(value) = table.get(idx)? {
                sum = sum.wrapping_add(value.value());
            }
        }

        Ok(sum)
    }

    fn read_random_rayon(&self, indices: &[u64]) -> Result<u64> {
        thread_local! {
            static TXN_CACHE: RefCell<Option<(redb::ReadTransaction, redb::ReadOnlyTable<u64, u64>)>> = const { RefCell::new(None) };
        }

        let db = &self.db;
        let sum = indices
            .par_iter()
            .map(|&idx| {
                TXN_CACHE.with(|cache| {
                    let mut cache_opt = cache.borrow_mut();
                    if cache_opt.is_none()
                        && let Ok(read_txn) = db.begin_read()
                        && let Ok(table) = read_txn.open_table(TABLE)
                    {
                        *cache_opt = Some((read_txn, table));
                    }
                    let table = &cache_opt.as_ref().unwrap().1;
                    if let Ok(Some(value)) = table.get(idx) {
                        return value.value();
                    }
                    0
                })
            })
            .reduce(|| 0, |a, b| a.wrapping_add(b));

        Ok(sum)
    }

    fn flush(&mut self) -> Result<()> {
        // redb automatically syncs on commit, but we can force a checkpoint
        // No explicit flush needed - commits are durable
        Ok(())
    }

    fn disk_size(path: &Path) -> Result<u64> {
        let db_path = path.join("bench.redb");
        if db_path.exists() {
            Ok(std::fs::metadata(&db_path)?.len())
        } else {
            Ok(0)
        }
    }
}
