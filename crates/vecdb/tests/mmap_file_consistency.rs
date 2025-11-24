use tempfile::TempDir;
use vecdb::{
    AnyStoredVec, Database, EagerVec, Exit, GenericStoredVec, Importable, IterableVec, PcoVec,
};

#[test]
fn test_mmap_write_file_read_consistency() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(&temp_dir.path().join("test.db")).unwrap();
    let exit = Exit::new();

    // Create a compressed vec (which uses mmap for writes)
    let mut vec: EagerVec<PcoVec<usize, u64>> =
        EagerVec::forced_import(&db, "test_vec", vecdb::Version::ONE).unwrap();

    // Write some data
    for i in 0..1000usize {
        vec.truncate_push(i, i as u64 * 100).unwrap();
    }

    // Flush the vec (writes to mmap)
    vec.safe_flush(&exit).unwrap();

    println!("After flush, checking data consistency...");

    // Now create an iterator (which uses file handle for reads)
    let mut iter = vec.iter();

    // Check if iterator sees the written data
    for i in 0..1000u32 {
        let value = iter.next().expect("Should have value");
        let expected = i as u64 * 100;

        if value != expected {
            panic!(
                "Inconsistency detected at index {}: got {}, expected {}",
                i, value, expected
            );
        }
    }

    println!("Test passed! All values consistent.");
}

#[test]
fn test_immediate_read_after_write() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(&temp_dir.path().join("test2.db")).unwrap();
    let exit = Exit::new();

    let mut vec: EagerVec<PcoVec<usize, u64>> =
        EagerVec::forced_import(&db, "test_vec", vecdb::Version::ONE).unwrap();

    // Write, flush, read immediately (mimics the txinindex -> txindex pattern)
    for batch in 0..10 {
        let start = batch * 100;

        // Write batch
        for i in 0..100usize {
            vec.truncate_push(start + i, (start + i) as u64 * 100)
                .unwrap();
        }

        // Flush
        vec.safe_flush(&exit).unwrap();

        // Immediately read back using read_at_unwrap_once
        for i in 0..100usize {
            let idx = start + i;
            let value = vec.read_at_unwrap_once(idx);
            let expected = (start + i) as u64 * 100;

            if value != expected {
                panic!(
                    "Batch {} inconsistency at index {}: got {}, expected {}",
                    batch, idx, value, expected
                );
            }
        }
    }

    println!("Immediate read test passed!");
}
