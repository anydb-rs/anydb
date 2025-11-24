use tempfile::TempDir;
use vecdb::{
    AnyStoredVec, BytesVec, Database, EagerVec, Exit, GenericStoredVec, Importable, IterableVec,
    LZ4Vec, PcoVec, StoredVec, ZeroCopyVec, ZstdVec,
};

/// Generic test function for mmap write/file read consistency
fn run_mmap_write_file_read_consistency<V>()
where
    V: StoredVec<I = usize, T = u64>,
{
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(&temp_dir.path().join("test.db")).unwrap();
    let exit = Exit::new();

    // Create a vec (which uses mmap for writes)
    let mut vec: EagerVec<V> =
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

/// Generic test function for immediate read after write
fn run_immediate_read_after_write<V>()
where
    V: StoredVec<I = usize, T = u64>,
{
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(&temp_dir.path().join("test2.db")).unwrap();
    let exit = Exit::new();

    let mut vec: EagerVec<V> =
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

// ============================================================================
// Concrete Test Instances
// ============================================================================

#[test]
fn test_bytes_mmap_write_file_read_consistency() {
    run_mmap_write_file_read_consistency::<BytesVec<usize, u64>>();
}

#[test]
fn test_zerocopy_mmap_write_file_read_consistency() {
    run_mmap_write_file_read_consistency::<ZeroCopyVec<usize, u64>>();
}

#[test]
fn test_pco_mmap_write_file_read_consistency() {
    run_mmap_write_file_read_consistency::<PcoVec<usize, u64>>();
}

#[test]
fn test_lz4_mmap_write_file_read_consistency() {
    run_mmap_write_file_read_consistency::<LZ4Vec<usize, u64>>();
}

#[test]
fn test_zstd_mmap_write_file_read_consistency() {
    run_mmap_write_file_read_consistency::<ZstdVec<usize, u64>>();
}

#[test]
fn test_bytes_immediate_read_after_write() {
    run_immediate_read_after_write::<BytesVec<usize, u64>>();
}

#[test]
fn test_zerocopy_immediate_read_after_write() {
    run_immediate_read_after_write::<ZeroCopyVec<usize, u64>>();
}

#[test]
fn test_pco_immediate_read_after_write() {
    run_immediate_read_after_write::<PcoVec<usize, u64>>();
}

#[test]
fn test_lz4_immediate_read_after_write() {
    run_immediate_read_after_write::<LZ4Vec<usize, u64>>();
}

#[test]
fn test_zstd_immediate_read_after_write() {
    run_immediate_read_after_write::<ZstdVec<usize, u64>>();
}
