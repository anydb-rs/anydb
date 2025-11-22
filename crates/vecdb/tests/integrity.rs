use rawdb::Database;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;
use vecdb::{
    AnyStoredVec, AnyVec, CollectableVec, GenericStoredVec, ImportOptions, RawVec, Result, Stamp,
    Version,
};

/// Helper to create a temporary test database
pub fn setup_test_db() -> Result<(Database, TempDir)> {
    let temp_dir = TempDir::new()?;
    let db = Database::open(temp_dir.path())?;
    Ok((db, temp_dir))
}

/// Compute SHA-256 hash of the vecdb data file and regions directory
/// Only hashes data (file) and regions/*, ignoring changes directory
fn compute_directory_hash(dir: &Path) -> Result<String, Box<dyn std::error::Error>> {
    let mut hasher = Sha256::new();

    // Collect all files in sorted order for deterministic hashing
    let mut files: Vec<PathBuf> = Vec::new();

    // Hash the data file if it exists
    let data_file = dir.join("data");
    if data_file.exists() && data_file.is_file() {
        files.push(data_file);
    }

    // Hash files in the regions directory, excluding changes subdirectory
    let regions_dir = dir.join("regions");
    if regions_dir.exists() {
        for entry in walkdir::WalkDir::new(&regions_dir)
            .follow_links(false)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            let path = entry.path();

            // Skip the changes directory
            if path.components().any(|c| c.as_os_str() == "changes") {
                continue;
            }

            if entry.file_type().is_file() {
                files.push(path.to_path_buf());
            }
        }
    }

    files.sort();

    // Hash each file's relative path and contents
    for file_path in &files {
        // Hash the relative path
        if let Ok(rel_path) = file_path.strip_prefix(dir) {
            hasher.update(rel_path.to_string_lossy().as_bytes());
        }

        // Hash the file contents
        let contents = fs::read(file_path)?;
        hasher.update(&contents);
    }

    Ok(format!("{:x}", hasher.finalize()))
}

/// Test data integrity after flush operations with undo/redo
///
/// This test verifies that after rollback + flush + close + reopen:
/// 1. Data can be correctly read back using individual gets
/// 2. Data can be correctly read back using iterators
/// 3. Redo operations produce the same readable state
#[test]
fn test_data_integrity_rollback_flush_reopen() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Data Integrity Test: Rollback + Flush + Reopen ===\n");
    println!("This test verifies:");
    println!("  • Rollback + flush + reopen preserves data correctly");
    println!("  • Data readable via both gets and iterators");
    println!("  • File hashes track physical layout differences\n");

    // Create database
    let (database, temp) = setup_test_db()?;
    let test_path = temp.path();
    let options: ImportOptions = (&database, "vec", Version::TWO).into();
    let options = options.with_saved_stamped_changes(10); // Enable rollback with history

    let mut vec: RawVec<usize, u32> = RawVec::forced_import_with(options)?;
    println!("✓ Created vecdb");

    // Step 1: Do initial work
    println!("\n--- Phase 1: Initial work ---");
    for i in 0..5 {
        vec.push(i);
    }
    vec.stamped_flush_with_changes(Stamp::new(1))?;
    println!("✓ Added values [0, 1, 2, 3, 4] and flushed (stamp 1)");
    println!("  Current data: {:?}", vec.collect());

    // Step 2: More work and flush
    println!("\n--- Phase 2: More work ---");
    for i in 5..10 {
        vec.push(i);
    }
    vec.stamped_flush_with_changes(Stamp::new(2))?;
    println!("✓ Added values [5, 6, 7, 8, 9] and flushed (stamp 2)");
    println!("  Current data: {:?}", vec.collect());

    // Step 3: Checkpoint 1 - Save hash and data state
    println!("\n--- Checkpoint 1 ---");
    let checkpoint1_data = vec.collect_holed()?;
    let checkpoint1_stamp = vec.stamp();
    let checkpoint1_hash = compute_directory_hash(test_path)?;
    println!("✓ Saved checkpoint1 at stamp {:?}", checkpoint1_stamp);
    println!("  Data: {:?}", checkpoint1_data);
    println!("  Length: {}", vec.len());
    println!("  File hash: {}", checkpoint1_hash);

    // Step 4-6: Three more operations with flush
    println!("\n--- Phase 3: Three more operations ---");

    // Operation 1: Update some values
    vec.update(2, 100)?;
    vec.update(7, 200)?;
    vec.stamped_flush_with_changes(Stamp::new(3))?;
    println!("✓ Operation 1: Updated index 2→100, 7→200 (stamp 3)");
    println!("  Current data: {:?}", vec.collect());

    // Operation 2: Add more values
    vec.push(20);
    vec.push(21);
    vec.stamped_flush_with_changes(Stamp::new(4))?;
    println!("✓ Operation 2: Added values [20, 21] (stamp 4)");
    println!("  Current data: {:?}", vec.collect());

    // Operation 3: Create a hole and add value
    let reader = vec.create_reader();
    vec.take(5, &reader)?;
    drop(reader);
    vec.push(30);
    vec.stamped_flush_with_changes(Stamp::new(5))?;
    println!("✓ Operation 3: Removed index 5, added value 30 (stamp 5)");
    println!("  Current data: {:?}", vec.collect());
    println!("  Data with holes: {:?}", vec.collect_holed()?);

    // Step 7: Checkpoint 2 - Save hash and data state
    println!("\n--- Checkpoint 2 ---");
    let checkpoint2_data = vec.collect_holed()?;
    let checkpoint2_stamp = vec.stamp();
    let checkpoint2_hash = compute_directory_hash(test_path)?;
    println!("✓ Saved checkpoint2 at stamp {:?}", checkpoint2_stamp);
    println!("  Data: {:?}", checkpoint2_data);
    println!("  Length: {}", vec.len());
    println!("  File hash: {}", checkpoint2_hash);

    // Step 8: Undo last 3 operations
    println!("\n--- Phase 4: Undo last 3 operations ---");
    vec.rollback()?;
    println!("✓ Rollback 1: Now at stamp {:?}", vec.stamp());
    println!("  Current data: {:?}", vec.collect());

    vec.rollback()?;
    println!("✓ Rollback 2: Now at stamp {:?}", vec.stamp());
    println!("  Current data: {:?}", vec.collect());

    vec.rollback()?;
    println!("✓ Rollback 3: Now at stamp {:?}", vec.stamp());
    println!("  Current data: {:?}", vec.collect());

    // Step 9: Verify in-memory data matches checkpoint1
    println!("\n--- Verification 1: After undo (in-memory) ---");
    let after_undo_data = vec.collect_holed()?;
    let after_undo_stamp = vec.stamp();
    println!("In-memory state after rollback:");
    println!(
        "  Stamp: {:?} (expected: {:?})",
        after_undo_stamp, checkpoint1_stamp
    );
    println!("  Data: {:?}", after_undo_data);

    assert_eq!(
        after_undo_stamp, checkpoint1_stamp,
        "Stamp mismatch after undo!"
    );
    assert_eq!(
        after_undo_data, checkpoint1_data,
        "In-memory data mismatch after undo!"
    );
    println!("✓ PASS: In-memory data matches checkpoint1 after undo");

    // Flush and close
    println!("\n--- Step 10: Flush, close, and reopen ---");
    vec.stamped_flush_with_changes(checkpoint1_stamp)?;
    let after_flush_hash = compute_directory_hash(test_path)?;
    println!("✓ Flushed to disk");
    println!("  File hash: {}", after_flush_hash);
    if after_flush_hash != checkpoint1_hash {
        println!(
            "  Note: Hash differs from checkpoint1 ({}) due to region allocation bug",
            checkpoint1_hash
        );
    }

    // Drop the vec to close files
    drop(vec);
    println!("✓ Closed vecdb");

    // Reopen the database
    let options: ImportOptions = (&database, "vec", Version::TWO).into();
    let options = options.with_saved_stamped_changes(10);
    let mut vec: RawVec<usize, u32> = RawVec::forced_import_with(options)?;
    println!("✓ Reopened vecdb");
    println!("  Stamp after reopen: {:?}", vec.stamp());
    println!("  Length after reopen: {}", vec.len());

    // Verify using individual gets
    println!("\n--- Verification 2: After reopen (using gets) ---");
    let reader = vec.create_reader();
    let mut data_via_gets = Vec::new();
    for i in 0..vec.len() {
        let value = vec.get_or_read(i, &reader)?;
        data_via_gets.push(value);
    }
    drop(reader);

    println!("Data read via gets: {:?}", data_via_gets);
    assert_eq!(
        data_via_gets, checkpoint1_data,
        "Data mismatch reading via gets after reopen!"
    );
    println!("✓ PASS: Data correct when reading via gets");

    // Verify using iterator
    println!("\n--- Verification 3: After reopen (using iterator) ---");
    let data_via_iter = vec.collect_holed()?;
    println!("Data read via iterator: {:?}", data_via_iter);
    assert_eq!(
        data_via_iter, checkpoint1_data,
        "Data mismatch reading via iterator after reopen!"
    );
    println!("✓ PASS: Data correct when reading via iterator");

    // Also test the clean iterator (non-holed)
    let data_via_clean_iter: Vec<u32> = vec.collect();
    let expected_clean: Vec<u32> = checkpoint1_data.iter().filter_map(|x| *x).collect();
    println!("Data via clean iterator: {:?}", data_via_clean_iter);
    assert_eq!(
        data_via_clean_iter, expected_clean,
        "Data mismatch via clean iterator!"
    );
    println!("✓ PASS: Data correct when reading via clean iterator");

    println!("\n✓ ALL VERIFICATION PASSED: Rollback + flush + reopen preserved data correctly!");

    // Step 10: Redo the same 3 operations
    println!("\n--- Phase 5: Redo same 3 operations ---");

    // Redo Operation 1: Update same values
    vec.update(2, 100)?;
    vec.update(7, 200)?;
    vec.stamped_flush_with_changes(Stamp::new(3))?;
    println!("✓ Redo operation 1: Updated index 2→100, 7→200 (stamp 3)");
    println!("  Current data: {:?}", vec.collect());

    // Redo Operation 2: Add same values
    vec.push(20);
    vec.push(21);
    vec.stamped_flush_with_changes(Stamp::new(4))?;
    println!("✓ Redo operation 2: Added values [20, 21] (stamp 4)");
    println!("  Current data: {:?}", vec.collect());

    // Redo Operation 3: Create hole and add value
    let reader = vec.create_reader();
    vec.take(5, &reader)?;
    drop(reader);
    vec.push(30);
    vec.stamped_flush_with_changes(Stamp::new(5))?;
    println!("✓ Redo operation 3: Removed index 5, added value 30 (stamp 5)");
    println!("  Current data: {:?}", vec.collect());
    println!("  Data with holes: {:?}", vec.collect_holed()?);

    // Step 11: Verify in-memory data matches checkpoint2
    println!("\n--- Verification 4: After redo (in-memory) ---");
    let after_redo_data = vec.collect_holed()?;
    let after_redo_stamp = vec.stamp();
    println!("In-memory state after redo:");
    println!(
        "  Stamp: {:?} (expected: {:?})",
        after_redo_stamp, checkpoint2_stamp
    );
    println!("  Data: {:?}", after_redo_data);

    assert_eq!(
        after_redo_stamp, checkpoint2_stamp,
        "Stamp mismatch after redo!"
    );
    assert_eq!(
        after_redo_data, checkpoint2_data,
        "In-memory data mismatch after redo!"
    );
    println!("✓ PASS: In-memory data matches checkpoint2 after redo");

    // Flush and close
    println!("\n--- Step 12: Flush, close, and reopen (after redo) ---");
    vec.stamped_flush_with_changes(checkpoint2_stamp)?;
    let after_redo_flush_hash = compute_directory_hash(test_path)?;
    println!("✓ Flushed to disk");
    println!("  File hash: {}", after_redo_flush_hash);
    if after_redo_flush_hash == checkpoint2_hash {
        println!("  ✓ Hash matches checkpoint2 (operations were deterministic)");
    } else {
        println!(
            "  Note: Hash differs from checkpoint2 ({})",
            checkpoint2_hash
        );
    }

    // Drop and reopen
    drop(vec);
    println!("✓ Closed vecdb");

    let options: ImportOptions = (&database, "vec", Version::TWO).into();
    let options = options.with_saved_stamped_changes(10);
    let vec: RawVec<usize, u32> = RawVec::forced_import_with(options)?;
    println!("✓ Reopened vecdb");
    println!("  Stamp after reopen: {:?}", vec.stamp());
    println!("  Length after reopen: {}", vec.len());

    // Verify using individual gets
    println!("\n--- Verification 5: After reopen (using gets) ---");
    let reader = vec.create_reader();
    let mut data_via_gets = Vec::new();
    for i in 0..vec.len() {
        let value = vec.get_or_read(i, &reader)?;
        data_via_gets.push(value);
    }
    drop(reader);

    println!("Data read via gets: {:?}", data_via_gets);
    assert_eq!(
        data_via_gets, checkpoint2_data,
        "Data mismatch reading via gets after reopen!"
    );
    println!("✓ PASS: Data correct when reading via gets");

    // Verify using iterator
    println!("\n--- Verification 6: After reopen (using iterator) ---");
    let data_via_iter = vec.collect_holed()?;
    println!("Data read via iterator: {:?}", data_via_iter);
    assert_eq!(
        data_via_iter, checkpoint2_data,
        "Data mismatch reading via iterator after reopen!"
    );
    println!("✓ PASS: Data correct when reading via iterator");

    // Also test the clean iterator (non-holed)
    let data_via_clean_iter: Vec<u32> = vec.collect();
    let expected_clean: Vec<u32> = checkpoint2_data.iter().filter_map(|x| *x).collect();
    println!("Data via clean iterator: {:?}", data_via_clean_iter);
    assert_eq!(
        data_via_clean_iter, expected_clean,
        "Data mismatch via clean iterator!"
    );
    println!("✓ PASS: Data correct when reading via clean iterator");

    println!("\n✓ ALL VERIFICATION PASSED: Redo + flush + reopen preserved data correctly!");

    println!("\n=== Test Results ===");
    println!("\nData integrity:");
    println!("✓ Rollback + flush + reopen: Data correctly preserved");
    println!("✓ Redo + flush + reopen: Data correctly preserved");
    println!("✓ Gets work correctly after reopen");
    println!("✓ Iterators (holed and clean) work correctly after reopen");
    println!("\nFile layout:");
    println!("  Checkpoint1 hash: {}", checkpoint1_hash);
    println!("  After rollback:   {}", after_flush_hash);
    if after_flush_hash != checkpoint1_hash {
        println!("  ⚠ Hashes differ due to region allocation bug (regions not rolled back)");
    }
    println!("  Checkpoint2 hash: {}", checkpoint2_hash);
    println!("  After redo:       {}", after_redo_flush_hash);
    if after_redo_flush_hash == checkpoint2_hash {
        println!("  ✓ Hashes match (redo is deterministic)");
    }
    println!("\n✓ ALL TESTS PASSED - Data integrity maintained across rollback/redo cycle!");

    Ok(())
}
