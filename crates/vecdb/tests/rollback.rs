use rawdb::Database;
use tempfile::TempDir;
use vecdb::{
    AnyStoredVec, AnyVec, CollectableVec, GenericStoredVec, ImportOptions, Importable, Result,
    Stamp, Version, ZeroCopyVec,
};

#[allow(clippy::upper_case_acronyms)]
type VEC = ZeroCopyVec<usize, u32>;

/// Helper to create a temporary test database
pub fn setup_test_db() -> Result<(Database, TempDir)> {
    let temp_dir = TempDir::new()?;
    let db = Database::open(temp_dir.path())?;
    Ok((db, temp_dir))
}

#[test]
fn test_rollback_comprehensive() -> Result<(), Box<dyn std::error::Error>> {
    let empty_vec: Vec<u32> = vec![];

    let version = Version::TWO;
    let (database, _temp) = setup_test_db()?;
    let mut options: ImportOptions = (&database, "vec", version).into();
    options = options.with_saved_stamped_changes(10);

    println!("\n=== TEST 1: Basic single rollback ===");
    {
        let mut vec: VEC = ZeroCopyVec::forced_import_with(options)?;

        // Initial state: [0, 1, 2, 3, 4]
        for i in 0..5 {
            vec.push(i);
        }
        vec.stamped_flush_with_changes(Stamp::new(1))?;
        println!("After stamp 1: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![0, 1, 2, 3, 4]);
        assert_eq!(vec.stamp(), Stamp::new(1));

        // Modify to [0, 1, 99, 3, 4]
        vec.update(2, 99)?;
        vec.stamped_flush_with_changes(Stamp::new(2))?;
        println!("After stamp 2: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![0, 1, 99, 3, 4]);
        assert_eq!(vec.stamp(), Stamp::new(2));

        // Rollback to stamp 1
        vec.rollback()?;
        println!("After rollback: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![0, 1, 2, 3, 4]);
        assert_eq!(vec.stamp(), Stamp::new(1));

        // Flush the rolled-back state so Test 2 can load it
        vec.stamped_flush_with_changes(Stamp::new(1))?;

        println!("✓ TEST 1 PASSED\n");
    }

    println!("=== TEST 2: Rollback with truncation ===");
    {
        let mut vec: VEC = ZeroCopyVec::forced_import_with(options)?;

        // Start from [0, 1, 2, 3, 4]
        vec.stamped_flush_with_changes(Stamp::new(1))?;
        println!("After stamp 1: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![0, 1, 2, 3, 4]);

        // Add more: [0, 1, 2, 3, 4, 5, 6, 7]
        vec.push(5);
        vec.push(6);
        vec.push(7);
        vec.stamped_flush_with_changes(Stamp::new(2))?;
        println!("After stamp 2: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![0, 1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(vec.stamp(), Stamp::new(2));

        // Rollback - should restore to [0, 1, 2, 3, 4]
        vec.rollback()?;
        println!("After rollback: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![0, 1, 2, 3, 4]);
        assert_eq!(vec.stamp(), Stamp::new(1));

        // Flush for next test
        vec.stamped_flush_with_changes(Stamp::new(1))?;

        println!("✓ TEST 2 PASSED\n");
    }

    println!("=== TEST 3: Multiple sequential rollbacks ===");
    {
        let mut vec: VEC = ZeroCopyVec::forced_import_with(options)?;

        // Stamp 1: [0, 1, 2, 3, 4]
        vec.stamped_flush_with_changes(Stamp::new(1))?;
        println!("Stamp 1: {:?}", vec.collect());

        // Stamp 2: [0, 1, 2, 3, 4, 5]
        vec.push(5);
        vec.stamped_flush_with_changes(Stamp::new(2))?;
        println!("Stamp 2: {:?}", vec.collect());

        // Stamp 3: [0, 1, 2, 3, 4, 5, 6]
        vec.push(6);
        vec.stamped_flush_with_changes(Stamp::new(3))?;
        println!("Stamp 3: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![0, 1, 2, 3, 4, 5, 6]);

        // Rollback to stamp 2
        vec.rollback()?;
        println!("After rollback to 2: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![0, 1, 2, 3, 4, 5]);
        assert_eq!(vec.stamp(), Stamp::new(2));

        // Rollback to stamp 1
        vec.rollback()?;
        println!("After rollback to 1: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![0, 1, 2, 3, 4]);
        assert_eq!(vec.stamp(), Stamp::new(1));

        // Flush for next test
        vec.stamped_flush_with_changes(Stamp::new(1))?;

        println!("✓ TEST 3 PASSED\n");
    }

    println!("=== TEST 4: Rollback then save new state ===");
    {
        let mut vec: VEC = ZeroCopyVec::forced_import_with(options)?;

        // Stamp 1: [0, 1, 2, 3, 4]
        vec.stamped_flush_with_changes(Stamp::new(1))?;

        // Stamp 2: [0, 1, 2, 3, 4, 5]
        vec.push(5);
        vec.stamped_flush_with_changes(Stamp::new(2))?;
        println!("Stamp 2: {:?}", vec.collect());

        // Rollback to stamp 1
        vec.rollback()?;
        println!("After rollback: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![0, 1, 2, 3, 4]);

        // Now save a different state 2: [0, 1, 2, 3, 4, 99]
        vec.push(99);
        vec.stamped_flush_with_changes(Stamp::new(2))?;
        println!("New stamp 2: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![0, 1, 2, 3, 4, 99]);
        assert_eq!(vec.stamp(), Stamp::new(2));

        println!("✓ TEST 4 PASSED\n");
    }

    println!("=== TEST 5: Complex blockchain reorg scenario ===");
    {
        let mut vec: VEC = ZeroCopyVec::forced_import_with(options)?;

        // Reset to clean state
        vec.reset()?;
        for i in 0..10 {
            vec.push(i * 10); // [0, 10, 20, 30, 40, 50, 60, 70, 80, 90]
        }
        vec.stamped_flush_with_changes(Stamp::new(1))?;
        println!("Block 1: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![0, 10, 20, 30, 40, 50, 60, 70, 80, 90]);

        // Block 2: Update some existing + push new
        vec.update(0, 5)?; // Change first element
        vec.update(3, 35)?; // Change middle element
        vec.push(100);
        vec.push(110);
        vec.stamped_flush_with_changes(Stamp::new(2))?;
        println!("Block 2: {:?}", vec.collect());
        assert_eq!(
            vec.collect(),
            vec![5, 10, 20, 35, 40, 50, 60, 70, 80, 90, 100, 110]
        );

        // Block 3: Delete some items (create holes)
        let reader = vec.create_reader();
        vec.take(1, &reader)?;
        vec.take(4, &reader)?;
        drop(reader);
        vec.push(120);
        vec.stamped_flush_with_changes(Stamp::new(3))?;
        println!("Block 3: {:?}", vec.collect());
        assert_eq!(
            vec.collect(),
            vec![5, 20, 35, 50, 60, 70, 80, 90, 100, 110, 120]
        );

        // Block 4: Mix of updates, holes, and pushes
        vec.update(0, 999)?; // Update first
        let reader = vec.create_reader();
        vec.take(5, &reader)?; // Delete index 5 (removes value 50)
        drop(reader);
        vec.push(130);
        vec.push(140);
        vec.stamped_flush_with_changes(Stamp::new(4))?;
        println!("Block 4: {:?}", vec.collect());
        assert_eq!(
            vec.collect(),
            vec![999, 20, 35, 60, 70, 80, 90, 100, 110, 120, 130, 140]
        ); // 50 is gone

        // Block 5: Update same indices multiple times
        vec.update(0, 1000)?;
        vec.update(2, 1035)?;
        vec.push(150);
        vec.stamped_flush_with_changes(Stamp::new(5))?;
        println!("Block 5: {:?}", vec.collect());
        // Holes at 1,4,5. Values: 0→1000, 2→1035, 3→35, 6→60, 7→70, 8→80, 9→90, 10→100, 11→110, 12→120, 13→130, 14→140, 15→150
        assert_eq!(
            vec.collect(),
            vec![1000, 1035, 35, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150]
        );

        // Block 6: More complex operations
        vec.update(0, 2000)?;
        vec.update(3, 2050)?;
        let reader = vec.create_reader();
        vec.take(8, &reader)?; // Delete index 8 (value 80)
        drop(reader);
        vec.push(160);
        vec.stamped_flush_with_changes(Stamp::new(6))?;
        println!("Block 6: {:?}", vec.collect());
        // Holes at 1,4,5,8. Values: 0→2000, 2→1035, 3→2050, 6→60, 7→70, 9→90, 10→100, 11→110, 12→120, 13→130, 14→140, 15→150, 16→160
        assert_eq!(
            vec.collect(),
            vec![
                2000, 1035, 2050, 60, 70, 90, 100, 110, 120, 130, 140, 150, 160
            ]
        );

        // Block 7: Continue the main chain
        vec.update(0, 3000)?;
        vec.push(170);
        vec.push(180);
        vec.stamped_flush_with_changes(Stamp::new(7))?;
        println!("Block 7 (main chain): {:?}", vec.collect());
        // Holes at 1,4,5,8. Values: 0→3000, 2→1035, 3→2050, 6→60, 7→70, 9→90, 10→100, 11→110, 12→120, 13→130, 14→140, 15→150, 16→160, 17→170, 18→180
        assert_eq!(
            vec.collect(),
            vec![
                3000, 1035, 2050, 60, 70, 90, 100, 110, 120, 130, 140, 150, 160, 170, 180
            ]
        );

        // === REORG: Rollback to block 4 and create alternative chain ===
        println!("\n--- REORG: Rolling back to block 4 ---");
        vec.rollback_before(Stamp::new(5))?;
        println!("After rollback to 4: {:?}", vec.collect());
        // Should match Block 4 state: holes at 1,4,5
        assert_eq!(
            vec.collect(),
            vec![999, 20, 35, 60, 70, 80, 90, 100, 110, 120, 130, 140]
        );
        assert_eq!(vec.stamp(), Stamp::new(4));

        // Fork A - Block 5: Different operations than original
        vec.update(0, 5000)?; // Different update
        vec.update(2, 5035)?;
        let reader = vec.create_reader();
        vec.take(10, &reader)?; // Delete storage index 10 (value 100)
        drop(reader);
        vec.push(5150);
        vec.stamped_flush_with_changes(Stamp::new(5))?;
        println!("Fork A Block 5: {:?}", vec.collect());
        // Holes at 1,4,5,10. Values: 0→5000, 2→5035, 3→35, 6→60, 7→70, 8→80, 9→90, 11→110, 12→120, 13→130, 14→140, 15→5150
        assert_eq!(
            vec.collect(),
            vec![5000, 5035, 35, 60, 70, 80, 90, 110, 120, 130, 140, 5150]
        );

        // Fork A - Block 6: Continue with more changes
        vec.update(0, 6000)?;
        vec.update(2, 6020)?;
        vec.push(6160);
        vec.push(6170);
        vec.stamped_flush_with_changes(Stamp::new(6))?;
        println!("Fork A Block 6: {:?}", vec.collect());
        // Holes at 1,4,5,10. Values: 0→6000, 2→6020, 3→35, 6→60, 7→70, 8→80, 9→90, 11→110, 12→120, 13→130, 14→140, 15→5150, 16→6160, 17→6170
        assert_eq!(
            vec.collect(),
            vec![
                6000, 6020, 35, 60, 70, 80, 90, 110, 120, 130, 140, 5150, 6160, 6170
            ]
        );

        // === ANOTHER REORG: Rollback to block 4 again ===
        println!("\n--- ANOTHER REORG: Rolling back to block 4 again ---");
        vec.rollback_before(Stamp::new(5))?;
        println!("After second rollback to 4: {:?}", vec.collect());
        assert_eq!(
            vec.collect(),
            vec![999, 20, 35, 60, 70, 80, 90, 100, 110, 120, 130, 140]
        );
        assert_eq!(vec.stamp(), Stamp::new(4));

        // Fork B - Block 5: Yet another alternative
        vec.update(0, 7000)?;
        vec.update(2, 7020)?; // Update index 2, not 1 (which is a hole)
        vec.update(3, 7035)?;
        let reader = vec.create_reader();
        vec.take(6, &reader)?; // Delete index 6 (value 60)
        vec.take(9, &reader)?; // Delete index 9 (value 90)
        drop(reader);
        vec.push(7150);
        vec.stamped_flush_with_changes(Stamp::new(5))?;
        println!("Fork B Block 5: {:?}", vec.collect());
        // Holes at 1,4,5,6,9. Values: 0→7000, 2→7020, 3→7035, 7→70, 8→80, 10→100, 11→110, 12→120, 13→130, 14→140, 15→7150
        assert_eq!(
            vec.collect(),
            vec![7000, 7020, 7035, 70, 80, 100, 110, 120, 130, 140, 7150]
        );

        // Fork B - Block 6
        vec.update(0, 8000)?;
        vec.push(8160);
        vec.stamped_flush_with_changes(Stamp::new(6))?;
        println!("Fork B Block 6: {:?}", vec.collect());
        // Holes at 1,4,5,6,9. Values: 0→8000, 2→7020, 3→7035, 7→70, 8→80, 10→100, 11→110, 12→120, 13→130, 14→140, 15→7150, 16→8160
        assert_eq!(
            vec.collect(),
            vec![
                8000, 7020, 7035, 70, 80, 100, 110, 120, 130, 140, 7150, 8160
            ]
        );

        // === Rollback to block 3 (deeper reorg) ===
        println!("\n--- DEEPER REORG: Rolling back to block 3 ---");
        vec.rollback_before(Stamp::new(4))?;
        println!("After rollback to 3: {:?}", vec.collect());
        assert_eq!(
            vec.collect(),
            vec![5, 20, 35, 50, 60, 70, 80, 90, 100, 110, 120]
        );
        assert_eq!(vec.stamp(), Stamp::new(3));

        // Fork C - Build new chain from block 3
        vec.update(0, 9000)?;
        vec.update(10, 9120)?; // Update storage[10] (value 100)
        vec.push(9999);
        vec.stamped_flush_with_changes(Stamp::new(4))?;
        println!("Fork C Block 4: {:?}", vec.collect());
        assert_eq!(
            vec.collect(),
            vec![9000, 20, 35, 50, 60, 70, 80, 90, 9120, 110, 120, 9999]
        );

        println!("✓ TEST 5 PASSED (COMPLEX BLOCKCHAIN REORG)\n");
    }

    println!("=== TEST 6: Rollback with updates ===");
    {
        let mut vec: VEC = ZeroCopyVec::forced_import_with(options)?;

        // Reset to clean state
        vec.reset()?;
        for i in 0..5 {
            vec.push(i);
        }

        // Stamp 1: [0, 1, 2, 3, 4]
        vec.stamped_flush_with_changes(Stamp::new(1))?;
        println!("Stamp 1: {:?}", vec.collect());

        // Stamp 2: [0, 99, 2, 88, 4] - update multiple values
        vec.update(1, 99)?;
        vec.update(3, 88)?;
        vec.stamped_flush_with_changes(Stamp::new(2))?;
        println!("Stamp 2: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![0, 99, 2, 88, 4]);

        // Rollback to stamp 1 - should restore original values
        vec.rollback()?;
        println!("After rollback: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![0, 1, 2, 3, 4]);
        assert_eq!(vec.stamp(), Stamp::new(1));

        println!("✓ TEST 6 PASSED\n");
    }

    println!("=== TEST 7: Rollback with holes ===");
    {
        let mut vec: VEC = ZeroCopyVec::forced_import_with(options)?;

        // Reset to clean state
        vec.reset()?;
        for i in 0..5 {
            vec.push(i);
        }

        // Stamp 1: [0, 1, 2, 3, 4]
        vec.stamped_flush_with_changes(Stamp::new(1))?;
        println!("Stamp 1: {:?}", vec.collect());

        // Stamp 2: delete some items (creating holes)
        let reader = vec.create_reader();
        vec.take(1, &reader)?;
        vec.take(3, &reader)?;
        drop(reader);
        vec.stamped_flush_with_changes(Stamp::new(2))?;
        println!("Stamp 2: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![0, 2, 4]);

        // Rollback to stamp 1 - should restore deleted items
        vec.rollback()?;
        println!("After rollback: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![0, 1, 2, 3, 4]);
        assert_eq!(vec.stamp(), Stamp::new(1));

        println!("✓ TEST 7 PASSED\n");
    }

    println!("=== TEST 8: Rollback with truncation + updates ===");
    {
        let mut vec: VEC = ZeroCopyVec::forced_import_with(options)?;

        // Reset to clean state
        vec.reset()?;
        for i in 0..5 {
            vec.push(i);
        }

        // Stamp 1: [0, 1, 2, 3, 4]
        vec.stamped_flush_with_changes(Stamp::new(1))?;
        println!("Stamp 1: {:?}", vec.collect());

        // Stamp 2: extend + update
        // [0, 99, 2, 3, 4, 5, 6]
        vec.update(1, 99)?;
        vec.push(5);
        vec.push(6);
        vec.stamped_flush_with_changes(Stamp::new(2))?;
        println!("Stamp 2: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![0, 99, 2, 3, 4, 5, 6]);

        // Rollback - should restore length AND value
        vec.rollback()?;
        println!("After rollback: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![0, 1, 2, 3, 4]);
        assert_eq!(vec.stamp(), Stamp::new(1));

        println!("✓ TEST 8 PASSED\n");
    }

    println!("=== TEST 9: Rollback with holes + updates ===");
    {
        let mut vec: VEC = ZeroCopyVec::forced_import_with(options)?;

        // Reset to clean state
        vec.reset()?;
        for i in 0..5 {
            vec.push(i);
        }

        // Stamp 1: [0, 1, 2, 3, 4]
        vec.stamped_flush_with_changes(Stamp::new(1))?;
        println!("Stamp 1: {:?}", vec.collect());

        // Stamp 2: delete + update
        // [0, None, 99, 3, 4]
        let reader = vec.create_reader();
        vec.take(1, &reader)?;
        vec.update(2, 99)?;
        drop(reader);
        vec.stamped_flush_with_changes(Stamp::new(2))?;
        println!("Stamp 2: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![0, 99, 3, 4]);

        // Rollback - should restore deleted item AND original value
        vec.rollback()?;
        println!("After rollback: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![0, 1, 2, 3, 4]);
        assert_eq!(vec.stamp(), Stamp::new(1));

        println!("✓ TEST 9 PASSED\n");
    }

    println!("=== TEST 10: Multiple updates to same index ===");
    {
        let mut vec: VEC = ZeroCopyVec::forced_import_with(options)?;

        // Reset to clean state
        vec.reset()?;
        for i in 0..5 {
            vec.push(i);
        }

        // Stamp 1: [0, 1, 2, 3, 4]
        vec.stamped_flush_with_changes(Stamp::new(1))?;
        println!("Stamp 1: {:?}", vec.collect());

        // Stamp 2: [100, 1, 2, 3, 4]
        vec.update(0, 100)?;
        vec.stamped_flush_with_changes(Stamp::new(2))?;
        println!("Stamp 2: {:?}", vec.collect());

        // Stamp 3: [200, 1, 2, 3, 4]
        vec.update(0, 200)?;
        vec.stamped_flush_with_changes(Stamp::new(3))?;
        println!("Stamp 3: {:?}", vec.collect());

        // Stamp 4: [300, 1, 2, 3, 4]
        vec.update(0, 300)?;
        vec.stamped_flush_with_changes(Stamp::new(4))?;
        println!("Stamp 4: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![300, 1, 2, 3, 4]);

        // Rollback to stamp 3
        vec.rollback()?;
        println!("After rollback to 3: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![200, 1, 2, 3, 4]);

        // Rollback to stamp 2
        vec.rollback()?;
        println!("After rollback to 2: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![100, 1, 2, 3, 4]);

        // Rollback to stamp 1
        vec.rollback()?;
        println!("After rollback to 1: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![0, 1, 2, 3, 4]);

        println!("✓ TEST 10 PASSED\n");
    }

    println!("=== TEST 11: Complex mixed operations in one stamp ===");
    {
        let mut vec: VEC = ZeroCopyVec::forced_import_with(options)?;

        // Reset to clean state
        vec.reset()?;
        for i in 0..10 {
            vec.push(i);
        }

        // Stamp 1: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        vec.stamped_flush_with_changes(Stamp::new(1))?;
        println!("Stamp 1: {:?}", vec.collect());

        // Stamp 2: Complex operations
        // - Delete indices 1, 3, 5
        // - Update indices 2, 6, 8
        // - Push new values 100, 101
        let reader = vec.create_reader();
        vec.take(1, &reader)?;
        vec.take(3, &reader)?;
        vec.take(5, &reader)?;
        drop(reader);
        vec.update(2, 222)?;
        vec.update(6, 666)?;
        vec.update(8, 888)?;
        vec.push(100);
        vec.push(101);
        vec.stamped_flush_with_changes(Stamp::new(2))?;
        println!("Stamp 2: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![0, 222, 4, 666, 7, 888, 9, 100, 101]);

        // Rollback - should restore everything
        vec.rollback()?;
        println!("After rollback: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);

        println!("✓ TEST 11 PASSED\n");
    }

    println!("=== TEST 12: Rollback to empty state ===");
    {
        let mut vec: VEC = ZeroCopyVec::forced_import_with(options)?;

        // Reset to empty state
        vec.reset()?;

        // Stamp 1: []
        vec.stamped_flush_with_changes(Stamp::new(1))?;
        println!("Stamp 1: {:?} (empty)", vec.collect());
        assert_eq!(vec.collect(), empty_vec);

        // Stamp 2: [0, 1, 2]
        vec.push(0);
        vec.push(1);
        vec.push(2);
        vec.stamped_flush_with_changes(Stamp::new(2))?;
        println!("Stamp 2: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![0, 1, 2]);

        // Rollback to empty
        vec.rollback()?;
        println!("After rollback: {:?} (empty)", vec.collect());
        assert_eq!(vec.collect(), empty_vec);

        println!("✓ TEST 12 PASSED\n");
    }

    println!("=== TEST 13: Deep rollback chain ===");
    {
        let mut vec: VEC = ZeroCopyVec::forced_import_with(options)?;

        // Reset to clean state
        vec.reset()?;

        // Build a chain of 10 stamps with different operations
        vec.stamped_flush_with_changes(Stamp::new(1))?; // []

        vec.push(0);
        vec.stamped_flush_with_changes(Stamp::new(2))?; // [0]

        vec.push(1);
        vec.stamped_flush_with_changes(Stamp::new(3))?; // [0, 1]

        vec.update(0, 10)?;
        vec.stamped_flush_with_changes(Stamp::new(4))?; // [10, 1]

        vec.push(2);
        vec.stamped_flush_with_changes(Stamp::new(5))?; // [10, 1, 2]

        let reader = vec.create_reader();
        vec.take(1, &reader)?;
        drop(reader);
        vec.stamped_flush_with_changes(Stamp::new(6))?; // [10, 2]

        vec.push(3);
        vec.stamped_flush_with_changes(Stamp::new(7))?; // [10, 2, 3]

        vec.update(0, 20)?;
        vec.stamped_flush_with_changes(Stamp::new(8))?; // [20, 2, 3]

        vec.push(4);
        vec.push(5);
        vec.stamped_flush_with_changes(Stamp::new(9))?; // [20, 2, 3, 4, 5]

        vec.update(2, 33)?; // Update storage[2] (value 2 → 33)
        vec.stamped_flush_with_changes(Stamp::new(10))?; // [20, 33, 3, 4, 5]
        println!("Stamp 10: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![20, 33, 3, 4, 5]);

        // Rollback through the chain
        vec.rollback()?; // -> 9
        assert_eq!(vec.collect(), vec![20, 2, 3, 4, 5]);

        vec.rollback()?; // -> 8
        assert_eq!(vec.collect(), vec![20, 2, 3]);

        vec.rollback()?; // -> 7
        assert_eq!(vec.collect(), vec![10, 2, 3]);

        vec.rollback()?; // -> 6
        assert_eq!(vec.collect(), vec![10, 2]);

        vec.rollback()?; // -> 5
        assert_eq!(vec.collect(), vec![10, 1, 2]);

        vec.rollback()?; // -> 4
        assert_eq!(vec.collect(), vec![10, 1]);

        vec.rollback()?; // -> 3
        assert_eq!(vec.collect(), vec![0, 1]);

        vec.rollback()?; // -> 2
        assert_eq!(vec.collect(), vec![0]);

        vec.rollback()?; // -> 1
        println!("After rollback to 1: {:?} (empty)", vec.collect());
        assert_eq!(vec.collect(), empty_vec);

        println!("✓ TEST 13 PASSED\n");
    }

    println!("=== TEST 14: Rollback with all elements updated ===");
    {
        let mut vec: VEC = ZeroCopyVec::forced_import_with(options)?;

        // Reset to clean state
        vec.reset()?;
        for i in 0..5 {
            vec.push(i);
        }

        // Stamp 1: [0, 1, 2, 3, 4]
        vec.stamped_flush_with_changes(Stamp::new(1))?;
        println!("Stamp 1: {:?}", vec.collect());

        // Stamp 2: Update ALL elements
        for i in 0..5 {
            vec.update(i, (i * 100) as u32)?;
        }
        vec.stamped_flush_with_changes(Stamp::new(2))?;
        println!("Stamp 2: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![0, 100, 200, 300, 400]);

        // Rollback - should restore all original values
        vec.rollback()?;
        println!("After rollback: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![0, 1, 2, 3, 4]);

        println!("✓ TEST 14 PASSED\n");
    }

    println!("=== TEST 15: Multiple holes then rollback ===");
    {
        let mut vec: VEC = ZeroCopyVec::forced_import_with(options)?;

        // Reset to clean state
        vec.reset()?;
        for i in 0..10 {
            vec.push(i);
        }

        // Stamp 1: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        vec.stamped_flush_with_changes(Stamp::new(1))?;
        println!("Stamp 1: {:?}", vec.collect());

        // Stamp 2: Delete every other element
        let reader = vec.create_reader();
        for i in (0..10).step_by(2) {
            vec.take(i, &reader)?;
        }
        drop(reader);
        vec.stamped_flush_with_changes(Stamp::new(2))?;
        println!("Stamp 2: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![1, 3, 5, 7, 9]);

        // Rollback - should restore all deleted items
        vec.rollback()?;
        println!("After rollback: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);

        println!("✓ TEST 15 PASSED\n");
    }

    println!("=== TEST 16: Update same index multiple times before flush ===");
    {
        let mut vec: VEC = ZeroCopyVec::forced_import_with(options)?;

        // Reset to clean state
        vec.reset()?;
        for i in 0..5 {
            vec.push(i);
        }

        // Stamp 1: [0, 1, 2, 3, 4]
        vec.stamped_flush_with_changes(Stamp::new(1))?;
        println!("Stamp 1: {:?}", vec.collect());

        // Stamp 2: Update index 2 multiple times in memory
        vec.update(2, 100)?;
        vec.update(2, 200)?;
        vec.update(2, 300)?;
        vec.stamped_flush_with_changes(Stamp::new(2))?;
        println!("Stamp 2: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![0, 1, 300, 3, 4]);

        // Rollback - should restore original value
        vec.rollback()?;
        println!("After rollback: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![0, 1, 2, 3, 4]);

        println!("✓ TEST 16 PASSED\n");
    }

    println!("=== TEST 17: Complex blockchain fork scenario ===");
    {
        let mut vec: VEC = ZeroCopyVec::forced_import_with(options)?;

        // Reset to clean state
        vec.reset()?;
        for i in 0..5 {
            vec.push(i);
        }

        // Main chain: stamps 1-5
        vec.stamped_flush_with_changes(Stamp::new(1))?; // [0, 1, 2, 3, 4]

        vec.push(5);
        vec.stamped_flush_with_changes(Stamp::new(2))?; // [0, 1, 2, 3, 4, 5]

        vec.push(6);
        vec.stamped_flush_with_changes(Stamp::new(3))?; // [0, 1, 2, 3, 4, 5, 6]

        vec.push(7);
        vec.stamped_flush_with_changes(Stamp::new(4))?; // [0, 1, 2, 3, 4, 5, 6, 7]

        vec.push(8);
        vec.stamped_flush_with_changes(Stamp::new(5))?; // [0, 1, 2, 3, 4, 5, 6, 7, 8]
        println!("Main chain stamp 5: {:?}", vec.collect());

        // Fork A: rollback to 3, create alternative stamps 4-6
        vec.rollback_before(Stamp::new(4))?;
        assert_eq!(vec.collect(), vec![0, 1, 2, 3, 4, 5, 6]);

        vec.update(6, 666)?;
        vec.stamped_flush_with_changes(Stamp::new(4))?; // [0, 1, 2, 3, 4, 5, 666]

        vec.push(77);
        vec.stamped_flush_with_changes(Stamp::new(5))?; // [0, 1, 2, 3, 4, 5, 666, 77]

        vec.push(88);
        vec.stamped_flush_with_changes(Stamp::new(6))?; // [0, 1, 2, 3, 4, 5, 666, 77, 88]
        println!("Fork A stamp 6: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![0, 1, 2, 3, 4, 5, 666, 77, 88]);

        // Fork B: rollback to 3 again, create different stamps 4-5
        vec.rollback_before(Stamp::new(4))?;
        assert_eq!(vec.collect(), vec![0, 1, 2, 3, 4, 5, 6]);

        vec.update(5, 555)?;
        vec.stamped_flush_with_changes(Stamp::new(4))?; // [0, 1, 2, 3, 4, 555, 6]

        vec.push(99);
        vec.stamped_flush_with_changes(Stamp::new(5))?; // [0, 1, 2, 3, 4, 555, 6, 99]
        println!("Fork B stamp 5: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![0, 1, 2, 3, 4, 555, 6, 99]);

        // Rollback to stamp 3 one more time
        vec.rollback_before(Stamp::new(4))?;
        println!("Final rollback to 3: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![0, 1, 2, 3, 4, 5, 6]);

        println!("✓ TEST 17 PASSED (COMPLEX BLOCKCHAIN FORK)\n");
    }

    println!("=== TEST 18: Large-scale rollback ===");
    {
        let mut vec: VEC = ZeroCopyVec::forced_import_with(options)?;

        // Reset to clean state
        vec.reset()?;

        // Stamp 1: 1000 elements
        for i in 0..1000 {
            vec.push(i);
        }
        vec.stamped_flush_with_changes(Stamp::new(1))?;
        println!("Stamp 1: {} elements", vec.len());
        assert_eq!(vec.len(), 1000);

        // Stamp 2: Update half of them
        for i in (0..1000).step_by(2) {
            vec.update(i, i as u32 + 10000)?;
        }
        vec.stamped_flush_with_changes(Stamp::new(2))?;
        println!("Stamp 2: {} elements (half updated)", vec.len());
        let reader = vec.create_reader();
        assert_eq!(vec.read_at(0, &reader)?, 10000);
        assert_eq!(vec.read_at(1, &reader)?, 1);
        drop(reader);

        // Rollback
        vec.rollback()?;
        println!("After rollback: {} elements", vec.len());
        let reader = vec.create_reader();
        assert_eq!(vec.len(), 1000);
        // After rollback, state is dirty - use get_any_or_read() to check updated map
        assert_eq!(vec.get_any_or_read(0, &reader)?, Some(0));
        assert_eq!(vec.get_any_or_read(1, &reader)?, Some(1));
        assert_eq!(vec.get_any_or_read(999, &reader)?, Some(999));
        drop(reader);

        println!("✓ TEST 18 PASSED (LARGE-SCALE)\n");
    }

    println!("=== TEST 19: Holes + truncation combination ===");
    {
        let mut vec: VEC = ZeroCopyVec::forced_import_with(options)?;

        // Reset to clean state
        vec.reset()?;
        for i in 0..10 {
            vec.push(i);
        }

        // Stamp 1: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        vec.stamped_flush_with_changes(Stamp::new(1))?;
        println!("Stamp 1: {:?}", vec.collect());

        // Stamp 2: Delete some items, then extend
        let reader = vec.create_reader();
        vec.take(2, &reader)?;
        vec.take(5, &reader)?;
        vec.take(7, &reader)?;
        drop(reader);
        vec.push(100);
        vec.push(101);
        vec.push(102);
        vec.stamped_flush_with_changes(Stamp::new(2))?;
        println!("Stamp 2: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![0, 1, 3, 4, 6, 8, 9, 100, 101, 102]);

        // Rollback - should restore deleted items and remove pushed ones
        vec.rollback()?;
        println!("After rollback: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);

        println!("✓ TEST 19 PASSED\n");
    }

    println!("=== TEST 20: Rollback after reads via iterator ===");
    {
        let mut vec: VEC = ZeroCopyVec::forced_import_with(options)?;

        // Reset to clean state
        vec.reset()?;
        for i in 0..5 {
            vec.push(i);
        }

        // Stamp 1: [0, 1, 2, 3, 4]
        vec.stamped_flush_with_changes(Stamp::new(1))?;
        println!("Stamp 1: {:?}", vec.collect());

        // Read via iterator
        let sum1: u32 = vec.iter()?.sum();
        println!("Sum before change: {}", sum1);
        assert_eq!(sum1, 10);

        // Stamp 2: [0, 10, 20, 30, 40]
        for i in 0..5 {
            vec.update(i, i as u32 * 10)?;
        }
        vec.stamped_flush_with_changes(Stamp::new(2))?;
        println!("Stamp 2: {:?}", vec.collect());

        // Read via iterator again
        let sum2: u32 = vec.iter()?.sum();
        println!("Sum after change: {}", sum2);
        assert_eq!(sum2, 100);

        // Rollback
        vec.rollback()?;
        println!("After rollback: {:?}", vec.collect());
        assert_eq!(vec.collect(), vec![0, 1, 2, 3, 4]);

        // Read via iterator after rollback
        let sum3: u32 = vec.iter()?.sum();
        println!("Sum after rollback: {}", sum3);
        assert_eq!(sum3, 10);

        println!("✓ TEST 20 PASSED\n");
    }

    println!("=== ALL {} TESTS PASSED ===", 20);
    Ok(())
}
