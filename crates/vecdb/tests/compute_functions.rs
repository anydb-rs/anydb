use rawdb::Database;
use tempfile::TempDir;
use vecdb::{AnyStoredVec, CollectableVec, EagerVec, Exit, GenericStoredVec, Result, Version};

/// Helper to create a temporary test database
fn setup_test_db() -> Result<(Database, TempDir)> {
    let temp_dir = TempDir::new()?;
    let db = Database::open(temp_dir.path())?;
    Ok((db, temp_dir))
}

/// Helper to assert f32 values are approximately equal
fn assert_f32_eq(actual: f32, expected: f32, tolerance: f32, message: &str) {
    assert!(
        (actual - expected).abs() < tolerance,
        "{}: expected {}, got {} (diff: {})",
        message,
        expected,
        actual,
        (actual - expected).abs()
    );
}

#[test]
fn test_compute_sum_of_others() -> Result<()> {
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    // Create source vectors
    let mut vec1: EagerVec<usize, u64> =
        EagerVec::forced_import_compressed(&db, "vec1", Version::ONE)?;
    let mut vec2: EagerVec<usize, u64> =
        EagerVec::forced_import_compressed(&db, "vec2", Version::ONE)?;
    let mut vec3: EagerVec<usize, u64> =
        EagerVec::forced_import_compressed(&db, "vec3", Version::ONE)?;

    // Fill with test data
    for i in 0..10 {
        vec1.forced_push(i, (i * 10) as u64)?;
        vec2.forced_push(i, (i * 5) as u64)?;
        vec3.forced_push(i, i as u64)?;
    }
    vec1.safe_flush(&exit)?;
    vec2.safe_flush(&exit)?;
    vec3.safe_flush(&exit)?;

    // Compute sum of others
    let mut result: EagerVec<usize, u64> =
        EagerVec::forced_import_compressed(&db, "result", Version::ONE)?;
    result.compute_sum_of_others(0, &[&vec1, &vec2, &vec3], &exit)?;
    result.safe_flush(&exit)?;

    // Verify results
    for i in 0..10 {
        let expected = ((i * 10) + (i * 5) + i) as u64;
        let actual = result.read_at_unwrap_once(i);
        assert_eq!(
            actual, expected,
            "Sum mismatch at index {}: expected {}, got {}",
            i, expected, actual
        );
    }

    Ok(())
}

#[test]
fn test_compute_min_of_others() -> Result<()> {
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut vec1: EagerVec<usize, u64> =
        EagerVec::forced_import_compressed(&db, "vec1", Version::ONE)?;
    let mut vec2: EagerVec<usize, u64> =
        EagerVec::forced_import_compressed(&db, "vec2", Version::ONE)?;
    let mut vec3: EagerVec<usize, u64> =
        EagerVec::forced_import_compressed(&db, "vec3", Version::ONE)?;

    // Test data: [50, 51, 52...], [10, 11, 12...], [100, 101, 102...]
    for i in 0..10 {
        vec1.forced_push(i, (50 + i) as u64)?;
        vec2.forced_push(i, (10 + i) as u64)?;
        vec3.forced_push(i, (100 + i) as u64)?;
    }
    vec1.safe_flush(&exit)?;
    vec2.safe_flush(&exit)?;
    vec3.safe_flush(&exit)?;

    let mut result: EagerVec<usize, u64> =
        EagerVec::forced_import_compressed(&db, "result", Version::ONE)?;
    result.compute_min_of_others(0, &[&vec1, &vec2, &vec3], &exit)?;
    result.safe_flush(&exit)?;

    // Minimum should always be from vec2
    for i in 0..10 {
        let expected = (10 + i) as u64;
        let actual = result.read_at_unwrap_once(i);
        assert_eq!(
            actual, expected,
            "Min mismatch at index {}: expected {}, got {}",
            i, expected, actual
        );
    }

    Ok(())
}

#[test]
fn test_compute_max_of_others() -> Result<()> {
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut vec1: EagerVec<usize, u64> =
        EagerVec::forced_import_compressed(&db, "vec1", Version::ONE)?;
    let mut vec2: EagerVec<usize, u64> =
        EagerVec::forced_import_compressed(&db, "vec2", Version::ONE)?;
    let mut vec3: EagerVec<usize, u64> =
        EagerVec::forced_import_compressed(&db, "vec3", Version::ONE)?;

    for i in 0..10 {
        vec1.forced_push(i, (50 + i) as u64)?;
        vec2.forced_push(i, (10 + i) as u64)?;
        vec3.forced_push(i, (100 + i) as u64)?;
    }
    dbg!(vec1.collect(), vec1.region());
    dbg!(vec2.collect(), vec2.region());
    dbg!(vec3.collect(), vec3.region());
    vec1.safe_flush(&exit)?;
    vec2.safe_flush(&exit)?;
    vec3.safe_flush(&exit)?;
    dbg!(vec1.collect(), vec1.region());
    dbg!(vec2.collect(), vec2.region());
    dbg!(vec3.collect(), vec3.region());

    let mut result: EagerVec<usize, u64> =
        EagerVec::forced_import_compressed(&db, "result", Version::ONE)?;
    result.compute_max_of_others(0, &[&vec1, &vec2, &vec3], &exit)?;
    result.safe_flush(&exit)?;

    // Maximum should always be from vec3
    for i in 0..10 {
        let expected = (100 + i) as u64;
        let actual = result.read_at_unwrap_once(i);
        assert_eq!(
            actual, expected,
            "Max mismatch at index {}: expected {}, got {}",
            i, expected, actual
        );
    }

    Ok(())
}

#[test]
fn test_compute_previous_value() -> Result<()> {
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut source: EagerVec<usize, u16> =
        EagerVec::forced_import_compressed(&db, "source", Version::ONE)?;

    // Fill with test data: [10, 20, 30, 40, 50]
    for i in 0..5 {
        source.forced_push(i, ((i + 1) * 10) as u16)?;
    }
    source.safe_flush(&exit)?;

    let mut result: EagerVec<usize, f32> =
        EagerVec::forced_import_compressed(&db, "result", Version::ONE)?;
    result.compute_previous_value(0, &source, 1, &exit)?;
    result.safe_flush(&exit)?;

    // Check that each element is the previous value
    // Index 0: should be NaN (no previous exists)
    // Index 1: should be 10.0 (previous of 20)
    // Index 2: should be 20.0 (previous of 30)
    let actual_0 = result.read_at_unwrap_once(0);
    assert!(
        actual_0.is_nan(),
        "First element should be NaN when no previous value exists"
    );

    let expected = [10.0, 20.0, 30.0, 40.0];
    for (i, v) in expected.into_iter().enumerate() {
        let actual = result.read_at_unwrap_once(i + 1);
        assert_eq!(
            actual,
            v,
            "Previous value mismatch at index {}: expected {}, got {}",
            i + 1,
            v,
            actual
        );
    }

    Ok(())
}

#[test]
fn test_compute_change() -> Result<()> {
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut source: EagerVec<usize, u32> =
        EagerVec::forced_import_compressed(&db, "source", Version::ONE)?;

    // Fill with test data: [10, 20, 25, 30, 50]
    let values = [10, 20, 25, 30, 50];
    for (i, &v) in values.iter().enumerate() {
        source.forced_push(i, v)?;
    }
    source.safe_flush(&exit)?;

    let mut result: EagerVec<usize, u32> =
        EagerVec::forced_import_compressed(&db, "result", Version::ONE)?;
    result.compute_change(0, &source, 1, &exit)?;
    result.safe_flush(&exit)?;

    // Check changes
    // Index 0: 0 (no previous value, so no change)
    // Index 1: 20 - 10 = 10
    // Index 2: 25 - 20 = 5
    // Index 3: 30 - 25 = 5
    // Index 4: 50 - 30 = 20
    let expected = [0, 10, 5, 5, 20];
    for (i, v) in expected.into_iter().enumerate() {
        let actual = result.read_at_unwrap_once(i);
        assert_eq!(
            actual, v,
            "Change mismatch at index {}: expected {}, got {}",
            i, v, actual
        );
    }

    Ok(())
}

#[test]
fn test_compute_percentage_change() -> Result<()> {
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut source: EagerVec<usize, u16> =
        EagerVec::forced_import_compressed(&db, "source", Version::ONE)?;

    // Fill with test data: [100, 110, 121, 133]
    let values = [100, 110, 121, 133];
    for (i, &v) in values.iter().enumerate() {
        source.forced_push(i, v)?;
    }
    source.safe_flush(&exit)?;

    let mut result: EagerVec<usize, f32> =
        EagerVec::forced_import_compressed(&db, "result", Version::ONE)?;
    result.compute_percentage_change(0, &source, 1, &exit)?;
    result.safe_flush(&exit)?;

    // Check percentage changes
    // Index 0: NaN (no previous value, division by default 0)
    // Index 1: (110/100 - 1) * 100 = 10.0%
    // Index 2: (121/110 - 1) * 100 = 10.0%
    // Index 3: (133/121 - 1) * 100 ≈ 9.917%
    let actual_0 = result.read_at_unwrap_once(0);
    let actual_1 = result.read_at_unwrap_once(1);
    let actual_2 = result.read_at_unwrap_once(2);
    let actual_3 = result.read_at_unwrap_once(3);

    assert!(
        actual_0.is_nan(),
        "First element should be NaN when no previous value exists"
    );
    assert!((actual_1 - 10.0).abs() < 0.01);
    assert!((actual_2 - 10.0).abs() < 0.01);
    assert!((actual_3 - 9.917).abs() < 0.01);

    Ok(())
}

#[test]
fn test_compute_sliding_window_max() -> Result<()> {
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut source: EagerVec<usize, u32> =
        EagerVec::forced_import_compressed(&db, "source", Version::ONE)?;

    // Test data: [3, 1, 4, 1, 5, 9, 2, 6]
    let values = [3, 1, 4, 1, 5, 9, 2, 6];
    for (i, &v) in values.iter().enumerate() {
        source.forced_push(i, v)?;
    }
    source.safe_flush(&exit)?;

    let mut result: EagerVec<usize, u32> =
        EagerVec::forced_import_compressed(&db, "result", Version::ONE)?;
    result.compute_max(0, &source, 3, &exit)?; // Window size 3
    result.safe_flush(&exit)?;

    // Expected max in sliding window of 3
    // [3, 1, 4] -> 4
    // [1, 4, 1] -> 4
    // [4, 1, 5] -> 5
    // [1, 5, 9] -> 9
    // [5, 9, 2] -> 9
    // [9, 2, 6] -> 9
    let expected = [3, 3, 4, 4, 5, 9, 9, 9];
    for (i, v) in expected.into_iter().enumerate() {
        let actual = result.read_at_unwrap_once(i);
        assert_eq!(
            actual, v,
            "Sliding window max mismatch at index {}: expected {}, got {}",
            i, v, actual
        );
    }

    Ok(())
}

#[test]
fn test_compute_sliding_window_min() -> Result<()> {
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut source: EagerVec<usize, u32> =
        EagerVec::forced_import_compressed(&db, "source", Version::ONE)?;

    // Test data: [3, 1, 4, 1, 5, 9, 2, 6]
    let values = [3, 1, 4, 1, 5, 9, 2, 6];
    for (i, &v) in values.iter().enumerate() {
        source.forced_push(i, v)?;
    }
    source.safe_flush(&exit)?;

    let mut result: EagerVec<usize, u32> =
        EagerVec::forced_import_compressed(&db, "result", Version::ONE)?;
    result.compute_min(0, &source, 3, &exit)?; // Window size 3
    result.safe_flush(&exit)?;

    // Expected min in sliding window of 3
    // [3, 1, 4] -> 1
    // [1, 4, 1] -> 1
    // [4, 1, 5] -> 1
    // [1, 5, 9] -> 1
    // [5, 9, 2] -> 2
    // [9, 2, 6] -> 2
    let expected = [3, 1, 1, 1, 1, 1, 2, 2];
    for (i, v) in expected.into_iter().enumerate() {
        let actual = result.read_at_unwrap_once(i);
        assert_eq!(
            actual, v,
            "Sliding window min mismatch at index {}: expected {}, got {}",
            i, v, actual
        );
    }

    Ok(())
}

#[test]
fn test_compute_all_time_high() -> Result<()> {
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut source: EagerVec<usize, u32> =
        EagerVec::forced_import_compressed(&db, "source", Version::ONE)?;

    // Test data: [10, 15, 12, 20, 18, 25, 22]
    let values = [10, 15, 12, 20, 18, 25, 22];
    for (i, &v) in values.iter().enumerate() {
        source.forced_push(i, v)?;
    }
    source.safe_flush(&exit)?;

    let mut result: EagerVec<usize, u32> =
        EagerVec::forced_import_compressed(&db, "result", Version::ONE)?;
    result.compute_all_time_high(0, &source, &exit)?;
    result.safe_flush(&exit)?;

    // Expected all-time high at each index
    let expected = [10, 15, 15, 20, 20, 25, 25];
    for (i, v) in expected.into_iter().enumerate() {
        let actual = result.read_at_unwrap_once(i);
        assert_eq!(
            actual, v,
            "All-time high mismatch at index {}: expected {}, got {}",
            i, v, actual
        );
    }

    Ok(())
}

#[test]
fn test_compute_all_time_low() -> Result<()> {
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut source: EagerVec<usize, u32> =
        EagerVec::forced_import_compressed(&db, "source", Version::ONE)?;

    // Test data: [10, 5, 12, 3, 18, 2, 22]
    let values = [10, 5, 12, 3, 18, 2, 22];
    for (i, &v) in values.iter().enumerate() {
        source.forced_push(i, v)?;
    }
    source.safe_flush(&exit)?;

    let mut result: EagerVec<usize, u32> =
        EagerVec::forced_import_compressed(&db, "result", Version::ONE)?;
    result.compute_all_time_low_(0, &source, &exit, false)?;
    result.safe_flush(&exit)?;

    // Expected all-time low at each index
    let expected = [10, 5, 5, 3, 3, 2, 2];
    for (i, v) in expected.into_iter().enumerate() {
        let actual = result.read_at_unwrap_once(i);
        assert_eq!(
            actual, v,
            "All-time low mismatch at index {}: expected {}, got {}",
            i, v, actual
        );
    }

    Ok(())
}

#[test]
fn test_compute_cagr() -> Result<()> {
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut percentage_returns: EagerVec<usize, f32> =
        EagerVec::forced_import_compressed(&db, "returns", Version::ONE)?;

    // Test data: 100% return over different periods
    // CAGR for 100% over 1 year = 100%
    // CAGR for 100% over 2 years = 41.42%
    // CAGR for 100% over 3 years = 25.99%
    for i in 0..5 {
        percentage_returns.forced_push(i, 100.0)?; // 100% total return
    }
    percentage_returns.safe_flush(&exit)?;

    let mut result: EagerVec<usize, f32> =
        EagerVec::forced_import_compressed(&db, "result", Version::ONE)?;
    result.compute_cagr(0, &percentage_returns, 730, &exit)?; // 2 years (730 days)
    result.safe_flush(&exit)?;

    // CAGR = ((1 + 1.0)^(1/2) - 1) * 100 = 41.42%
    for i in 0..5 {
        let actual = result.read_at_unwrap_once(i);
        let expected = 41.42;
        assert!(
            (actual - expected).abs() < 0.01,
            "CAGR mismatch at index {}: expected {}, got {}",
            i,
            expected,
            actual
        );
    }

    Ok(())
}

#[test]
fn test_compute_zscore() -> Result<()> {
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut source: EagerVec<usize, f32> =
        EagerVec::forced_import_compressed(&db, "source", Version::ONE)?;
    let mut sma: EagerVec<usize, f32> =
        EagerVec::forced_import_compressed(&db, "sma", Version::ONE)?;
    let mut sd: EagerVec<usize, f32> = EagerVec::forced_import_compressed(&db, "sd", Version::ONE)?;

    // Test data
    // Source: [10.0, 12.0, 14.0, 16.0]
    // SMA:    [10.0, 10.0, 10.0, 10.0]
    // SD:     [2.0,  2.0,  2.0,  2.0]
    // Z-score = (value - mean) / sd
    for i in 0..4 {
        source.forced_push(i, 10.0 + i as f32 * 2.0)?;
        sma.forced_push(i, 10.0)?;
        sd.forced_push(i, 2.0)?;
    }
    source.safe_flush(&exit)?;
    sma.safe_flush(&exit)?;
    sd.safe_flush(&exit)?;

    let mut result: EagerVec<usize, f32> =
        EagerVec::forced_import_compressed(&db, "result", Version::ONE)?;
    result.compute_zscore(0, &source, &sma, &sd, &exit)?;
    result.safe_flush(&exit)?;

    // Expected z-scores
    // Index 0: (10 - 10) / 2 = 0.0
    // Index 1: (12 - 10) / 2 = 1.0
    // Index 2: (14 - 10) / 2 = 2.0
    // Index 3: (16 - 10) / 2 = 3.0
    let expected = [0.0, 1.0, 2.0, 3.0];
    for (i, v) in expected.into_iter().enumerate() {
        let actual = result.read_at_unwrap_once(i);
        assert!(
            (actual - v).abs() < 0.01,
            "Z-score mismatch at index {}: expected {}, got {}",
            i,
            v,
            actual
        );
    }

    Ok(())
}

#[test]
fn test_compute_functions_with_resume() -> Result<()> {
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    // Test that compute functions can resume properly
    let mut source: EagerVec<usize, u32> =
        EagerVec::forced_import_compressed(&db, "source", Version::ONE)?;
    let mut result: EagerVec<usize, u32> =
        EagerVec::forced_import_compressed(&db, "result", Version::ONE)?;

    // First batch: compute for first 5 elements
    for i in 0..5 {
        source.forced_push(i, (i * 10) as u32)?;
    }
    source.safe_flush(&exit)?;

    result.compute_all_time_high(0, &source, &exit)?;
    result.safe_flush(&exit)?;

    // Verify first batch
    for i in 0..5 {
        let actual = result.read_at_unwrap_once(i);
        let expected = (i * 10) as u32;
        assert_eq!(actual, expected);
    }

    // Add more data
    for i in 5..10 {
        source.forced_push(i, (i * 10) as u32)?;
    }
    source.safe_flush(&exit)?;

    // Resume computation
    result.compute_all_time_high(0, &source, &exit)?;
    result.safe_flush(&exit)?;

    // Verify all data
    for i in 0..10 {
        let actual = result.read_at_unwrap_once(i);
        let expected = (i * 10) as u32;
        assert_eq!(actual, expected);
    }

    Ok(())
}

#[test]
fn test_compute_add() -> Result<()> {
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut vec1: EagerVec<usize, u64> =
        EagerVec::forced_import_compressed(&db, "vec1", Version::ONE)?;
    let mut vec2: EagerVec<usize, u64> =
        EagerVec::forced_import_compressed(&db, "vec2", Version::ONE)?;

    for i in 0..10 {
        vec1.forced_push(i, (i * 10) as u64)?;
        vec2.forced_push(i, (i * 5) as u64)?;
    }
    vec1.safe_flush(&exit)?;
    vec2.safe_flush(&exit)?;

    let mut result: EagerVec<usize, u64> =
        EagerVec::forced_import_compressed(&db, "result", Version::ONE)?;
    result.compute_add(0, &vec1, &vec2, &exit)?;
    result.safe_flush(&exit)?;

    for i in 0..10 {
        let expected = (i * 10 + i * 5) as u64;
        let actual = result.read_at_unwrap_once(i);
        assert_eq!(actual, expected);
    }

    Ok(())
}

#[test]
fn test_compute_subtract() -> Result<()> {
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut vec1: EagerVec<usize, u64> =
        EagerVec::forced_import_compressed(&db, "vec1", Version::ONE)?;
    let mut vec2: EagerVec<usize, u64> =
        EagerVec::forced_import_compressed(&db, "vec2", Version::ONE)?;

    for i in 0..10 {
        vec1.forced_push(i, (100 + i * 10) as u64)?;
        vec2.forced_push(i, (i * 5) as u64)?;
    }
    vec1.safe_flush(&exit)?;
    vec2.safe_flush(&exit)?;

    let mut result: EagerVec<usize, u64> =
        EagerVec::forced_import_compressed(&db, "result", Version::ONE)?;
    result.compute_subtract(0, &vec1, &vec2, &exit)?;
    result.safe_flush(&exit)?;

    for i in 0..10 {
        let expected = (100 + i * 10 - i * 5) as u64;
        let actual = result.read_at_unwrap_once(i);
        assert_eq!(actual, expected);
    }

    Ok(())
}

#[test]
fn test_compute_multiply() -> Result<()> {
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut vec1: EagerVec<usize, u32> =
        EagerVec::forced_import_compressed(&db, "vec1", Version::ONE)?;
    let mut vec2: EagerVec<usize, u32> =
        EagerVec::forced_import_compressed(&db, "vec2", Version::ONE)?;

    for i in 0..10 {
        vec1.forced_push(i, (i + 1) as u32)?;
        vec2.forced_push(i, (i + 2) as u32)?;
    }
    vec1.safe_flush(&exit)?;
    vec2.safe_flush(&exit)?;

    let mut result: EagerVec<usize, u64> =
        EagerVec::forced_import_compressed(&db, "result", Version::ONE)?;
    result.compute_multiply(0, &vec1, &vec2, &exit)?;
    result.safe_flush(&exit)?;

    for i in 0..10 {
        let expected = ((i + 1) * (i + 2)) as u64;
        let actual = result.read_at_unwrap_once(i);
        assert_eq!(actual, expected);
    }

    Ok(())
}

#[test]
fn test_compute_divide() -> Result<()> {
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut vec1: EagerVec<usize, f32> =
        EagerVec::forced_import_compressed(&db, "vec1", Version::ONE)?;
    let mut vec2: EagerVec<usize, f32> =
        EagerVec::forced_import_compressed(&db, "vec2", Version::ONE)?;

    for i in 0..10 {
        vec1.forced_push(i, (100.0 + i as f32 * 10.0))?;
        vec2.forced_push(i, (i as f32 + 1.0))?;
    }
    vec1.safe_flush(&exit)?;
    vec2.safe_flush(&exit)?;

    let mut result: EagerVec<usize, f32> =
        EagerVec::forced_import_compressed(&db, "result", Version::ONE)?;
    result.compute_divide(0, &vec1, &vec2, &exit)?;
    result.safe_flush(&exit)?;

    for i in 0..10 {
        let expected = (100.0 + i as f32 * 10.0) / (i as f32 + 1.0);
        let actual = result.read_at_unwrap_once(i);
        assert_f32_eq(actual, expected, 0.001, &format!("Divide at index {}", i));
    }

    Ok(())
}

#[test]
fn test_compute_max() -> Result<()> {
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut source: EagerVec<usize, u64> =
        EagerVec::forced_import_compressed(&db, "source", Version::ONE)?;

    // Create data with a peak in the middle
    for i in 0..10 {
        let value = if i < 5 { i * 10 } else { (9 - i) * 10 };
        source.forced_push(i, value as u64)?;
    }
    source.safe_flush(&exit)?;

    let mut result: EagerVec<usize, u64> =
        EagerVec::forced_import_compressed(&db, "result", Version::ONE)?;
    result.compute_max(0, &source, &exit)?;
    result.safe_flush(&exit)?;

    // Verify max is accumulated correctly
    for i in 0..10 {
        let expected = if i < 5 {
            (i * 10) as u64
        } else {
            40u64 // Peak at index 4
        };
        let actual = result.read_at_unwrap_once(i);
        assert_eq!(actual, expected, "Max at index {}", i);
    }

    Ok(())
}

#[test]
fn test_compute_min() -> Result<()> {
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut source: EagerVec<usize, u64> =
        EagerVec::forced_import_compressed(&db, "source", Version::ONE)?;

    // Create data with a valley in the middle
    for i in 0..10 {
        let value = if i < 5 {
            100 - i * 10
        } else {
            50 + (i - 5) * 10
        };
        source.forced_push(i, value as u64)?;
    }
    source.safe_flush(&exit)?;

    let mut result: EagerVec<usize, u64> =
        EagerVec::forced_import_compressed(&db, "result", Version::ONE)?;
    result.compute_min(0, &source, &exit)?;
    result.safe_flush(&exit)?;

    // Verify min is accumulated correctly
    for i in 0..10 {
        let expected = if i < 5 {
            (100 - i * 10) as u64
        } else {
            50u64 // Valley at index 5
        };
        let actual = result.read_at_unwrap_once(i);
        assert_eq!(actual, expected, "Min at index {}", i);
    }

    Ok(())
}

#[test]
fn test_compute_sum() -> Result<()> {
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut source: EagerVec<usize, u64> =
        EagerVec::forced_import_compressed(&db, "source", Version::ONE)?;

    for i in 0..10 {
        source.forced_push(i, (i + 1) as u64)?;
    }
    source.safe_flush(&exit)?;

    let mut result: EagerVec<usize, u64> =
        EagerVec::forced_import_compressed(&db, "result", Version::ONE)?;
    result.compute_sum(0, &source, &exit)?;
    result.safe_flush(&exit)?;

    // Verify cumulative sum
    let mut expected_sum = 0u64;
    for i in 0..10 {
        expected_sum += (i + 1) as u64;
        let actual = result.read_at_unwrap_once(i);
        assert_eq!(actual, expected_sum, "Cumulative sum at index {}", i);
    }

    Ok(())
}

#[test]
fn test_compute_sma() -> Result<()> {
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut source: EagerVec<usize, u64> =
        EagerVec::forced_import_compressed(&db, "source", Version::ONE)?;

    for i in 0..10 {
        source.forced_push(i, (i * 10) as u64)?;
    }
    source.safe_flush(&exit)?;

    let mut result: EagerVec<usize, f32> =
        EagerVec::forced_import_compressed(&db, "result", Version::ONE)?;
    result.compute_sma(0, &source, 3, &exit)?;
    result.safe_flush(&exit)?;

    // Verify SMA with window of 3
    for i in 0..10 {
        let actual = result.read_at_unwrap_once(i);
        if i < 2 {
            // Not enough data for full window
            let sum: u64 = (0..=i).map(|j| j * 10).sum();
            let expected = sum as f32 / (i + 1) as f32;
            assert_f32_eq(actual, expected, 0.001, &format!("SMA at index {}", i));
        } else {
            // Full window
            let sum: u64 = (i - 2..=i).map(|j| j * 10).sum();
            let expected = sum as f32 / 3.0;
            assert_f32_eq(actual, expected, 0.001, &format!("SMA at index {}", i));
        }
    }

    Ok(())
}

#[test]
fn test_compute_ema() -> Result<()> {
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut source: EagerVec<usize, u64> =
        EagerVec::forced_import_compressed(&db, "source", Version::ONE)?;

    for i in 0..10 {
        source.forced_push(i, 100)?; // Constant value for easier verification
    }
    source.safe_flush(&exit)?;

    let mut result: EagerVec<usize, f32> =
        EagerVec::forced_import_compressed(&db, "result", Version::ONE)?;
    result.compute_ema(0, &source, 3, &exit)?;
    result.safe_flush(&exit)?;

    // For constant input, EMA should converge to the input value
    for i in 0..10 {
        let actual = result.read_at_unwrap_once(i);
        // EMA of constant values should be the constant
        assert_f32_eq(actual, 100.0, 0.1, &format!("EMA at index {}", i));
    }

    Ok(())
}

#[test]
fn test_compute_percentage() -> Result<()> {
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut numerator: EagerVec<usize, u32> =
        EagerVec::forced_import_compressed(&db, "numerator", Version::ONE)?;
    let mut denominator: EagerVec<usize, u32> =
        EagerVec::forced_import_compressed(&db, "denominator", Version::ONE)?;

    for i in 0..10 {
        numerator.forced_push(i, (i + 1) as u32)?;
        denominator.forced_push(i, 10)?;
    }
    numerator.safe_flush(&exit)?;
    denominator.safe_flush(&exit)?;

    let mut result: EagerVec<usize, u64> =
        EagerVec::forced_import_compressed(&db, "result", Version::ONE)?;
    result.compute_percentage(0, &numerator, &denominator, &exit)?;
    result.safe_flush(&exit)?;

    for i in 0..10 {
        let expected = ((i + 1) as u64 * 100) / 10;
        let actual = result.read_at_unwrap_once(i);
        assert_eq!(actual, expected, "Percentage at index {}", i);
    }

    Ok(())
}

#[test]
fn test_compute_percentage_difference() -> Result<()> {
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut vec1: EagerVec<usize, u32> =
        EagerVec::forced_import_compressed(&db, "vec1", Version::ONE)?;
    let mut vec2: EagerVec<usize, u32> =
        EagerVec::forced_import_compressed(&db, "vec2", Version::ONE)?;

    for i in 0..10 {
        vec1.forced_push(i, (100 + i * 10) as u32)?;
        vec2.forced_push(i, 100)?;
    }
    vec1.safe_flush(&exit)?;
    vec2.safe_flush(&exit)?;

    let mut result: EagerVec<usize, u64> =
        EagerVec::forced_import_compressed(&db, "result", Version::ONE)?;
    result.compute_percentage_difference(0, &vec1, &vec2, &exit)?;
    result.safe_flush(&exit)?;

    for i in 0..10 {
        let expected = ((100 + i * 10) as u64 - 100) * 100 / 100;
        let actual = result.read_at_unwrap_once(i);
        assert_eq!(actual, expected, "Percentage difference at index {}", i);
    }

    Ok(())
}
