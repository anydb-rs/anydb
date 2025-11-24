use rawdb::Database;
use tempfile::TempDir;
use vecdb::{
    AnyStoredVec, CollectableVec, EagerVec, Exit, GenericStoredVec, Importable, LZ4Vec, PcoVec,
    Result, StoredVec, Version, ZeroCopyVec, ZstdVec,
};

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

/// Generic test function for compute_sum_of_others
fn run_compute_sum_of_others<V>() -> Result<()>
where
    V: StoredVec<I = usize, T = u64>,
{
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    // Create source vectors
    let mut vec1: EagerVec<V> = EagerVec::forced_import(&db, "vec1", Version::ONE)?;
    let mut vec2: EagerVec<V> = EagerVec::forced_import(&db, "vec2", Version::ONE)?;
    let mut vec3: EagerVec<V> = EagerVec::forced_import(&db, "vec3", Version::ONE)?;

    // Fill with test data
    for i in 0..10 {
        vec1.truncate_push(i, (i * 10) as u64)?;
        vec2.truncate_push(i, (i * 5) as u64)?;
        vec3.truncate_push(i, i as u64)?;
    }
    vec1.safe_flush(&exit)?;
    vec2.safe_flush(&exit)?;
    vec3.safe_flush(&exit)?;

    // Compute sum of others
    let mut result: EagerVec<V> = EagerVec::forced_import(&db, "result", Version::ONE)?;
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

fn run_compute_min_of_others<V>() -> Result<()>
where
    V: StoredVec<I = usize, T = u64>,
{
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut vec1: EagerVec<V> = EagerVec::forced_import(&db, "vec1", Version::ONE)?;
    let mut vec2: EagerVec<V> = EagerVec::forced_import(&db, "vec2", Version::ONE)?;
    let mut vec3: EagerVec<V> = EagerVec::forced_import(&db, "vec3", Version::ONE)?;

    // Test data: [50, 51, 52...], [10, 11, 12...], [100, 101, 102...]
    for i in 0..10 {
        vec1.truncate_push(i, (50 + i) as u64)?;
        vec2.truncate_push(i, (10 + i) as u64)?;
        vec3.truncate_push(i, (100 + i) as u64)?;
    }
    vec1.safe_flush(&exit)?;
    vec2.safe_flush(&exit)?;
    vec3.safe_flush(&exit)?;

    let mut result: EagerVec<V> = EagerVec::forced_import(&db, "result", Version::ONE)?;
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

fn run_compute_max_of_others<V>() -> Result<()>
where
    V: StoredVec<I = usize, T = u64>,
{
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut vec1: EagerVec<V> = EagerVec::forced_import(&db, "vec1", Version::ONE)?;
    let mut vec2: EagerVec<V> = EagerVec::forced_import(&db, "vec2", Version::ONE)?;
    let mut vec3: EagerVec<V> = EagerVec::forced_import(&db, "vec3", Version::ONE)?;

    for i in 0..10 {
        vec1.truncate_push(i, (50 + i) as u64)?;
        vec2.truncate_push(i, (10 + i) as u64)?;
        vec3.truncate_push(i, (100 + i) as u64)?;
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

    let mut result: EagerVec<V> = EagerVec::forced_import(&db, "result", Version::ONE)?;
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

fn run_compute_previous_value<VS, VR>() -> Result<()>
where
    VS: StoredVec<I = usize, T = u16>,
    VR: StoredVec<I = usize, T = f32>,
{
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut source = EagerVec::<VS>::forced_import(&db, "source", Version::ONE)?;

    // Fill with test data: [10, 20, 30, 40, 50]
    for i in 0..5 {
        source.truncate_push(i, ((i + 1) * 10) as u16)?;
    }
    source.safe_flush(&exit)?;

    let mut result = EagerVec::<VR>::forced_import(&db, "result", Version::ONE)?;
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

fn run_compute_change<V>() -> Result<()>
where
    V: StoredVec<I = usize, T = u32>,
{
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut source: EagerVec<V> = EagerVec::forced_import(&db, "source", Version::ONE)?;

    // Fill with test data: [10, 20, 25, 30, 50]
    let values = [10, 20, 25, 30, 50];
    for (i, &v) in values.iter().enumerate() {
        source.truncate_push(i, v)?;
    }
    source.safe_flush(&exit)?;

    let mut result: EagerVec<V> = EagerVec::forced_import(&db, "result", Version::ONE)?;
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

fn run_compute_percentage_change<VS, VR>() -> Result<()>
where
    VS: StoredVec<I = usize, T = u16>,
    VR: StoredVec<I = usize, T = f32>,
{
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut source: EagerVec<VS> = EagerVec::forced_import(&db, "source", Version::ONE)?;

    // Fill with test data: [100, 110, 121, 133]
    let values = [100, 110, 121, 133];
    for (i, &v) in values.iter().enumerate() {
        source.truncate_push(i, v)?;
    }
    source.safe_flush(&exit)?;

    let mut result: EagerVec<VR> = EagerVec::forced_import(&db, "result", Version::ONE)?;
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

fn run_compute_sliding_window_max<V>() -> Result<()>
where
    V: StoredVec<I = usize, T = u32>,
{
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut source: EagerVec<V> = EagerVec::forced_import(&db, "source", Version::ONE)?;

    // Test data: [3, 1, 4, 1, 5, 9, 2, 6]
    let values = [3, 1, 4, 1, 5, 9, 2, 6];
    for (i, &v) in values.iter().enumerate() {
        source.truncate_push(i, v)?;
    }
    source.safe_flush(&exit)?;

    let mut result: EagerVec<V> = EagerVec::forced_import(&db, "result", Version::ONE)?;
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

fn run_compute_sliding_window_min<V>() -> Result<()>
where
    V: StoredVec<I = usize, T = u32>,
{
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut source: EagerVec<V> = EagerVec::forced_import(&db, "source", Version::ONE)?;

    // Test data: [3, 1, 4, 1, 5, 9, 2, 6]
    let values = [3, 1, 4, 1, 5, 9, 2, 6];
    for (i, &v) in values.iter().enumerate() {
        source.truncate_push(i, v)?;
    }
    source.safe_flush(&exit)?;

    let mut result: EagerVec<V> = EagerVec::forced_import(&db, "result", Version::ONE)?;
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

fn run_compute_all_time_high<V>() -> Result<()>
where
    V: StoredVec<I = usize, T = u32>,
{
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut source: EagerVec<V> = EagerVec::forced_import(&db, "source", Version::ONE)?;

    // Test data: [10, 15, 12, 20, 18, 25, 22]
    let values = [10, 15, 12, 20, 18, 25, 22];
    for (i, &v) in values.iter().enumerate() {
        source.truncate_push(i, v)?;
    }
    source.safe_flush(&exit)?;

    let mut result: EagerVec<V> = EagerVec::forced_import(&db, "result", Version::ONE)?;
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

fn run_compute_all_time_low<V>() -> Result<()>
where
    V: StoredVec<I = usize, T = u32>,
{
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut source: EagerVec<V> = EagerVec::forced_import(&db, "source", Version::ONE)?;

    // Test data: [10, 5, 12, 3, 18, 2, 22]
    let values = [10, 5, 12, 3, 18, 2, 22];
    for (i, &v) in values.iter().enumerate() {
        source.truncate_push(i, v)?;
    }
    source.safe_flush(&exit)?;

    let mut result: EagerVec<V> = EagerVec::forced_import(&db, "result", Version::ONE)?;
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

fn run_compute_cagr<V>() -> Result<()>
where
    V: StoredVec<I = usize, T = f32>,
{
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut percentage_returns: EagerVec<V> =
        EagerVec::forced_import(&db, "returns", Version::ONE)?;

    // Test data: 100% return over different periods
    // CAGR for 100% over 1 year = 100%
    // CAGR for 100% over 2 years = 41.42%
    // CAGR for 100% over 3 years = 25.99%
    for i in 0..5 {
        percentage_returns.truncate_push(i, 100.0)?; // 100% total return
    }
    percentage_returns.safe_flush(&exit)?;

    let mut result: EagerVec<V> = EagerVec::forced_import(&db, "result", Version::ONE)?;
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

fn run_compute_zscore<V>() -> Result<()>
where
    V: StoredVec<I = usize, T = f32>,
{
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut source: EagerVec<V> = EagerVec::forced_import(&db, "source", Version::ONE)?;
    let mut sma: EagerVec<V> = EagerVec::forced_import(&db, "sma", Version::ONE)?;
    let mut sd: EagerVec<V> = EagerVec::forced_import(&db, "sd", Version::ONE)?;

    // Test data
    // Source: [10.0, 12.0, 14.0, 16.0]
    // SMA:    [10.0, 10.0, 10.0, 10.0]
    // SD:     [2.0,  2.0,  2.0,  2.0]
    // Z-score = (value - mean) / sd
    for i in 0..4 {
        source.truncate_push(i, 10.0 + i as f32 * 2.0)?;
        sma.truncate_push(i, 10.0)?;
        sd.truncate_push(i, 2.0)?;
    }
    source.safe_flush(&exit)?;
    sma.safe_flush(&exit)?;
    sd.safe_flush(&exit)?;

    let mut result: EagerVec<V> = EagerVec::forced_import(&db, "result", Version::ONE)?;
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

fn run_compute_functions_with_resume<V>() -> Result<()>
where
    V: StoredVec<I = usize, T = u32>,
{
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    // Test that compute functions can resume properly
    let mut source: EagerVec<V> = EagerVec::forced_import(&db, "source", Version::ONE)?;
    let mut result: EagerVec<V> = EagerVec::forced_import(&db, "result", Version::ONE)?;

    // First batch: compute for first 5 elements
    for i in 0..5 {
        source.truncate_push(i, (i * 10) as u32)?;
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
        source.truncate_push(i, (i * 10) as u32)?;
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

fn run_compute_add<V>() -> Result<()>
where
    V: StoredVec<I = usize, T = u64>,
{
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut vec1: EagerVec<V> = EagerVec::forced_import(&db, "vec1", Version::ONE)?;
    let mut vec2: EagerVec<V> = EagerVec::forced_import(&db, "vec2", Version::ONE)?;

    for i in 0..10 {
        vec1.truncate_push(i, (i * 10) as u64)?;
        vec2.truncate_push(i, (i * 5) as u64)?;
    }
    vec1.safe_flush(&exit)?;
    vec2.safe_flush(&exit)?;

    let mut result: EagerVec<V> = EagerVec::forced_import(&db, "result", Version::ONE)?;
    result.compute_add(0, &vec1, &vec2, &exit)?;
    result.safe_flush(&exit)?;

    for i in 0..10 {
        let expected = (i * 10 + i * 5) as u64;
        let actual = result.read_at_unwrap_once(i);
        assert_eq!(actual, expected);
    }

    Ok(())
}

fn run_compute_subtract<V>() -> Result<()>
where
    V: StoredVec<I = usize, T = u64>,
{
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut vec1: EagerVec<V> = EagerVec::forced_import(&db, "vec1", Version::ONE)?;
    let mut vec2: EagerVec<V> = EagerVec::forced_import(&db, "vec2", Version::ONE)?;

    for i in 0..10 {
        vec1.truncate_push(i, (100 + i * 10) as u64)?;
        vec2.truncate_push(i, (i * 5) as u64)?;
    }
    vec1.safe_flush(&exit)?;
    vec2.safe_flush(&exit)?;

    let mut result: EagerVec<V> = EagerVec::forced_import(&db, "result", Version::ONE)?;
    result.compute_subtract(0, &vec1, &vec2, &exit)?;
    result.safe_flush(&exit)?;

    for i in 0..10 {
        let expected = (100 + i * 10 - i * 5) as u64;
        let actual = result.read_at_unwrap_once(i);
        assert_eq!(actual, expected);
    }

    Ok(())
}

fn run_compute_multiply<V>() -> Result<()>
where
    V: StoredVec<I = usize, T = u32>,
{
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut vec1: EagerVec<V> = EagerVec::forced_import(&db, "vec1", Version::ONE)?;
    let mut vec2: EagerVec<V> = EagerVec::forced_import(&db, "vec2", Version::ONE)?;

    for i in 0..10 {
        vec1.truncate_push(i, (i + 1) as u32)?;
        vec2.truncate_push(i, (i + 2) as u32)?;
    }
    vec1.safe_flush(&exit)?;
    vec2.safe_flush(&exit)?;

    let mut result: EagerVec<V> = EagerVec::forced_import(&db, "result", Version::ONE)?;
    result.compute_multiply(0, &vec1, &vec2, &exit)?;
    result.safe_flush(&exit)?;

    for i in 0..10 {
        let expected = ((i + 1) * (i + 2)) as u32;
        let actual = result.read_at_unwrap_once(i);
        assert_eq!(actual, expected);
    }

    Ok(())
}

fn run_compute_divide<V>() -> Result<()>
where
    V: StoredVec<I = usize, T = f32>,
{
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut vec1: EagerVec<V> = EagerVec::forced_import(&db, "vec1", Version::ONE)?;
    let mut vec2: EagerVec<V> = EagerVec::forced_import(&db, "vec2", Version::ONE)?;

    for i in 0..10 {
        vec1.truncate_push(i, 100.0 + i as f32 * 10.0)?;
        vec2.truncate_push(i, i as f32 + 1.0)?;
    }
    vec1.safe_flush(&exit)?;
    vec2.safe_flush(&exit)?;

    let mut result: EagerVec<V> = EagerVec::forced_import(&db, "result", Version::ONE)?;
    result.compute_divide(0, &vec1, &vec2, &exit)?;
    result.safe_flush(&exit)?;

    for i in 0..10 {
        let expected = (100.0 + i as f32 * 10.0) / (i as f32 + 1.0);
        let actual = result.read_at_unwrap_once(i);
        assert_f32_eq(actual, expected, 0.001, &format!("Divide at index {}", i));
    }

    Ok(())
}

fn run_compute_max<V>() -> Result<()>
where
    V: StoredVec<I = usize, T = u64>,
{
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut source: EagerVec<V> = EagerVec::forced_import(&db, "source", Version::ONE)?;

    // Create data with a peak in the middle
    for i in 0..10 {
        let value = if i < 5 { i * 10 } else { (9 - i) * 10 };
        source.truncate_push(i, value as u64)?;
    }
    source.safe_flush(&exit)?;

    let mut result: EagerVec<V> = EagerVec::forced_import(&db, "result", Version::ONE)?;
    result.compute_max(0, &source, usize::MAX, &exit)?;
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

fn run_compute_min<V>() -> Result<()>
where
    V: StoredVec<I = usize, T = u64>,
{
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut source: EagerVec<V> = EagerVec::forced_import(&db, "source", Version::ONE)?;

    // Create data with a valley in the middle
    for i in 0..10 {
        let value = if i < 5 {
            100 - i * 10
        } else {
            50 + (i - 5) * 10
        };
        source.truncate_push(i, value as u64)?;
    }
    source.safe_flush(&exit)?;

    let mut result: EagerVec<V> = EagerVec::forced_import(&db, "result", Version::ONE)?;
    result.compute_min(0, &source, usize::MAX, &exit)?;
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

fn run_compute_sum<V>() -> Result<()>
where
    V: StoredVec<I = usize, T = u64>,
{
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut source: EagerVec<V> = EagerVec::forced_import(&db, "source", Version::ONE)?;

    for i in 0..10 {
        source.truncate_push(i, (i + 1) as u64)?;
    }
    source.safe_flush(&exit)?;

    let mut result: EagerVec<V> = EagerVec::forced_import(&db, "result", Version::ONE)?;
    result.compute_sum(0, &source, usize::MAX, &exit)?;
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

fn run_compute_sma<VS, VR>() -> Result<()>
where
    VS: StoredVec<I = usize, T = u16>,
    VR: StoredVec<I = usize, T = f32>,
{
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut source: EagerVec<VS> = EagerVec::forced_import(&db, "source", Version::ONE)?;

    for i in 0..10 {
        source.truncate_push(i, (i * 10) as u16)?;
    }
    source.safe_flush(&exit)?;

    let mut result: EagerVec<VR> = EagerVec::forced_import(&db, "result", Version::ONE)?;
    result.compute_sma(0, &source, 3, &exit)?;
    result.safe_flush(&exit)?;

    // Verify SMA with window of 3
    for i in 0..10_u64 {
        let actual = result.read_at_unwrap_once(i as usize);
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

fn run_compute_ema<VS, VR>() -> Result<()>
where
    VS: StoredVec<I = usize, T = u16>,
    VR: StoredVec<I = usize, T = f32>,
{
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut source: EagerVec<VS> = EagerVec::forced_import(&db, "source", Version::ONE)?;

    for i in 0..10 {
        source.truncate_push(i, 100)?; // Constant value for easier verification
    }
    source.safe_flush(&exit)?;

    let mut result: EagerVec<VR> = EagerVec::forced_import(&db, "result", Version::ONE)?;
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

fn run_compute_percentage<VS, VR>() -> Result<()>
where
    VS: StoredVec<I = usize, T = u16>,
    VR: StoredVec<I = usize, T = f32>,
{
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut numerator: EagerVec<VS> = EagerVec::forced_import(&db, "numerator", Version::ONE)?;
    let mut denominator: EagerVec<VS> = EagerVec::forced_import(&db, "denominator", Version::ONE)?;

    for i in 0..10 {
        numerator.truncate_push(i, (i + 1) as u16)?;
        denominator.truncate_push(i, 10)?;
    }
    numerator.safe_flush(&exit)?;
    denominator.safe_flush(&exit)?;

    let mut result: EagerVec<VR> = EagerVec::forced_import(&db, "result", Version::ONE)?;
    result.compute_percentage(0, &numerator, &denominator, &exit)?;
    result.safe_flush(&exit)?;

    for i in 0..10 {
        let expected = ((i + 1) as f32 / 10.0) * 100.0;
        let actual = result.read_at_unwrap_once(i);
        assert_f32_eq(
            actual,
            expected,
            0.001,
            &format!("Percentage at index {}", i),
        );
    }

    Ok(())
}

fn run_compute_percentage_difference<VS, VR>() -> Result<()>
where
    VS: StoredVec<I = usize, T = u16>,
    VR: StoredVec<I = usize, T = f32>,
{
    let (db, _temp) = setup_test_db()?;
    let exit = Exit::new();

    let mut vec1: EagerVec<VS> = EagerVec::forced_import(&db, "vec1", Version::ONE)?;
    let mut vec2: EagerVec<VS> = EagerVec::forced_import(&db, "vec2", Version::ONE)?;

    for i in 0..10 {
        vec1.truncate_push(i, (100 + i * 10) as u16)?;
        vec2.truncate_push(i, 100)?;
    }
    vec1.safe_flush(&exit)?;
    vec2.safe_flush(&exit)?;

    let mut result: EagerVec<VR> = EagerVec::forced_import(&db, "result", Version::ONE)?;
    result.compute_percentage_difference(0, &vec1, &vec2, &exit)?;
    result.safe_flush(&exit)?;

    for i in 0..10 {
        let expected = (((100 + i * 10) as f32 - 100.0) / 100.0) * 100.0;
        let actual = result.read_at_unwrap_once(i);
        assert_f32_eq(
            actual,
            expected,
            0.001,
            &format!("Percentage difference at index {}", i),
        );
    }

    Ok(())
}

// ============================================================================
// Concrete test instances for all tests
// ============================================================================

// test_compute_sum_of_others
#[test]
fn test_pco_compute_sum_of_others() -> Result<()> {
    run_compute_sum_of_others::<PcoVec<usize, u64>>()
}

#[test]
fn test_zerocopy_compute_sum_of_others() -> Result<()> {
    run_compute_sum_of_others::<ZeroCopyVec<usize, u64>>()
}

#[test]
fn test_lz4_compute_sum_of_others() -> Result<()> {
    run_compute_sum_of_others::<LZ4Vec<usize, u64>>()
}

#[test]
fn test_zstd_compute_sum_of_others() -> Result<()> {
    run_compute_sum_of_others::<ZstdVec<usize, u64>>()
}

// test_compute_min_of_others
#[test]
fn test_pco_compute_min_of_others() -> Result<()> {
    run_compute_min_of_others::<PcoVec<usize, u64>>()
}

#[test]
fn test_zerocopy_compute_min_of_others() -> Result<()> {
    run_compute_min_of_others::<ZeroCopyVec<usize, u64>>()
}

#[test]
fn test_lz4_compute_min_of_others() -> Result<()> {
    run_compute_min_of_others::<LZ4Vec<usize, u64>>()
}

#[test]
fn test_zstd_compute_min_of_others() -> Result<()> {
    run_compute_min_of_others::<ZstdVec<usize, u64>>()
}

// test_compute_max_of_others
#[test]
fn test_pco_compute_max_of_others() -> Result<()> {
    run_compute_max_of_others::<PcoVec<usize, u64>>()
}

#[test]
fn test_zerocopy_compute_max_of_others() -> Result<()> {
    run_compute_max_of_others::<ZeroCopyVec<usize, u64>>()
}

#[test]
fn test_lz4_compute_max_of_others() -> Result<()> {
    run_compute_max_of_others::<LZ4Vec<usize, u64>>()
}

#[test]
fn test_zstd_compute_max_of_others() -> Result<()> {
    run_compute_max_of_others::<ZstdVec<usize, u64>>()
}

// test_compute_previous_value
#[test]
fn test_pco_compute_previous_value() -> Result<()> {
    run_compute_previous_value::<PcoVec<usize, u16>, PcoVec<usize, f32>>()
}

#[test]
fn test_zerocopy_compute_previous_value() -> Result<()> {
    run_compute_previous_value::<ZeroCopyVec<usize, u16>, ZeroCopyVec<usize, f32>>()
}

#[test]
fn test_lz4_compute_previous_value() -> Result<()> {
    run_compute_previous_value::<LZ4Vec<usize, u16>, LZ4Vec<usize, f32>>()
}

#[test]
fn test_zstd_compute_previous_value() -> Result<()> {
    run_compute_previous_value::<ZstdVec<usize, u16>, ZstdVec<usize, f32>>()
}

// test_compute_change
#[test]
fn test_pco_compute_change() -> Result<()> {
    run_compute_change::<PcoVec<usize, u32>>()
}

#[test]
fn test_zerocopy_compute_change() -> Result<()> {
    run_compute_change::<ZeroCopyVec<usize, u32>>()
}

#[test]
fn test_lz4_compute_change() -> Result<()> {
    run_compute_change::<LZ4Vec<usize, u32>>()
}

#[test]
fn test_zstd_compute_change() -> Result<()> {
    run_compute_change::<ZstdVec<usize, u32>>()
}

// test_compute_percentage_change
#[test]
fn test_pco_compute_percentage_change() -> Result<()> {
    run_compute_percentage_change::<PcoVec<usize, u16>, PcoVec<usize, f32>>()
}

#[test]
fn test_zerocopy_compute_percentage_change() -> Result<()> {
    run_compute_percentage_change::<ZeroCopyVec<usize, u16>, ZeroCopyVec<usize, f32>>()
}

#[test]
fn test_lz4_compute_percentage_change() -> Result<()> {
    run_compute_percentage_change::<LZ4Vec<usize, u16>, LZ4Vec<usize, f32>>()
}

#[test]
fn test_zstd_compute_percentage_change() -> Result<()> {
    run_compute_percentage_change::<ZstdVec<usize, u16>, ZstdVec<usize, f32>>()
}

// test_compute_sliding_window_max
#[test]
fn test_pco_compute_sliding_window_max() -> Result<()> {
    run_compute_sliding_window_max::<PcoVec<usize, u32>>()
}

#[test]
fn test_zerocopy_compute_sliding_window_max() -> Result<()> {
    run_compute_sliding_window_max::<ZeroCopyVec<usize, u32>>()
}

#[test]
fn test_lz4_compute_sliding_window_max() -> Result<()> {
    run_compute_sliding_window_max::<LZ4Vec<usize, u32>>()
}

#[test]
fn test_zstd_compute_sliding_window_max() -> Result<()> {
    run_compute_sliding_window_max::<ZstdVec<usize, u32>>()
}

// test_compute_sliding_window_min
#[test]
fn test_pco_compute_sliding_window_min() -> Result<()> {
    run_compute_sliding_window_min::<PcoVec<usize, u32>>()
}

#[test]
fn test_zerocopy_compute_sliding_window_min() -> Result<()> {
    run_compute_sliding_window_min::<ZeroCopyVec<usize, u32>>()
}

#[test]
fn test_lz4_compute_sliding_window_min() -> Result<()> {
    run_compute_sliding_window_min::<LZ4Vec<usize, u32>>()
}

#[test]
fn test_zstd_compute_sliding_window_min() -> Result<()> {
    run_compute_sliding_window_min::<ZstdVec<usize, u32>>()
}

// test_compute_all_time_high
#[test]
fn test_pco_compute_all_time_high() -> Result<()> {
    run_compute_all_time_high::<PcoVec<usize, u32>>()
}

#[test]
fn test_zerocopy_compute_all_time_high() -> Result<()> {
    run_compute_all_time_high::<ZeroCopyVec<usize, u32>>()
}

#[test]
fn test_lz4_compute_all_time_high() -> Result<()> {
    run_compute_all_time_high::<LZ4Vec<usize, u32>>()
}

#[test]
fn test_zstd_compute_all_time_high() -> Result<()> {
    run_compute_all_time_high::<ZstdVec<usize, u32>>()
}

// test_compute_all_time_low
#[test]
fn test_pco_compute_all_time_low() -> Result<()> {
    run_compute_all_time_low::<PcoVec<usize, u32>>()
}

#[test]
fn test_zerocopy_compute_all_time_low() -> Result<()> {
    run_compute_all_time_low::<ZeroCopyVec<usize, u32>>()
}

#[test]
fn test_lz4_compute_all_time_low() -> Result<()> {
    run_compute_all_time_low::<LZ4Vec<usize, u32>>()
}

#[test]
fn test_zstd_compute_all_time_low() -> Result<()> {
    run_compute_all_time_low::<ZstdVec<usize, u32>>()
}

// test_compute_cagr
#[test]
fn test_pco_compute_cagr() -> Result<()> {
    run_compute_cagr::<PcoVec<usize, f32>>()
}

#[test]
fn test_zerocopy_compute_cagr() -> Result<()> {
    run_compute_cagr::<ZeroCopyVec<usize, f32>>()
}

#[test]
fn test_lz4_compute_cagr() -> Result<()> {
    run_compute_cagr::<LZ4Vec<usize, f32>>()
}

#[test]
fn test_zstd_compute_cagr() -> Result<()> {
    run_compute_cagr::<ZstdVec<usize, f32>>()
}

// test_compute_zscore
#[test]
fn test_pco_compute_zscore() -> Result<()> {
    run_compute_zscore::<PcoVec<usize, f32>>()
}

#[test]
fn test_zerocopy_compute_zscore() -> Result<()> {
    run_compute_zscore::<ZeroCopyVec<usize, f32>>()
}

#[test]
fn test_lz4_compute_zscore() -> Result<()> {
    run_compute_zscore::<LZ4Vec<usize, f32>>()
}

#[test]
fn test_zstd_compute_zscore() -> Result<()> {
    run_compute_zscore::<ZstdVec<usize, f32>>()
}

// test_compute_functions_with_resume
#[test]
fn test_pco_compute_functions_with_resume() -> Result<()> {
    run_compute_functions_with_resume::<PcoVec<usize, u32>>()
}

#[test]
fn test_zerocopy_compute_functions_with_resume() -> Result<()> {
    run_compute_functions_with_resume::<ZeroCopyVec<usize, u32>>()
}

#[test]
fn test_lz4_compute_functions_with_resume() -> Result<()> {
    run_compute_functions_with_resume::<LZ4Vec<usize, u32>>()
}

#[test]
fn test_zstd_compute_functions_with_resume() -> Result<()> {
    run_compute_functions_with_resume::<ZstdVec<usize, u32>>()
}

// test_compute_add
#[test]
fn test_pco_compute_add() -> Result<()> {
    run_compute_add::<PcoVec<usize, u64>>()
}

#[test]
fn test_zerocopy_compute_add() -> Result<()> {
    run_compute_add::<ZeroCopyVec<usize, u64>>()
}

#[test]
fn test_lz4_compute_add() -> Result<()> {
    run_compute_add::<LZ4Vec<usize, u64>>()
}

#[test]
fn test_zstd_compute_add() -> Result<()> {
    run_compute_add::<ZstdVec<usize, u64>>()
}

// test_compute_subtract
#[test]
fn test_pco_compute_subtract() -> Result<()> {
    run_compute_subtract::<PcoVec<usize, u64>>()
}

#[test]
fn test_zerocopy_compute_subtract() -> Result<()> {
    run_compute_subtract::<ZeroCopyVec<usize, u64>>()
}

#[test]
fn test_lz4_compute_subtract() -> Result<()> {
    run_compute_subtract::<LZ4Vec<usize, u64>>()
}

#[test]
fn test_zstd_compute_subtract() -> Result<()> {
    run_compute_subtract::<ZstdVec<usize, u64>>()
}

// test_compute_multiply
#[test]
fn test_pco_compute_multiply() -> Result<()> {
    run_compute_multiply::<PcoVec<usize, u32>>()
}

#[test]
fn test_zerocopy_compute_multiply() -> Result<()> {
    run_compute_multiply::<ZeroCopyVec<usize, u32>>()
}

#[test]
fn test_lz4_compute_multiply() -> Result<()> {
    run_compute_multiply::<LZ4Vec<usize, u32>>()
}

#[test]
fn test_zstd_compute_multiply() -> Result<()> {
    run_compute_multiply::<ZstdVec<usize, u32>>()
}

// test_compute_divide
#[test]
fn test_pco_compute_divide() -> Result<()> {
    run_compute_divide::<PcoVec<usize, f32>>()
}

#[test]
fn test_zerocopy_compute_divide() -> Result<()> {
    run_compute_divide::<ZeroCopyVec<usize, f32>>()
}

#[test]
fn test_lz4_compute_divide() -> Result<()> {
    run_compute_divide::<LZ4Vec<usize, f32>>()
}

#[test]
fn test_zstd_compute_divide() -> Result<()> {
    run_compute_divide::<ZstdVec<usize, f32>>()
}

// test_compute_max
#[test]
fn test_pco_compute_max() -> Result<()> {
    run_compute_max::<PcoVec<usize, u64>>()
}

#[test]
fn test_zerocopy_compute_max() -> Result<()> {
    run_compute_max::<ZeroCopyVec<usize, u64>>()
}

#[test]
fn test_lz4_compute_max() -> Result<()> {
    run_compute_max::<LZ4Vec<usize, u64>>()
}

#[test]
fn test_zstd_compute_max() -> Result<()> {
    run_compute_max::<ZstdVec<usize, u64>>()
}

// test_compute_min
#[test]
fn test_pco_compute_min() -> Result<()> {
    run_compute_min::<PcoVec<usize, u64>>()
}

#[test]
fn test_zerocopy_compute_min() -> Result<()> {
    run_compute_min::<ZeroCopyVec<usize, u64>>()
}

#[test]
fn test_lz4_compute_min() -> Result<()> {
    run_compute_min::<LZ4Vec<usize, u64>>()
}

#[test]
fn test_zstd_compute_min() -> Result<()> {
    run_compute_min::<ZstdVec<usize, u64>>()
}

// test_compute_sum
#[test]
fn test_pco_compute_sum() -> Result<()> {
    run_compute_sum::<PcoVec<usize, u64>>()
}

#[test]
fn test_zerocopy_compute_sum() -> Result<()> {
    run_compute_sum::<ZeroCopyVec<usize, u64>>()
}

#[test]
fn test_lz4_compute_sum() -> Result<()> {
    run_compute_sum::<LZ4Vec<usize, u64>>()
}

#[test]
fn test_zstd_compute_sum() -> Result<()> {
    run_compute_sum::<ZstdVec<usize, u64>>()
}

// test_compute_sma
#[test]
fn test_pco_compute_sma() -> Result<()> {
    run_compute_sma::<PcoVec<usize, u16>, PcoVec<usize, f32>>()
}

#[test]
fn test_zerocopy_compute_sma() -> Result<()> {
    run_compute_sma::<ZeroCopyVec<usize, u16>, ZeroCopyVec<usize, f32>>()
}

#[test]
fn test_lz4_compute_sma() -> Result<()> {
    run_compute_sma::<LZ4Vec<usize, u16>, LZ4Vec<usize, f32>>()
}

#[test]
fn test_zstd_compute_sma() -> Result<()> {
    run_compute_sma::<ZstdVec<usize, u16>, ZstdVec<usize, f32>>()
}

// test_compute_ema
#[test]
fn test_pco_compute_ema() -> Result<()> {
    run_compute_ema::<PcoVec<usize, u16>, PcoVec<usize, f32>>()
}

#[test]
fn test_zerocopy_compute_ema() -> Result<()> {
    run_compute_ema::<ZeroCopyVec<usize, u16>, ZeroCopyVec<usize, f32>>()
}

#[test]
fn test_lz4_compute_ema() -> Result<()> {
    run_compute_ema::<LZ4Vec<usize, u16>, LZ4Vec<usize, f32>>()
}

#[test]
fn test_zstd_compute_ema() -> Result<()> {
    run_compute_ema::<ZstdVec<usize, u16>, ZstdVec<usize, f32>>()
}

// test_compute_percentage
#[test]
fn test_pco_compute_percentage() -> Result<()> {
    run_compute_percentage::<PcoVec<usize, u16>, PcoVec<usize, f32>>()
}

#[test]
fn test_zerocopy_compute_percentage() -> Result<()> {
    run_compute_percentage::<ZeroCopyVec<usize, u16>, ZeroCopyVec<usize, f32>>()
}

#[test]
fn test_lz4_compute_percentage() -> Result<()> {
    run_compute_percentage::<LZ4Vec<usize, u16>, LZ4Vec<usize, f32>>()
}

#[test]
fn test_zstd_compute_percentage() -> Result<()> {
    run_compute_percentage::<ZstdVec<usize, u16>, ZstdVec<usize, f32>>()
}

// test_compute_percentage_difference
#[test]
fn test_pco_compute_percentage_difference() -> Result<()> {
    run_compute_percentage_difference::<PcoVec<usize, u16>, PcoVec<usize, f32>>()
}

#[test]
fn test_zerocopy_compute_percentage_difference() -> Result<()> {
    run_compute_percentage_difference::<ZeroCopyVec<usize, u16>, ZeroCopyVec<usize, f32>>()
}

#[test]
fn test_lz4_compute_percentage_difference() -> Result<()> {
    run_compute_percentage_difference::<LZ4Vec<usize, u16>, LZ4Vec<usize, f32>>()
}

#[test]
fn test_zstd_compute_percentage_difference() -> Result<()> {
    run_compute_percentage_difference::<ZstdVec<usize, u16>, ZstdVec<usize, f32>>()
}
