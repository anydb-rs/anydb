//! Generic iterator tests for all vec types.
//!
//! These tests run against any type implementing `StoredVec`, ensuring
//! consistent iterator behavior across BytesVec, ZeroCopyVec, PcoVec, LZ4Vec, and ZstdVec.

use rawdb::Database;
use tempfile::TempDir;
use vecdb::{Result, StoredVec, Version};

// ============================================================================
// Test Setup Helpers
// ============================================================================

fn setup_db() -> Result<(Database, TempDir)> {
    let temp = TempDir::new()?;
    let db = Database::open(temp.path())?;
    Ok((db, temp))
}

// ============================================================================
// Generic Clean Iterator Tests
// ============================================================================

mod clean_iter {
    use super::*;

    fn run_basic<V>() -> Result<()>
    where
        V: StoredVec<I = usize, T = i32>,
    {
        let (db, _temp) = setup_db()?;
        let mut vec = V::forced_import(&db, "test", Version::ONE)?;

        for i in 0..100 {
            vec.push(i);
        }
        vec.write()?;

        let collected: Vec<i32> = vec.iter().collect();
        assert_eq!(collected.len(), 100);
        assert_eq!(collected[0], 0);
        assert_eq!(collected[99], 99);
        Ok(())
    }

    fn run_nth<V>() -> Result<()>
    where
        V: StoredVec<I = usize, T = i32>,
    {
        let (db, _temp) = setup_db()?;
        let mut vec = V::forced_import(&db, "test", Version::ONE)?;

        for i in 0..100 {
            vec.push(i);
        }
        vec.write()?;

        let mut iter = vec.iter();
        assert_eq!(iter.next(), Some(0));
        assert_eq!(iter.nth(9), Some(10));
        assert_eq!(iter.next(), Some(11));
        Ok(())
    }

    fn run_skip<V>() -> Result<()>
    where
        V: StoredVec<I = usize, T = i32>,
    {
        let (db, _temp) = setup_db()?;
        let mut vec = V::forced_import(&db, "test", Version::ONE)?;

        for i in 0..100 {
            vec.push(i);
        }
        vec.write()?;

        let collected: Vec<i32> = vec.iter().skip(50).collect();
        assert_eq!(collected.len(), 50);
        assert_eq!(collected[0], 50);
        Ok(())
    }

    fn run_take<V>() -> Result<()>
    where
        V: StoredVec<I = usize, T = i32>,
    {
        let (db, _temp) = setup_db()?;
        let mut vec = V::forced_import(&db, "test", Version::ONE)?;

        for i in 0..100 {
            vec.push(i);
        }
        vec.write()?;

        let collected: Vec<i32> = vec.iter().take(25).collect();
        assert_eq!(collected.len(), 25);
        assert_eq!(collected[24], 24);
        Ok(())
    }

    fn run_set_position<V>() -> Result<()>
    where
        V: StoredVec<I = usize, T = i32>,
    {
        let (db, _temp) = setup_db()?;
        let mut vec = V::forced_import(&db, "test", Version::ONE)?;

        for i in 0..100 {
            vec.push(i);
        }
        vec.write()?;

        let mut iter = vec.iter();
        iter.set_position_to(50);
        assert_eq!(iter.next(), Some(50));
        assert_eq!(iter.next(), Some(51));
        Ok(())
    }

    fn run_set_end<V>() -> Result<()>
    where
        V: StoredVec<I = usize, T = i32>,
    {
        let (db, _temp) = setup_db()?;
        let mut vec = V::forced_import(&db, "test", Version::ONE)?;

        for i in 0..100 {
            vec.push(i);
        }
        vec.write()?;

        let mut iter = vec.iter();
        iter.set_end_to(50);
        let collected: Vec<i32> = iter.collect();
        assert_eq!(collected.len(), 50);
        Ok(())
    }

    fn run_last<V>() -> Result<()>
    where
        V: StoredVec<I = usize, T = i32>,
    {
        let (db, _temp) = setup_db()?;
        let mut vec = V::forced_import(&db, "test", Version::ONE)?;

        for i in 0..100 {
            vec.push(i);
        }
        vec.write()?;

        let iter = vec.iter();
        assert_eq!(iter.last(), Some(99));
        Ok(())
    }

    fn run_last_empty<V>() -> Result<()>
    where
        V: StoredVec<I = usize, T = i32>,
    {
        let (db, _temp) = setup_db()?;
        let vec = V::forced_import(&db, "test", Version::ONE)?;

        let iter = vec.iter();
        assert_eq!(iter.last(), None);
        Ok(())
    }

    fn run_exact_size<V>() -> Result<()>
    where
        V: StoredVec<I = usize, T = i32>,
    {
        let (db, _temp) = setup_db()?;
        let mut vec = V::forced_import(&db, "test", Version::ONE)?;

        for i in 0..100 {
            vec.push(i);
        }
        vec.write()?;

        let mut iter = vec.iter();
        assert_eq!(iter.len(), 100);
        iter.next();
        assert_eq!(iter.len(), 99);
        Ok(())
    }

    fn run_buffer_crossing<V>() -> Result<()>
    where
        V: StoredVec<I = usize, T = i32>,
    {
        let (db, _temp) = setup_db()?;
        let mut vec = V::forced_import(&db, "test", Version::ONE)?;

        for i in 0..10000 {
            vec.push(i);
        }
        vec.write()?;

        let collected: Vec<i32> = vec.iter().collect();
        assert_eq!(collected.len(), 10000);

        for (i, &val) in collected.iter().enumerate() {
            assert_eq!(val, i as i32);
        }
        Ok(())
    }

    fn run_multiple_skip_take<V>() -> Result<()>
    where
        V: StoredVec<I = usize, T = i32>,
    {
        let (db, _temp) = setup_db()?;
        let mut vec = V::forced_import(&db, "test", Version::ONE)?;

        for i in 0..1000 {
            vec.push(i);
        }
        vec.write()?;

        let collected: Vec<i32> = vec.iter().skip(100).take(200).skip(50).take(100).collect();

        assert_eq!(collected.len(), 100);
        assert_eq!(collected[0], 150);
        assert_eq!(collected[99], 249);
        Ok(())
    }

    fn run_set_position_multiple<V>() -> Result<()>
    where
        V: StoredVec<I = usize, T = i32>,
    {
        let (db, _temp) = setup_db()?;
        let mut vec = V::forced_import(&db, "test", Version::ONE)?;

        for i in 0..1000 {
            vec.push(i);
        }
        vec.write()?;

        let mut iter = vec.iter();

        iter.set_position_to(100);
        assert_eq!(iter.next(), Some(100));

        iter.set_position_to(500);
        assert_eq!(iter.next(), Some(500));

        iter.set_position_to(50);
        assert_eq!(iter.next(), Some(50));
        Ok(())
    }

    fn run_nth_beyond_end<V>() -> Result<()>
    where
        V: StoredVec<I = usize, T = i32>,
    {
        let (db, _temp) = setup_db()?;
        let mut vec = V::forced_import(&db, "test", Version::ONE)?;

        for i in 0..10 {
            vec.push(i);
        }
        vec.write()?;

        let mut iter = vec.iter();
        assert_eq!(iter.nth(20), None);
        assert_eq!(iter.next(), None);
        Ok(())
    }

    fn run_skip_all<V>() -> Result<()>
    where
        V: StoredVec<I = usize, T = i32>,
    {
        let (db, _temp) = setup_db()?;
        let mut vec = V::forced_import(&db, "test", Version::ONE)?;

        for i in 0..100 {
            vec.push(i);
        }
        vec.write()?;

        let mut iter = vec.iter().skip(100);
        assert_eq!(iter.next(), None);
        Ok(())
    }

    fn run_take_zero<V>() -> Result<()>
    where
        V: StoredVec<I = usize, T = i32>,
    {
        let (db, _temp) = setup_db()?;
        let mut vec = V::forced_import(&db, "test", Version::ONE)?;

        for i in 0..100 {
            vec.push(i);
        }
        vec.write()?;

        let collected: Vec<i32> = vec.iter().take(0).collect();
        assert_eq!(collected.len(), 0);
        Ok(())
    }

    fn run_size_hint_consistency<V>() -> Result<()>
    where
        V: StoredVec<I = usize, T = i32>,
    {
        let (db, _temp) = setup_db()?;
        let mut vec = V::forced_import(&db, "test", Version::ONE)?;

        for i in 0..100 {
            vec.push(i);
        }
        vec.write()?;

        let mut iter = vec.iter();

        for i in 0..100 {
            let (lower, upper) = iter.size_hint();
            assert_eq!(lower, 100 - i);
            assert_eq!(upper, Some(100 - i));
            assert_eq!(iter.len(), 100 - i);
            iter.next();
        }
        Ok(())
    }

    // ============================================================================
    // Test instantiation for each vec type
    // ============================================================================

    mod bytes {
        use super::*;
        use vecdb::BytesVec;
        type V = BytesVec<usize, i32>;

        #[test]
        fn basic() -> Result<()> {
            run_basic::<V>()
        }
        #[test]
        fn nth() -> Result<()> {
            run_nth::<V>()
        }
        #[test]
        fn skip() -> Result<()> {
            run_skip::<V>()
        }
        #[test]
        fn take() -> Result<()> {
            run_take::<V>()
        }
        #[test]
        fn set_position() -> Result<()> {
            run_set_position::<V>()
        }
        #[test]
        fn set_end() -> Result<()> {
            run_set_end::<V>()
        }
        #[test]
        fn last() -> Result<()> {
            run_last::<V>()
        }
        #[test]
        fn last_empty() -> Result<()> {
            run_last_empty::<V>()
        }
        #[test]
        fn exact_size() -> Result<()> {
            run_exact_size::<V>()
        }
        #[test]
        fn buffer_crossing() -> Result<()> {
            run_buffer_crossing::<V>()
        }
        #[test]
        fn multiple_skip_take() -> Result<()> {
            run_multiple_skip_take::<V>()
        }
        #[test]
        fn set_position_multiple() -> Result<()> {
            run_set_position_multiple::<V>()
        }
        #[test]
        fn nth_beyond_end() -> Result<()> {
            run_nth_beyond_end::<V>()
        }
        #[test]
        fn skip_all() -> Result<()> {
            run_skip_all::<V>()
        }
        #[test]
        fn take_zero() -> Result<()> {
            run_take_zero::<V>()
        }
        #[test]
        fn size_hint_consistency() -> Result<()> {
            run_size_hint_consistency::<V>()
        }
    }

    #[cfg(feature = "zerocopy")]
    mod zerocopy {
        use super::*;
        use vecdb::ZeroCopyVec;
        type V = ZeroCopyVec<usize, i32>;

        #[test]
        fn basic() -> Result<()> {
            run_basic::<V>()
        }
        #[test]
        fn nth() -> Result<()> {
            run_nth::<V>()
        }
        #[test]
        fn skip() -> Result<()> {
            run_skip::<V>()
        }
        #[test]
        fn take() -> Result<()> {
            run_take::<V>()
        }
        #[test]
        fn set_position() -> Result<()> {
            run_set_position::<V>()
        }
        #[test]
        fn set_end() -> Result<()> {
            run_set_end::<V>()
        }
        #[test]
        fn last() -> Result<()> {
            run_last::<V>()
        }
        #[test]
        fn last_empty() -> Result<()> {
            run_last_empty::<V>()
        }
        #[test]
        fn exact_size() -> Result<()> {
            run_exact_size::<V>()
        }
        #[test]
        fn buffer_crossing() -> Result<()> {
            run_buffer_crossing::<V>()
        }
        #[test]
        fn multiple_skip_take() -> Result<()> {
            run_multiple_skip_take::<V>()
        }
        #[test]
        fn set_position_multiple() -> Result<()> {
            run_set_position_multiple::<V>()
        }
        #[test]
        fn nth_beyond_end() -> Result<()> {
            run_nth_beyond_end::<V>()
        }
        #[test]
        fn skip_all() -> Result<()> {
            run_skip_all::<V>()
        }
        #[test]
        fn take_zero() -> Result<()> {
            run_take_zero::<V>()
        }
        #[test]
        fn size_hint_consistency() -> Result<()> {
            run_size_hint_consistency::<V>()
        }
    }

    #[cfg(feature = "pco")]
    mod pco {
        use super::*;
        use vecdb::PcoVec;
        type V = PcoVec<usize, i32>;

        #[test]
        fn basic() -> Result<()> {
            run_basic::<V>()
        }
        #[test]
        fn nth() -> Result<()> {
            run_nth::<V>()
        }
        #[test]
        fn skip() -> Result<()> {
            run_skip::<V>()
        }
        #[test]
        fn take() -> Result<()> {
            run_take::<V>()
        }
        #[test]
        fn set_position() -> Result<()> {
            run_set_position::<V>()
        }
        #[test]
        fn set_end() -> Result<()> {
            run_set_end::<V>()
        }
        #[test]
        fn last() -> Result<()> {
            run_last::<V>()
        }
        #[test]
        fn last_empty() -> Result<()> {
            run_last_empty::<V>()
        }
        #[test]
        fn exact_size() -> Result<()> {
            run_exact_size::<V>()
        }
        #[test]
        fn buffer_crossing() -> Result<()> {
            run_buffer_crossing::<V>()
        }
        #[test]
        fn multiple_skip_take() -> Result<()> {
            run_multiple_skip_take::<V>()
        }
        #[test]
        fn set_position_multiple() -> Result<()> {
            run_set_position_multiple::<V>()
        }
        #[test]
        fn nth_beyond_end() -> Result<()> {
            run_nth_beyond_end::<V>()
        }
        #[test]
        fn skip_all() -> Result<()> {
            run_skip_all::<V>()
        }
        #[test]
        fn take_zero() -> Result<()> {
            run_take_zero::<V>()
        }
        #[test]
        fn size_hint_consistency() -> Result<()> {
            run_size_hint_consistency::<V>()
        }
    }

    #[cfg(feature = "lz4")]
    mod lz4 {
        use super::*;
        use vecdb::LZ4Vec;
        type V = LZ4Vec<usize, i32>;

        #[test]
        fn basic() -> Result<()> {
            run_basic::<V>()
        }
        #[test]
        fn nth() -> Result<()> {
            run_nth::<V>()
        }
        #[test]
        fn skip() -> Result<()> {
            run_skip::<V>()
        }
        #[test]
        fn take() -> Result<()> {
            run_take::<V>()
        }
        #[test]
        fn set_position() -> Result<()> {
            run_set_position::<V>()
        }
        #[test]
        fn set_end() -> Result<()> {
            run_set_end::<V>()
        }
        #[test]
        fn last() -> Result<()> {
            run_last::<V>()
        }
        #[test]
        fn last_empty() -> Result<()> {
            run_last_empty::<V>()
        }
        #[test]
        fn exact_size() -> Result<()> {
            run_exact_size::<V>()
        }
        #[test]
        fn buffer_crossing() -> Result<()> {
            run_buffer_crossing::<V>()
        }
        #[test]
        fn multiple_skip_take() -> Result<()> {
            run_multiple_skip_take::<V>()
        }
        #[test]
        fn set_position_multiple() -> Result<()> {
            run_set_position_multiple::<V>()
        }
        #[test]
        fn nth_beyond_end() -> Result<()> {
            run_nth_beyond_end::<V>()
        }
        #[test]
        fn skip_all() -> Result<()> {
            run_skip_all::<V>()
        }
        #[test]
        fn take_zero() -> Result<()> {
            run_take_zero::<V>()
        }
        #[test]
        fn size_hint_consistency() -> Result<()> {
            run_size_hint_consistency::<V>()
        }
    }

    #[cfg(feature = "zstd")]
    mod zstd {
        use super::*;
        use vecdb::ZstdVec;
        type V = ZstdVec<usize, i32>;

        #[test]
        fn basic() -> Result<()> {
            run_basic::<V>()
        }
        #[test]
        fn nth() -> Result<()> {
            run_nth::<V>()
        }
        #[test]
        fn skip() -> Result<()> {
            run_skip::<V>()
        }
        #[test]
        fn take() -> Result<()> {
            run_take::<V>()
        }
        #[test]
        fn set_position() -> Result<()> {
            run_set_position::<V>()
        }
        #[test]
        fn set_end() -> Result<()> {
            run_set_end::<V>()
        }
        #[test]
        fn last() -> Result<()> {
            run_last::<V>()
        }
        #[test]
        fn last_empty() -> Result<()> {
            run_last_empty::<V>()
        }
        #[test]
        fn exact_size() -> Result<()> {
            run_exact_size::<V>()
        }
        #[test]
        fn buffer_crossing() -> Result<()> {
            run_buffer_crossing::<V>()
        }
        #[test]
        fn multiple_skip_take() -> Result<()> {
            run_multiple_skip_take::<V>()
        }
        #[test]
        fn set_position_multiple() -> Result<()> {
            run_set_position_multiple::<V>()
        }
        #[test]
        fn nth_beyond_end() -> Result<()> {
            run_nth_beyond_end::<V>()
        }
        #[test]
        fn skip_all() -> Result<()> {
            run_skip_all::<V>()
        }
        #[test]
        fn take_zero() -> Result<()> {
            run_take_zero::<V>()
        }
        #[test]
        fn size_hint_consistency() -> Result<()> {
            run_size_hint_consistency::<V>()
        }
    }

    // ============================================================================
    // EagerVec Tests (wrapping different underlying vec types)
    // ============================================================================

    #[cfg(feature = "zerocopy")]
    mod eager_zerocopy {
        use super::*;
        use vecdb::{EagerVec, ZeroCopyVec};
        type V = EagerVec<ZeroCopyVec<usize, i32>>;

        #[test]
        fn basic() -> Result<()> {
            run_basic::<V>()
        }
        #[test]
        fn nth() -> Result<()> {
            run_nth::<V>()
        }
        #[test]
        fn skip() -> Result<()> {
            run_skip::<V>()
        }
        #[test]
        fn take() -> Result<()> {
            run_take::<V>()
        }
        #[test]
        fn set_position() -> Result<()> {
            run_set_position::<V>()
        }
        #[test]
        fn set_end() -> Result<()> {
            run_set_end::<V>()
        }
        #[test]
        fn last() -> Result<()> {
            run_last::<V>()
        }
        #[test]
        fn last_empty() -> Result<()> {
            run_last_empty::<V>()
        }
        #[test]
        fn exact_size() -> Result<()> {
            run_exact_size::<V>()
        }
        #[test]
        fn buffer_crossing() -> Result<()> {
            run_buffer_crossing::<V>()
        }
        #[test]
        fn multiple_skip_take() -> Result<()> {
            run_multiple_skip_take::<V>()
        }
        #[test]
        fn set_position_multiple() -> Result<()> {
            run_set_position_multiple::<V>()
        }
        #[test]
        fn nth_beyond_end() -> Result<()> {
            run_nth_beyond_end::<V>()
        }
        #[test]
        fn skip_all() -> Result<()> {
            run_skip_all::<V>()
        }
        #[test]
        fn take_zero() -> Result<()> {
            run_take_zero::<V>()
        }
        #[test]
        fn size_hint_consistency() -> Result<()> {
            run_size_hint_consistency::<V>()
        }
    }

    #[cfg(feature = "pco")]
    mod eager_pco {
        use super::*;
        use vecdb::{EagerVec, PcoVec};
        type V = EagerVec<PcoVec<usize, i32>>;

        #[test]
        fn basic() -> Result<()> {
            run_basic::<V>()
        }
        #[test]
        fn nth() -> Result<()> {
            run_nth::<V>()
        }
        #[test]
        fn skip() -> Result<()> {
            run_skip::<V>()
        }
        #[test]
        fn take() -> Result<()> {
            run_take::<V>()
        }
        #[test]
        fn set_position() -> Result<()> {
            run_set_position::<V>()
        }
        #[test]
        fn set_end() -> Result<()> {
            run_set_end::<V>()
        }
        #[test]
        fn last() -> Result<()> {
            run_last::<V>()
        }
        #[test]
        fn last_empty() -> Result<()> {
            run_last_empty::<V>()
        }
        #[test]
        fn exact_size() -> Result<()> {
            run_exact_size::<V>()
        }
        #[test]
        fn buffer_crossing() -> Result<()> {
            run_buffer_crossing::<V>()
        }
        #[test]
        fn multiple_skip_take() -> Result<()> {
            run_multiple_skip_take::<V>()
        }
        #[test]
        fn set_position_multiple() -> Result<()> {
            run_set_position_multiple::<V>()
        }
        #[test]
        fn nth_beyond_end() -> Result<()> {
            run_nth_beyond_end::<V>()
        }
        #[test]
        fn skip_all() -> Result<()> {
            run_skip_all::<V>()
        }
        #[test]
        fn take_zero() -> Result<()> {
            run_take_zero::<V>()
        }
        #[test]
        fn size_hint_consistency() -> Result<()> {
            run_size_hint_consistency::<V>()
        }
    }
}

// ============================================================================
// Generic Dirty Iterator Tests (stored + pushed data)
// ============================================================================

mod dirty_iter {
    use super::*;

    fn run_only_stored<V>() -> Result<()>
    where
        V: StoredVec<I = usize, T = i32>,
    {
        let (db, _temp) = setup_db()?;
        let mut vec = V::forced_import(&db, "test", Version::ONE)?;

        for i in 0..100 {
            vec.push(i);
        }
        vec.write()?;

        let collected: Vec<i32> = vec.iter().collect();
        assert_eq!(collected.len(), 100);
        assert_eq!(collected[0], 0);
        assert_eq!(collected[99], 99);
        Ok(())
    }

    fn run_only_pushed<V>() -> Result<()>
    where
        V: StoredVec<I = usize, T = i32>,
    {
        let (db, _temp) = setup_db()?;
        let mut vec = V::forced_import(&db, "test", Version::ONE)?;

        for i in 0..50 {
            vec.push(i);
        }
        // Don't flush

        let collected: Vec<i32> = vec.iter().collect();
        assert_eq!(collected.len(), 50);
        assert_eq!(collected[0], 0);
        assert_eq!(collected[49], 49);
        Ok(())
    }

    fn run_stored_and_pushed<V>() -> Result<()>
    where
        V: StoredVec<I = usize, T = i32>,
    {
        let (db, _temp) = setup_db()?;
        let mut vec = V::forced_import(&db, "test", Version::ONE)?;

        for i in 0..50 {
            vec.push(i);
        }
        vec.write()?;

        for i in 50..100 {
            vec.push(i);
        }

        let collected: Vec<i32> = vec.iter().collect();
        assert_eq!(collected.len(), 100);

        for (i, &val) in collected.iter().enumerate() {
            assert_eq!(val, i as i32);
        }
        Ok(())
    }

    fn run_skip_across_boundary<V>() -> Result<()>
    where
        V: StoredVec<I = usize, T = i32>,
    {
        let (db, _temp) = setup_db()?;
        let mut vec = V::forced_import(&db, "test", Version::ONE)?;

        for i in 0..50 {
            vec.push(i);
        }
        vec.write()?;

        for i in 50..100 {
            vec.push(i);
        }

        let collected: Vec<i32> = vec.iter().skip(40).collect();
        assert_eq!(collected.len(), 60);
        assert_eq!(collected[0], 40);
        assert_eq!(collected[59], 99);
        Ok(())
    }

    fn run_take_across_boundary<V>() -> Result<()>
    where
        V: StoredVec<I = usize, T = i32>,
    {
        let (db, _temp) = setup_db()?;
        let mut vec = V::forced_import(&db, "test", Version::ONE)?;

        for i in 0..50 {
            vec.push(i);
        }
        vec.write()?;

        for i in 50..100 {
            vec.push(i);
        }

        let collected: Vec<i32> = vec.iter().skip(40).take(20).collect();
        assert_eq!(collected.len(), 20);
        assert_eq!(collected[0], 40);
        assert_eq!(collected[19], 59);
        Ok(())
    }

    fn run_nth_across_boundary<V>() -> Result<()>
    where
        V: StoredVec<I = usize, T = i32>,
    {
        let (db, _temp) = setup_db()?;
        let mut vec = V::forced_import(&db, "test", Version::ONE)?;

        for i in 0..50 {
            vec.push(i);
        }
        vec.write()?;

        for i in 50..100 {
            vec.push(i);
        }

        let mut iter = vec.iter();
        assert_eq!(iter.nth(45), Some(45)); // In stored
        assert_eq!(iter.next(), Some(46)); // In stored
        assert_eq!(iter.nth(2), Some(49)); // In stored
        assert_eq!(iter.next(), Some(50)); // In pushed
        assert_eq!(iter.next(), Some(51)); // In pushed
        Ok(())
    }

    fn run_set_position_to_pushed<V>() -> Result<()>
    where
        V: StoredVec<I = usize, T = i32>,
    {
        let (db, _temp) = setup_db()?;
        let mut vec = V::forced_import(&db, "test", Version::ONE)?;

        for i in 0..50 {
            vec.push(i);
        }
        vec.write()?;

        for i in 50..100 {
            vec.push(i);
        }

        let mut iter = vec.iter();
        iter.set_position_to(75);
        assert_eq!(iter.next(), Some(75));
        assert_eq!(iter.next(), Some(76));
        Ok(())
    }

    fn run_last_in_pushed<V>() -> Result<()>
    where
        V: StoredVec<I = usize, T = i32>,
    {
        let (db, _temp) = setup_db()?;
        let mut vec = V::forced_import(&db, "test", Version::ONE)?;

        for i in 0..50 {
            vec.push(i);
        }
        vec.write()?;

        for i in 50..100 {
            vec.push(i);
        }

        let iter = vec.iter();
        assert_eq!(iter.last(), Some(99));
        Ok(())
    }

    fn run_last_in_stored<V>() -> Result<()>
    where
        V: StoredVec<I = usize, T = i32>,
    {
        let (db, _temp) = setup_db()?;
        let mut vec = V::forced_import(&db, "test", Version::ONE)?;

        for i in 0..100 {
            vec.push(i);
        }
        vec.write()?;

        let iter = vec.iter();
        assert_eq!(iter.last(), Some(99));
        Ok(())
    }

    fn run_exact_size_with_pushed<V>() -> Result<()>
    where
        V: StoredVec<I = usize, T = i32>,
    {
        let (db, _temp) = setup_db()?;
        let mut vec = V::forced_import(&db, "test", Version::ONE)?;

        for i in 0..50 {
            vec.push(i);
        }
        vec.write()?;

        for i in 50..75 {
            vec.push(i);
        }

        let mut iter = vec.iter();
        assert_eq!(iter.len(), 75);

        iter.next();
        assert_eq!(iter.len(), 74);

        iter.nth(49); // Cross boundary
        assert_eq!(iter.len(), 24);
        Ok(())
    }

    fn run_large_dataset_boundary<V>() -> Result<()>
    where
        V: StoredVec<I = usize, T = i32>,
    {
        let (db, _temp) = setup_db()?;
        let mut vec = V::forced_import(&db, "test", Version::ONE)?;

        // Large stored portion
        for i in 0..10000 {
            vec.push(i);
        }
        vec.write()?;

        // Small pushed portion
        for i in 10000..10100 {
            vec.push(i);
        }

        let collected: Vec<i32> = vec.iter().collect();
        assert_eq!(collected.len(), 10100);

        for (i, &val) in collected.iter().enumerate() {
            assert_eq!(val, i as i32);
        }
        Ok(())
    }

    fn run_skip_take_complex<V>() -> Result<()>
    where
        V: StoredVec<I = usize, T = i32>,
    {
        let (db, _temp) = setup_db()?;
        let mut vec = V::forced_import(&db, "test", Version::ONE)?;

        for i in 0..8000 {
            vec.push(i);
        }
        vec.write()?;

        for i in 8000..12000 {
            vec.push(i);
        }

        // Complex skip/take across boundary
        let collected: Vec<i32> = vec
            .iter()
            .skip(7000)
            .take(3000)
            .skip(500)
            .take(1000)
            .collect();

        assert_eq!(collected.len(), 1000);
        assert_eq!(collected[0], 7500);
        assert_eq!(collected[999], 8499);
        Ok(())
    }

    // ============================================================================
    // Test instantiation for each vec type
    // ============================================================================

    mod bytes {
        use super::*;
        use vecdb::BytesVec;
        type V = BytesVec<usize, i32>;

        #[test]
        fn only_stored() -> Result<()> {
            run_only_stored::<V>()
        }
        #[test]
        fn only_pushed() -> Result<()> {
            run_only_pushed::<V>()
        }
        #[test]
        fn stored_and_pushed() -> Result<()> {
            run_stored_and_pushed::<V>()
        }
        #[test]
        fn skip_across_boundary() -> Result<()> {
            run_skip_across_boundary::<V>()
        }
        #[test]
        fn take_across_boundary() -> Result<()> {
            run_take_across_boundary::<V>()
        }
        #[test]
        fn nth_across_boundary() -> Result<()> {
            run_nth_across_boundary::<V>()
        }
        #[test]
        fn set_position_to_pushed() -> Result<()> {
            run_set_position_to_pushed::<V>()
        }
        #[test]
        fn last_in_pushed() -> Result<()> {
            run_last_in_pushed::<V>()
        }
        #[test]
        fn last_in_stored() -> Result<()> {
            run_last_in_stored::<V>()
        }
        #[test]
        fn exact_size_with_pushed() -> Result<()> {
            run_exact_size_with_pushed::<V>()
        }
        #[test]
        fn large_dataset_boundary() -> Result<()> {
            run_large_dataset_boundary::<V>()
        }
        #[test]
        fn skip_take_complex() -> Result<()> {
            run_skip_take_complex::<V>()
        }
    }

    #[cfg(feature = "zerocopy")]
    mod zerocopy {
        use super::*;
        use vecdb::ZeroCopyVec;
        type V = ZeroCopyVec<usize, i32>;

        #[test]
        fn only_stored() -> Result<()> {
            run_only_stored::<V>()
        }
        #[test]
        fn only_pushed() -> Result<()> {
            run_only_pushed::<V>()
        }
        #[test]
        fn stored_and_pushed() -> Result<()> {
            run_stored_and_pushed::<V>()
        }
        #[test]
        fn skip_across_boundary() -> Result<()> {
            run_skip_across_boundary::<V>()
        }
        #[test]
        fn take_across_boundary() -> Result<()> {
            run_take_across_boundary::<V>()
        }
        #[test]
        fn nth_across_boundary() -> Result<()> {
            run_nth_across_boundary::<V>()
        }
        #[test]
        fn set_position_to_pushed() -> Result<()> {
            run_set_position_to_pushed::<V>()
        }
        #[test]
        fn last_in_pushed() -> Result<()> {
            run_last_in_pushed::<V>()
        }
        #[test]
        fn last_in_stored() -> Result<()> {
            run_last_in_stored::<V>()
        }
        #[test]
        fn exact_size_with_pushed() -> Result<()> {
            run_exact_size_with_pushed::<V>()
        }
        #[test]
        fn large_dataset_boundary() -> Result<()> {
            run_large_dataset_boundary::<V>()
        }
        #[test]
        fn skip_take_complex() -> Result<()> {
            run_skip_take_complex::<V>()
        }
    }

    #[cfg(feature = "pco")]
    mod pco {
        use super::*;
        use vecdb::PcoVec;
        type V = PcoVec<usize, i32>;

        #[test]
        fn only_stored() -> Result<()> {
            run_only_stored::<V>()
        }
        #[test]
        fn only_pushed() -> Result<()> {
            run_only_pushed::<V>()
        }
        #[test]
        fn stored_and_pushed() -> Result<()> {
            run_stored_and_pushed::<V>()
        }
        #[test]
        fn skip_across_boundary() -> Result<()> {
            run_skip_across_boundary::<V>()
        }
        #[test]
        fn take_across_boundary() -> Result<()> {
            run_take_across_boundary::<V>()
        }
        #[test]
        fn nth_across_boundary() -> Result<()> {
            run_nth_across_boundary::<V>()
        }
        #[test]
        fn set_position_to_pushed() -> Result<()> {
            run_set_position_to_pushed::<V>()
        }
        #[test]
        fn last_in_pushed() -> Result<()> {
            run_last_in_pushed::<V>()
        }
        #[test]
        fn last_in_stored() -> Result<()> {
            run_last_in_stored::<V>()
        }
        #[test]
        fn exact_size_with_pushed() -> Result<()> {
            run_exact_size_with_pushed::<V>()
        }
        #[test]
        fn large_dataset_boundary() -> Result<()> {
            run_large_dataset_boundary::<V>()
        }
        #[test]
        fn skip_take_complex() -> Result<()> {
            run_skip_take_complex::<V>()
        }
    }

    #[cfg(feature = "lz4")]
    mod lz4 {
        use super::*;
        use vecdb::LZ4Vec;
        type V = LZ4Vec<usize, i32>;

        #[test]
        fn only_stored() -> Result<()> {
            run_only_stored::<V>()
        }
        #[test]
        fn only_pushed() -> Result<()> {
            run_only_pushed::<V>()
        }
        #[test]
        fn stored_and_pushed() -> Result<()> {
            run_stored_and_pushed::<V>()
        }
        #[test]
        fn skip_across_boundary() -> Result<()> {
            run_skip_across_boundary::<V>()
        }
        #[test]
        fn take_across_boundary() -> Result<()> {
            run_take_across_boundary::<V>()
        }
        #[test]
        fn nth_across_boundary() -> Result<()> {
            run_nth_across_boundary::<V>()
        }
        #[test]
        fn set_position_to_pushed() -> Result<()> {
            run_set_position_to_pushed::<V>()
        }
        #[test]
        fn last_in_pushed() -> Result<()> {
            run_last_in_pushed::<V>()
        }
        #[test]
        fn last_in_stored() -> Result<()> {
            run_last_in_stored::<V>()
        }
        #[test]
        fn exact_size_with_pushed() -> Result<()> {
            run_exact_size_with_pushed::<V>()
        }
        #[test]
        fn large_dataset_boundary() -> Result<()> {
            run_large_dataset_boundary::<V>()
        }
        #[test]
        fn skip_take_complex() -> Result<()> {
            run_skip_take_complex::<V>()
        }
    }

    #[cfg(feature = "zstd")]
    mod zstd {
        use super::*;
        use vecdb::ZstdVec;
        type V = ZstdVec<usize, i32>;

        #[test]
        fn only_stored() -> Result<()> {
            run_only_stored::<V>()
        }
        #[test]
        fn only_pushed() -> Result<()> {
            run_only_pushed::<V>()
        }
        #[test]
        fn stored_and_pushed() -> Result<()> {
            run_stored_and_pushed::<V>()
        }
        #[test]
        fn skip_across_boundary() -> Result<()> {
            run_skip_across_boundary::<V>()
        }
        #[test]
        fn take_across_boundary() -> Result<()> {
            run_take_across_boundary::<V>()
        }
        #[test]
        fn nth_across_boundary() -> Result<()> {
            run_nth_across_boundary::<V>()
        }
        #[test]
        fn set_position_to_pushed() -> Result<()> {
            run_set_position_to_pushed::<V>()
        }
        #[test]
        fn last_in_pushed() -> Result<()> {
            run_last_in_pushed::<V>()
        }
        #[test]
        fn last_in_stored() -> Result<()> {
            run_last_in_stored::<V>()
        }
        #[test]
        fn exact_size_with_pushed() -> Result<()> {
            run_exact_size_with_pushed::<V>()
        }
        #[test]
        fn large_dataset_boundary() -> Result<()> {
            run_large_dataset_boundary::<V>()
        }
        #[test]
        fn skip_take_complex() -> Result<()> {
            run_skip_take_complex::<V>()
        }
    }

    // ============================================================================
    // EagerVec Tests (wrapping different underlying vec types)
    // ============================================================================

    #[cfg(feature = "zerocopy")]
    mod eager_zerocopy {
        use super::*;
        use vecdb::{EagerVec, ZeroCopyVec};
        type V = EagerVec<ZeroCopyVec<usize, i32>>;

        #[test]
        fn only_stored() -> Result<()> {
            run_only_stored::<V>()
        }
        #[test]
        fn only_pushed() -> Result<()> {
            run_only_pushed::<V>()
        }
        #[test]
        fn stored_and_pushed() -> Result<()> {
            run_stored_and_pushed::<V>()
        }
        #[test]
        fn skip_across_boundary() -> Result<()> {
            run_skip_across_boundary::<V>()
        }
        #[test]
        fn take_across_boundary() -> Result<()> {
            run_take_across_boundary::<V>()
        }
        #[test]
        fn nth_across_boundary() -> Result<()> {
            run_nth_across_boundary::<V>()
        }
        #[test]
        fn set_position_to_pushed() -> Result<()> {
            run_set_position_to_pushed::<V>()
        }
        #[test]
        fn last_in_pushed() -> Result<()> {
            run_last_in_pushed::<V>()
        }
        #[test]
        fn last_in_stored() -> Result<()> {
            run_last_in_stored::<V>()
        }
        #[test]
        fn exact_size_with_pushed() -> Result<()> {
            run_exact_size_with_pushed::<V>()
        }
        #[test]
        fn large_dataset_boundary() -> Result<()> {
            run_large_dataset_boundary::<V>()
        }
        #[test]
        fn skip_take_complex() -> Result<()> {
            run_skip_take_complex::<V>()
        }
    }

    #[cfg(feature = "pco")]
    mod eager_pco {
        use super::*;
        use vecdb::{EagerVec, PcoVec};
        type V = EagerVec<PcoVec<usize, i32>>;

        #[test]
        fn only_stored() -> Result<()> {
            run_only_stored::<V>()
        }
        #[test]
        fn only_pushed() -> Result<()> {
            run_only_pushed::<V>()
        }
        #[test]
        fn stored_and_pushed() -> Result<()> {
            run_stored_and_pushed::<V>()
        }
        #[test]
        fn skip_across_boundary() -> Result<()> {
            run_skip_across_boundary::<V>()
        }
        #[test]
        fn take_across_boundary() -> Result<()> {
            run_take_across_boundary::<V>()
        }
        #[test]
        fn nth_across_boundary() -> Result<()> {
            run_nth_across_boundary::<V>()
        }
        #[test]
        fn set_position_to_pushed() -> Result<()> {
            run_set_position_to_pushed::<V>()
        }
        #[test]
        fn last_in_pushed() -> Result<()> {
            run_last_in_pushed::<V>()
        }
        #[test]
        fn last_in_stored() -> Result<()> {
            run_last_in_stored::<V>()
        }
        #[test]
        fn exact_size_with_pushed() -> Result<()> {
            run_exact_size_with_pushed::<V>()
        }
        #[test]
        fn large_dataset_boundary() -> Result<()> {
            run_large_dataset_boundary::<V>()
        }
        #[test]
        fn skip_take_complex() -> Result<()> {
            run_skip_take_complex::<V>()
        }
    }
}

// ============================================================================
// Raw-specific Tests (holes and updates - only BytesVec and ZeroCopyVec)
// ============================================================================

mod raw_features {
    use super::*;
    use std::ops::DerefMut;
    use vecdb::{BytesVec, RawVecInner};

    // Generic test functions for raw vecs

    fn run_iter_skips_holes<V>() -> Result<()>
    where
        V: StoredVec<I = usize, T = i32> + DerefMut,
        V::Target: RawVecOps,
    {
        let (db, _temp) = setup_db()?;
        let mut vec = V::forced_import(&db, "test", Version::ONE)?;

        for i in 0..10 {
            vec.push(i);
        }
        vec.write()?;

        // Delete some values (create holes)
        vec.deref_mut().delete_at(3);
        vec.deref_mut().delete_at(5);
        vec.deref_mut().delete_at(7);

        let collected: Vec<i32> = vec.iter().collect();
        // Should skip holes: 0,1,2,4,6,8,9
        assert_eq!(collected, vec![0, 1, 2, 4, 6, 8, 9]);
        Ok(())
    }

    fn run_iter_with_updates<V>() -> Result<()>
    where
        V: StoredVec<I = usize, T = i32> + DerefMut,
        V::Target: RawVecOps,
    {
        let (db, _temp) = setup_db()?;
        let mut vec = V::forced_import(&db, "test", Version::ONE)?;

        for i in 0..10 {
            vec.push(i);
        }
        vec.write()?;

        // Update some values
        vec.deref_mut().update_at(2, 200)?;
        vec.deref_mut().update_at(5, 500)?;
        vec.deref_mut().update_at(8, 800)?;

        let collected: Vec<i32> = vec.iter().collect();
        assert_eq!(collected, vec![0, 1, 200, 3, 4, 500, 6, 7, 800, 9]);
        Ok(())
    }

    fn run_iter_with_holes_and_updates<V>() -> Result<()>
    where
        V: StoredVec<I = usize, T = i32> + DerefMut,
        V::Target: RawVecOps,
    {
        let (db, _temp) = setup_db()?;
        let mut vec = V::forced_import(&db, "test", Version::ONE)?;

        for i in 0..10 {
            vec.push(i);
        }
        vec.write()?;

        // Create holes and updates
        vec.deref_mut().delete_at(1);
        vec.deref_mut().delete_at(3);
        vec.deref_mut().update_at(2, 200)?;
        vec.deref_mut().update_at(5, 500)?;

        let collected: Vec<i32> = vec.iter().collect();
        // Should be: 0, (skip 1), 200, (skip 3), 4, 500, 6, 7, 8, 9
        assert_eq!(collected, vec![0, 200, 4, 500, 6, 7, 8, 9]);
        Ok(())
    }

    fn run_iter_holes_and_pushed<V>() -> Result<()>
    where
        V: StoredVec<I = usize, T = i32> + DerefMut,
        V::Target: RawVecOps,
    {
        let (db, _temp) = setup_db()?;
        let mut vec = V::forced_import(&db, "test", Version::ONE)?;

        for i in 0..5 {
            vec.push(i);
        }
        vec.write()?;

        // Create holes in stored data
        vec.deref_mut().delete_at(1);
        vec.deref_mut().delete_at(3);

        // Push more data
        for i in 5..10 {
            vec.push(i);
        }

        let collected: Vec<i32> = vec.iter().collect();
        // Should be: 0, (skip 1), 2, (skip 3), 4, 5, 6, 7, 8, 9
        assert_eq!(collected, vec![0, 2, 4, 5, 6, 7, 8, 9]);
        Ok(())
    }

    fn run_iter_updates_and_pushed<V>() -> Result<()>
    where
        V: StoredVec<I = usize, T = i32> + DerefMut,
        V::Target: RawVecOps,
    {
        let (db, _temp) = setup_db()?;
        let mut vec = V::forced_import(&db, "test", Version::ONE)?;

        for i in 0..5 {
            vec.push(i);
        }
        vec.write()?;

        // Update some stored values
        vec.deref_mut().update_at(1, 100)?;
        vec.deref_mut().update_at(3, 300)?;

        // Push more data
        for i in 5..10 {
            vec.push(i);
        }

        let collected: Vec<i32> = vec.iter().collect();
        assert_eq!(collected, vec![0, 100, 2, 300, 4, 5, 6, 7, 8, 9]);
        Ok(())
    }

    fn run_iter_skip_over_holes<V>() -> Result<()>
    where
        V: StoredVec<I = usize, T = i32> + DerefMut,
        V::Target: RawVecOps,
    {
        let (db, _temp) = setup_db()?;
        let mut vec = V::forced_import(&db, "test", Version::ONE)?;

        for i in 0..20 {
            vec.push(i);
        }
        vec.write()?;

        // Create holes at indices 5, 6, 7
        vec.deref_mut().delete_at(5);
        vec.deref_mut().delete_at(6);
        vec.deref_mut().delete_at(7);

        // Skip past the holes
        let collected: Vec<i32> = vec.iter().skip(5).take(5).collect();
        // After skipping 5 (0,1,2,3,4), next are 8,9,10,11,12 (holes are skipped automatically)
        assert_eq!(collected, vec![8, 9, 10, 11, 12]);
        Ok(())
    }

    fn run_fill_holes<V>() -> Result<()>
    where
        V: StoredVec<I = usize, T = i32> + DerefMut,
        V::Target: RawVecOps,
    {
        let (db, _temp) = setup_db()?;
        let mut vec = V::forced_import(&db, "test", Version::ONE)?;

        for i in 0..10 {
            vec.push(i);
        }
        vec.write()?;

        // Create holes
        vec.deref_mut().delete_at(2);
        vec.deref_mut().delete_at(5);

        // Fill first hole
        let idx = vec.deref_mut().fill_first_hole_or_push(999)?;
        assert_eq!(idx, 2);

        let collected: Vec<i32> = vec.iter().collect();
        // 0,1,999,3,4,(skip 5),6,7,8,9
        assert_eq!(collected, vec![0, 1, 999, 3, 4, 6, 7, 8, 9]);
        Ok(())
    }

    // Helper trait for raw vec operations
    pub trait RawVecOps {
        fn delete_at(&mut self, index: usize);
        fn update_at(&mut self, index: usize, value: i32) -> Result<()>;
        fn fill_first_hole_or_push(&mut self, value: i32) -> Result<usize>;
    }

    impl<I, T, S> RawVecOps for RawVecInner<I, T, S>
    where
        I: vecdb::VecIndex,
        T: vecdb::VecValue + From<i32> + Into<i32>,
        S: vecdb::RawStrategy<T>,
    {
        fn delete_at(&mut self, index: usize) {
            vecdb::RawVecInner::delete_at(self, index)
        }
        fn update_at(&mut self, index: usize, value: i32) -> Result<()> {
            vecdb::RawVecInner::update_at(self, index, T::from(value))
        }
        fn fill_first_hole_or_push(&mut self, value: i32) -> Result<usize> {
            vecdb::RawVecInner::fill_first_hole_or_push(self, T::from(value)).map(|i| i.to_usize())
        }
    }

    // ============================================================================
    // BytesVec Tests
    // ============================================================================

    mod bytes {
        use super::*;
        type V = BytesVec<usize, i32>;

        #[test]
        fn iter_skips_holes() -> Result<()> {
            run_iter_skips_holes::<V>()
        }
        #[test]
        fn iter_with_updates() -> Result<()> {
            run_iter_with_updates::<V>()
        }
        #[test]
        fn iter_with_holes_and_updates() -> Result<()> {
            run_iter_with_holes_and_updates::<V>()
        }
        #[test]
        fn iter_holes_and_pushed() -> Result<()> {
            run_iter_holes_and_pushed::<V>()
        }
        #[test]
        fn iter_updates_and_pushed() -> Result<()> {
            run_iter_updates_and_pushed::<V>()
        }
        #[test]
        fn iter_skip_over_holes() -> Result<()> {
            run_iter_skip_over_holes::<V>()
        }
        #[test]
        fn fill_holes() -> Result<()> {
            run_fill_holes::<V>()
        }
    }

    // ============================================================================
    // ZeroCopyVec Tests
    // ============================================================================

    #[cfg(feature = "zerocopy")]
    mod zerocopy {
        use super::*;
        use vecdb::ZeroCopyVec;
        type V = ZeroCopyVec<usize, i32>;

        #[test]
        fn iter_skips_holes() -> Result<()> {
            run_iter_skips_holes::<V>()
        }
        #[test]
        fn iter_with_updates() -> Result<()> {
            run_iter_with_updates::<V>()
        }
        #[test]
        fn iter_with_holes_and_updates() -> Result<()> {
            run_iter_with_holes_and_updates::<V>()
        }
        #[test]
        fn iter_holes_and_pushed() -> Result<()> {
            run_iter_holes_and_pushed::<V>()
        }
        #[test]
        fn iter_updates_and_pushed() -> Result<()> {
            run_iter_updates_and_pushed::<V>()
        }
        #[test]
        fn iter_skip_over_holes() -> Result<()> {
            run_iter_skip_over_holes::<V>()
        }
        #[test]
        fn fill_holes() -> Result<()> {
            run_fill_holes::<V>()
        }
    }
}
