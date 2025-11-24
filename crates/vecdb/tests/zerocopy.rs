use rawdb::Database;
use std::collections::BTreeSet;
use tempfile::TempDir;
use vecdb::{
    AnyStoredVec, AnyVec, CollectableVec, GenericStoredVec, Importable, Result, Stamp,
    TypedVecIterator, Version, ZeroCopyVec,
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
fn test_raw_vec_comprehensive() -> Result<(), Box<dyn std::error::Error>> {
    let version = Version::TWO;
    let (database, _temp) = setup_test_db()?;
    let mut options = (&database, "vec", version).into();

    {
        let mut vec: VEC = ZeroCopyVec::forced_import_with(options)?;

        (0..21_u32).for_each(|v| {
            vec.push(v);
        });

        let mut iter = vec.into_iter();
        assert!(iter.get(0) == Some(0));
        assert!(iter.get(1) == Some(1));
        assert!(iter.get(2) == Some(2));
        assert!(iter.get(20) == Some(20));
        assert!(iter.get(21).is_none());
        drop(iter);

        vec.write()?;

        assert!(vec.header().stamp() == Stamp::new(0));
    }

    {
        let mut vec: VEC = ZeroCopyVec::forced_import_with(options)?;

        vec.mut_header().update_stamp(Stamp::new(100));

        assert_eq!(vec.header().stamp(), Stamp::new(100));

        let mut iter = vec.iter()?;
        assert_eq!(iter.get(0), Some(0));
        assert_eq!(iter.get(1), Some(1));
        assert_eq!(iter.get(2), Some(2));
        assert_eq!(iter.get(3), Some(3));
        assert_eq!(iter.get(4), Some(4));
        assert_eq!(iter.get(5), Some(5));
        assert_eq!(iter.get(20), Some(20));
        assert_eq!(iter.get(20), Some(20));
        assert_eq!(iter.get(0), Some(0));
        drop(iter);

        vec.push(21);
        vec.push(22);

        assert_eq!(vec.stored_len(), 21);
        assert_eq!(vec.pushed_len(), 2);
        assert_eq!(vec.len(), 23);

        let mut iter = vec.into_iter();
        assert_eq!(iter.get(20), Some(20));
        assert_eq!(iter.get(21), Some(21));
        assert_eq!(iter.get(22), Some(22));
        assert!(iter.get(23).is_none());
        drop(iter);

        vec.write()?;
    }

    {
        let mut vec: VEC = ZeroCopyVec::forced_import_with(options)?;

        assert_eq!(vec.header().stamp(), Stamp::new(100));

        assert_eq!(vec.stored_len(), 23);
        assert_eq!(vec.pushed_len(), 0);
        assert_eq!(vec.len(), 23);

        let mut iter = vec.into_iter();
        assert_eq!(iter.get(0), Some(0));
        assert_eq!(iter.get(20), Some(20));
        assert_eq!(iter.get(21), Some(21));
        assert_eq!(iter.get(22), Some(22));
        drop(iter);

        vec.truncate_if_needed(14)?;

        assert_eq!(vec.stored_len(), 14);
        assert_eq!(vec.pushed_len(), 0);
        assert_eq!(vec.len(), 14);

        let mut iter = vec.into_iter();
        assert_eq!(iter.get(0), Some(0));
        assert_eq!(iter.get(5), Some(5));
        assert_eq!(iter.get(20), None);
        drop(iter);

        assert_eq!(
            vec.collect_signed_range(Some(-5), None),
            vec![9, 10, 11, 12, 13]
        );

        vec.push(vec.len() as u32);
        assert_eq!(vec.iter()?.last(), Some(14));

        assert_eq!(
            vec.into_iter().collect::<Vec<_>>(),
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]
        );

        vec.write()?;
    }

    {
        let mut vec: VEC = ZeroCopyVec::forced_import_with(options)?;

        assert_eq!(vec.iter()?.last(), Some(14));

        assert_eq!(
            vec.into_iter().collect::<Vec<_>>(),
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]
        );

        vec.reset()?;

        assert_eq!(vec.pushed_len(), 0);
        assert_eq!(vec.stored_len(), 0);
        assert_eq!(vec.len(), 0);

        (0..21_u32).for_each(|v| {
            vec.push(v);
        });

        assert_eq!(vec.pushed_len(), 21);
        assert_eq!(vec.stored_len(), 0);
        assert_eq!(vec.len(), 21);

        let mut iter = vec.into_iter();
        assert_eq!(iter.get(0), Some(0));
        assert_eq!(iter.get(20), Some(20));
        assert_eq!(iter.get(21), None);
        drop(iter);

        let reader = vec.create_reader();
        assert_eq!(vec.take(10, &reader)?, Some(10));
        assert_eq!(vec.holes(), &BTreeSet::from([10]));
        assert_eq!(vec.get_any_or_read(10, &reader)?, None);
        drop(reader);

        vec.write()?;

        assert!(vec.holes() == &BTreeSet::from([10]));
    }

    {
        let mut vec: VEC = ZeroCopyVec::forced_import_with(options)?;

        assert!(vec.holes() == &BTreeSet::from([10]));

        let reader = vec.create_reader();
        assert!(vec.get_any_or_read(10, &reader)?.is_none());
        drop(reader);

        vec.update(10, 10)?;
        vec.update(0, 10)?;

        let reader = vec.create_reader();
        assert_eq!(vec.holes(), &BTreeSet::new());
        assert_eq!(vec.get_any_or_read(0, &reader)?, Some(10));
        assert_eq!(vec.get_any_or_read(10, &reader)?, Some(10));
        drop(reader);

        vec.write()?;
    }

    options = options.with_saved_stamped_changes(10);

    {
        let mut vec: VEC = ZeroCopyVec::forced_import_with(options)?;

        assert_eq!(
            vec.collect(),
            vec![
                10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
            ]
        );

        vec.truncate_if_needed(10)?;

        let reader = vec.create_reader();
        vec.take(5, &reader)?;
        vec.update(3, 5)?;
        vec.push(21);
        drop(reader);

        assert_eq!(
            vec.collect_holed()?,
            vec![
                Some(10),
                Some(1),
                Some(2),
                Some(5),
                Some(4),
                None,
                Some(6),
                Some(7),
                Some(8),
                Some(9),
                Some(21)
            ]
        );

        vec.stamped_flush_with_changes(Stamp::new(1))?;
    }

    {
        let mut vec: VEC = ZeroCopyVec::forced_import_with(options)?;

        assert_eq!(vec.collect(), vec![10, 1, 2, 5, 4, 6, 7, 8, 9, 21]);

        let reader = vec.create_reader();
        vec.take(0, &reader)?;
        vec.update(1, 5)?;
        vec.push(5);
        vec.push(6);
        vec.push(7);
        drop(reader);

        assert_eq!(
            vec.collect_holed()?,
            vec![
                None,
                Some(5),
                Some(2),
                Some(5),
                Some(4),
                None,
                Some(6),
                Some(7),
                Some(8),
                Some(9),
                Some(21),
                Some(5),
                Some(6),
                Some(7)
            ]
        );

        vec.stamped_flush_with_changes(Stamp::new(2))?;
    }

    {
        let mut vec: VEC = ZeroCopyVec::forced_import_with(options)?;

        assert_eq!(
            vec.collect_holed()?,
            vec![
                None,
                Some(5),
                Some(2),
                Some(5),
                Some(4),
                None,
                Some(6),
                Some(7),
                Some(8),
                Some(9),
                Some(21),
                Some(5),
                Some(6),
                Some(7)
            ]
        );

        vec.rollback()?;

        assert_eq!(vec.stamp(), Stamp::new(1));

        assert_eq!(
            vec.collect_holed()?,
            vec![
                Some(10),
                Some(1),
                Some(2),
                Some(5),
                Some(4),
                None,
                Some(6),
                Some(7),
                Some(8),
                Some(9),
                Some(21)
            ]
        );

        vec.rollback()?;

        assert_eq!(
            vec.collect(),
            vec![
                10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
            ]
        );

        vec.stamped_flush(Stamp::new(0))?;
    }

    {
        let mut vec: VEC = ZeroCopyVec::forced_import_with(options)?;

        assert_eq!(
            vec.collect(),
            vec![
                10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
            ]
        );

        vec.truncate_if_needed(10)?;

        let reader = vec.create_reader();
        vec.take(5, &reader)?;
        vec.update(3, 5)?;
        vec.push(21);
        drop(reader);

        assert_eq!(
            vec.collect_holed()?,
            vec![
                Some(10),
                Some(1),
                Some(2),
                Some(5),
                Some(4),
                None,
                Some(6),
                Some(7),
                Some(8),
                Some(9),
                Some(21)
            ]
        );

        vec.stamped_flush_with_changes(Stamp::new(1))?;
    }

    {
        let mut vec: VEC = ZeroCopyVec::forced_import_with(options)?;

        assert_eq!(vec.collect(), vec![10, 1, 2, 5, 4, 6, 7, 8, 9, 21]);

        let reader = vec.create_reader();
        vec.take(0, &reader)?;
        vec.update(1, 5)?;
        vec.push(5);
        vec.push(6);
        vec.push(7);
        drop(reader);

        assert_eq!(
            vec.collect_holed()?,
            vec![
                None,
                Some(5),
                Some(2),
                Some(5),
                Some(4),
                None,
                Some(6),
                Some(7),
                Some(8),
                Some(9),
                Some(21),
                Some(5),
                Some(6),
                Some(7)
            ]
        );

        vec.stamped_flush_with_changes(Stamp::new(2))?;
    }

    {
        let mut vec: VEC = ZeroCopyVec::forced_import_with(options)?;

        assert_eq!(
            vec.collect_holed()?,
            vec![
                None,
                Some(5),
                Some(2),
                Some(5),
                Some(4),
                None,
                Some(6),
                Some(7),
                Some(8),
                Some(9),
                Some(21),
                Some(5),
                Some(6),
                Some(7)
            ]
        );

        vec.rollback_before(Stamp::new(1))?;

        assert_eq!(
            vec.collect(),
            vec![
                10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
            ]
        );

        vec.stamped_flush(Stamp::new(0))?;

        vec.truncate_if_needed(10)?;
        let reader = vec.create_reader();
        vec.take(5, &reader)?;
        vec.update(3, 5)?;
        vec.push(21);
        drop(reader);

        let reader = vec.create_reader();
        vec.take(0, &reader)?;
        vec.update(1, 5)?;
        vec.push(5);
        vec.push(6);
        vec.push(7);
        drop(reader);

        assert_eq!(
            vec.collect_holed()?,
            vec![
                None,
                Some(5),
                Some(2),
                Some(5),
                Some(4),
                None,
                Some(6),
                Some(7),
                Some(8),
                Some(9),
                Some(21),
                Some(5),
                Some(6),
                Some(7)
            ]
        );
    }

    {
        let mut vec: VEC = ZeroCopyVec::forced_import_with(options)?;

        assert_eq!(
            vec.collect(),
            vec![
                10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
            ]
        );

        vec.truncate_if_needed(10)?;
        let reader = vec.create_reader();
        vec.take(5, &reader)?;
        vec.update(3, 5)?;
        vec.push(21);
        drop(reader);

        vec.stamped_flush_with_changes(Stamp::new(1))?;
        assert_eq!(vec.stamp(), Stamp::new(1));

        let reader = vec.create_reader();
        vec.take(0, &reader)?;
        vec.update(1, 5)?;
        vec.push(5);
        vec.push(6);
        vec.push(7);
        drop(reader);

        vec.stamped_flush_with_changes(Stamp::new(2))?;

        assert_eq!(
            vec.collect_holed()?,
            vec![
                None,
                Some(5),
                Some(2),
                Some(5),
                Some(4),
                None,
                Some(6),
                Some(7),
                Some(8),
                Some(9),
                Some(21),
                Some(5),
                Some(6),
                Some(7)
            ]
        );

        vec.rollback_before(Stamp::new(1))?;

        assert_eq!(vec.stamp(), Stamp::new(0));

        assert_eq!(
            vec.collect(),
            vec![
                10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
            ]
        );

        vec.truncate_if_needed(10)?;
        let reader = vec.create_reader();
        vec.take(5, &reader)?;
        vec.update(3, 5)?;
        vec.push(21);
        drop(reader);

        let reader = vec.create_reader();
        vec.take(0, &reader)?;
        vec.update(1, 5)?;
        vec.push(5);
        vec.push(6);
        vec.push(7);
        drop(reader);

        assert_eq!(
            vec.collect_holed()?,
            vec![
                None,
                Some(5),
                Some(2),
                Some(5),
                Some(4),
                None,
                Some(6),
                Some(7),
                Some(8),
                Some(9),
                Some(21),
                Some(5),
                Some(6),
                Some(7)
            ]
        );

        assert_eq!(vec.stamp(), Stamp::new(0));
        vec.stamped_flush_with_changes(Stamp::new(2))?;
        assert_eq!(vec.stamp(), Stamp::new(2));

        vec.rollback_before(Stamp::new(1))?;

        assert_eq!(vec.stamp(), Stamp::new(0));

        assert_eq!(
            vec.collect(),
            vec![
                10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
            ]
        );

        vec.stamped_flush_with_changes(Stamp::new(0))?;

        let vec: VEC = ZeroCopyVec::forced_import_with(options)?;

        assert_eq!(
            vec.collect(),
            vec![
                10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
            ]
        );
    }

    Ok(())
}
