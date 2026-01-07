use std::{collections::BTreeMap, mem};

use log::debug;
use smallvec::SmallVec;

use crate::{Error, Region, Regions, Result};

/// Tracks the layout of regions and holes in the database file.
///
/// Maintains a dual-index for holes: `start_to_hole` for O(1) lookup by position,
/// and `hole_to_starts` for O(log n) lookup of smallest adequate hole.
#[derive(Debug, Default)]
pub struct Layout {
    start_to_region: BTreeMap<usize, Region>,
    start_to_hole: BTreeMap<usize, usize>,
    /// Secondary index: hole size -> list of starts with that size.
    /// Enables O(log n) best-fit hole search.
    hole_to_starts: BTreeMap<usize, SmallVec<[usize; 1]>>,
    start_to_reserved: BTreeMap<usize, usize>,
    /// Holes from region moves that can't be reused until flush
    pending_holes: BTreeMap<usize, usize>,
}

impl From<&Regions> for Layout {
    fn from(regions: &Regions) -> Self {
        let mut start_to_region = BTreeMap::new();

        regions
            .index_to_region()
            .iter()
            .flatten()
            .for_each(|region| {
                start_to_region.insert(region.meta().start(), region.clone());
            });

        let mut layout = Self {
            start_to_region: start_to_region.clone(),
            start_to_hole: BTreeMap::default(),
            hole_to_starts: BTreeMap::default(),
            start_to_reserved: BTreeMap::default(),
            pending_holes: BTreeMap::default(),
        };

        let mut prev_end = 0;
        for (start, region) in start_to_region {
            if prev_end != start {
                let size = start - prev_end;
                layout.insert_hole(prev_end, size);
            }
            let reserved = region.meta().reserved();
            prev_end = start + reserved;
        }

        layout
    }
}

impl Layout {
    /// Inserts a hole and updates both indexes.
    fn insert_hole(&mut self, start: usize, size: usize) {
        self.start_to_hole.insert(start, size);
        self.hole_to_starts.entry(size).or_default().push(start);
    }

    /// Removes a hole and updates both indexes.
    fn remove_hole(&mut self, start: usize) -> Option<usize> {
        let size = self.start_to_hole.remove(&start)?;

        if let Some(starts) = self.hole_to_starts.get_mut(&size) {
            starts.retain(|s| *s != start);
            if starts.is_empty() {
                self.hole_to_starts.remove(&size);
            }
        }

        Some(size)
    }

    pub fn start_to_region(&self) -> &BTreeMap<usize, Region> {
        &self.start_to_region
    }

    pub fn start_to_hole(&self) -> &BTreeMap<usize, usize> {
        &self.start_to_hole
    }

    pub fn len(&self) -> usize {
        let mut len = 0;
        let mut start = 0;
        if let Some((start_reserved, reserved)) = self.get_last_reserved() {
            start = start_reserved;
            len = start + reserved;
        }
        if let Some((hole_start, gap)) = self.get_last_hole()
            && hole_start >= start
        {
            start = hole_start;
            len = start + gap;
        }
        // Include pending holes (from recently deleted regions, not yet promoted)
        if let Some((&pending_start, &pending_size)) = self.pending_holes.last_key_value()
            && pending_start >= start
        {
            start = pending_start;
            len = start + pending_size;
        }
        if let Some((region_start, region)) = self.get_last_region()
            && region_start >= start
        {
            len = region_start + region.meta().reserved();
        }
        len
    }

    pub fn get_last_region(&self) -> Option<(usize, &Region)> {
        self.start_to_region
            .last_key_value()
            .map(|(start, region)| (*start, region))
    }

    fn get_last_hole(&self) -> Option<(usize, usize)> {
        self.start_to_hole
            .last_key_value()
            .map(|(start, gap)| (*start, *gap))
    }

    fn get_last_reserved(&self) -> Option<(usize, usize)> {
        self.start_to_reserved
            .last_key_value()
            .map(|(start, reserved)| (*start, *reserved))
    }

    pub fn is_last_anything(&self, region: &Region) -> bool {
        let Some((last_start, last_region)) = self.get_last_region() else {
            return false;
        };

        last_region.index() == region.index()
            && self
                .get_last_hole()
                .is_none_or(|(hole_start, _)| last_start > hole_start)
            && self
                .get_last_reserved()
                .is_none_or(|(reserved_start, _)| last_start > reserved_start)
            && self
                .pending_holes
                .last_key_value()
                .is_none_or(|(&pending_start, _)| last_start > pending_start)
    }

    pub fn insert_region(&mut self, start: usize, region: &Region) {
        assert!(self.start_to_region.insert(start, region.clone()).is_none())
    }

    pub fn move_region(&mut self, new_start: usize, region: &Region) -> Result<()> {
        self.remove_region(region)?;
        self.insert_region(new_start, region);
        Ok(())
    }

    pub fn remove_region(&mut self, region: &Region) -> Result<()> {
        let region_meta = region.meta();
        let start = region_meta.start();
        let reserved = region_meta.reserved();

        let removed = self.start_to_region.remove(&start);

        if removed
            .as_ref()
            .is_none_or(|region_| region.index() != region_.index())
        {
            return Err(Error::RegionIndexMismatch);
        }

        self.pending_holes.insert(start, reserved);

        Ok(())
    }

    pub fn get_hole(&self, start: usize) -> Option<usize> {
        self.start_to_hole.get(&start).copied()
    }

    /// Finds the smallest hole that can fit the requested size.
    ///
    /// Uses a secondary index for O(log n) best-fit lookup.
    /// Returns the start position of the smallest adequate hole.
    pub fn find_smallest_adequate_hole(&self, min_size: usize) -> Option<usize> {
        // Find the smallest size >= min_size
        self.hole_to_starts
            .range(min_size..)
            .next()
            .and_then(|(_, starts)| starts.first().copied())
    }

    pub fn remove_or_compress_hole(&mut self, start: usize, compress_by: usize) -> Result<()> {
        let Some(size) = self.remove_hole(start) else {
            return Ok(());
        };

        if size == compress_by {
            // Hole fully consumed, already removed
            Ok(())
        } else if size > compress_by {
            // Hole partially consumed, insert remainder
            let new_start = start + compress_by;
            let new_size = size - compress_by;
            self.insert_hole(new_start, new_size);
            Ok(())
        } else {
            Err(Error::HoleTooSmall {
                hole_size: size,
                requested: compress_by,
            })
        }
    }

    pub fn reserve(&mut self, start: usize, reserved: usize) {
        if self.start_to_reserved.insert(start, reserved).is_some() {
            unreachable!();
        }
    }

    /// Takes the reserved space at the given start position, removing it from tracking.
    /// Returns None if no reservation exists at this position.
    pub fn take_reserved(&mut self, start: usize) -> Option<usize> {
        self.start_to_reserved.remove(&start)
    }

    /// Promote pending holes to real holes after flush.
    /// Safe to reuse now that metadata changes are durable.
    pub fn promote_pending_holes(&mut self, name: &str) {
        let count = self.pending_holes.len();
        if count > 0 {
            debug!("{}: promoted {} pending holes", name, count);
        }
        for (start, mut size) in mem::take(&mut self.pending_holes) {
            let mut final_start = start;

            // Coalesce with adjacent real hole BEFORE
            if let Some((&hole_start, &hole_size)) = self.start_to_hole.range(..start).next_back()
                && hole_start + hole_size == start
            {
                self.remove_hole(hole_start);
                final_start = hole_start;
                size += hole_size;
            }

            // Coalesce with adjacent real hole AFTER
            if let Some(hole_after_size) = self.remove_hole(final_start + size) {
                size += hole_after_size;
            }

            self.insert_hole(final_start, size);
        }
    }
}
