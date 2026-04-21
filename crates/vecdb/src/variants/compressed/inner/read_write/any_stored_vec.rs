use std::{mem, path::PathBuf};

use rawdb::{Database, Region};

use crate::{AnyStoredVec, AnyVec, Error, Header, Result, Stamp, VecIndex, VecValue, WritableVec};

use super::super::{CompressionStrategy, Page};
use super::ReadWriteCompressedVec;

impl<I, T, S> AnyStoredVec for ReadWriteCompressedVec<I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: CompressionStrategy<T>,
{
    #[inline]
    fn db_path(&self) -> PathBuf {
        self.base.db_path()
    }

    #[inline]
    fn region(&self) -> &Region {
        self.base.region()
    }

    #[inline]
    fn header(&self) -> &Header {
        self.base.header()
    }

    #[inline]
    fn mut_header(&mut self) -> &mut Header {
        self.base.mut_header()
    }

    #[inline]
    fn saved_stamped_changes(&self) -> u16 {
        self.base.saved_stamped_changes()
    }

    #[inline]
    fn stored_len(&self) -> usize {
        self.base.stored_len()
    }

    #[inline]
    fn real_stored_len(&self) -> usize {
        self.pages.read().stored_len(Self::PER_PAGE)
    }

    fn write(&mut self) -> Result<bool> {
        self.base.write_header_if_needed()?;

        let stored_len = self.stored_len();
        let pushed_len = self.base.pushed().len();

        // Phase 1a: Copy metadata snapshot (minimal lock scope)
        let (truncate_at, starting_page_index, partial_page) = {
            let pages = self.pages.read();

            let real_stored_len = pages.stored_len(Self::PER_PAGE);
            if stored_len > real_stored_len {
                return Err(Error::CorruptedRegion {
                    name: self.name().to_string(),
                    region_len: real_stored_len,
                });
            }

            if pushed_len == 0 && stored_len == real_stored_len {
                return Ok(false);
            }

            let starting_page_index = Self::index_to_page_index(stored_len);
            if starting_page_index > pages.len() {
                return Err(Error::CorruptedRegion {
                    name: self.name().to_string(),
                    region_len: pages.len(),
                });
            }

            if starting_page_index < pages.len() {
                let partial_len = stored_len % Self::PER_PAGE;
                let page = *pages
                    .get(starting_page_index)
                    .ok_or(Error::ExpectVecToHaveIndex)?;
                (
                    page.start,
                    starting_page_index,
                    if partial_len != 0 {
                        Some((page, partial_len))
                    } else {
                        None
                    },
                )
            } else {
                (pages.next_start(), starting_page_index, None)
            }
        };
        // Pages lock released — decompression happens without blocking readers

        // Fast path: append to existing raw page without reading it back.
        // When the last page is raw, not truncated, and won't overflow, just
        // write the new pushed bytes at the end of the existing page data.
        if let Some((page, partial_len)) = partial_page
            && page.is_raw()
            && partial_len == page.values_count() as usize
            && partial_len + pushed_len < Self::PER_PAGE
        {
            let taken = mem::take(self.base.mut_pushed());
            let raw = S::values_to_bytes(&taken);
            let append_at = page.end() as usize;
            self.region().truncate_write(append_at, &raw)?;

            let mut pages = self.pages.write();
            pages.truncate(starting_page_index);
            pages.checked_push(
                starting_page_index,
                Page::raw(
                    page.start,
                    page.bytes + raw.len() as u32,
                    (partial_len + pushed_len) as u32,
                ),
            )?;
            self.base.update_stored_len(stored_len + pushed_len);
            pages.flush()?;
            return Ok(true);
        }

        // Phase 1b: Read partial page (if needed) outside lock
        let mut values = if let Some((page, partial_len)) = partial_page {
            let reader = self.create_reader();
            let data = reader.unchecked_read(page.start as usize, page.bytes as usize);
            let mut page_values = S::decode_page(data, &page)?;
            page_values.truncate(partial_len);
            page_values
        } else {
            vec![]
        };

        // Phase 2: Encode pages (no locks held)
        // Full pages are compressed; the last partial page is stored raw.
        let taken = mem::take(self.base.mut_pushed());
        if values.is_empty() {
            values = taken;
        } else {
            values.extend_from_slice(&taken);
        }

        let num_pages = values.len().div_ceil(Self::PER_PAGE);
        let mut buf = Vec::with_capacity(values.len() * Self::SIZE_OF_T);
        let mut page_sizes: Vec<(usize, usize, bool)> = Vec::with_capacity(num_pages);
        for chunk in values.chunks(Self::PER_PAGE) {
            if chunk.len() == Self::PER_PAGE {
                let compressed = Self::compress_page(chunk)?;
                page_sizes.push((compressed.len(), chunk.len(), false));
                buf.extend_from_slice(&compressed);
            } else {
                let raw = S::values_to_bytes(chunk);
                page_sizes.push((raw.len(), chunk.len(), true));
                buf.extend_from_slice(&raw);
            }
        }

        // Phase 3: Write to region first (without holding pages lock to avoid deadlock)
        self.region().truncate_write(truncate_at as usize, &buf)?;

        let mut pages = self.pages.write();
        pages.truncate(starting_page_index);

        for (i, &(byte_len, values_len, is_raw)) in page_sizes.iter().enumerate() {
            let start = pages.next_start();
            let page = if is_raw {
                Page::raw(start, byte_len as u32, values_len as u32)
            } else {
                Page::compressed(start, byte_len as u32, values_len as u32)
            };
            pages.checked_push(starting_page_index + i, page)?;
        }

        self.base.update_stored_len(stored_len + pushed_len);
        pages.flush()?;

        Ok(true)
    }

    #[inline]
    fn serialize_changes(&self) -> Result<Vec<u8>> {
        self.serialize_compressed_changes()
    }

    #[inline]
    fn db(&self) -> Database {
        self.base.db()
    }

    fn any_stamped_write_with_changes(&mut self, stamp: Stamp) -> Result<()> {
        <Self as WritableVec<I, T>>::stamped_write_with_changes(self, stamp)
    }

    fn remove(self) -> Result<()> {
        Self::remove(self)
    }

    fn any_truncate_if_needed_at(&mut self, index: usize) -> Result<()> {
        <Self as WritableVec<I, T>>::truncate_if_needed_at(self, index)
    }

    fn any_reset(&mut self) -> Result<()> {
        <Self as WritableVec<I, T>>::reset(self)
    }
}
