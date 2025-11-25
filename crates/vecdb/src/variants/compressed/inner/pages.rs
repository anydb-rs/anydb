use rawdb::{Database, Region};

use crate::{Bytes, Result};

use super::Page;

#[derive(Debug, Clone)]
pub struct Pages {
    region: Region,
    vec: Vec<Page>,
    change_at: Option<usize>,
}

impl Pages {
    const SIZE_OF_PAGE: usize = size_of::<Page>();

    pub fn import(db: &Database, name: &str) -> Result<Self> {
        let region = db.create_region_if_needed(name)?;

        let vec = region
            .create_reader()
            .read_all()
            .chunks(Self::SIZE_OF_PAGE)
            .map(Page::from_bytes)
            .collect::<Result<_>>()?;

        Ok(Self {
            region,
            vec,
            change_at: None,
        })
    }

    pub fn flush(&mut self) -> Result<()> {
        if self.change_at.is_none() {
            return Ok(());
        }

        let change_at = self.change_at.take().unwrap();
        let at = change_at * Self::SIZE_OF_PAGE;

        let bytes: Vec<u8> = self.vec[change_at..]
            .iter()
            .flat_map(|page| page.to_bytes())
            .collect();

        self.region.truncate_write(at, &bytes)?;

        Ok(())
    }

    pub fn len(&self) -> usize {
        self.vec.len()
    }

    pub fn get(&self, page_index: usize) -> Option<&Page> {
        self.vec.get(page_index)
    }

    pub fn last(&self) -> Option<&Page> {
        self.vec.last()
    }

    pub fn checked_push(&mut self, page_index: usize, page: Page) {
        if page_index != self.vec.len() {
            panic!();
        }

        self.set_changed_at(page_index);

        self.vec.push(page);
    }

    fn set_changed_at(&mut self, page_index: usize) {
        if self.change_at.is_none_or(|pi| pi > page_index) {
            self.change_at.replace(page_index);
        }
    }

    pub fn reset(&mut self) {
        self.truncate(0);
    }

    pub fn truncate(&mut self, page_index: usize) -> Option<Page> {
        let page = self.get(page_index).cloned();
        self.vec.truncate(page_index);
        self.set_changed_at(page_index);
        page
    }

    pub fn stored_len(&self, per_page: usize) -> usize {
        if let Some(last) = self.last() {
            (self.len() - 1) * per_page + last.values as usize
        } else {
            0
        }
    }

    pub fn remove(self) -> Result<()> {
        self.region.remove()?;
        Ok(())
    }
}
