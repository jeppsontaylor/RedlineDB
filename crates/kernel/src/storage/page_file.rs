use std::path::Path;
use std::sync::Mutex;

use crate::format::{Page, PageId};
use crate::io::{FileHandle, FileSystem, StdFileSystem};
use crate::{Error, Result};

#[derive(Debug)]
pub struct PageFile<Fs: FileSystem = StdFileSystem> {
    file: Mutex<Fs::File>,
    page_size: usize,
}

impl PageFile<StdFileSystem> {
    pub fn create(path: impl AsRef<Path>, page_size: usize) -> Result<Self> {
        Self::create_with_fs(path, page_size, StdFileSystem)
    }

    pub fn open(path: impl AsRef<Path>, page_size: usize) -> Result<Self> {
        Self::open_with_fs(path, page_size, StdFileSystem)
    }
}

impl<Fs: FileSystem> PageFile<Fs> {
    pub fn create_with_fs(path: impl AsRef<Path>, page_size: usize, fs: Fs) -> Result<Self> {
        validate_page_size(page_size)?;
        let file = fs.open_rw_create(path.as_ref())?;
        file.set_len(0)?;
        Ok(Self {
            file: Mutex::new(file),
            page_size,
        })
    }

    pub fn open_with_fs(path: impl AsRef<Path>, page_size: usize, fs: Fs) -> Result<Self> {
        validate_page_size(page_size)?;
        let file = fs.open_rw_create(path.as_ref())?;
        Ok(Self {
            file: Mutex::new(file),
            page_size,
        })
    }

    pub fn page_size(&self) -> usize {
        self.page_size
    }

    pub fn page_count(&self) -> Result<u64> {
        let file = self
            .file
            .lock()
            .map_err(|_| Error::CorruptPage("page file mutex poisoned"))?;
        Ok(file.len()? / self.page_size as u64)
    }

    pub fn read_page(&self, page_id: PageId) -> Result<Page> {
        if page_id.0 == 0 {
            return Err(Error::CorruptPage("page id zero is invalid"));
        }
        let offset = self.offset(page_id)?;
        let mut bytes = vec![0; self.page_size];
        let mut file = self
            .file
            .lock()
            .map_err(|_| Error::CorruptPage("page file mutex poisoned"))?;
        file.read_exact_at(offset, &mut bytes)?;
        Page::from_bytes(bytes)
    }

    pub fn write_page(&self, page: &Page) -> Result<()> {
        if page.as_bytes().len() != self.page_size {
            return Err(Error::BufferTooSmall {
                needed: self.page_size,
                actual: page.as_bytes().len(),
            });
        }
        page.validate()?;
        let page_id = page.header()?.page_id;
        if page_id.0 == 0 {
            return Err(Error::CorruptPage("page id zero is invalid"));
        }
        let offset = self.offset(page_id)?;
        let mut file = self
            .file
            .lock()
            .map_err(|_| Error::CorruptPage("page file mutex poisoned"))?;
        file.write_all_at(offset, page.as_bytes())
    }

    pub fn sync_data(&self) -> Result<()> {
        let file = self
            .file
            .lock()
            .map_err(|_| Error::CorruptPage("page file mutex poisoned"))?;
        file.sync_data()
    }

    fn offset(&self, page_id: PageId) -> Result<u64> {
        page_id
            .0
            .checked_sub(1)
            .and_then(|idx| idx.checked_mul(self.page_size as u64))
            .ok_or(Error::CorruptPage("page offset overflow"))
    }
}

fn validate_page_size(page_size: usize) -> Result<()> {
    if page_size < crate::format::PAGE_HEADER_LEN + crate::format::SLOT_LEN {
        return Err(Error::BufferTooSmall {
            needed: crate::format::PAGE_HEADER_LEN + crate::format::SLOT_LEN,
            actual: page_size,
        });
    }
    if page_size > u16::MAX as usize {
        return Err(Error::CorruptPage("page size exceeds on-page u16 bounds"));
    }
    Ok(())
}
