// SPDX-License-Identifier: BUSL-1.1

//! Shared MDBX page-size constants and detection used by the node databases and the CLI.

use std::{io::Read, path::Path};

/// Page size for newly created MDBX datafiles (16 KiB), used unless an explicit size is given.
///
/// libmdbx fixes the page size when a datafile is created and ignores this setting when opening an
/// existing one, so existing databases keep the page size they were created with.
pub const DEFAULT_MDBX_PAGE_SIZE: usize = 16 * 1024;

/// The MDBX datafile name inside a database directory.
pub const MDBX_DAT: &str = "mdbx.dat";

/// Smallest page size we accept, the common OS page size; libmdbx itself allows down to 256 B.
pub const MIN_MDBX_PAGE_SIZE: usize = 4 * 1024;
/// Largest page size libmdbx accepts, matching its own `MDBX_MAX_PAGESIZE` constant.
pub const MAX_MDBX_PAGE_SIZE: usize = 64 * 1024;

/// Returns `page_size` if it is a power of two from [`MIN_MDBX_PAGE_SIZE`] to
/// [`MAX_MDBX_PAGE_SIZE`].
pub fn check_page_size(page_size: usize) -> Result<usize, String> {
    if !(MIN_MDBX_PAGE_SIZE..=MAX_MDBX_PAGE_SIZE).contains(&page_size) {
        return Err(format!(
            "must be between {MIN_MDBX_PAGE_SIZE} and {MAX_MDBX_PAGE_SIZE} bytes, got {page_size}"
        ));
    }
    if !page_size.is_power_of_two() {
        return Err(format!("must be a power of two, got {page_size}"));
    }
    Ok(page_size)
}

/// Page size for a new or empty datafile; `None` lets an existing one keep its own geometry.
pub fn new_datafile_page_size(
    db_dir: &Path,
    configured: Option<usize>,
) -> Result<Option<usize>, String> {
    // libmdbx creates the database in an empty datafile, so that one counts as new.
    let dat_len = std::fs::metadata(db_dir.join(MDBX_DAT)).map(|m| m.len()).unwrap_or(0);
    if dat_len > 0 {
        return Ok(None);
    }
    check_page_size(configured.unwrap_or(DEFAULT_MDBX_PAGE_SIZE)).map(Some)
}

/// Reads the datafile's page size from a surviving backup meta page; `None` if neither survives.
pub fn detect_page_size(dat: &Path) -> Option<usize> {
    detect_page_size_in(std::fs::File::open(dat).ok()?)
}

/// Finds the page size from a reader over the datafile head, split out so a test can bound its
/// reads.
fn detect_page_size_in(reader: impl Read) -> Option<usize> {
    // libmdbx writes this 56-bit magic little-endian in every meta page header.
    const MDBX_MAGIC: u64 = 0x59659DBDEF4C11;
    // The magic follows the 20-byte page header and the meta's version byte.
    const MAGIC_OFFSET: usize = 21;
    // The page header stores the page number here; meta pages are pages 0, 1 and 2.
    const PGNO_OFFSET: usize = 16;
    // The meta pages sit at the datafile start, so this head covers every page size up to the max.
    const HEAD_BYTES: u64 = 1 << 18;

    let magic = MDBX_MAGIC.to_le_bytes();
    // The magic is only 7 bytes, so drop the zero padding byte that to_le_bytes adds to the u64.
    let magic = &magic[..7];
    let mut head = Vec::new();
    reader.take(HEAD_BYTES).read_to_end(&mut head).ok()?;
    head.windows(magic.len()).enumerate().filter(|(_, w)| *w == magic).find_map(|(hit, _)| {
        let page_start = hit.checked_sub(MAGIC_OFFSET)?;
        let pgno = head.get(page_start + PGNO_OFFSET..page_start + PGNO_OFFSET + 4)?;
        let pgno = u32::from_le_bytes(pgno.try_into().ok()?) as usize;
        // Meta page 0 starts at offset 0, so only a backup meta page reveals the page size.
        let ps = (matches!(pgno, 1 | 2) && page_start % pgno == 0).then(|| page_start / pgno)?;
        check_page_size(ps).ok()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a datafile head holding meta pages `metas`, laid out as libmdbx writes them.
    /// The magic is hardcoded here so the test catches a wrong `MDBX_MAGIC` in the code.
    fn synthetic(ps: usize, total: usize, metas: &[u32]) -> Vec<u8> {
        let magic = [0x11u8, 0x4C, 0xEF, 0xBD, 0x9D, 0x65, 0x59];
        let mut buf = vec![0u8; total];
        for &pgno in metas {
            let page = pgno as usize * ps;
            buf[page + 16..page + 20].copy_from_slice(&pgno.to_le_bytes());
            buf[page + 21..page + 28].copy_from_slice(&magic);
        }
        buf
    }

    #[test]
    fn detects_page_size_from_either_backup_meta_page() {
        for ps in [MIN_MDBX_PAGE_SIZE, 16384, MAX_MDBX_PAGE_SIZE] {
            for metas in [&[0, 1, 2][..], &[1, 2], &[1], &[2]] {
                let head = synthetic(ps, 1 << 18, metas);
                assert_eq!(detect_page_size_in(&head[..]), Some(ps), "{ps} with metas {metas:?}");
            }
        }
    }

    #[test]
    fn rejects_a_file_without_a_backup_meta_page() {
        assert_eq!(detect_page_size_in(&b"not an MDBX datafile"[..]), None);
        assert_eq!(detect_page_size_in(&synthetic(16384, 1 << 18, &[0])[..]), None);
        // A layout below the supported minimum is not trusted.
        assert_eq!(detect_page_size_in(&synthetic(2048, 1 << 18, &[1, 2])[..]), None);
    }

    #[test]
    fn checks_page_size_range_and_power_of_two() {
        for ok in [4096, 8192, 16384, 32768, 65536] {
            assert_eq!(check_page_size(ok), Ok(ok));
        }
        for bad in [0, 256, 2048, 12 * 1024, 128 * 1024] {
            assert!(check_page_size(bad).is_err(), "{bad}");
        }
    }

    /// Detection reads only the bounded head, so a huge datafile is never loaded whole.
    #[test]
    fn reads_only_the_head() {
        use std::{cell::Cell, rc::Rc};

        struct Counting<R> {
            inner: R,
            read: Rc<Cell<usize>>,
        }
        impl<R: Read> Read for Counting<R> {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                let n = self.inner.read(buf)?;
                self.read.set(self.read.get() + n);
                Ok(n)
            }
        }

        // An 8 MiB file: a whole-file read would pull far more than the bounded head.
        let data = synthetic(16384, 8 * 1024 * 1024, &[0, 1, 2]);
        let read = Rc::new(Cell::new(0usize));
        let counted = Counting { inner: std::io::Cursor::new(&data), read: read.clone() };
        assert_eq!(detect_page_size_in(counted), Some(16384));
        assert!(read.get() <= 1 << 18, "read {} bytes, expected at most 256 KiB", read.get());
    }
}
