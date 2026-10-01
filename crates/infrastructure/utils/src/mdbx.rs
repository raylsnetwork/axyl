// SPDX-License-Identifier: BUSL-1.1

//! Shared MDBX page-size constants and detection used by the node databases and the CLI.

use std::{io::Read, path::Path};

/// Page size for newly created MDBX datafiles (16 KiB), used unless an explicit size is given.
///
/// libmdbx fixes the page size when a datafile is created and ignores this setting when opening an
/// existing one, so existing databases keep the page size they were created with.
pub const DEFAULT_MDBX_PAGE_SIZE: usize = 16 * 1024;

/// Smallest page size libmdbx accepts, matching its own `MDBX_MIN_PAGESIZE` constant.
pub const MIN_MDBX_PAGE_SIZE: usize = 256;
/// Largest page size libmdbx accepts, matching its own `MDBX_MAX_PAGESIZE` constant.
pub const MAX_MDBX_PAGE_SIZE: usize = 64 * 1024;

/// Reads the datafile's page size from the spacing of its meta pages; `None` if under two survive.
pub fn detect_page_size(dat: &Path) -> Option<usize> {
    detect_page_size_in(std::fs::File::open(dat).ok()?)
}

/// Finds the page size from a reader over the datafile head, split out so a test can bound its
/// reads.
fn detect_page_size_in(reader: impl Read) -> Option<usize> {
    // libmdbx writes this 56-bit magic little-endian in every meta page header.
    const MDBX_MAGIC: u64 = 0x59659DBDEF4C11;
    // The meta pages sit at the datafile start, so this head covers every page size up to the max.
    const HEAD_BYTES: u64 = 1 << 18;

    let magic = MDBX_MAGIC.to_le_bytes();
    // Drop the trailing byte so the match ignores the version byte libmdbx packs beside the magic.
    let magic = &magic[..7];
    let mut head = Vec::new();
    reader.take(HEAD_BYTES).read_to_end(&mut head).ok()?;
    let mut hits =
        head.windows(magic.len()).enumerate().filter(|(_, w)| *w == magic).map(|(i, _)| i);
    let first = hits.next()?;
    let second = hits.next()?;
    let ps = second - first;
    (ps.is_power_of_two() && (MIN_MDBX_PAGE_SIZE..=MAX_MDBX_PAGE_SIZE).contains(&ps)).then_some(ps)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a datafile head with the 7-byte MDBX magic repeated every `ps` bytes.
    /// The magic is hardcoded here so the test catches a wrong `MDBX_MAGIC` in the code.
    fn synthetic(ps: usize, total: usize) -> Vec<u8> {
        let magic = [0x11u8, 0x4C, 0xEF, 0xBD, 0x9D, 0x65, 0x59];
        let mut buf = vec![0u8; total];
        let mut off = 8;
        while off + magic.len() <= buf.len() {
            buf[off..off + magic.len()].copy_from_slice(&magic);
            off += ps;
        }
        buf
    }

    #[test]
    fn detects_page_size_from_meta_spacing() {
        assert_eq!(detect_page_size_in(&synthetic(16384, 1 << 19)[..]), Some(16384));
    }

    #[test]
    fn rejects_a_file_without_two_meta_pages() {
        assert_eq!(detect_page_size_in(&b"not an MDBX datafile"[..]), None);
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
        let data = synthetic(16384, 8 * 1024 * 1024);
        let read = Rc::new(Cell::new(0usize));
        let counted = Counting { inner: std::io::Cursor::new(&data), read: read.clone() };
        assert_eq!(detect_page_size_in(counted), Some(16384));
        assert!(read.get() <= 1 << 18, "read {} bytes, expected at most 256 KiB", read.get());
    }
}
