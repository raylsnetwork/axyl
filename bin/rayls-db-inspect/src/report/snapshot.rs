// SPDX-License-Identifier: BUSL-1.1
//! `snapshot --to DIR`: one stopped node's consensus database copied as a single committed hot
//! state, plus its sealed cold jars, for inspection elsewhere or as evidence.
//!
//! From a readable database, MDBX performs the copy inside a read transaction (`MDBX_CP_COMPACT`:
//! it walks every page through cursors, which also validates them, and writes only used pages;
//! the file stays sparse at the geometry's floor), so the copy's head meta is steady and it opens
//! read-only with no recovery step.
//!
//! From a database whose last commit was never synced (a killed or crashed node), MDBX refuses
//! to read until it is recovered, and recovery writes. Nothing else writes to a stopped node, so
//! its files are copied as they are and the copy is recovered, leaving the original untouched
//! for the node's own restart.
//!
//! Like every command, this refuses a database another process holds open.

use crate::{
    node_db::{NodeDb, OpenOptions},
    report::summary::{summary, SummaryNodeView},
};
use eyre::{bail, eyre, WrapErr as _};
use rayls_infrastructure_storage::{ColdConfig, ColdStore};
use rayls_infrastructure_types::Epoch;
use serde::Serialize;
use std::{
    fs,
    io::{Read as _, Seek as _, SeekFrom, Write as _},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Serialize)]
pub struct SnapshotReport {
    /// True when the source could not be read (stopped, last commit unsynced) and its files were
    /// copied as they were, then recovered in the copy; false for the MDBX copy of a readable
    /// source.
    pub recovered_copy: bool,
    /// Label of the copied node.
    pub source: String,
    /// The consensus-db directory that was copied.
    pub source_path: String,
    pub destination: String,
    /// Unix time (seconds) when the copy started.
    pub taken_at_unix: u64,
    /// Bytes `mdbx.dat` occupies on disk (MDBX keeps the file's apparent size at the geometry's
    /// floor, so the compacted copy is sparse).
    pub mdbx_bytes: u64,
    /// Epochs whose sealed jars were copied (the consensus_blocks segment decides sealing).
    pub cold_epochs: Vec<Epoch>,
    pub cold_files: usize,
    pub cold_bytes: u64,
    /// The copy read back after writing: what the snapshot holds, as `summary` would show it.
    pub copy: SummaryNodeView,
}

/// Copies `db` into `to`, which must not exist or be an empty directory outside the source.
pub fn snapshot(db: &NodeDb, to: &Path) -> eyre::Result<SnapshotReport> {
    let source = fs::canonicalize(&db.path)
        .wrap_err_with(|| format!("{}: resolve the source", db.path.display()))?;
    let created = prepare_destination(&source, to)?;
    finish(to, created, copy_all(db, &source, to))
}

/// Copies the stopped, unsynced database at `source` (label `label`) into `to` as its files
/// stand, then recovers the copy so it opens; the source is not modified.
pub fn snapshot_stopped_unsynced(
    label: &str,
    source: &Path,
    to: &Path,
) -> eyre::Result<SnapshotReport> {
    let source = fs::canonicalize(source)
        .wrap_err_with(|| format!("{}: resolve the source", source.display()))?;
    let created = prepare_destination(&source, to)?;
    finish(to, created, copy_raw_and_recover(label, &source, to))
}

/// Checks and creates the destination; returns whether this call created it.
fn prepare_destination(source: &Path, to: &Path) -> eyre::Result<bool> {
    let created = !to.exists();
    if created {
        // the parent must exist; the destination it will hold must lie outside the source
        let parent = match to.parent() {
            Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
            _ => PathBuf::from("."),
        };
        let parent = fs::canonicalize(&parent)
            .wrap_err_with(|| format!("{}: the destination's parent directory", to.display()))?;
        let name = to.file_name().ok_or_else(|| eyre!("{}: no directory name", to.display()))?;
        refuse_overlap(source, &parent.join(name), to)?;
        // `create_dir`, not `create_dir_all`: a second run racing for the same destination must
        // fail here rather than share it
        fs::create_dir(to).wrap_err_with(|| format!("{}: create the destination", to.display()))?;
    } else {
        if !to.is_dir() {
            bail!("{}: destination exists and is not a directory", to.display());
        }
        if fs::read_dir(to)?.next().is_some() {
            bail!("{}: destination directory is not empty", to.display());
        }
        refuse_overlap(source, &fs::canonicalize(to)?, to)?;
    }
    Ok(created)
}

/// On failure, removes what was written so a half copy never passes for a whole one.
fn finish(
    to: &Path,
    created: bool,
    result: eyre::Result<SnapshotReport>,
) -> eyre::Result<SnapshotReport> {
    if result.is_err() {
        if created {
            let _ = fs::remove_dir_all(to);
        } else {
            remove_snapshot_files(to);
        }
    }
    result
}

fn refuse_overlap(source: &Path, destination: &Path, shown: &Path) -> eyre::Result<()> {
    if destination.starts_with(source) || source.starts_with(destination) {
        bail!(
            "{}: destination lies inside the source {} (or contains it); snapshot elsewhere",
            shown.display(),
            source.display()
        );
    }
    Ok(())
}

fn unix_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or_default()
}

fn copy_all(db: &NodeDb, source: &Path, to: &Path) -> eyre::Result<SnapshotReport> {
    let taken_at_unix = unix_now();
    // 1. the database: one read transaction, so the copy is exactly one committed state
    let dat = to.join("mdbx.dat");
    db.copy_mdbx_to(&dat).map_err(|e| {
        let text = format!("{e:#}");
        if text.contains("Invalid argument") {
            eyre!(
                "copy the MDBX environment to {}: {text}; MDBX writes the copy with O_DIRECT, \
                 which tmpfs and some network filesystems refuse: choose a disk-backed destination",
                dat.display()
            )
        } else {
            eyre!("copy the MDBX environment to {}: {text}", dat.display())
        }
    })?;
    let mdbx_bytes = allocated_bytes(&fs::metadata(&dat)?);
    // 2. the sealed cold jars
    let (mut cold_epochs, mut cold_files, mut cold_bytes) = (Vec::new(), 0usize, 0u64);
    if let Some(cold) = db.cold_store() {
        for epoch in cold.consensus_blocks().sealed_epochs() {
            for (segment, name) in
                [(cold.consensus_blocks(), "consensus_blocks"), (cold.batches(), "batches")]
            {
                // a batches jar without its consensus_blocks jar is a torn seal left by an
                // interrupted archival; the consensus_blocks segment decides what is sealed
                if !segment.is_epoch_sealed(epoch) {
                    continue;
                }
                let out_dir = to.join("cold").join(name);
                fs::create_dir_all(&out_dir)?;
                for file in segment.jar_files(epoch) {
                    let Some(file_name) = file.file_name() else { continue };
                    let target = out_dir.join(file_name);
                    cold_bytes += fs::copy(&file, &target).wrap_err_with(|| {
                        format!("copy {} to {}", file.display(), target.display())
                    })?;
                    cold_files += 1;
                }
            }
            cold_epochs.push(epoch);
        }
    }
    // 3. the copied jars, read back: a torn or short jar is revealed by the reopened index (rows >
    //    0) and a read at each end
    if !cold_epochs.is_empty() {
        let copied = ColdStore::open(&ColdConfig { dir: to.join("cold") })
            .map_err(|e| eyre!("reopen the copied cold tier: {e}"))?;
        let sealed = copied.consensus_blocks().sealed_epochs();
        for epoch in &cold_epochs {
            let Some(range) = copied.consensus_blocks().key_range_for_epoch(*epoch) else {
                bail!(
                    "copied jar of epoch {epoch} is not sealed in the copy: torn during the copy"
                );
            };
            if !sealed.contains(epoch) {
                bail!("copied jar of epoch {epoch} is missing from the copy's index");
            }
            for number in [*range.start(), *range.end()] {
                if copied
                    .read_consensus_block_checked(number)
                    .map_err(|e| eyre!("read header {number} from the copied jar: {e}"))?
                    .is_none()
                {
                    bail!("header {number} is missing from the copied jar of epoch {epoch}");
                }
            }
        }
    }
    // 4. read the copy back: proves it opens with no recovery and records what it holds
    let copy = read_back(to)?;
    Ok(SnapshotReport {
        recovered_copy: false,
        source: db.label.clone(),
        source_path: source.display().to_string(),
        destination: to.display().to_string(),
        taken_at_unix,
        mdbx_bytes,
        cold_epochs,
        cold_files,
        cold_bytes,
        copy,
    })
}

/// Opens the finished copy and summarises it.
fn read_back(to: &Path) -> eyre::Result<SummaryNodeView> {
    let copy_db = NodeDb::open(&format!("snapshot={}", to.display()), &OpenOptions::default())
        .wrap_err("open the finished copy")?;
    let mut copies = summary(std::slice::from_ref(&copy_db))?.nodes;
    copies.pop().ok_or_else(|| eyre!("the copy produced no summary"))
}

/// Copies the files of a stopped database and recovers the copy.
fn copy_raw_and_recover(label: &str, source: &Path, to: &Path) -> eyre::Result<SnapshotReport> {
    let taken_at_unix = unix_now();
    let dat = to.join("mdbx.dat");
    copy_sparse(&source.join("mdbx.dat"), &dat)
        .wrap_err_with(|| format!("copy {}", source.join("mdbx.dat").display()))?;
    let (mut cold_epochs, mut cold_files, mut cold_bytes) = (Vec::new(), 0usize, 0u64);
    let cold_dir = source.join("cold");
    if cold_dir.join("consensus_blocks").is_dir() && cold_dir.join("batches").is_dir() {
        let cold = ColdStore::open(&ColdConfig { dir: cold_dir })
            .map_err(|e| eyre!("{label}: open the cold tier: {e}"))?;
        for epoch in cold.consensus_blocks().sealed_epochs() {
            for (segment, name) in
                [(cold.consensus_blocks(), "consensus_blocks"), (cold.batches(), "batches")]
            {
                if !segment.is_epoch_sealed(epoch) {
                    continue;
                }
                let out_dir = to.join("cold").join(name);
                fs::create_dir_all(&out_dir)?;
                for file in segment.jar_files(epoch) {
                    let Some(file_name) = file.file_name() else { continue };
                    cold_bytes += fs::copy(&file, out_dir.join(file_name))
                        .wrap_err_with(|| format!("copy {}", file.display()))?;
                    cold_files += 1;
                }
            }
            cold_epochs.push(epoch);
        }
    }
    // the copy, not the source, gets the recovery pass
    crate::node_db::recover(to)
        .wrap_err_with(|| format!("recover the copy at {}", to.display()))?;
    let mdbx_bytes = allocated_bytes(&fs::metadata(&dat)?);
    let copy = read_back(to)?;
    Ok(SnapshotReport {
        recovered_copy: true,
        source: label.to_owned(),
        source_path: source.display().to_string(),
        destination: to.display().to_string(),
        taken_at_unix,
        mdbx_bytes,
        cold_epochs,
        cold_files,
        cold_bytes,
        copy,
    })
}

/// Copies a file without materialising its holes: MDBX datafiles are sparse at the geometry's
/// floor, so a plain read-write copy would inflate them.
fn copy_sparse(from: &Path, to: &Path) -> std::io::Result<()> {
    let mut src = fs::File::open(from)?;
    let mut dst = fs::File::create(to)?;
    let len = src.metadata()?.len();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = src.read(&mut buf)?;
        if n == 0 {
            break;
        }
        if buf[..n].iter().all(|b| *b == 0) {
            dst.seek(SeekFrom::Current(n as i64))?;
        } else {
            dst.write_all(&buf[..n])?;
        }
    }
    dst.set_len(len)?;
    dst.flush()
}

/// Removes what a snapshot writes, and nothing else, from a directory that existed before.
fn remove_snapshot_files(to: &Path) {
    for name in ["mdbx.dat", "mdbx.lck"] {
        let _ = fs::remove_file(to.join(name));
    }
    let _ = fs::remove_dir_all(to.join("cold"));
}

/// Bytes a file occupies on disk; its apparent length where the platform does not say.
fn allocated_bytes(meta: &fs::Metadata) -> u64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        meta.blocks() * 512
    }
    #[cfg(not(unix))]
    {
        meta.len()
    }
}
