//! Benchmark for the cold-tier read path, proving the open-jar cache optimization lands:
//!
//! - `jar_cache`: point reads that hit the warm open-jar cache skip the per-read mmap an uncached
//!   segment would pay. Reading one row from each of N distinct epochs (N > cache capacity) forces
//!   a reload per read; reading the same epoch repeatedly is served from the cached mmap. Both
//!   touch the same row count, so the delta is the per-read open+mmap cost the cache removes.

// A bench deliberately exercises only a slice of the crate's dependency surface.
#![allow(unused_crate_dependencies)]

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use rayls_infrastructure_storage::{ColdSegment, ColdSegmentKind};
use tempfile::TempDir;

/// Rows per sealed epoch jar.
const ROWS_PER_EPOCH: u64 = 256;
/// Synthetic row payload size, a stand-in for a bcs-encoded `ConsensusHeader`.
const ROW_BYTES: usize = 512;

/// Builds a consensus_blocks segment with `epochs` sealed jars, each holding `ROWS_PER_EPOCH` rows.
///
/// Returns the `TempDir` (kept alive so the on-disk jars survive) alongside the opened segment.
fn build_segment(epochs: u64) -> (TempDir, ColdSegment) {
    let tmp = TempDir::new().expect("tempdir");
    let segment = ColdSegment::open(tmp.path(), ColdSegmentKind::ConsensusBlocks).expect("open");
    let row = vec![0xABu8; ROW_BYTES];
    let mut number = 0u64;
    for epoch in 0..epochs {
        // Epoch ids are 1-based u32; start_key is the first contiguous block number of the jar.
        segment.begin_epoch((epoch + 1) as u32, number).expect("begin_epoch");
        for _ in 0..ROWS_PER_EPOCH {
            segment.append_row(&[row.as_slice()]).expect("append_row");
            number += 1;
        }
        segment.commit().expect("commit");
    }
    (tmp, segment)
}

/// Cache hit vs miss on point reads.
///
/// `cache_miss_distinct_epochs` reads row 0 of each of `EPOCHS` distinct epochs; with `EPOCHS`
/// beyond the segment's jar-cache capacity, every read reloads its jar. `cache_hit_same_epoch`
/// reads the same epoch repeatedly, served from the cached mmap. Both read `EPOCHS` rows, so the
/// per-element delta is the open+mmap cost the cache removes.
fn bench_jar_cache(c: &mut Criterion) {
    const EPOCHS: u64 = 64;
    let (_tmp, segment) = build_segment(EPOCHS);

    let mut group = c.benchmark_group("jar_cache");
    group.throughput(Throughput::Elements(EPOCHS));

    group.bench_function("cache_miss_distinct_epochs", |b| {
        b.iter(|| {
            for epoch_idx in 0..EPOCHS {
                // Row 0 of a different epoch each step, so the bounded cache never holds it.
                let number = epoch_idx * ROWS_PER_EPOCH;
                black_box(segment.read_by_number(number).expect("read"));
            }
        });
    });

    group.bench_function("cache_hit_same_epoch", |b| {
        b.iter(|| {
            for _ in 0..EPOCHS {
                // Always epoch 1, so after the first read the mmap is served from the cache.
                black_box(segment.read_by_number(0).expect("read"));
            }
        });
    });

    group.finish();
}

criterion_group!(benches, bench_jar_cache);
criterion_main!(benches);
