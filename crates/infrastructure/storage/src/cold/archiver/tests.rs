//! Failure-reporting regressions for the archival driver.

use std::sync::Arc;

use tempfile::TempDir;

use super::ColdArchiver;
use crate::cold::{probe::ProbeDb, ColdConfig, ColdStore};

/// A hot read that fails must surface as an error, never as "nothing left to archive": the
/// caller counts the failure and retries, where a `Drained` would stop archival for good while
/// every health signal still reads clean.
#[test]
fn unreadable_hot_tier_fails_the_pass() {
    let tmp = TempDir::new().expect("temp dir");
    let cold = Arc::new(
        ColdStore::open(&ColdConfig { dir: tmp.path().to_path_buf() }).expect("open cold store"),
    );
    let archiver = ColdArchiver::new(ProbeDb::failing_reads(), cold);

    let sealed = archiver.seal_due(1, || false);
    assert!(sealed.is_err(), "an unreadable hot tier must fail the seal, got {sealed:?}");

    let archived = archiver.archive_due(1, None);
    assert!(archived.is_err(), "an unreadable hot tier must fail the drain, got {archived:?}");
}
