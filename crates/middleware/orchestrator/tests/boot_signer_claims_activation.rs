//! I16: the node's boot row reads must see the activation the chainspec schedules, not the handle
//! default.
//!
//! `run()` (`epoch_manager/core.rs`) reads consensus rows long before any consensus component is
//! built: a typed `last_record::<ConsensusBlocks>()` of the tip, and `catchup_accumulator`'s
//! reverse walk, whose `skip` of the certificates region only lands on the leader header if the
//! activation handle in `types`' `primary/fork.rs` matches the era the rows were written in.
//! Under the `never` default a post-activation row projects an error *or* plausible garbage, so a
//! restart over post-activation history is broken at boot, before the handle is set. That is the
//! placement this target pins: the handle comes from `apply_signer_claims_activation`, called by
//! `launch_node` above the DB open, never from a consensus component.
//!
//! The handle is process-global and write-once, so the process that wrote the post-activation rows
//! (handle = the scheduled epoch) and the process that boots with it unset cannot be the same one.
//! This file's process is the booting node: it never touches the handle until it calls the
//! production init site, and spawns a copy of itself to seed the database the way a node that had
//! already activated would have left it.
//!
//! A dedicated target, not a test alongside `tests/it`: everything else in the crate must keep
//! seeing the `never` default.
#![allow(unused_crate_dependencies)]

use rayls_execution_evm::chainspec::{RaylsHardFork, ScheduledFork};
use rayls_infrastructure_storage::{
    mdbx::{MdbxConfig, MdbxDatabase},
    open_db_with_consensus_config,
    tables::ConsensusBlocks,
};
use rayls_infrastructure_types::{
    gas_accumulator::GasAccumulator, AuthorityIdentifier, BlockHash, Certificate,
    CertificateDigest, CommittedSubDag, ConsensusHeader, ConsensusHeaderMeta, Database, DbTx as _,
    Epoch, HeaderBuilder, ReputationScores, SignerClaim,
};
use rayls_middleware_orchestrator::epoch_manager::{
    apply_signer_claims_activation, catchup_accumulator,
};
use rayls_testing_test_utils::default_test_execution_node;
use std::{
    collections::BTreeSet,
    env,
    panic::{self, AssertUnwindSafe},
    path::Path,
    process::Command,
};
use tempfile::tempdir;

// Deliberately not the never sentinel: this is what a scheduled fork looks like, and it is what the
// chainspec accessor reports once the fixture schedule carries it.
const ACTIVATION_EPOCH: Epoch = 5_000;
const BLOCK_NUMBER: u64 = 7;
const WRITER_ENV: &str = "RAYLS_ORCH_BOOT_ACTIVATION_WRITER";
const DB_PATH_ENV: &str = "RAYLS_ORCH_BOOT_ACTIVATION_DB";

// The chainspec schedule the node profile would resolve to, with the SignerClaims fork given an
// epoch activation. `RaylsChainHardforks::signer_claims_activation_epoch` (through
// `apply_signer_claims_activation`) is the only reader of that field here.
fn scheduled() -> Vec<ScheduledFork> {
    let mut schedule: Vec<ScheduledFork> = RaylsHardFork::devnet().into_iter().collect();
    let entry = schedule
        .iter_mut()
        .find(|fork| fork.fork == RaylsHardFork::SignerClaims)
        .expect("the schedule lists every hardfork variant");
    assert_eq!(
        entry.epoch_activation,
        Epoch::MAX,
        "the fixture must start from an unactivated fork"
    );
    entry.epoch_activation = ACTIVATION_EPOCH;
    schedule
}

fn claim() -> SignerClaim {
    SignerClaim {
        target_digest: CertificateDigest::new([9; 32]),
        target_epoch: ACTIVATION_EPOCH,
        target_round: 5,
        ranks: vec![0, 1, 2],
        signature: Default::default(),
    }
}

// A certificate whose header sits in the activated era, optionally carrying a claims region.
fn cert(author: u8, round: u32, claims: Vec<SignerClaim>) -> Certificate {
    let header = HeaderBuilder::default()
        .author(AuthorityIdentifier::dummy_for_test(author))
        .round(round)
        .epoch(ACTIVATION_EPOCH)
        .created_at(1_700_000_000)
        .payload(Default::default())
        .parents(BTreeSet::new())
        .build()
        .with_signer_claims(claims);
    let mut cert = Certificate::default();
    cert.update_header_for_test(header);
    cert
}

// One committed output at the activation: the leader header's epoch is the scheduled one, and the
// certificates carry claims regions so the projection's skip has something to shift over.
//
// The header digests inside it are era-dependent, so only a process whose handle says "activated"
// can build a row that compares equal to one read back off disk.
fn activated_row() -> ConsensusHeader {
    let certificates = vec![cert(0x11, 3, vec![claim()]), cert(0x22, 3, Vec::new())];
    let leader = cert(0xAA, 4, vec![claim()]);
    let sub_dag =
        CommittedSubDag::new(certificates, leader, BLOCK_NUMBER, ReputationScores::default(), None);
    ConsensusHeader {
        parent_hash: BlockHash::repeat_byte(0xAB),
        sub_dag,
        number: BLOCK_NUMBER,
        extra: BlockHash::ZERO,
    }
}

// The seeding process: a node that booted with the scheduled activation installed writes its rows
// with the claims region on the wire, exactly as the production write path does.
fn writer(db_path: &Path) -> eyre::Result<()> {
    apply_signer_claims_activation(scheduled());
    let raw = MdbxDatabase::open_with_config(db_path, MdbxConfig::default())?;
    raw.open_table::<ConsensusBlocks>()?;
    raw.insert::<ConsensusBlocks>(&BLOCK_NUMBER, &activated_row())?;
    Ok(())
}

fn spawn_writer(db_path: &Path) -> eyre::Result<()> {
    let out = Command::new(env::current_exe()?)
        .env(WRITER_ENV, "1")
        .env(DB_PATH_ENV, db_path)
        .args(["--exact", "test_boot_row_reads_see_scheduled_activation", "--test-threads", "1"])
        .output()?;
    if !out.status.success() {
        panic!(
            "the seeding process failed:\n{}\n{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        );
    }
    Ok(())
}

// The raw bytes of the tip row: what `catchup_accumulator`'s walk feeds to
// `ConsensusHeaderMeta::from_bytes`, read back out of the same table the walk iterates.
fn tip_row_bytes<DB: Database>(db: &DB) -> eyre::Result<Vec<u8>> {
    db.with_read_txn(|txn| {
        let (_key, value) = txn
            .reverse_raw_iter::<ConsensusBlocks>()
            .next()
            .expect("the seeding process wrote one row");
        Ok(value.into_owned())
    })
}

// A typed `last_record::<ConsensusBlocks>()`, which decodes through `types`' panicking `decode`,
// captured as "what the boot would have seen".
fn tip_header<DB: Database>(db: &DB) -> (bool, Option<ConsensusHeader>) {
    let previous_hook = panic::take_hook();
    panic::set_hook(Box::new(|_| {}));
    let read = panic::catch_unwind(AssertUnwindSafe(|| db.last_record::<ConsensusBlocks>()));
    panic::set_hook(previous_hook);
    match read {
        Ok(record) => (false, record.map(|(_, header)| header)),
        Err(_) => (true, None),
    }
}

// `ConsensusHeaderMeta` carries no `PartialEq`, so compare the three leader fields the walk reads.
fn projected_as(meta: Option<&ConsensusHeaderMeta>, expected: &ConsensusHeaderMeta) -> bool {
    meta.is_some_and(|meta| {
        meta.leader_author == expected.leader_author
            && meta.leader_round == expected.leader_round
            && meta.leader_epoch == expected.leader_epoch
    })
}

#[tokio::test]
async fn test_boot_row_reads_see_scheduled_activation() -> eyre::Result<()> {
    if env::var_os(WRITER_ENV).is_some() {
        let db_path = env::var_os(DB_PATH_ENV).expect("the boot process passes the db path");
        return writer(Path::new(&db_path));
    }

    let tmp = tempdir()?;
    let db_path = tmp.path().join("consensus-db");
    std::fs::create_dir_all(&db_path)?;
    spawn_writer(&db_path)?;

    // The production boot stack: `LayeredDatabase<MdbxDatabase>`, the type `open_consensus_db`
    // hands the epoch manager. The rows were seeded straight into the persistent layer, so the
    // boot walk reads them off disk.
    let cfg = MdbxConfig::default();
    let db = open_db_with_consensus_config(&db_path, &cfg);
    let bytes = tip_row_bytes(&db)?;

    // `catchup_accumulator` consumes the execution environment, so each pass through the boot
    // readers gets its own handle on the same freshly genesis-ed EL.
    let execution_node =
        default_test_execution_node(None, None, &tmp.path().join("reth"), None).await?;

    // Phase 1: this process has not set the handle, so it reads every header with the
    // pre-activation layout — the boot `spawn_worker_node_components` used to give the node.
    let meta_before = ConsensusHeaderMeta::from_bytes(&bytes);
    let header_before = tip_header(&db);
    let catchup_before = {
        let gas = GasAccumulator::new(1);
        catchup_accumulator(&db, execution_node.get_reth_env().await, &gas)
    };
    let expected_meta = ConsensusHeaderMeta {
        leader_author: AuthorityIdentifier::dummy_for_test(0xAA),
        leader_round: 4,
        leader_epoch: ACTIVATION_EPOCH,
    };
    assert!(
        !projected_as(meta_before.as_ref().ok(), &expected_meta),
        "the walk must not read these bytes correctly with the handle unset, or the boot placement \
         this test guards is vacuous: got {meta_before:?}"
    );
    assert!(
        !header_before.1.as_ref().is_some_and(|header| {
            header.sub_dag.leader.header().epoch == ACTIVATION_EPOCH
                && !header.sub_dag.certificates[0].header().signer_claims.is_empty()
        }),
        "the typed tip read must not hand back a post-activation row with the handle unset either: \
         {header_before:?}"
    );
    assert!(
        catchup_before.is_err(),
        "the boot walk must not report success over a row it cannot project: {catchup_before:?}"
    );

    // Phase 2: the production init site, the same call `launch_node` makes above the DB open.
    apply_signer_claims_activation(scheduled());

    let meta_after = ConsensusHeaderMeta::from_bytes(&bytes);
    let (panicked, header_after) = tip_header(&db);
    let catchup_after = {
        let gas = GasAccumulator::new(1);
        catchup_accumulator(&db, execution_node.get_reth_env().await, &gas)
    };
    let meta_after = meta_after.map_err(|e| {
        eyre::eyre!("with the activation installed the projection must decode: {e}")
    })?;
    assert_eq!(
        (meta_after.leader_author, meta_after.leader_round, meta_after.leader_epoch,),
        (expected_meta.leader_author, expected_meta.leader_round, expected_meta.leader_epoch,),
        "the leader author/round/epoch must come out of the region the writer left behind"
    );
    assert!(!panicked, "the typed tip read must not die at boot with the handle installed");
    let header = header_after.expect("the tip row must be readable");
    // Built now, with the handle installed, so its cached digests are the post-activation ones.
    assert_eq!(header, activated_row(), "the boot must read back the row the activated node wrote");
    assert_eq!(
        header.sub_dag.certificates[0].header().signer_claims,
        vec![claim()],
        "the claims region must survive the read path"
    );
    catchup_after.map_err(|e| eyre::eyre!("the boot walk must succeed: {e}"))?;

    Ok(())
}
