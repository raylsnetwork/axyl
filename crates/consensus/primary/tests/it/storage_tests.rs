//! Tests for the storage crate that need to use test-utils.
//! Put them here to avoid circular dependancies with storage/test-utils (via ConsensusConfig).

use futures::future::join_all;
use rayls_consensus_primary::test_utils::temp_dir;
use rayls_execution_evm::test_utils::fixture_batch_with_transactions;
use rayls_infrastructure_storage::{
    mem_db::MemDatabase,
    open_db,
    tables::{
        CertificateDigestByOrigin, ConsensusBlockNumbersByDigest, ConsensusBlocks,
        ConsensusBlocksCache,
    },
    CertificateStore, ConsensusStore, ProposerStore, ReadTimeout,
};
use rayls_infrastructure_types::{
    AuthorityIdentifier, Certificate, CertificateDigest, CommittedSubDag, ConsensusHeader,
    Database, DbTxMut, Hash as _, Header, HeaderBuilder, ReputationScores, Round, B256,
};
use rayls_testing_test_utils_committee::CommitteeFixture;
use std::{
    collections::{BTreeSet, HashSet},
    time::Instant,
};
use tempfile::TempDir;

fn create_header_for_round(round: Round) -> Header {
    let builder = HeaderBuilder::default();
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let primary = fixture.authorities().next().unwrap();
    let id = primary.id();
    builder
        .author(id)
        .round(round)
        .epoch(fixture.committee().epoch())
        .parents([CertificateDigest::default()].iter().cloned().collect())
        .with_payload_batch(fixture_batch_with_transactions(10), 0)
        .build()
}

// helper method that creates certificates for the provided
// number of rounds.
fn certificates(rounds: Round) -> Vec<Certificate> {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let mut current_round: Vec<_> =
        Certificate::genesis(&committee).into_iter().map(|cert| cert.header().clone()).collect();

    let mut result: Vec<Certificate> = Vec::new();
    for i in 0..rounds {
        let parents: BTreeSet<_> =
            current_round.iter().map(|header| fixture.certificate(header).digest()).collect();
        (_, current_round) = fixture.headers_round(i, &parents);

        result.extend(
            current_round.iter().map(|h| fixture.certificate(h)).collect::<Vec<Certificate>>(),
        );
    }

    result
}

#[tokio::test]
async fn test_proposer_store_writes() {
    let temp_dir = TempDir::new().unwrap();
    let store = open_db(temp_dir.path());
    let header_1 = create_header_for_round(1);

    let out = store.write_last_proposed(&header_1);
    assert!(out.is_ok());

    let result = store.get_last_proposed().expect("error on last proposed");
    assert_eq!(result.unwrap(), header_1);

    let header_2 = create_header_for_round(2);
    let out = store.write_last_proposed(&header_2);
    assert!(out.is_ok());

    let should_exist = store.get_last_proposed().expect("error on last proposed");
    assert_eq!(should_exist.unwrap(), header_2);
}

#[tokio::test]
async fn test_proposer_store_reads() {
    let temp_dir = TempDir::new().unwrap();
    let store = open_db(temp_dir.path());

    let should_not_exist = store.get_last_proposed().unwrap();
    assert_eq!(should_not_exist, None);

    let header_1 = create_header_for_round(1);
    let out = store.write_last_proposed(&header_1);
    assert!(out.is_ok());

    let should_exist = store.get_last_proposed().unwrap();
    assert_eq!(should_exist.unwrap(), header_1);
}

#[tokio::test]
async fn test_consensus_store_read_latest_final_reputation_scores() {
    // GIVEN
    let temp_dir = TempDir::new().unwrap();
    let store = open_db(temp_dir.path());
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();

    // AND we add some commits without any final scores
    for sequence_number in 0..10 {
        let sub_dag = CommittedSubDag::new(
            vec![],
            Certificate::default(),
            sequence_number,
            ReputationScores::new(&committee),
            None,
        );

        store.write_subdag_for_test(sequence_number, sub_dag);
    }

    // WHEN we try to read the final schedule. The one of sub dag sequence 12 should be returned
    let commit = store.read_latest_commit_with_final_reputation_scores(committee.epoch());

    // THEN no commit is returned
    assert!(commit.is_none());

    // AND when adding more commits with some final scores amongst them
    for sequence_number in 10..=20 {
        let mut scores = ReputationScores::new(&committee);

        // we mark the sequence 14 & 20 committed sub dag as with final schedule
        if sequence_number == 14 || sequence_number == 20 {
            scores.final_of_schedule = true;
        }

        let sub_dag =
            CommittedSubDag::new(vec![], Certificate::default(), sequence_number, scores, None);

        store.write_subdag_for_test(sequence_number, sub_dag);
    }
    store.persist().await.unwrap();

    // WHEN we try to read the final schedule. The one of sub dag sequence 20 should be returned
    let commit = store.read_latest_commit_with_final_reputation_scores(committee.epoch()).unwrap();

    assert!(commit.reputation_score.final_of_schedule);
}

/// Regression: a matching cache row must not be masked by a divergent canonical block at the same
/// number, or `get_consensus_by_hash` hides a header the node actually holds.
#[tokio::test]
async fn test_get_consensus_by_hash_prefers_matching_cache_over_divergent_canonical() {
    let temp_dir = TempDir::new().unwrap();
    let store = open_db(temp_dir.path());

    let header = |number: u64, parent: B256| ConsensusHeader {
        parent_hash: parent,
        sub_dag: CommittedSubDag::new(
            vec![],
            Certificate::default(),
            0,
            ReputationScores::default(),
            None,
        ),
        number,
        extra: B256::default(),
    };

    let canonical = header(9, B256::repeat_byte(0x11));
    let cached = header(9, B256::repeat_byte(0x22));
    assert_ne!(canonical.digest(), cached.digest(), "precondition: the two rows diverge");
    store
        .with_write_txn(|txn| {
            txn.insert::<ConsensusBlocks>(&canonical.number, &canonical)?;
            txn.insert::<ConsensusBlockNumbersByDigest>(&canonical.digest(), &canonical.number)?;
            txn.insert::<ConsensusBlocksCache>(&cached.number, &cached)?;
            txn.insert::<ConsensusBlockNumbersByDigest>(&cached.digest(), &cached.number)?;
            Ok(())
        })
        .unwrap();

    assert_eq!(
        store.get_consensus_by_hash(cached.digest()).map(|h| h.digest()),
        Some(cached.digest()),
        "a matching cache row must not be masked by a divergent canonical block at the same number"
    );
    assert_eq!(
        store.get_consensus_by_hash(canonical.digest()).map(|h| h.digest()),
        Some(canonical.digest())
    );
}

#[tokio::test]
async fn test_certificate_store_write_and_read() {
    let db = temp_dir();
    let db = open_db(db.path());
    test_write_and_read_by_store_type(db).await;
}

async fn test_write_and_read_by_store_type<DB: CertificateStore>(store: DB) {
    // GIVEN
    // create certificates for 10 rounds
    let certs = certificates(10);
    let digests = certs.iter().map(|c| c.digest()).collect::<Vec<_>>();

    // verify certs not in the store
    for cert in &certs {
        assert!(!store.contains(&cert.digest()).unwrap());
        assert!(&store.read(cert.digest()).unwrap().is_none());
    }

    let found = store.multi_contains(digests.iter()).unwrap();
    assert_eq!(found.len(), certs.len());
    for hit in found {
        assert!(!hit);
    }

    // store the certs
    for cert in &certs {
        store.write(cert.clone()).unwrap();
    }

    // verify certs in the store
    for cert in &certs {
        assert!(store.contains(&cert.digest()).unwrap());
        assert_eq!(cert, &store.read(cert.digest()).unwrap().unwrap())
    }

    let found = store.multi_contains(digests.iter()).unwrap();
    assert_eq!(found.len(), certs.len());
    for hit in found {
        assert!(hit);
    }
}

#[tokio::test]
async fn test_certificate_store_write_all_and_read_all() {
    let db = temp_dir();
    let db = open_db(db.path());
    test_write_all_and_read_all_by_store_type(db).await;
}

async fn test_write_all_and_read_all_by_store_type<DB: CertificateStore>(store: DB) {
    // GIVEN
    // create certificates for 10 rounds
    let certs = certificates(10);
    let ids = certs.iter().map(|c| c.digest()).collect::<Vec<CertificateDigest>>();

    // store them in both main and secondary index
    store.write_all(certs.clone()).unwrap();

    // WHEN
    let result = store.read_all(ids).unwrap();

    // THEN
    assert_eq!(certs.len(), result.len());

    for (i, cert) in result.into_iter().enumerate() {
        let c = cert.expect("Certificate should have been found");

        assert_eq!(&c, certs.get(i).unwrap());
    }
}

#[tokio::test]
async fn test_certificate_store_next_round_number() {
    // GIVEN
    let db = temp_dir();
    let store = open_db(db.path());

    // Create certificates for round 1, 2, 4, 6, 9, 10.
    let cert = certificates(1).first().unwrap().clone();
    let origin = cert.origin();
    let rounds = vec![1, 2, 4, 6, 9, 10];
    let mut certs = Vec::new();
    for r in &rounds {
        let mut c = cert.clone();
        c.header_mut_for_test().update_round_for_test(*r);
        certs.push(c);
    }

    store.write_all(certs).unwrap();
    store.persist().await.unwrap();

    // THEN
    let mut i = 0;
    let mut current_round = 0;
    while let Some(r) = store.next_round_number(origin, current_round).unwrap() {
        assert_eq!(rounds[i], r);
        i += 1;
        current_round = r;
    }
}

#[tokio::test]
async fn test_certificate_store_last_two_rounds() {
    // GIVEN
    let db = temp_dir();
    let store = open_db(db.path());

    // create certificates for 50 rounds
    let certs = certificates(50);
    let origin = certs[0].origin().clone();

    // store them in both main and secondary index
    store.write_all(certs).unwrap();
    store.persist().await.unwrap();

    // WHEN
    let result = store.last_two_rounds_certs().unwrap();
    let last_round_cert = store.last_round(&origin).unwrap().unwrap();
    let last_round_number = store.last_round_number(&origin).unwrap().unwrap();
    let highest_round_number = store.highest_round_number();

    // THEN
    assert_eq!(result.len(), 8);
    assert_eq!(last_round_cert.round(), 50);
    assert_eq!(last_round_number, 50);
    assert_eq!(highest_round_number, 50);
    for certificate in result {
        assert!(
            (certificate.round() == last_round_number)
                || (certificate.round() == last_round_number - 1)
        );
    }
}

#[tokio::test]
async fn test_certificate_store_last_round_in_empty_store() {
    // GIVEN
    let db = temp_dir();
    let store = open_db(db.path());

    // WHEN
    let result = store.last_two_rounds_certs().unwrap();
    let last_round_cert = store.last_round(&AuthorityIdentifier::default()).unwrap();
    let last_round_number = store.last_round_number(&AuthorityIdentifier::default()).unwrap();
    let highest_round_number = store.highest_round_number();

    // THEN
    assert!(result.is_empty());
    assert!(last_round_cert.is_none());
    assert!(last_round_number.is_none());
    assert_eq!(highest_round_number, 0);
}

#[tokio::test]
async fn test_certificate_store_after_round() {
    // GIVEN
    let db = temp_dir();
    let store = open_db(db.path());
    let total_rounds = 100;

    // create certificates for 50 rounds
    let now = Instant::now();

    tracing::debug!("Generating certificates");

    let certs = certificates(total_rounds);
    tracing::debug!("Created certificates: {} seconds", now.elapsed().as_secs_f32());

    let now = Instant::now();
    tracing::debug!("Storing certificates");

    // store them in both main and secondary index
    store.write_all(certs.clone()).unwrap();
    store.persist().await.unwrap();

    tracing::debug!("Stored certificates: {} seconds", now.elapsed().as_secs_f32());

    // Large enough to avoid certificate store GC.
    let round_cutoff: Round = 41;

    // now filter the certificates over round 21
    let mut certs_ids_over_cutoff_round = certs
        .into_iter()
        .filter_map(|c| if c.round() >= round_cutoff { Some(c.digest()) } else { None })
        .collect::<HashSet<_>>();

    // WHEN
    tracing::debug!("Access after round {round_cutoff}, before {total_rounds}");
    let now = Instant::now();
    let result = store
        .after_round(round_cutoff, ReadTimeout::Enforced)
        .expect("Error returned while reading after_round");

    tracing::debug!("Total time: {} seconds", now.elapsed().as_secs_f32());

    // THEN
    let certs_per_round = 4;
    assert_eq!(result.len() as u32, (total_rounds - round_cutoff + 1) * certs_per_round);

    // AND result certificates should be returned in increasing order
    let mut last_round = 0;
    for certificate in result {
        assert!(certificate.round() >= last_round);
        last_round = certificate.round();

        // should be amongst the certificates of the cut-off round
        assert!(certs_ids_over_cutoff_round.remove(&certificate.digest()));
    }

    // AND none should be left in the original set
    assert!(certs_ids_over_cutoff_round.is_empty());

    // WHEN get rounds per origin.
    let rounds = store
        .origins_after_round(round_cutoff)
        .expect("Error returned while reading origins_after_round");
    assert_eq!(rounds.len(), (total_rounds - round_cutoff + 1) as usize);
    for origins in rounds.values() {
        assert_eq!(origins.len(), 4);
    }
}

#[tokio::test]
async fn test_certificate_store_notify_read() {
    let db = temp_dir();
    let store = open_db(db.path());

    // run the tests a few times
    for _ in 0..10 {
        let mut certs = certificates(3);
        let mut ids = certs.iter().map(|c| c.digest()).collect::<Vec<CertificateDigest>>();

        let cloned_store = store.clone();

        // now populate a certificate
        let c1 = certs.remove(0);
        store.write(c1.clone()).unwrap();

        // spawn a task to notify_read on the certificate's id - we testing
        // the scenario where the value is already populated before
        // calling the notify read.
        let id = ids.remove(0);
        let handle_1 = tokio::spawn(async move { cloned_store.notify_read(id).await });

        // now spawn a series of tasks before writing anything in store
        let mut handles = vec![];
        for id in ids {
            let cloned_store = store.clone();
            let handle = tokio::spawn(async move {
                // wait until the certificate gets populated
                cloned_store.notify_read(id).await
            });

            handles.push(handle)
        }

        // and populate the rest with a write_all
        store.write_all(certs).unwrap();

        // now wait on handle an assert result for a single certificate
        let received_certificate =
            handle_1.await.expect("error").expect("shouldn't receive store error");

        assert_eq!(received_certificate, c1);

        let result = join_all(handles).await;
        for r in result {
            let certificate_result = r.unwrap();
            assert!(certificate_result.is_ok());
        }

        // clear the store before next run
        store.clear().unwrap();
    }
}

#[tokio::test]
async fn test_certificate_store_write_all_and_clear() {
    let db = temp_dir();
    let store = open_db(db.path());

    // create certificates for 10 rounds
    let certs = certificates(10);

    // store them in both main and secondary index
    store.write_all(certs).unwrap();

    // confirm store is not empty
    assert!(!store.is_empty_certs());

    // now clear the store
    store.clear().unwrap();

    // now confirm that store is empty
    assert!(store.is_empty_certs());
}

/// Test new store.
///
/// workaround for error:
/// ```text
/// thread 'certificate_store::test::test_delete_by_store_type' panicked at crates/consensus/typed-store/src/metrics.rs:268:14:
/// called `Result::unwrap()` on an `Err` value: AlreadyReg
/// ```
#[tokio::test]
async fn test_certificate_store_delete_store() {
    let db = temp_dir();
    let store = open_db(db.path());
    // GIVEN
    // create certificates for 10 rounds
    let certs = certificates(10);

    // store them in both main and secondary index
    store.write_all(certs.clone()).unwrap();

    // WHEN now delete a couple of certificates
    let to_delete = certs.iter().take(2).map(|c| c.digest()).collect::<Vec<_>>();

    let key_0 = (certs[0].origin().clone(), certs[0].round());
    assert!(store.get::<CertificateDigestByOrigin>(&key_0).unwrap().is_some());

    store.delete(to_delete[0]).unwrap();
    store.delete(to_delete[1]).unwrap();
    store.persist().await.unwrap(); // Make sure the deletes are complete...

    // THEN
    assert!(store.read(to_delete[0]).unwrap().is_none());
    assert!(store.read(to_delete[1]).unwrap().is_none());
    assert!(store.get::<CertificateDigestByOrigin>(&key_0).unwrap().is_none());
}
