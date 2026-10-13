//! State sync tests for primaries.

use crate::{network::MissingCertificatesRequest, state_sync::CertificateCollector};
use rayls_infrastructure_storage::{mem_db::MemDatabase, CertificateStore};
use rayls_infrastructure_types::{
    encode, AuthorityIdentifier, Certificate, Hash as _, SignatureVerificationState,
};
use rayls_testing_test_utils_committee::CommitteeFixture;
use std::{collections::BTreeSet, num::NonZeroUsize};

#[test]
fn test_certificate_iterator() {
    // authority fixtures
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let primary = fixture.authorities().next().expect("primary in committee fixture");
    let consensus_config = primary.consensus_config();
    let certificate_store = primary.consensus_config().node_storage().clone();

    // setup dummy data
    let mut current_round: Vec<_> = Certificate::genesis(&fixture.committee())
        .into_iter()
        .map(|cert| cert.header().clone())
        .collect();
    let mut headers = vec![];
    let total_rounds = 4;
    for i in 0..total_rounds {
        let parents: BTreeSet<_> =
            current_round.into_iter().map(|header| fixture.certificate(&header).digest()).collect();
        (_, current_round) = fixture.headers_round(i, &parents);
        headers.extend(current_round.clone());
    }
    let total_authorities = fixture.authorities().count();
    let total_certificates = total_authorities * total_rounds as usize;
    // Create certificates test data.
    let mut certificates = vec![];
    for header in headers.into_iter() {
        certificates.push(fixture.certificate(&header));
    }
    assert_eq!(certificates.len(), total_certificates);
    assert_eq!(16, total_certificates);

    // Populate certificate store such that each authority has the following rounds:
    // Authority 0: 1
    // Authority 1: 1 2
    // Authority 2: 1 2 3
    // Authority 3: 1 2 3 4
    // This is unrealistic because in practice a certificate can only be stored with 2f+1 parents
    // already in store. But this does not matter for testing here.
    let mut authorities = Vec::<AuthorityIdentifier>::new();
    for i in 0..total_authorities {
        authorities.push(certificates[i].header().author().clone());
        for j in 0..=i {
            let mut cert = certificates[i + j * total_authorities].clone();
            assert_eq!(&cert.header().author(), &authorities.last().unwrap());
            // Simulating only 1 directly verified certificate (Auth 3 Round 4) being stored.
            cert.set_signature_verification_state(SignatureVerificationState::VerifiedDirectly(
                cert.aggregated_signature().expect("Invalid Signature"),
            ));

            certificate_store.write(cert).expect("Writing certificate to store failed");
        }
    }

    // Each test case contains (lower bound round, skip rounds, max items, expected output).
    let test_cases = vec![
        (0, vec![vec![], vec![], vec![], vec![]], 20, vec![1_u32, 1, 1, 1, 2, 2, 2, 3, 3, 4]),
        (0, vec![vec![1u32], vec![1], vec![], vec![]], 20, vec![1, 1, 2, 2, 2, 3, 3, 4]),
        (0, vec![vec![], vec![], vec![1], vec![1]], 20, vec![1, 1, 2, 2, 2, 3, 3, 4]),
        (1, vec![vec![], vec![], vec![2], vec![2]], 4, vec![2, 3, 3, 4]),
        (1, vec![vec![], vec![], vec![2], vec![2]], 2, vec![2]),
        (0, vec![vec![1], vec![1], vec![1, 2, 3], vec![1, 2, 3]], 2, vec![2, 4]),
        (2, vec![vec![], vec![], vec![], vec![]], 3, vec![3, 3, 4]),
        (2, vec![vec![], vec![], vec![], vec![]], 2, vec![3, 3]),
        // Check that round 2 and 4 are fetched for the last authority, skipping round 3.
        (1, vec![vec![], vec![], vec![3], vec![3]], 5, vec![2, 2, 2, 4]),
    ];

    // calculate reasonable response size
    let sample_cert = &certificates[0];
    let single_cert_size = encode(sample_cert).len();
    let message_overhead = encode(&MissingCertificatesRequest::default()).len();

    for (lower_bound_round, skip_rounds_vec, max_items, expected_rounds) in test_cases.clone() {
        // estimate response size based on max_items returned
        let response_size = single_cert_size * max_items + message_overhead;
        let req = MissingCertificatesRequest::default()
            .set_bounds(
                lower_bound_round,
                authorities
                    .clone()
                    .into_iter()
                    .zip(skip_rounds_vec.into_iter().map(|rounds| rounds.into_iter().collect()))
                    .collect(),
            )
            .expect("bounds within range")
            .set_max_response_size(response_size);

        // collect from database
        let mut missing = Vec::new();
        let collector = CertificateCollector::new(req, consensus_config.clone())
            .expect("certificate collector process valid request");

        // Collect certificates from iterator
        for certs in collector {
            missing.push(certs.expect("cert recovered correctly"));
        }

        assert_eq!(missing.iter().map(|cert| cert.round()).collect::<Vec<u32>>(), expected_rounds);
    }

    // assert MAX is capped by this node
    for (lower_bound_round, skip_rounds_vec, _max_items, expected_rounds) in test_cases {
        // estimate response size based on max_items returned
        let req = MissingCertificatesRequest::default()
            .set_bounds(
                lower_bound_round,
                authorities
                    .clone()
                    .into_iter()
                    .zip(skip_rounds_vec.into_iter().map(|rounds| rounds.into_iter().collect()))
                    .collect(),
            )
            .expect("bounds within range")
            .set_max_response_size(usize::MAX);

        // collect from database
        let mut missing = Vec::new();
        let collector = CertificateCollector::new(req, consensus_config.clone())
            .expect("certificate collector process valid request");

        // Collect certificates from iterator
        for certs in collector {
            missing.push(certs.expect("cert recovered correctly"));
        }

        // assert at least the expected rounds are returned
        // NOTE: the expected_rounds tests requestor's limits are respected
        assert!(missing.iter().map(|cert| cert.round()).collect::<Vec<u32>>() >= expected_rounds);
    }
}

fn overflow_fixture() -> CommitteeFixture<MemDatabase> {
    CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build()
}

fn serialize_bitmap(rounds: &[u32]) -> Vec<u8> {
    let mut bm = roaring::RoaringBitmap::new();
    for &r in rounds {
        bm.insert(r);
    }
    let mut buf = Vec::new();
    bm.serialize_into(&mut buf).expect("bitmap");
    buf
}

/// A request whose lower bound is u32::MAX and whose skip bitmap is empty must be refused.
/// The empty bitmap bypasses the checked_add guard in get_bounds (zero iterations), so the
/// overflow reaches next_round_number at certificate_store.rs:415.
#[test]
fn test_collector_refuses_max_lower_bound_with_empty_skip_bitmap() {
    let fixture = overflow_fixture();
    let config = fixture.authorities().next().unwrap().consensus_config();
    let origin = fixture.authorities().nth(1).unwrap().id();
    let req = MissingCertificatesRequest {
        exclusive_lower_bound: u32::MAX,
        skip_rounds: vec![(origin, serialize_bitmap(&[]))],
        max_response_size: 1_000,
        exclusive_upper_bound: None,
    };
    assert!(CertificateCollector::new(req, config).is_err());
}

/// A request with lower bound u32::MAX and a skip bitmap containing only offset 0 (absolute
/// round u32::MAX) passes get_bounds (checked_add(u32::MAX, 0) = u32::MAX) but must still be
/// refused before next_round_number tries round + 1.
#[test]
fn test_collector_refuses_max_lower_bound_with_zero_offset_skip() {
    let fixture = overflow_fixture();
    let config = fixture.authorities().next().unwrap().consensus_config();
    let origin = fixture.authorities().nth(1).unwrap().id();
    let req = MissingCertificatesRequest {
        exclusive_lower_bound: u32::MAX,
        skip_rounds: vec![(origin, serialize_bitmap(&[0]))],
        max_response_size: 1_000,
        exclusive_upper_bound: None,
    };
    assert!(CertificateCollector::new(req, config).is_err());
}

/// A request with lower bound u32::MAX and no authorities listed does not overflow (the
/// collector's fetch queue is empty), but the request is vacuous. This test ensures the
/// collector handles it gracefully.
#[test]
fn test_collector_handles_max_lower_bound_no_authorities() {
    let fixture = overflow_fixture();
    let config = fixture.authorities().next().unwrap().consensus_config();
    let req = MissingCertificatesRequest {
        exclusive_lower_bound: u32::MAX,
        skip_rounds: vec![],
        max_response_size: 1_000,
        exclusive_upper_bound: None,
    };
    let result = CertificateCollector::new(req, config);
    // Either an error (refused) or an empty collector is acceptable — just no panic.
    if let Ok(collector) = result {
        assert_eq!(collector.count(), 0);
    }
}

/// Multiple authorities with empty skip bitmaps at u32::MAX must all be refused.
#[test]
fn test_collector_refuses_max_lower_bound_multiple_empty_origins() {
    let fixture = overflow_fixture();
    let config = fixture.authorities().next().unwrap().consensus_config();
    let origins: Vec<_> = fixture.authorities().map(|a| a.id()).collect();
    let req = MissingCertificatesRequest {
        exclusive_lower_bound: u32::MAX,
        skip_rounds: origins.into_iter().map(|id| (id, serialize_bitmap(&[]))).collect(),
        max_response_size: 1_000,
        exclusive_upper_bound: None,
    };
    assert!(CertificateCollector::new(req, config).is_err());
}
