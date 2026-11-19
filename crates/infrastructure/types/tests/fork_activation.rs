//! I15: post-activation header bytes must be refused by a process that never initialized the
//! activation handle, not silently digested as pre-activation ones.
//!
//! The handle in `primary/fork.rs` is process-global, so the writer that sets it and the reader
//! that never did cannot share a process. This file's own process is the reader — its handle is
//! never touched, so it reads as "never" — and it spawns a copy of itself as the writer, then
//! checks the bytes that writer published.
#![allow(unused_crate_dependencies)]

use rayls_infrastructure_types::{
    header_canonical_bytes, header_is_post_activation, init_signer_claims_activation_epoch,
    signer_claims_region_bytes, AuthorityIdentifier, BlsSignature, CertificateDigest, Epoch,
    Header, HeaderBuilder, SignerClaim,
};
use std::{env, panic, process::Command, sync::OnceLock};

// Deliberately not the never sentinel: this is what a scheduled fork looks like.
const ACTIVATION_EPOCH: Epoch = 5_000;
const WRITER_ENV: &str = "RAYLS_TYPES_FORK_ACTIVATION_WRITER";
const MARKER: &str = "POST_BYTES";

struct Published {
    bytes: Vec<u8>,
    author_digest: String,
}

fn header_at(epoch: Epoch) -> Header {
    HeaderBuilder::default()
        .author(AuthorityIdentifier::dummy_for_test(0xAA))
        .round(4)
        .epoch(epoch)
        .created_at(1_700_000_000)
        .payload(Default::default())
        .parents(std::collections::BTreeSet::new())
        .build()
}

fn claims() -> Vec<SignerClaim> {
    vec![SignerClaim {
        target_digest: CertificateDigest::new([9; 32]),
        target_epoch: ACTIVATION_EPOCH,
        target_round: 5,
        ranks: vec![0, 1, 2],
        signature: BlsSignature::default(),
    }]
}

// The writer process: sets the handle, encodes a post-activation header, reads it back, and
// publishes what it produced for the reader to check.
fn writer() {
    init_signer_claims_activation_epoch(ACTIVATION_EPOCH);
    assert!(
        header_is_post_activation(&header_at(ACTIVATION_EPOCH)),
        "the writer must be reading the scheduled epoch"
    );

    let claims = claims();
    let header = header_at(ACTIVATION_EPOCH).with_signer_claims(claims.clone());
    let bytes = header_canonical_bytes(&header);
    let back = bcs::from_bytes::<Header>(&bytes).expect("the writer must read its own bytes back");
    assert_eq!(back.signer_claims, claims, "the region survives the decode");
    assert_eq!(back.digest(), header.digest(), "encode/decode must preserve the author's digest");
    assert_eq!(header_canonical_bytes(&back), bytes, "re-encoding must be stable");

    // A second init with a different epoch must panic, and must not move the handle.
    let previous_hook = panic::take_hook();
    panic::set_hook(Box::new(|_| {}));
    let panicked =
        panic::catch_unwind(|| init_signer_claims_activation_epoch(ACTIVATION_EPOCH + 1)).is_err();
    panic::set_hook(previous_hook);
    assert!(panicked, "a second init with a different epoch must panic");
    assert_eq!(
        header_canonical_bytes(&header_at(ACTIVATION_EPOCH).with_signer_claims(claims)),
        bytes,
        "the rejected init must leave the first epoch installed"
    );

    println!("{MARKER} {} {}", hex::encode(back.digest().as_ref()), hex::encode(&bytes));
}

fn published() -> &'static Published {
    static PUBLISHED: OnceLock<Published> = OnceLock::new();
    PUBLISHED.get_or_init(|| {
        let out = Command::new(env::current_exe().expect("path to this test binary"))
            .env(WRITER_ENV, "1")
            .args([
                "--exact",
                "test_post_activation_bytes_rejected_when_uninitialized",
                "--test-threads",
                "1",
                "--nocapture",
            ])
            .output()
            .expect("spawn the writer process");
        if !out.status.success() {
            panic!(
                "the writer process failed:\n{}\n{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
        }
        // libtest writes `test <name> ... ` without a newline before running, so the marker
        // lands at the end of that line rather than at the start of its own.
        let stdout = String::from_utf8_lossy(&out.stdout);
        let line = stdout
            .lines()
            .find_map(|line| line.split_once(MARKER).map(|(_, rest)| rest.trim()))
            .expect("the writer must publish its header bytes")
            .to_string();
        let mut parts = line.split(' ');
        match (parts.next(), parts.next()) {
            (Some(digest), Some(bytes)) => Published {
                bytes: hex::decode(bytes).expect("writer published hex"),
                author_digest: digest.to_string(),
            },
            _ => panic!("writer output is malformed: {line}"),
        }
    })
}

#[test]
fn test_post_activation_bytes_rejected_when_uninitialized() {
    if env::var_os(WRITER_ENV).is_some() {
        writer();
        return;
    }

    // This process never initialized the handle: an epoch below the never sentinel is pre-era.
    assert!(
        !header_is_post_activation(&header_at(ACTIVATION_EPOCH)),
        "the reader process must still be reading the never default"
    );

    let published = published();
    let ours = header_canonical_bytes(&header_at(ACTIVATION_EPOCH));
    let region = signer_claims_region_bytes(&claims());
    assert_eq!(
        published.bytes.len(),
        ours.len() + region.len(),
        "the writer's header is this process's bytes plus the claims region"
    );
    assert_eq!(
        &published.bytes[..ours.len()],
        ours.as_slice(),
        "the writer's bytes carry the same 7 struct fields this process would write"
    );
    assert_eq!(
        &published.bytes[ours.len()..],
        region.as_slice(),
        "and then the length-prefixed claims region this process would not write"
    );

    let err = bcs::from_bytes::<Header>(&published.bytes)
        .expect_err("an uninitialized reader must not accept a header carrying a region");
    assert_eq!(
        err,
        bcs::Error::RemainingInput,
        "the unconsumed region must trip bcs's end() check, not decode into a different digest"
    );
}

#[test]
fn test_post_activation_roundtrip_under_nondefault_handle() {
    if env::var_os(WRITER_ENV).is_some() {
        return;
    }

    // The writer asserts encode -> decode -> digest equality under the handle it set; this
    // process can only verify it got there, because it cannot hold a non-default handle without
    // becoming the very reader the test above exists to protect.
    let published = published();
    let era_blind = header_at(ACTIVATION_EPOCH).digest();
    assert_ne!(
        hex::encode(era_blind.as_ref()),
        published.author_digest,
        "digesting these fields without the handle would have been the silent wrong answer"
    );
}
