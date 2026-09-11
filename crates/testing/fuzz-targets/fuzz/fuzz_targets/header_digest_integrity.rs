//! Fuzz target: Header digest integrity.
//!
//! Tests that Axyl's Header digest computation:
//! 1. Is deterministic (same fields → same digest)
//! 2. Has avalanche property (changing any single field changes the digest)
//! 3. Is consistent with Hash::digest (the recomputed digest matches)
//!
//! This is critical because Header digests are certificate references in the
//! DAG. A collision or inconsistency breaks consensus.
//!
//! Run: cargo +nightly-2026-06-24 fuzz run header_digest_integrity

#![no_main]
use libfuzzer_sys::fuzz_target;
use rayls_infrastructure_types::{
    AuthorityIdentifier, BlockHash, BlockNumHash, CertificateDigest, Hash, Header,
};
use indexmap::IndexMap;
use std::collections::BTreeSet;

#[derive(arbitrary::Arbitrary, Debug)]
struct FuzzHeader {
    author_byte: u8,
    round: u32,
    epoch: u32,
    // Up to 3 payload entries
    payload_digests: Vec<[u8; 32]>,
    payload_worker_ids: Vec<u16>,
    // Up to 5 parent certificate digests
    parent_digests: Vec<[u8; 32]>,
    // Latest execution block
    exec_block_number: u64,
    exec_block_hash: [u8; 32],
    // Explicit timestamp so the harness is fully deterministic. `Header::new()`
    // would stamp `created_at: now()`, making corpus entries time-dependent
    // (the same fuzz input would produce different bytes/digests across runs).
    created_at: u64,
    // Which byte to mutate for avalanche test
    mutate_byte: u8,
}

fuzz_target!(|input: FuzzHeader| {
    let payload: IndexMap<BlockHash, u16> = input
        .payload_digests
        .iter()
        .take(3)
        .zip(input.payload_worker_ids.iter().take(3))
        .map(|(d, w)| (BlockHash::from_slice(d), *w))
        .collect();

    let parents: BTreeSet<CertificateDigest> = input
        .parent_digests
        .iter()
        .take(5)
        .map(|d| CertificateDigest::new(*d))
        .collect();

    let latest_execution_block = BlockNumHash::new(
        input.exec_block_number,
        BlockHash::from_slice(&input.exec_block_hash),
    );

    // Construct the header with an EXPLICIT created_at (not Header::new(), which
    // stamps now()) so the harness/corpus is deterministic. The digest OnceCell is
    // left empty and computed lazily by digest() below from these fixed fields.
    let header = Header {
        author: AuthorityIdentifier::dummy_for_test(input.author_byte),
        round: input.round,
        epoch: input.epoch,
        created_at: input.created_at,
        payload: payload.clone(),
        parents: parents.clone(),
        latest_execution_block,
        digest: Default::default(),
    };

    let digest1 = header.digest();

    // Consistency: recomputing Hash::digest must match the cached value.
    let recomputed = Hash::digest(&header);
    assert_eq!(digest1, recomputed, "cached digest != recomputed");

    // Avalanche: mutating a field must change the digest.
    // Serialize, flip a byte, check if the digest changed.
    let encoded = bcs::to_bytes(&header).expect("encode succeeds");
    if encoded.len() > 1 {
        let mut mutated = encoded.clone();
        let idx = (input.mutate_byte as usize) % mutated.len();
        mutated[idx] = mutated[idx].wrapping_add(1);

        if let Ok(mutated_header) = bcs::from_bytes::<Header>(&mutated) {
            let re_encoded = bcs::to_bytes(&mutated_header).expect("re-encode");
            // Only assert if the bytes actually differ after round-trip.
            if re_encoded != encoded {
                let mutated_digest = Hash::digest(&mutated_header);
                assert_ne!(
                    digest1, mutated_digest,
                    "different headers produced same digest (hash collision or bug)"
                );
            }
        }
    }
});
