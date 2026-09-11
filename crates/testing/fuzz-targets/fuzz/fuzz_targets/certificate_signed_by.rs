//! Fuzz target: Certificate::signed_by() with adversarial bitmaps.
//!
//! A Byzantine peer can craft a certificate with an arbitrary RoaringBitmap
//! in `signed_authorities`. This target exercises the bitmap interpretation
//! against committees of varying sizes, checking:
//! 1. No panic on any bitmap (including indices >> committee size)
//! 2. Weight never exceeds committee size
//! 3. Weight equals the number of returned public keys
//!
//! Note: BLS keys in the test committee are all `BlsPublicKey::default()`
//! (identical), so we cannot test that returned keys are distinct. The primary
//! value of this target is panic-safety: `signed_by` must never crash on
//! malformed bitmaps with out-of-range indices, gaps, or duplicates.
//!
//! Run: cargo +nightly-2026-06-24 fuzz run certificate_signed_by

#![no_main]
use libfuzzer_sys::fuzz_target;
use rayls_infrastructure_types::{Certificate, BlsPublicKey};

#[derive(arbitrary::Arbitrary, Debug)]
struct FuzzInput {
    cert_bytes: Vec<u8>,
    committee_sizes: Vec<u8>,
}

fuzz_target!(|input: FuzzInput| {
    if input.cert_bytes.len() > 4096 {
        return;
    }

    let cert: Certificate = match bcs::from_bytes(&input.cert_bytes) {
        Ok(c) => c,
        Err(_) => return,
    };

    let sizes: Vec<usize> = input
        .committee_sizes
        .iter()
        .take(5)
        .map(|&s| (s as usize).max(1).min(50))
        .collect();

    let mut all_sizes = vec![1, 4, 10];
    all_sizes.extend(sizes);
    all_sizes.sort();
    all_sizes.dedup();

    for committee_size in all_sizes {
        // All keys are identical (BlsPublicKey::default) — we can't construct
        // distinct BLS keys without the full crypto stack. The goal here is
        // purely panic-safety testing with arbitrary bitmaps.
        let dummy_keys: Vec<BlsPublicKey> = (0..committee_size)
            .map(|_| BlsPublicKey::default())
            .collect();

        // Must not panic regardless of bitmap content.
        let (weight, pks) = cert.signed_by(&dummy_keys);

        // Weight must equal the count of returned keys.
        assert_eq!(
            weight as usize,
            pks.len(),
            "weight/pks mismatch for committee_size={committee_size}"
        );

        // Weight must not exceed committee size.
        assert!(
            (weight as usize) <= committee_size,
            "weight {weight} > committee {committee_size}"
        );
    }
});
