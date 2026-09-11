//! Fuzz target: Header nonce encoding round-trip.
//!
//! The nonce field encodes `(epoch << 32) | round`. This target calls the REAL
//! production encoder `Header::nonce()` (not an inlined copy), so a bug in the
//! production packing — e.g. `epoch << 31` instead of `<< 32`, or truncating the
//! round — would be caught here. It verifies:
//! - `Header::nonce()` never panics
//! - the high 32 bits recover `epoch` and the low 32 bits recover `round`
//!   (this is exactly what `RethEnv::deconstruct_nonce()` does on the EL side)
//!
//! Run: cargo +nightly-2026-06-24 fuzz run header_nonce_roundtrip

#![no_main]
use indexmap::IndexMap;
use libfuzzer_sys::fuzz_target;
use rayls_infrastructure_types::{
    AuthorityIdentifier, BlockHash, BlockNumHash, CertificateDigest, Header,
};
use std::collections::BTreeSet;

fuzz_target!(|data: (u32, u32)| {
    let (epoch, round) = data;

    // Build a minimal header and call the REAL production encoder.
    let header = Header::new(
        AuthorityIdentifier::dummy_for_test(0),
        round,
        epoch,
        IndexMap::<BlockHash, u16>::new(),
        BTreeSet::<CertificateDigest>::new(),
        BlockNumHash::new(0, BlockHash::default()),
    );
    let nonce: u64 = header.nonce();

    // Decode the way the EL does (RethEnv::deconstruct_nonce): high 32 = epoch,
    // low 32 = round. If production `nonce()` packed the bits differently, these
    // assertions fail.
    let decoded_epoch = (nonce >> 32) as u32;
    let decoded_round = nonce as u32;

    assert_eq!(decoded_epoch, epoch, "epoch round-trip failed (nonce={nonce:#x})");
    assert_eq!(decoded_round, round, "round round-trip failed (nonce={nonce:#x})");
});
