//! Fuzz target: BCS round-trip property for consensus types.
//!
//! For any bytes that successfully decode into a consensus type,
//! re-encoding and re-decoding must produce the same value.
//! This catches inconsistencies in serde implementations that could
//! cause validators to disagree on certificate/batch digests.
//!
//! Run: `cargo +nightly-2026-06-24 fuzz run bcs_roundtrip -- -max_total_time=300`

#![no_main]
use libfuzzer_sys::fuzz_target;
use rayls_infrastructure_types::{header_canonical_bytes, Certificate, Header, SealedBatch};

// This harness has no chainspec, so the activation handle stays at its never default and every
// target here fuzzes the pre-activation era (plus the `epoch == Epoch::MAX` sentinel).
fuzz_target!(|data: &[u8]| {
    // Try decoding as each consensus type and verify round-trip stability.

    // --- Certificate ---
    if let Ok(cert) = bcs::from_bytes::<Certificate>(data) {
        let encoded = bcs::to_bytes(&cert).expect("encode must succeed");
        let decoded: Certificate =
            bcs::from_bytes(&encoded).expect("re-decode must succeed");
        let re_encoded = bcs::to_bytes(&decoded).expect("re-encode must succeed");
        assert_eq!(encoded, re_encoded, "Certificate round-trip produced different bytes");
    }

    // --- Header ---
    if let Ok(header) = bcs::from_bytes::<Header>(data) {
        let encoded = header_canonical_bytes(&header);
        let decoded: Header =
            bcs::from_bytes(&encoded).expect("re-decode must succeed");
        let re_encoded = header_canonical_bytes(&decoded);
        assert_eq!(encoded, re_encoded, "Header round-trip produced different bytes");
    }

    // --- SealedBatch ---
    if let Ok(batch) = bcs::from_bytes::<SealedBatch>(data) {
        let encoded = bcs::to_bytes(&batch).expect("encode must succeed");
        let decoded: SealedBatch =
            bcs::from_bytes(&encoded).expect("re-decode must succeed");
        let re_encoded = bcs::to_bytes(&decoded).expect("re-encode must succeed");
        assert_eq!(encoded, re_encoded, "SealedBatch round-trip produced different bytes");
    }
});
