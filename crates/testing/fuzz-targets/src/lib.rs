// SPDX-License-Identifier: BUSL-1.1
//! Shared helpers for fuzz targets.
//!
//! This crate provides utility functions used by the `cargo-fuzz` harnesses
//! in the `fuzz/` subdirectory. It is intentionally NOT a workspace member
//! because `cargo-fuzz` manages its own `fuzz/Cargo.toml`.
//!
//! ## What the fuzz targets test
//!
//! These targets exercise **Axyl-specific logic**, not external crates:
//!
//! - `quorum_threshold`: arithmetic correctness of the `((2*n)/3)+1` formula
//!   at boundary values, including overflow for large committee sizes
//! - `header_nonce_roundtrip`: `(epoch<<32)|round` encoding/decoding,
//!   verifying no information loss (Axyl's custom EVM header field packing)
//! - `batch_digest`: `Batch::digest()` and `seal_slow()` determinism and
//!   collision resistance with arbitrary batch payloads
//! - `certificate_signed_by`: `Certificate::signed_by()` RoaringBitmap
//!   interpretation against committees of varying sizes — must never panic
//!   on out-of-range indices from a Byzantine peer
//! - `bcs_roundtrip`: serde round-trip stability for Certificate, Header,
//!   and SealedBatch — catches inconsistencies in Axyl's custom serde
//!   implementations (SignerBitmapSerde, serde(skip), OnceCell digest)
//!   that would cause validators to disagree on digests

/// Re-export infrastructure types used by fuzz targets.
pub use rayls_infrastructure_types as types;
