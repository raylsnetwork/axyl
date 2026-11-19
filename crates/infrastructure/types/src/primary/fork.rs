use super::header::Header;
use crate::Epoch;
use std::sync::OnceLock;

// Chainspec owns this epoch (`RaylsChainHardforks::signer_claims_activation_epoch` in
// crates/execution/evm/src/chainspec/hardforks.rs); this is only the process handle the wiring
// layer pushes it into. "installed" and "never" are separate states: an unset handle reads as the
// "never" sentinel, so a process that never calls `init_signer_claims_activation_epoch` behaves
// exactly as it did with the sentinel default, and one that installs `Epoch::MAX` is now
// *installed*-never and cannot be re-set to anything else.
// A `OnceLock` (not a sentinel-valued `AtomicU32`) is what makes those two states distinct, so the
// first call wins with whatever value it carries.
static SIGNER_CLAIMS_ACTIVATION_EPOCH: OnceLock<Epoch> = OnceLock::new();

// The value an unset handle reads as. `epoch_is_post_activation`'s arithmetic is unchanged by this
// distinction, which is what keeps `bin/rayls-db-inspect` and the fuzz targets — they never call
// init — byte-identical.
const NEVER: Epoch = Epoch::MAX;

// Set this before any header is built or digested in the process: the digest is computed lazily
// and cached, so a header digested earlier keeps its pre-activation bytes. First call stores, with
// whatever value it carries (including `Epoch::MAX`); a later call with the same value is a no-op
// (a runtime booting several nodes with one profile installs it repeatedly); a later call with a
// different value panics. A predicate read never installs anything, so a process that never calls
// this one keeps reading "never".
pub fn init_signer_claims_activation_epoch(epoch: Epoch) {
    let installed = SIGNER_CLAIMS_ACTIVATION_EPOCH.get_or_init(|| epoch);
    if *installed != epoch {
        panic!(
            "signer-claims activation epoch is already set to {installed}, cannot be re-set to {epoch}"
        );
    }
}

// The predicate reads the header's own epoch field and never local state.
pub fn header_is_post_activation(header: &Header) -> bool {
    epoch_is_post_activation(header.epoch)
}

pub(crate) fn epoch_is_post_activation(epoch: Epoch) -> bool {
    epoch >= SIGNER_CLAIMS_ACTIVATION_EPOCH.get().copied().unwrap_or(NEVER)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Hash, HeaderBuilder};

    #[test]
    fn test_digest_predicate_is_pure() {
        let build_at = |epoch: u32| {
            HeaderBuilder::default()
                .author(crate::AuthorityIdentifier::dummy_for_test(0xAA))
                .round(4)
                .epoch(epoch)
                .created_at(1_700_000_000)
                .payload(Default::default())
                .parents(std::collections::BTreeSet::new())
                .build()
        };
        for epoch in [0u32, 1, 7, 999, u32::MAX - 1] {
            assert!(!header_is_post_activation(&build_at(epoch)));
        }
        assert!(header_is_post_activation(&build_at(u32::MAX)));

        let a = build_at(7);
        let b = build_at(7);
        assert_eq!(Hash::digest(&a), Hash::digest(&b));
        assert_eq!(a.digest(), b.digest());
    }
}
