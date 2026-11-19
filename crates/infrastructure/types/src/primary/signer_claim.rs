use crate::{
    bcs_layout::{BcsCursor, BcsLayout, BcsLayoutError},
    BlsSignature, CertificateDigest, Epoch, Round,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignerClaim {
    pub target_digest: CertificateDigest,
    pub target_epoch: Epoch,
    pub target_round: Round,
    // Ranks are positions in Committee::bls_keys(), never Committee::authorities().
    pub ranks: Vec<u32>,
    pub signature: BlsSignature,
}

impl BcsLayout for SignerClaim {
    fn skip(c: &mut BcsCursor<'_>) -> Result<(), BcsLayoutError> {
        c.skip::<CertificateDigest>()?
            .skip::<Epoch>()?
            .skip::<Round>()?
            .skip::<Vec<u32>>()?
            .skip::<BlsSignature>()?;
        Ok(())
    }
}

pub fn signer_claims_region_bytes(claims: &[SignerClaim]) -> Vec<u8> {
    let mut out = Vec::new();
    crate::encode_into_buffer(&mut out, claims).expect("claims serialize");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claim(i: u8) -> SignerClaim {
        SignerClaim {
            target_digest: CertificateDigest::new([i; 32]),
            target_epoch: 7,
            target_round: 123,
            ranks: vec![0, 2, 3],
            signature: BlsSignature::default(),
        }
    }

    #[test]
    fn test_region_encoding_is_deterministic() {
        // Region bytes are a pure function of the claims and their order: no map
        // iteration order, no clock, nothing local.
        let a = vec![claim(1), claim(2)];
        let b = vec![claim(1), claim(2)];
        assert_eq!(signer_claims_region_bytes(&a), signer_claims_region_bytes(&b));
        assert_eq!(signer_claims_region_bytes(&a), crate::encode(&a));
        assert_eq!(signer_claims_region_bytes(&[]), vec![0u8]);
    }
}
