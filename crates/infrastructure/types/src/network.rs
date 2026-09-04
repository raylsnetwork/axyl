//! Rayls network profiles with baked-in hardfork schedules.

pub const MIN_RAYLS_PROTOCOL_BASE_FEE: u64 = 48000000000;
/// Rayls network profiles with baked-in hardfork schedules.
///
/// Each variant selects a different set of activation blocks, following the
/// same pattern as [`EthereumHardfork::mainnet()`] / [`EthereumHardfork::sepolia()`].
#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
#[cfg_attr(feature = "cli", derive(clap::ValueEnum))]
#[serde(rename_all = "lowercase")]
pub enum RaylsNetwork {
    /// All forks active from genesis (block 0).
    Devnet,
    /// Forks at predetermined test network blocks.
    #[default]
    Testnet,
    /// Production fork schedule.
    Mainnet,
    /// Local development network with all mainnet forks activated
    Local,
}

impl std::fmt::Display for RaylsNetwork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Devnet => write!(f, "devnet"),
            Self::Testnet => write!(f, "testnet"),
            Self::Mainnet => write!(f, "mainnet"),
            Self::Local => write!(f, "local"),
        }
    }
}

impl RaylsNetwork {
    /// The chain-id a datadir for this network must carry.
    ///
    /// Mainnet is `487`; testnet, devnet and local all run `2017` today (dev
    /// bootstrap, genesis CLI default and faucet). The node verifies the
    /// datadir's genesis chain-id against this at boot, so a datadir from the
    /// wrong network (or client) is refused before anything runs.
    pub const fn chain_id(self) -> u64 {
        match self {
            Self::Mainnet => 487,
            Self::Testnet | Self::Devnet | Self::Local => 2017,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::RaylsNetwork;

    #[test]
    fn chain_id_mainnet() {
        assert_eq!(RaylsNetwork::Mainnet.chain_id(), 487);
    }

    #[test]
    fn chain_id_non_mainnet_is_2017() {
        assert_eq!(RaylsNetwork::Testnet.chain_id(), 2017);
        assert_eq!(RaylsNetwork::Devnet.chain_id(), 2017);
        assert_eq!(RaylsNetwork::Local.chain_id(), 2017);
    }
}
