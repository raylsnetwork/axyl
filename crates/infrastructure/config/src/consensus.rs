//! Configuration for consensus network (primary and worker).
use crate::{Config, ConfigFmt, ConfigTrait as _, KeyConfig, NetworkConfig, Parameters, RaylsDirs};
use rayls_infrastructure_network_types::local::LocalNetwork;
use rayls_infrastructure_storage::EpochStore as _;
use rayls_infrastructure_types::{
    Authority, AuthorityIdentifier, BlsPublicKey, Certificate, CertificateDigest, Committee,
    CommitteeLookahead, Database, Epoch, Hash as _, Multiaddr, NetworkPublicKey, Notifier,
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tracing::info;

#[derive(Debug)]
struct ConsensusConfigInner<DB> {
    config: Config,
    committee: Committee,
    lookahead: CommitteeLookahead,
    node_storage: DB,
    key_config: KeyConfig,
    authority: Option<Authority>,
    local_network: LocalNetwork,
    network_config: NetworkConfig,
    genesis: HashMap<CertificateDigest, Certificate>,
}

/// The configuration for consensus.
///
/// This structure holds all necessary configuration data for both primary and worker
/// consensus components. It manages committee membership, cryptographic keys, network
/// topology, and genesis state required for consensus participation.
///
/// The configuration is designed to be shared across consensus components and provides
/// both authority-specific and network-wide configuration access.
#[derive(Debug, Clone)]
pub struct ConsensusConfig<DB> {
    inner: Arc<ConsensusConfigInner<DB>>,
    shutdown: Notifier,
    /// Epoch boundary timestamp (seconds) for this epoch. Fixed at config creation by the
    /// orchestrator and never changes for the life of the epoch, so it is held by value rather
    /// than shared/atomic. Default `u64::MAX` disables epoch-boundary cuts until a real boundary
    /// is installed (e.g. initial startup load and tests).
    epoch_boundary: u64,
    /// True if this is the first epoch since the node process started (no prior state to sync
    /// from). Fixed for the life of the epoch, so it is held by value. The certifier uses it to
    /// skip the cert-fetcher grace period on initial startup.
    initial_epoch: bool,
}

impl<DB> ConsensusConfig<DB>
where
    DB: Database,
{
    /// Creates a new consensus configuration by loading committee and worker cache from disk.
    ///
    /// This is the primary constructor that loads configuration from the filesystem,
    /// including committee membership and worker topology from YAML files.
    pub fn new<RLD: RaylsDirs + 'static>(
        config: Config,
        rayls_datadir: &RLD,
        node_storage: DB,
        key_config: KeyConfig,
        network_config: NetworkConfig,
    ) -> eyre::Result<Self> {
        // load committee from file
        let committee: Committee =
            Config::load_from_path_or_default(rayls_datadir.committee_path(), ConfigFmt::YAML)?;
        committee.load();
        info!(target: "rayls", "committee loaded");

        info!(target: "rayls", "worker cache loaded");
        Self::new_with_committee(
            config,
            node_storage,
            key_config,
            committee,
            CommitteeLookahead::default(),
            network_config,
            u64::MAX,
            true,
        )
    }

    /// Creates a new configuration with a pre-loaded committee for testing purposes.
    ///
    /// **WARNING: This method is exposed publicly for testing ONLY.**
    /// Production code should use `new()` or `new_for_epoch()` to ensure proper configuration
    /// loading.
    pub fn new_with_committee_for_test(
        config: Config,
        node_storage: DB,
        key_config: KeyConfig,
        committee: Committee,
        network_config: NetworkConfig,
    ) -> eyre::Result<Self> {
        Self::new_with_committee(
            config,
            node_storage,
            key_config,
            committee,
            CommitteeLookahead::default(),
            network_config,
            u64::MAX,
            true,
        )
    }

    /// Creates configuration for the next consensus epoch.
    ///
    /// This constructor is used during epoch transitions to initialize configuration
    /// with updated committee membership and worker topology for the new epoch.
    pub fn new_for_epoch(
        config: Config,
        node_storage: DB,
        key_config: KeyConfig,
        committee: Committee,
        lookahead: CommitteeLookahead,
        network_config: NetworkConfig,
        epoch_boundary: u64,
        initial_epoch: bool,
    ) -> eyre::Result<Self> {
        Self::new_with_committee(
            config,
            node_storage,
            key_config,
            committee,
            lookahead,
            network_config,
            epoch_boundary,
            initial_epoch,
        )
    }

    /// Internal constructor that initializes consensus configuration with provided committee.
    ///
    /// This method performs the core initialization logic including:
    /// - Setting up local network identity
    /// - Resolving authority status within the committee
    /// - Creating genesis certificates
    /// - Initializing shutdown notification system
    fn new_with_committee(
        config: Config,
        node_storage: DB,
        key_config: KeyConfig,
        committee: Committee,
        lookahead: CommitteeLookahead,
        network_config: NetworkConfig,
        epoch_boundary: u64,
        initial_epoch: bool,
    ) -> eyre::Result<Self> {
        let local_network = LocalNetwork::new(key_config.primary_public_key());

        let primary_public_key = key_config.primary_public_key();
        let authority = committee.authority_by_key(&primary_public_key);

        let shutdown = Notifier::new();
        let genesis = Certificate::genesis(&committee)
            .into_iter()
            .map(|cert| (cert.digest(), cert))
            .collect();

        Ok(Self {
            inner: Arc::new(ConsensusConfigInner {
                config,
                committee,
                lookahead,
                node_storage,
                key_config,
                authority,
                local_network,
                network_config,
                genesis,
            }),
            shutdown,
            epoch_boundary,
            initial_epoch,
        })
    }

    /// Returns a reference to the shutdown notifier.
    ///
    /// The shutdown notifier can be used to either subscribe to shutdown events
    /// or trigger shutdown across consensus components.
    pub fn shutdown(&self) -> &Notifier {
        &self.shutdown
    }

    /// Returns a reference to the inner config parameters.
    pub fn config(&self) -> &Config {
        &self.inner.config
    }

    /// Returns a reference to the genesis certificate collection.
    ///
    /// Genesis certificates establish the initial state and authority set
    /// for the consensus protocol to produce the first primary `Header`.
    pub fn genesis(&self) -> &HashMap<CertificateDigest, Certificate> {
        &self.inner.genesis
    }

    /// Returns a reference to the current committee membership.
    ///
    /// The committee defines the set of authorities participating in consensus
    /// for the current epoch.
    pub fn committee(&self) -> &Committee {
        &self.inner.committee
    }

    /// Returns the epoch boundary timestamp (seconds) for this epoch.
    ///
    /// This is the single source of truth for "is this subdag beyond the boundary". It is fixed
    /// when the config is created for the epoch and never changes, so reads need no
    /// synchronization. `u64::MAX` means no boundary is installed (cuts disabled).
    pub fn epoch_boundary(&self) -> u64 {
        self.epoch_boundary
    }

    /// Overrides the epoch boundary on this config. Intended for tests that drive consensus
    /// components directly; production sets the boundary via [`Self::new_for_epoch`].
    pub fn set_epoch_boundary(&mut self, epoch_boundary: u64) {
        self.epoch_boundary = epoch_boundary;
    }

    /// True if this is the first epoch since the node process started (no prior state to sync
    /// from). Fixed when the config is created for the epoch; the certifier reads it to skip the
    /// cert-fetcher grace period on initial startup.
    pub fn is_initial_epoch(&self) -> bool {
        self.initial_epoch
    }

    /// Returns a reference to the node's persistent storage database for the current epoch.
    pub fn node_storage(&self) -> &DB {
        &self.inner.node_storage
    }

    /// Returns a reference to the cryptographic key configuration.
    ///
    /// Contains both primary and worker cryptographic keys used for
    /// consensus participation and network communication.
    pub fn key_config(&self) -> &KeyConfig {
        &self.inner.key_config
    }

    /// Returns the authority information for this node. Optional if it is a committee member or
    /// not.
    pub fn authority(&self) -> &Option<Authority> {
        &self.inner.authority
    }

    /// Returns the authority identifier for this node, if it is a committee member.
    pub fn authority_id(&self) -> Option<AuthorityIdentifier> {
        self.inner.authority.as_ref().map(|a| a.id())
    }

    /// Returns a reference to the consensus protocol parameters.
    ///
    /// Parameters include timing constraints, batch sizes, and other
    /// protocol-specific configuration values.
    pub fn parameters(&self) -> &Parameters {
        &self.inner.config.parameters
    }

    /// Returns a reference to the local network configuration.
    ///
    /// Contains network identity and local networking setup information.
    /// This is how Primary <-> Workers communicate.
    pub fn local_network(&self) -> &LocalNetwork {
        &self.inner.local_network
    }

    /// Returns a reference to the network configuration.
    ///
    /// Contains p2p settings and connectivity parameters for libp2p.
    pub fn network_config(&self) -> &NetworkConfig {
        &self.inner.network_config
    }

    /// The current epoch for [Committee].
    pub fn epoch(&self) -> Epoch {
        self.inner.committee.epoch()
    }

    /// Return the committee lookahead cache for upcoming epochs.
    pub fn lookahead(&self) -> &CommitteeLookahead {
        &self.inner.lookahead
    }

    /// Retrieve the BLS committee keys for a given epoch.
    ///
    /// Falls back through: in-memory committee (current epoch), DB epoch records, then
    /// lookahead cache.
    pub fn get_committee_keys_for_epoch(&self, epoch: Epoch) -> Option<Vec<BlsPublicKey>> {
        let current = self.inner.committee.epoch();
        if epoch == current {
            return Some(self.inner.committee.bls_keys());
        }
        self.inner
            .node_storage
            .get_committee_keys(epoch)
            .or_else(|| self.inner.lookahead.get(epoch).map(|keys| keys.to_vec()))
    }

    /// Committee network peer ids.
    pub fn committee_pub_keys(&self) -> HashSet<BlsPublicKey> {
        self.inner.committee.authorities().iter().map(|a| a.protocol_key()).copied().collect()
    }

    /// Retrieve the primaries network address.
    pub fn primary_address(&self) -> Multiaddr {
        self.inner.config.node_info.p2p_info.primary.network_address.clone()
    }

    /// Retrieve the primaries network address.
    pub fn primary_networkkey(&self) -> NetworkPublicKey {
        self.inner.config.node_info.p2p_info.primary.network_key.clone()
    }

    /// Bool indicating if an authority identifier is in the current committee.
    pub fn in_committee(&self, id: &AuthorityIdentifier) -> bool {
        self.inner.committee.is_authority(id)
    }

    /// Retrieve the worker's network address by id.
    /// Note, will panic if id is not valid.
    pub fn worker_address(&self) -> Multiaddr {
        self.inner.config.node_info.p2p_info.worker.network_address.clone()
    }

    /// Retrieve the worker's network public key.
    pub fn worker_networkkey(&self) -> NetworkPublicKey {
        self.inner.config.node_info.p2p_info.worker.network_key.clone()
    }
}
