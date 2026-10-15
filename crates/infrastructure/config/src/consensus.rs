//! Configuration for consensus network (primary and worker).
use crate::{Config, ConfigFmt, ConfigTrait as _, KeyConfig, NetworkConfig, Parameters, RaylsDirs};
use rayls_infrastructure_network_types::local::LocalNetwork;
use rayls_infrastructure_storage::EpochStore as _;
use rayls_infrastructure_types::{
    dkg::{DealingParams, DkgOutcome, EpochScheme, EpochThreshold},
    Authority, AuthorityIdentifier, BlsPublicKey, Certificate, CertificateDigest, Committee,
    CommitteeLookahead, Database, Epoch, EpochKeys, Hash as _, Multiaddr, NetworkPublicKey,
    Notifier,
};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{Arc, RwLock},
};
use tracing::info;

/// The committee of the epoch before the current one, kept in memory for the life of the epoch.
///
/// A node keeps serving the previous epoch's certificate tail after a transition, and the
/// committee that produced it may differ from the current one. Holding it here lets request
/// handlers check membership for that epoch without a store read.
#[derive(Debug)]
pub struct PreviousCommittee {
    keys: Vec<BlsPublicKey>,
    ids: HashSet<AuthorityIdentifier>,
}

impl PreviousCommittee {
    fn new(keys: Vec<BlsPublicKey>) -> Self {
        let ids = keys.iter().copied().map(AuthorityIdentifier::from).collect();
        Self { keys, ids }
    }

    /// The BLS keys of the previous committee.
    pub fn keys(&self) -> &[BlsPublicKey] {
        &self.keys
    }

    /// Whether `id` was a member of the previous committee.
    pub fn contains(&self, id: &AuthorityIdentifier) -> bool {
        self.ids.contains(id)
    }
}

#[derive(Debug)]
struct ConsensusConfigInner<DB> {
    config: Config,
    committee: Committee,
    lookahead: CommitteeLookahead,
    prev_committee: Option<PreviousCommittee>,
    node_storage: DB,
    key_config: KeyConfig,
    authority: Option<Authority>,
    local_network: LocalNetwork,
    network_config: NetworkConfig,
    genesis: HashMap<CertificateDigest, Certificate>,
    /// How the current epoch's certificates are signed. `Pending` while the epoch's threshold
    /// key is not computed yet; resolved in place once it lands, so every clone sees it.
    scheme: RwLock<EpochScheme>,
    /// Which epochs dealt the next epoch's key, for verifying other epochs' certificates.
    dealt: DealtEpochs,
    /// Dealing parameters for the next epoch's key, when this epoch deals.
    dealing: Option<Arc<DealingParams>>,
}

/// Which epochs dealt the next epoch's threshold key, as the orchestrator's dealing gate answered
/// when it configured the epoch. Lets a verifier tell a legacy epoch from one whose key it has
/// not computed yet.
#[derive(Clone, Debug)]
pub struct DealtEpochs {
    /// The fork never activates: no epoch deals, so nothing is ever pending.
    never: bool,
    /// The gate's answers for the epochs it could read.
    known: BTreeMap<Epoch, bool>,
}

impl Default for DealtEpochs {
    fn default() -> Self {
        Self::never()
    }
}

impl DealtEpochs {
    /// No epoch deals: the fork is scheduled `Never`.
    pub fn never() -> Self {
        Self { never: true, known: BTreeMap::new() }
    }

    /// The gate's answers for `known` epochs. Block forks only activate upward, so an epoch
    /// below a known non-dealing one did not deal and one above a known dealing one did.
    pub fn from_known(known: BTreeMap<Epoch, bool>) -> Self {
        Self { never: false, known }
    }

    /// Whether `epoch` dealt the next epoch's key; `None` when the gate could not tell.
    pub fn dealt(&self, epoch: Epoch) -> Option<bool> {
        if self.never || epoch == 0 {
            return Some(false);
        }
        if let Some(dealt) = self.known.get(&epoch) {
            return Some(*dealt);
        }
        if let Some((_, false)) = self.known.range(epoch..).next() {
            return Some(false);
        }
        if let Some((_, true)) = self.known.range(..epoch).next_back() {
            return Some(true);
        }
        None
    }
}

/// What a verifier can do with one epoch's certificates.
// A transient lookup result, never stored in bulk; the size gap between the variants does not
// matter.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EpochKeysStatus {
    /// The epoch's committee keys and certificate scheme.
    Ready(EpochKeys),
    /// No committee keys for the epoch on this node yet.
    NoCommittee,
    /// The epoch uses threshold certificates whose key this node has not computed yet (it is
    /// above the current epoch, or its dealing epoch's outcome is not stored). Verifying as
    /// aggregate would reject honest certificates, so callers wait or drop the item.
    Pending,
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
    /// Activation block of the `CertificateFetchEpochV2` fork, `None` while it is `Never`. Read
    /// from the chain spec by the orchestrator when the epoch config is built; the certificate
    /// fetcher compares it with the executed tip to pick the request variant it sends.
    certificate_fetch_epoch_v2_block: Option<u64>,
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
            EpochScheme::Aggregate,
            DealtEpochs::never(),
            None,
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
            EpochScheme::Aggregate,
            DealtEpochs::never(),
            None,
        )
    }

    /// Creates configuration for the next consensus epoch.
    ///
    /// This constructor is used during epoch transitions to initialize configuration
    /// with updated committee membership and worker topology for the new epoch.
    #[allow(clippy::too_many_arguments)]
    pub fn new_for_epoch(
        config: Config,
        node_storage: DB,
        key_config: KeyConfig,
        committee: Committee,
        lookahead: CommitteeLookahead,
        network_config: NetworkConfig,
        epoch_boundary: u64,
        initial_epoch: bool,
        scheme: EpochScheme,
        dealt: DealtEpochs,
        dealing: Option<Arc<DealingParams>>,
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
            scheme,
            dealt,
            dealing,
        )
    }

    /// Internal constructor that initializes consensus configuration with provided committee.
    ///
    /// This method performs the core initialization logic including:
    /// - Setting up local network identity
    /// - Resolving authority status within the committee
    /// - Creating genesis certificates
    /// - Initializing shutdown notification system
    #[allow(clippy::too_many_arguments)]
    fn new_with_committee(
        config: Config,
        node_storage: DB,
        key_config: KeyConfig,
        committee: Committee,
        lookahead: CommitteeLookahead,
        network_config: NetworkConfig,
        epoch_boundary: u64,
        initial_epoch: bool,
        scheme: EpochScheme,
        dealt: DealtEpochs,
        dealing: Option<Arc<DealingParams>>,
    ) -> eyre::Result<Self> {
        let local_network = LocalNetwork::new(key_config.primary_public_key());

        let primary_public_key = key_config.primary_public_key();
        let authority = committee.authority_by_key(&primary_public_key);

        let shutdown = Notifier::new();
        let genesis = Certificate::genesis(&committee)
            .into_iter()
            .map(|cert| (cert.digest(), cert))
            .collect();

        // Read once per epoch here, so membership checks against the previous epoch's
        // certificate tail never go to the store per request.
        let prev_committee = committee
            .epoch()
            .checked_sub(1)
            .and_then(|epoch| node_storage.get_committee_keys(epoch))
            .map(PreviousCommittee::new);

        Ok(Self {
            inner: Arc::new(ConsensusConfigInner {
                config,
                committee,
                lookahead,
                prev_committee,
                node_storage,
                key_config,
                authority,
                local_network,
                network_config,
                genesis,
                scheme: RwLock::new(scheme),
                dealt,
                dealing,
            }),
            shutdown,
            epoch_boundary,
            initial_epoch,
            certificate_fetch_epoch_v2_block: None,
        })
    }

    /// Install the activation block of the `CertificateFetchEpochV2` fork (`None` for `Never`).
    pub fn with_certificate_fetch_epoch_v2_block(mut self, block: Option<u64>) -> Self {
        self.certificate_fetch_epoch_v2_block = block;
        self
    }

    /// How the current epoch's certificates are signed.
    pub fn scheme(&self) -> EpochScheme {
        self.inner.scheme.read().unwrap_or_else(|poisoned| poisoned.into_inner()).clone()
    }

    /// Resolve the current epoch's scheme, once a pending threshold key lands. The committee's
    /// threshold key is the caller's to set first.
    pub fn set_scheme(&self, scheme: EpochScheme) {
        *self.inner.scheme.write().unwrap_or_else(|poisoned| poisoned.into_inner()) = scheme;
    }

    /// True while the current epoch's threshold key is not computed yet.
    pub fn scheme_pending(&self) -> bool {
        self.inner.scheme.read().unwrap_or_else(|poisoned| poisoned.into_inner()).is_pending()
    }

    /// The current epoch's threshold material, when the epoch uses threshold certificates.
    pub fn threshold(&self) -> Option<Arc<EpochThreshold>> {
        self.scheme().threshold().cloned()
    }

    /// Dealing parameters for the next epoch's key, when this epoch deals.
    pub fn dealing(&self) -> Option<&Arc<DealingParams>> {
        self.inner.dealing.as_ref()
    }

    /// Keys and scheme to verify certificates of `epoch`, when they are known now. See
    /// [`Self::epoch_keys_status`] for why they may not be.
    pub fn epoch_keys(&self, epoch: Epoch) -> Option<EpochKeys> {
        match self.epoch_keys_status(epoch) {
            EpochKeysStatus::Ready(keys) => Some(keys),
            EpochKeysStatus::NoCommittee | EpochKeysStatus::Pending => None,
        }
    }

    /// Keys and scheme to verify certificates of `epoch`. Committee keys fall back through the
    /// same sources as `get_committee_keys_for_epoch`. The scheme is the current epoch's own, or
    /// the stored DKG outcome; without one, epochs 0 and 1 and any epoch whose previous epoch did
    /// not deal are legacy, and the rest are `Pending`.
    pub fn epoch_keys_status(&self, epoch: Epoch) -> EpochKeysStatus {
        if epoch == self.inner.committee.epoch() {
            let group_key = match self.scheme() {
                EpochScheme::Pending => return EpochKeysStatus::Pending,
                EpochScheme::Threshold(threshold) => Some(threshold.key.group_key()),
                EpochScheme::Aggregate => None,
            };
            let keys = self.inner.committee.bls_keys();
            return EpochKeysStatus::Ready(EpochKeys { keys, group_key });
        }
        let Some(keys) = self.get_committee_keys_for_epoch(epoch) else {
            return EpochKeysStatus::NoCommittee;
        };
        let group_key = match self.inner.node_storage.get_dkg_outcome(epoch) {
            Some(DkgOutcome::Key(key)) => Some(key.group_key()),
            Some(DkgOutcome::Fallback) => None,
            None if epoch <= 1 || self.inner.dealt.dealt(epoch - 1) == Some(false) => None,
            None => return EpochKeysStatus::Pending,
        };
        EpochKeysStatus::Ready(EpochKeys { keys, group_key })
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

    /// Activation block of the `CertificateFetchEpochV2` fork, `None` while it is `Never`.
    pub fn certificate_fetch_epoch_v2_block(&self) -> Option<u64> {
        self.certificate_fetch_epoch_v2_block
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

    /// The committee of the previous epoch, if the epoch store had it when this config was built.
    ///
    /// `None` for epoch 0, and for a node whose store held no record of the previous epoch at
    /// that time. This is a snapshot: a record written later in the epoch does not fill it in.
    /// [`Self::get_committee_keys_for_epoch`] falls back to the store in that case.
    pub fn previous_committee(&self) -> Option<&PreviousCommittee> {
        self.inner.prev_committee.as_ref()
    }

    /// Retrieve the BLS committee keys for a given epoch.
    ///
    /// Falls back through: in-memory committee (current epoch), in-memory previous committee,
    /// DB epoch records, then lookahead cache.
    pub fn get_committee_keys_for_epoch(&self, epoch: Epoch) -> Option<Vec<BlsPublicKey>> {
        let current = self.inner.committee.epoch();
        if epoch == current {
            return Some(self.inner.committee.bls_keys());
        }
        if Some(epoch) == current.checked_sub(1) {
            if let Some(prev) = &self.inner.prev_committee {
                return Some(prev.keys.clone());
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{rngs::StdRng, SeedableRng};
    use std::collections::BTreeMap;
    use rayls_infrastructure_storage::{mem_db::MemDatabase, tables::EpochRecords, EpochStore};
    use rayls_infrastructure_types::{Address, BlsKeypair, CommitteeBuilder, EpochRecord};

    /// A config for a two-member committee at `epoch` (this node and one other), built on `db`.
    fn config_at_epoch(
        epoch: Epoch,
        db: MemDatabase,
        keypair: BlsKeypair,
        rng: &mut StdRng,
    ) -> ConsensusConfig<MemDatabase> {
        let mut builder = CommitteeBuilder::new(epoch);
        builder.add_authority(*keypair.public(), 1, Address::ZERO);
        builder.add_authority(*BlsKeypair::generate(rng).public(), 1, Address::ZERO);
        ConsensusConfig::new_with_committee_for_test(
            Config::default(),
            db,
            KeyConfig::new_with_testing_key(keypair),
            builder.build(),
            NetworkConfig::default(),
        )
        .expect("config")
    }

    #[test]
    fn previous_committee_is_kept_in_memory_from_the_epoch_store() {
        let mut rng = StdRng::seed_from_u64(1);
        let current = BlsKeypair::generate(&mut rng);
        let previous = *BlsKeypair::generate(&mut rng).public();
        let db = MemDatabase::default();
        let record = EpochRecord { epoch: 0, committee: vec![previous], ..Default::default() };
        db.save_epoch_record(&record).expect("record");

        let config = config_at_epoch(1, db, current, &mut rng);

        // Drop the record: the previous committee must now come from memory, not the store.
        config.node_storage().remove::<EpochRecords>(&0).expect("remove");
        let prev = config.previous_committee().expect("previous committee");
        assert_eq!(prev.keys(), &[previous]);
        assert!(prev.contains(&AuthorityIdentifier::from(previous)));
        let me = AuthorityIdentifier::from(config.key_config().primary_public_key());
        assert!(!prev.contains(&me));
        assert_eq!(config.get_committee_keys_for_epoch(0), Some(vec![previous]));
    }

    #[test]
    fn no_previous_committee_at_epoch_zero_or_without_a_record() {
        let mut rng = StdRng::seed_from_u64(2);
        let keypair = BlsKeypair::generate(&mut rng);
        let at_zero = config_at_epoch(0, MemDatabase::default(), keypair, &mut rng);
        assert!(at_zero.previous_committee().is_none());

        let keypair = BlsKeypair::generate(&mut rng);
        let unknown = config_at_epoch(1, MemDatabase::default(), keypair, &mut rng);
        assert!(unknown.previous_committee().is_none());
        assert_eq!(unknown.get_committee_keys_for_epoch(0), None);

        // A record written after the config was built does not fill the snapshot, but the key
        // lookup still finds it in the store.
        let previous = *BlsKeypair::generate(&mut rng).public();
        let record = EpochRecord { epoch: 0, committee: vec![previous], ..Default::default() };
        unknown.node_storage().save_epoch_record(&record).expect("record");
        assert!(unknown.previous_committee().is_none());
        assert_eq!(unknown.get_committee_keys_for_epoch(0), Some(vec![previous]));
    }

    #[test]
    fn dealt_epochs_answer_from_the_gate_and_block_fork_monotonicity() {
        let never = DealtEpochs::never();
        assert_eq!(never.dealt(7), Some(false));
        let known = DealtEpochs::from_known(BTreeMap::from([(4, false), (5, true), (6, true)]));
        assert_eq!(known.dealt(0), Some(false), "epoch 0 never deals");
        assert_eq!(known.dealt(5), Some(true));
        // Below a non-dealing epoch nothing dealt; above a dealing one everything does.
        assert_eq!(known.dealt(2), Some(false));
        assert_eq!(known.dealt(9), Some(true));
        let unknown = DealtEpochs::from_known(BTreeMap::from([(4, true)]));
        assert_eq!(unknown.dealt(3), None);
        assert_eq!(DealtEpochs::from_known(BTreeMap::new()).dealt(3), None);
    }
}
