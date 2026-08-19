//! Committee of validators that reach consensus, and the authority records it holds.

use crate::{
    bcs_layout::{BcsCursor, BcsLayout, BcsLayoutError, BcsRead},
    crypto::{BlsPublicKey, NetworkPublicKey},
    Address, Multiaddr,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fmt::{Display, Formatter},
    num::NonZeroU64,
    sync::Arc,
};

/// The epoch number.
/// Becomes the upper 32 bits of a nonce (with rounds the low bits).
pub type Epoch = u32;

/// The voting power an authority has within the committee.
pub type VotingPower = u64;

/// A multiaddr and network public key for a libp2p node.
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct P2pNode {
    /// The network address of the node.
    pub network_address: Multiaddr,
    /// Network key of the node.
    pub network_key: NetworkPublicKey,
}

impl From<(Multiaddr, NetworkPublicKey)> for P2pNode {
    fn from(value: (Multiaddr, NetworkPublicKey)) -> Self {
        Self { network_address: value.0, network_key: value.1 }
    }
}

impl From<(NetworkPublicKey, Multiaddr)> for P2pNode {
    fn from(value: (NetworkPublicKey, Multiaddr)) -> Self {
        Self { network_address: value.1, network_key: value.0 }
    }
}

/// Bootstrap p2p server info to join the network.
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct BootstrapServer {
    /// The p2p info the primary.
    pub primary: P2pNode,
    /// The p2p info the worker.
    pub worker: P2pNode,
}

impl BootstrapServer {
    /// Creates a bootstrap record from the primary's and worker's p2p info.
    pub fn new(primary_node: P2pNode, worker_node: P2pNode) -> Self {
        Self { primary: primary_node, worker: worker_node }
    }
}

/// Immutable authority data.
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
struct AuthorityInner {
    /// The authority's main BlsPublicKey which is used to verify the content they sign.
    protocol_key: BlsPublicKey,
    /// The voting power of this authority.
    voting_power: VotingPower,
    /// The execution address for the authority.
    /// This address will be used as the suggested fee recipient.
    execution_address: Address,
}

/// An Authority, a member of the committee.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Authority {
    inner: Arc<AuthorityInner>,
    /// Cached at construction: deriving the id hashes the BLS key and allocates,
    /// and callers ask for it on every round.
    id: AuthorityIdentifier,
}

impl Authority {
    /// Private by design: authorities are created through [CommitteeBuilder] so that every
    /// authority a caller holds belongs to a committee and cannot go stale against it.
    fn new(
        protocol_key: BlsPublicKey,
        voting_power: VotingPower,
        execution_address: Address,
    ) -> Self {
        let id = Self::derive_id(&protocol_key);
        Self {
            inner: Arc::new(AuthorityInner { protocol_key, voting_power, execution_address }),
            id,
        }
    }

    /// Builds an authority outside a committee. Test-only; production code must go through
    /// [CommitteeBuilder] (see `new`).
    pub fn new_for_test(
        protocol_key: BlsPublicKey,
        voting_power: VotingPower,
        execution_address: Address,
    ) -> Self {
        let id = Self::derive_id(&protocol_key);
        Self {
            inner: Arc::new(AuthorityInner { protocol_key, voting_power, execution_address }),
            id,
        }
    }

    /// Derives the identifier, a pure function of the protocol key, once per authority.
    fn derive_id(protocol_key: &BlsPublicKey) -> AuthorityIdentifier {
        let bytes = protocol_key.to_bytes();
        let mut hasher = crate::DefaultHashFunction::new();
        hasher.update(&bytes);
        AuthorityIdentifier(Arc::new(*hasher.finalize().as_bytes()))
    }

    /// Returns the authority's identifier.
    pub fn id(&self) -> AuthorityIdentifier {
        self.id.clone()
    }

    /// Returns the BLS key the authority signs consensus messages with.
    pub fn protocol_key(&self) -> &BlsPublicKey {
        &self.inner.protocol_key
    }

    /// Returns the authority's voting power.
    pub fn voting_power(&self) -> VotingPower {
        self.inner.voting_power
    }

    /// Returns the address that receives the authority's fees.
    pub fn execution_address(&self) -> Address {
        self.inner.execution_address
    }
}

impl Serialize for Authority {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let ok = self.inner.serialize(serializer)?;
        Ok(ok)
    }
}

impl<'de> Deserialize<'de> for Authority {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let inner = AuthorityInner::deserialize(deserializer)?;
        let id = Self::derive_id(&inner.protocol_key);
        Ok(Self { inner: Arc::new(inner), id })
    }
}

/// The committee lists all validators that participate in consensus.
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Default)]
struct CommitteeInner {
    /// The authorities of epoch.
    authorities: BTreeMap<BlsPublicKey, Authority>,
    /// Index of the authorities by identifier.
    #[serde(skip)]
    authorities_by_id: BTreeMap<AuthorityIdentifier, Authority>,
    /// The epoch number of this committee.
    epoch: Epoch,
    /// The quorum threshold (2f+1).
    #[serde(skip)]
    quorum_threshold: VotingPower,
    /// The validity threshold (f+1).
    #[serde(skip)]
    validity_threshold: VotingPower,
    /// The bootstrap servers to initially join a network (probably the initial committee).
    bootstrap_servers: BTreeMap<BlsPublicKey, BootstrapServer>,
}

impl CommitteeInner {
    /// Updates the committee internal secondary indexes.
    fn load(&mut self) {
        self.authorities_by_id = self
            .authorities
            .values()
            .map(|authority| {
                let id = authority.id();
                (id, authority.clone())
            })
            .collect();

        self.validity_threshold = self.calculate_validity_threshold().get();
        self.quorum_threshold = self.calculate_quorum_threshold().get();
        #[cfg(not(feature = "dev-single-node-setup"))]
        assert!(self.authorities_by_id.len() > 1, "committee size must be larger than 1");
        // Dev builds relax the floor to allow a single-validator committee. The
        // single-node-only invariant is enforced at node startup (see node.rs), not
        // here - this constructor is shared by the multi-validator consensus test suite.
        #[cfg(feature = "dev-single-node-setup")]
        assert!(!self.authorities_by_id.is_empty(), "committee size must be at least 1");
    }

    fn calculate_quorum_threshold(&self) -> NonZeroU64 {
        // If N = 3f + 1 + k (0 <= k < 3)
        // then (2 N + 3) / 3 = 2f + 1 + (2k + 2)/3 = 2f + 1 + k = N - f
        let total_votes: VotingPower = self.total_voting_power();
        NonZeroU64::new(2 * total_votes / 3 + 1).expect("arithmetic always produces result above 0")
    }

    fn calculate_validity_threshold(&self) -> NonZeroU64 {
        // If N = 3f + 1 + k (0 <= k < 3)
        // then (N + 2) / 3 = f + 1 + k/3 = f + 1
        let total_votes: VotingPower = self.total_voting_power();
        NonZeroU64::new(total_votes.div_ceil(3)).unwrap_or(NonZeroU64::new(1).expect("1 is NOT 0!"))
    }

    fn total_voting_power(&self) -> VotingPower {
        self.authorities.values().map(|x| x.inner.voting_power).sum()
    }
}

/// The committee lists all validators that participate in consensus.
#[derive(Clone, Debug, Default)]
pub struct Committee {
    inner: Arc<RwLock<CommitteeInner>>,
}

impl Serialize for Committee {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let ok = self.inner.read().serialize(serializer)?;
        Ok(ok)
    }
}

impl<'de> Deserialize<'de> for Committee {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let inner = CommitteeInner::deserialize(deserializer)?;
        Ok(Self { inner: Arc::new(RwLock::new(inner)) })
    }
}

impl PartialEq for Committee {
    fn eq(&self, other: &Self) -> bool {
        self.inner.read().eq(&*other.inner.read())
    }
}

impl Eq for Committee {}

/// Unique identifier of an authority: the hash of its BLS key, so it is stable across epochs.
#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Hash, Serialize, Deserialize)]
pub struct AuthorityIdentifier(Arc<[u8; 32]>);

impl AuthorityIdentifier {
    /// Builds an identifier from a repeated byte, for tests that need distinct ids.
    pub fn dummy_for_test(byte: u8) -> Self {
        Self(Arc::new([byte; 32]))
    }
}

impl From<BlsPublicKey> for AuthorityIdentifier {
    fn from(value: BlsPublicKey) -> Self {
        let bytes = value.to_bytes();
        let mut hasher = crate::DefaultHashFunction::new();
        hasher.update(&bytes);
        AuthorityIdentifier(Arc::new(*hasher.finalize().as_bytes()))
    }
}

impl Default for AuthorityIdentifier {
    fn default() -> Self {
        Self(Arc::new([0_u8; 32]))
    }
}

impl Display for AuthorityIdentifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&bs58::encode(&*self.0).into_string())
    }
}

impl std::fmt::Debug for AuthorityIdentifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&bs58::encode(&*self.0).into_string())
    }
}

/// BCS layout: the inner `[u8; 32]`, serialized verbatim (no length prefix).
/// Keep in lockstep with the inner repr.
impl BcsLayout for AuthorityIdentifier {
    #[inline]
    fn skip(c: &mut BcsCursor<'_>) -> Result<(), BcsLayoutError> {
        c.skip::<[u8; 32]>().map(drop)
    }
}

impl BcsRead for AuthorityIdentifier {
    #[inline]
    fn read(c: &mut BcsCursor<'_>) -> Result<Self, BcsLayoutError> {
        c.read::<[u8; 32]>().map(Self::from)
    }
}

impl From<[u8; 32]> for AuthorityIdentifier {
    fn from(bytes: [u8; 32]) -> Self {
        Self(Arc::new(bytes))
    }
}

impl Committee {
    /// Any committee should be created via the [CommitteeBuilder] - this is intentionally
    /// a private method.
    fn new(
        authorities: BTreeMap<BlsPublicKey, Authority>,
        epoch: Epoch,
        bootstrap_servers: BTreeMap<BlsPublicKey, BootstrapServer>,
    ) -> Self {
        let mut committee = CommitteeInner {
            authorities,
            epoch,
            authorities_by_id: Default::default(),
            validity_threshold: 0,
            quorum_threshold: 0,
            bootstrap_servers,
        };
        committee.load();

        // Some sanity checks to ensure that we'll not end up in invalid state
        assert_eq!(committee.authorities_by_id.len(), committee.authorities.len());

        assert_eq!(committee.validity_threshold, committee.calculate_validity_threshold().get());
        assert_eq!(committee.quorum_threshold, committee.calculate_quorum_threshold().get());

        Self { inner: Arc::new(RwLock::new(committee)) }
    }

    /// Builds a committee without the builder. Test-only; production code must go through
    /// [CommitteeBuilder] (see `new`).
    pub fn new_for_test(
        authorities: BTreeMap<BlsPublicKey, Authority>,
        epoch: Epoch,
        bootstrap_servers: BTreeMap<BlsPublicKey, BootstrapServer>,
    ) -> Self {
        let mut committee = CommitteeInner {
            authorities,
            epoch,
            authorities_by_id: Default::default(),
            validity_threshold: 0,
            quorum_threshold: 0,
            bootstrap_servers,
        };

        committee.authorities_by_id = committee
            .authorities
            .values()
            .map(|authority| (authority.id(), authority.clone()))
            .collect();
        committee.validity_threshold = committee.calculate_validity_threshold().get();
        committee.quorum_threshold = committee.calculate_quorum_threshold().get();
        #[cfg(not(feature = "dev-single-node-setup"))]
        assert!(committee.authorities_by_id.len() > 1, "committee size must be larger than 1");
        // Dev builds relax the floor to allow a single-validator committee. The
        // single-node-only invariant is enforced at node startup (see node.rs), not
        // here - this constructor is shared by the multi-validator consensus test suite.
        #[cfg(feature = "dev-single-node-setup")]
        assert!(!committee.authorities_by_id.is_empty(), "committee size must be at least 1");
        // Some sanity checks to ensure that we'll not end up in invalid state
        assert_eq!(committee.authorities_by_id.len(), committee.authorities.len());

        Self { inner: Arc::new(RwLock::new(committee)) }
    }

    /// Updates the committee internal secondary indexes.
    pub fn load(&self) {
        self.inner.write().load()
    }

    /// Returns the current epoch.
    pub fn epoch(&self) -> Epoch {
        self.inner.read().epoch
    }

    /// Returns the authority with the given identifier.
    pub fn authority(&self, identifier: &AuthorityIdentifier) -> Option<Authority> {
        self.inner.read().authorities_by_id.get(identifier).cloned()
    }

    /// Returns the authority with the given BLS key.
    pub fn authority_by_key(&self, key: &BlsPublicKey) -> Option<Authority> {
        self.inner.read().authorities.get(key).cloned()
    }

    /// Returns all authorities, sorted by identifier.
    ///
    /// Callers rely on the order (leader schedules index into it), so it comes from the id-keyed
    /// index, never from the key-keyed map.
    pub fn authorities(&self) -> Vec<Authority> {
        self.inner.read().authorities_by_id.values().cloned().collect()
    }

    /// Returns true if the authority for id is in the committee.
    pub fn is_authority(&self, id: &AuthorityIdentifier) -> bool {
        self.inner.read().authorities_by_id.contains_key(id)
    }

    /// Returns the number of authorities.
    pub fn size(&self) -> usize {
        self.inner.read().authorities.len()
    }

    /// Returns the voting power of the authority with the given key, or 0 if absent.
    pub fn voting_power(&self, name: &BlsPublicKey) -> VotingPower {
        self.inner.read().authorities.get(&name.clone()).map_or_else(|| 0, |x| x.inner.voting_power)
    }

    /// Returns the voting power of the authority with the given id, or 0 if absent.
    pub fn voting_power_by_id(&self, id: &AuthorityIdentifier) -> VotingPower {
        self.inner
            .read()
            .authorities_by_id
            .get(id)
            .map_or_else(|| 0, |authority| authority.inner.voting_power)
    }

    /// Returns the stake required to reach a quorum (2f+1).
    pub fn quorum_threshold(&self) -> VotingPower {
        self.inner.read().quorum_threshold
    }

    /// Returns the stake required to reach availability (f+1).
    pub fn validity_threshold(&self) -> VotingPower {
        self.inner.read().validity_threshold
    }

    /// Returns true if the provided stake has reached quorum (2f+1).
    pub fn reached_quorum(&self, voting_power: VotingPower) -> bool {
        voting_power >= self.quorum_threshold()
    }

    /// Returns true if the provided stake has reached availability (f+1).
    pub fn reached_validity(&self, voting_power: VotingPower) -> bool {
        voting_power >= self.validity_threshold()
    }

    /// Returns the summed voting power of the committee.
    pub fn total_voting_power(&self) -> VotingPower {
        self.inner.read().total_voting_power()
    }

    /// Returns the (identifier, BLS key) of every primary except `myself`.
    pub fn others_primaries_by_id(
        &self,
        myself: Option<&AuthorityIdentifier>,
    ) -> Vec<(AuthorityIdentifier, BlsPublicKey)> {
        self.inner
            .read()
            .authorities
            .iter()
            .filter(
                |(_, authority)| {
                    if let Some(myself) = myself {
                        &authority.id() != myself
                    } else {
                        true
                    }
                },
            )
            .map(|(_, authority)| (authority.id(), *authority.protocol_key()))
            .collect()
    }

    /// Returns the bls keys of all members except `myself`.
    pub fn others_keys_except(&self, myself: &BlsPublicKey) -> Vec<BlsPublicKey> {
        self.inner
            .read()
            .authorities
            .iter()
            .filter_map(|(_, authority)| {
                if authority.protocol_key() == myself {
                    None
                } else {
                    Some(*authority.protocol_key())
                }
            })
            .collect()
    }

    /// Returns all the bls keys of all members.
    pub fn bls_keys(&self) -> Vec<BlsPublicKey> {
        self.inner.read().authorities.values().map(|authority| *authority.protocol_key()).collect()
    }

    /// Returns the bootstrap record for key if it exists.
    pub fn get_bootstrap(&self, key: &BlsPublicKey) -> Option<BootstrapServer> {
        self.inner.read().bootstrap_servers.get(key).cloned()
    }

    /// Returns the map of bootstrap servers.
    pub fn bootstrap_servers(&self) -> BTreeMap<BlsPublicKey, BootstrapServer> {
        self.inner.read().bootstrap_servers.clone()
    }

    /// Builds a copy of this committee at a new epoch. Test-only.
    pub fn advance_epoch_for_test(&self, new_epoch: Epoch) -> Committee {
        Committee::new_for_test(
            self.inner.read().authorities.clone(),
            new_epoch,
            self.inner.read().bootstrap_servers.clone(),
        )
    }

    /// Returns the number of workers per authority.
    ///
    /// A protocol-level value: every node must agree on it and run that many workers, so a change
    /// can only land at an epoch boundary.
    pub fn number_of_workers(&self) -> usize {
        1
    }
}

impl std::fmt::Display for Committee {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Committee E{}: {:?}",
            self.epoch(),
            self.inner
                .read()
                .authorities
                .keys()
                .map(|x| {
                    if let Some(k) = x.encode_base58().get(0..16) {
                        k.to_owned()
                    } else {
                        format!("Invalid key: {x}")
                    }
                })
                .collect::<Vec<_>>()
        )
    }
}

/// Type for building committees.
#[derive(Debug)]
pub struct CommitteeBuilder {
    /// The epoch for the committee.
    epoch: Epoch,
    /// The map of [BlsPublicKey] for each [Authority] in the committee.
    authorities: BTreeMap<BlsPublicKey, Authority>,
    /// The map of [BlsPublicKey] for each [BootstrapServer].
    bootstrap_server: BTreeMap<BlsPublicKey, BootstrapServer>,
}

impl CommitteeBuilder {
    /// Creates a builder for a [Committee] at the given epoch.
    pub fn new(epoch: Epoch) -> Self {
        Self { epoch, authorities: BTreeMap::default(), bootstrap_server: BTreeMap::default() }
    }

    /// Adds an authority and its bootstrap server.
    pub fn add_authority_and_bootstrap(
        &mut self,
        protocol_key: BlsPublicKey,
        stake: VotingPower,
        primary_node: P2pNode,
        worker_node: P2pNode,
        execution_address: Address,
    ) {
        let authority = Authority::new(protocol_key, stake, execution_address);
        self.authorities.insert(protocol_key, authority);
        let bootstrap = BootstrapServer::new(primary_node, worker_node);
        self.bootstrap_server.insert(protocol_key, bootstrap);
    }

    /// Adds an authority.
    pub fn add_authority(
        &mut self,
        protocol_key: BlsPublicKey,
        stake: VotingPower,
        execution_address: Address,
    ) {
        let authority = Authority::new(protocol_key, stake, execution_address);
        self.authorities.insert(protocol_key, authority);
    }

    /// Adds a bootstrap server.
    pub fn add_bootstrap_server(
        &mut self,
        protocol_key: BlsPublicKey,
        primary_node: P2pNode,
        worker_node: P2pNode,
    ) {
        let bootstrap = BootstrapServer::new(primary_node, worker_node);
        self.bootstrap_server.insert(protocol_key, bootstrap);
    }

    /// Builds the committee, computing its thresholds and indexes.
    pub fn build(self) -> Committee {
        Committee::new(self.authorities, self.epoch, self.bootstrap_server)
    }
}

/// Fallback committee keys for upcoming epochs, from on-chain state at boot.
#[derive(Debug, Clone, Default)]
pub struct CommitteeLookahead(BTreeMap<Epoch, Vec<BlsPublicKey>>);

impl CommitteeLookahead {
    /// Builds the lookahead, dropping epochs with no keys.
    pub fn from_entries(entries: impl IntoIterator<Item = (Epoch, Vec<BlsPublicKey>)>) -> Self {
        Self(entries.into_iter().filter(|(_, keys)| !keys.is_empty()).collect())
    }

    /// Returns the committee keys for `epoch`, if known.
    pub fn get(&self, epoch: Epoch) -> Option<&[BlsPublicKey]> {
        self.0.get(&epoch).map(|k| k.as_slice())
    }
}

/// Returns the quorum threshold (2f+1) for a committee whose members all have voting power 1.
pub fn quorum_threshold(committee_members: u64) -> u64 {
    ((2 * committee_members) / 3) + 1
}

#[cfg(test)]
mod tests {
    use crate::{
        Address, Authority, BlsKeypair, BlsPublicKey, BootstrapServer, Committee, Multiaddr,
        NetworkKeypair,
    };
    use rand::{rng, Rng};
    use std::collections::BTreeMap;

    #[test]
    fn committee_load() {
        // GIVEN
        let mut rng = rng();
        let num_of_authorities = 10;

        let authorities = (0..num_of_authorities)
            .map(|_| {
                let keypair = BlsKeypair::generate(&mut rng);
                let execution_address = Address::new(rng.random());

                let a = Authority::new(*keypair.public(), 1, execution_address);

                (*keypair.public(), a)
            })
            .collect::<BTreeMap<BlsPublicKey, Authority>>();

        let bootstrap_servers = authorities
            .keys()
            .map(|key| {
                let primary_keypair = NetworkKeypair::generate_ed25519();
                let worker_keypair = NetworkKeypair::generate_ed25519();

                let b = BootstrapServer::new(
                    (Multiaddr::empty(), primary_keypair.public().clone().into()).into(),
                    (Multiaddr::empty(), worker_keypair.public().clone().into()).into(),
                );

                (*key, b)
            })
            .collect::<BTreeMap<BlsPublicKey, BootstrapServer>>();

        // WHEN
        let committee = Committee::new(authorities, 10, bootstrap_servers);

        // THEN
        assert_eq!(committee.inner.read().authorities_by_id.len() as u64, num_of_authorities);
        assert_eq!(committee.inner.read().authorities.len() as u64, num_of_authorities);

        for (identifier, authority) in committee.inner.read().authorities_by_id.iter() {
            assert_eq!(*identifier, authority.id());
        }

        // AND ensure thresholds are calculated correctly
        assert_eq!(committee.quorum_threshold(), 7);
        assert_eq!(committee.validity_threshold(), 4);

        let guard = committee.inner.read();
        // AND ensure authorities are in both maps
        let mut total = 0;
        for ((public_key, authority_1), (boot_key, _)) in
            guard.authorities.iter().zip(guard.bootstrap_servers.iter())
        {
            assert_eq!(public_key, authority_1.protocol_key());
            assert_eq!(public_key, boot_key);
            let authority_2 = guard.authorities_by_id.get(&authority_1.id()).unwrap();
            assert_eq!(authority_1, authority_2);
            total += 1;
        }
        assert_eq!(total, num_of_authorities);
    }

    #[cfg(feature = "dev-single-node-setup")]
    #[test]
    fn committee_allows_single_authority() {
        // Thresholds collapse to 1 for a single-validator committee.
        let mut rng = rng();
        let keypair = BlsKeypair::generate(&mut rng);
        let execution_address = Address::new(rng.random());
        let authority = Authority::new(*keypair.public(), 1, execution_address);
        let authorities = BTreeMap::from([(*keypair.public(), authority)]);

        let primary_keypair = NetworkKeypair::generate_ed25519();
        let worker_keypair = NetworkKeypair::generate_ed25519();
        let bootstrap_servers = BTreeMap::from([(
            *keypair.public(),
            BootstrapServer::new(
                (Multiaddr::empty(), primary_keypair.public().clone().into()).into(),
                (Multiaddr::empty(), worker_keypair.public().clone().into()).into(),
            ),
        )]);

        let committee = Committee::new(authorities, 1, bootstrap_servers);

        assert_eq!(committee.size(), 1);
        assert_eq!(committee.quorum_threshold(), 1);
        assert_eq!(committee.validity_threshold(), 1);
    }
}
