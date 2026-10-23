use crate::{
    codec::RLMessage,
    consensus::types::{RecordInvalidReason, SECONDS_IN_FUTURE_RECORD_ALLOWANCE},
    peers::Penalty,
    types::{NetworkEvent, NetworkInfo, NetworkResult, NodeRecord},
    ConsensusNetwork,
};
use libp2p::{
    kad::{
        self,
        store::{Error, RecordStore},
        ProviderRecord, QueryId, RecordKey,
    },
    PeerId,
};
use rayls_infrastructure_types::{encode, now, try_decode, BlsPublicKey, Database, RaylsSender};
use tracing::{debug, error, info, trace, warn};

/// Fixed-window length for the per-peer inbound PutRecord budget.
const KAD_PUT_BUDGET_WINDOW_SECS: u64 = 60;
/// Maximum inbound PutRecord requests accepted per peer per window.
const KAD_PUT_BUDGET_MAX: u32 = 10;

/// Returns the per-peer PutRecord budget so a test can exhaust it.
#[cfg(test)]
pub(crate) fn kad_put_budget_max() -> u32 {
    KAD_PUT_BUDGET_MAX
}

impl<Req, Res, DB, Events> ConsensusNetwork<Req, Res, DB, Events>
where
    Req: RLMessage,
    Res: RLMessage,
    DB: Database,
    Events: RaylsSender<NetworkEvent<Req, Res>> + Send + 'static,
{
    /// Verify the record was signed by the key and the publisher matches the network key in the
    /// record.
    fn peer_record_valid_decoded(
        &self,
        key: &BlsPublicKey,
        publisher: Option<PeerId>,
        node_record: &NodeRecord,
    ) -> Result<(), String> {
        // verify bls signature
        if node_record.verify(key).is_err() {
            return Err(format!(
                "NodeRecord validation failed: invalid signature for record with key {:?}",
                key
            ));
        }

        // verify publisher matches the network public key in the record
        // this prevents replay attacks where malicious nodes republish outdated records
        let expected_peer_id: PeerId = node_record.info.pubkey.clone().into();
        if publisher.is_none_or(|p| p != expected_peer_id) {
            return Err(format!("NodeRecord validation failed: publisher {:?} doesn't match network key (expected {:?})", publisher, expected_peer_id));
        }

        Ok(())
    }

    /// Verify the record was signed by the key and the publisher matches the network key in the
    /// record.
    pub(super) fn peer_record_valid(
        &self,
        record: &kad::Record,
    ) -> Option<(BlsPublicKey, NodeRecord)> {
        let key = BlsPublicKey::from_literal_bytes(record.key.as_ref()).ok()?;
        let node_record = try_decode::<NodeRecord>(record.value.as_ref()).ok()?;

        // verify bls signature
        if let Err(e) = self.peer_record_valid_decoded(&key, record.publisher, &node_record) {
            warn!(target: "network-kad", ?e, "peer record failed validation");
            return None;
        }

        Some((key, node_record))
    }

    /// Process event from kademlia behavior.
    pub(super) fn process_kad_event(&mut self, event: kad::Event) -> NetworkResult<()> {
        match event {
            kad::Event::InboundRequest { request } => {
                trace!(target: "network-kad", "inbound {request:?}");
                match request {
                    kad::InboundRequest::FindNode { num_closer_peers: _ } => {}
                    kad::InboundRequest::GetProvider {
                        num_closer_peers: _,
                        num_provider_peers: _,
                    } => {}
                    kad::InboundRequest::AddProvider { record } => {
                        self.process_add_provider_request(record)
                    }
                    kad::InboundRequest::GetRecord { num_closer_peers: _, present_locally: _ } => {}
                    kad::InboundRequest::PutRecord { source, connection: _, record } => {
                        if let Some(record) = record {
                            self.process_kad_put_request(source, record);
                        }
                    }
                }
            }
            kad::Event::OutboundQueryProgressed { id: query_id, result, stats: _, step } => {
                match result {
                    kad::QueryResult::GetProviders(Ok(kad::GetProvidersOk::FoundProviders {
                        key,
                        providers,
                        ..
                    })) => {
                        debug!(
                            target: "network-kad",
                            key = ?BlsPublicKey::from_literal_bytes(key.as_ref()),
                            ?providers,
                            "kad::GetProviders::Ok"
                        );
                    }
                    kad::QueryResult::GetProviders(Err(err)) => {
                        error!(target: "network-kad", "Failed to get providers: {err:?}");
                    }
                    kad::QueryResult::GetRecord(Ok(kad::GetRecordOk::FoundRecord(
                        kad::PeerRecord { record, peer },
                    ))) => {
                        if let Some((key, value)) = self.peer_record_valid(&record) {
                            trace!(target: "network-kad", "Got record {key} {value:?}");
                            self.process_kad_query_result(&query_id, record, peer, step.last);
                        } else {
                            trace!(target: "network-kad", "Received invalid peer record!");

                            // assess penalty against the record's author, not the responder that
                            // relayed it (see `kad_record_fault_penalty`)
                            if let Some(peer_id) = peer {
                                self.apply_invalid_kad_request_penalty(
                                    &peer_id,
                                    record.publisher,
                                    &RecordInvalidReason::InvalidPeerRecord(
                                        "get record failed validation".to_string(),
                                    ),
                                );
                            }

                            // ensure query cleaned up
                            if step.last {
                                self.close_kad_query(&query_id);
                            }
                        }
                    }
                    kad::QueryResult::GetRecord(Ok(
                        kad::GetRecordOk::FinishedWithNoAdditionalRecord { cache_candidates },
                    )) => {
                        // TODO: configure caching and see issue #301
                        // self.swarm.behaviour_mut().kademlia.put_record_to(record, peers, quorum);

                        debug!(target: "network-kad", ?cache_candidates, "FinishedWithNoAdditionalRecord - failed to find record");
                        self.close_kad_query(&query_id);
                    }
                    kad::QueryResult::GetRecord(Err(err)) => {
                        debug!(
                            target: "network-kad",
                            key = ?BlsPublicKey::from_literal_bytes(err.key().as_ref()),
                            ?err,
                            "kad::GetRecord::Err"
                        );
                        self.close_kad_query(&query_id);
                    }
                    kad::QueryResult::PutRecord(Ok(kad::PutRecordOk { key })) => {
                        debug!(
                            target: "network-kad",
                            key = ?BlsPublicKey::from_literal_bytes(key.as_ref()),
                            "kad::PutRecordOk"
                        );
                    }
                    kad::QueryResult::PutRecord(Err(err)) => {
                        if !self.kad_expecting_to_fail_query_ids.contains(&query_id) {
                            error!(target: "network-kad", "Failed to put record: {err:?}, {query_id:?}");
                        }
                    }
                    kad::QueryResult::StartProviding(Ok(kad::AddProviderOk { key })) => {
                        debug!(
                            target: "network-kad",
                            key = ?BlsPublicKey::from_literal_bytes(key.as_ref()),
                            "kad::StartProviding::Ok"
                        );
                    }
                    kad::QueryResult::StartProviding(Err(err)) => {
                        warn!(
                            target: "network-kad",
                            key = ?BlsPublicKey::from_literal_bytes(err.key().as_ref()),
                            ?err,
                            "kad::StartProviding::Err"
                        );
                    }
                    kad::QueryResult::GetClosestPeers(Ok(result)) => {
                        // process peers for potential discovery attempts
                        debug!(target: "network-kad", ?result, "GetClosestPeers for discovery");
                        self.swarm
                            .behaviour_mut()
                            .peer_manager
                            .process_peers_for_discovery(result.peers);
                    }
                    _ => {}
                }
                self.kad_expecting_to_fail_query_ids.remove(&query_id);
            }
            kad::Event::RoutingUpdated { peer, is_new_peer, addresses, bucket_range, old_peer } => {
                debug!(target: "network-kad", "routing updated peer {peer:?} new {is_new_peer} addrs {addresses:?} bucketr {bucket_range:?} old {old_peer:?}");

                // update newly added peer
                if is_new_peer {
                    self.swarm.behaviour_mut().peer_manager.update_routing_for_peer(&peer, true);

                    // update old peer if evicted from routing table
                    if let Some(old) = old_peer {
                        self.swarm
                            .behaviour_mut()
                            .peer_manager
                            .update_routing_for_peer(&old, false);
                    }
                }
            }
            kad::Event::UnroutablePeer { peer } => {
                // unknown peer queried a record - noop
                trace!(target: "network-kad", "unroutable peer {peer:?}")
            }
            kad::Event::RoutablePeer { peer, address } => {
                // kad discovered a new peer - peer is added to table on `PeerEvent::Connected`
                trace!(target: "network-kad", "routable peer {peer:?}/{address:?}");
            }
            kad::Event::PendingRoutablePeer { peer, address } => {
                trace!(target: "network-kad", "pending routable peer {peer:?}/{address:?}")
            }
            kad::Event::ModeChanged { new_mode } => {
                trace!(target: "network-kad", "mode changed {new_mode:?}")
            }
        }
        Ok(())
    }

    /// Process an inbound kad put request.
    pub(super) fn process_kad_put_request(&mut self, source: PeerId, record: kad::Record) {
        trace!(
            target: "network-kad",
            ?source,
            publisher = ?record.publisher,
            value_len = record.value.len(),
            "received inbound kad PutRecord",
        );
        let Ok(decoded_record) = try_decode::<NodeRecord>(&record.value) else {
            error!(target: "network-kad", ?source, "failed to decode record value - rejecting put request");
            self.apply_invalid_kad_request_penalty(
                &source,
                record.publisher,
                &RecordInvalidReason::InvalidPeerRecord("decoding failed".to_string()),
            );
            return;
        };

        let Ok(key) = BlsPublicKey::from_literal_bytes(record.key.as_ref()) else {
            error!(target: "network-kad", ?source, "invalid record key format - rejecting put request");
            self.apply_invalid_kad_request_penalty(
                &source,
                record.publisher,
                &RecordInvalidReason::InvalidKeyFormat,
            );
            return;
        };
        // The key must be the canonical encoding of the BLS key: blst also parses the
        // uncompressed form, and the store slot is the raw key bytes, so a non-canonical key
        // would give a registered validator a second slot that no lookup ever reads. Dropped
        // without penalty: such records were accepted before this rule and are replicated by
        // honest peers until they expire, so charging the deliverer would ban honest relayers.
        if key.as_ref() != record.key.as_ref() {
            trace!(target: "network-kad", ?source, %key, "non-canonical record key - dropping put request");
            return;
        }

        // Budget gate before any per-record work. Not a penalty: an honest node can burst
        // republishes across an epoch change, so over-budget puts are dropped, never charged.
        if !self.within_kad_put_budget(&source) {
            trace!(target: "network-kad", ?source, "kad put budget exceeded - dropping put request");
            return;
        }

        // Nothing is removed from the store here: `publisher` is a wire field that has not been
        // authenticated at this point, so acting on it would let any peer empty a registered
        // validator's slot by naming a banned peer id, then fill it with a replayed older record.
        if let Err(reason) = self.check_kad_put_admissibility(&source, &record, &decoded_record) {
            self.apply_invalid_kad_request_penalty(&source, record.publisher, &reason);
            error!(target: "network-kad", ?source, ?reason, "failed to process kad put request");
            return;
        }

        // Dedup before the signature verify: a replayed or stale record costs a store lookup
        // instead of a BLS verification on the swarm event loop (a validly signed replay earns
        // no penalty, so verifying first is a free CPU-amplification vector).
        //
        // A duplicate already lives in our store from a prior put, so refreshing the in-memory
        // BLS mapping (wiped on restart) is safe, but only from the stored record's info, which
        // passed the signature check when it was admitted; the incoming duplicate is unverified.
        // A record that is never stored (unregistered key) always reaches the verify; the
        // per-source put budget above is what bounds that cost.
        //
        // The refresh is limited to what a fresh put of the same record could do: a stored copy
        // of a key that is no longer registered (not yet expired) must not hand out third-party
        // dial hints the admission rule below would refuse.
        let about: PeerId = decoded_record.info.pubkey.clone().into();
        let owner_delivered = source == about;
        let registered = self.swarm.behaviour().peer_manager.is_registered(&key);
        if !self.is_newer_record(&record.key, &key, &decoded_record) {
            trace!(target: "network-kad", ?source, "duplicate record, refreshing known_peers");
            let stored_info = self
                .swarm
                .behaviour_mut()
                .kademlia
                .store_mut()
                .get(&record.key)
                .and_then(|existing| try_decode::<NodeRecord>(&existing.value).ok())
                .map(|stored| stored.info);
            if let Some(info) = stored_info {
                // Ownership is judged on the stored (verified) info, not on the unverified
                // incoming value, which could name the sender to pass this gate.
                let stored_about: PeerId = info.pubkey.clone().into();
                if registered || source == stored_about {
                    self.admit_record_info(source, key, info);
                }
            }
            return;
        }

        // An unregistered key delivered by a third party stays unknown: its addresses are dial
        // hints for a key nobody will dial, and admitting them would let a closed store be
        // bypassed to populate `known_peers`. It is dropped whatever the signature says, so
        // there is nothing to verify: the pairing would only buy a penalty on a bad signature,
        // and honest peers replicate such records (from before this rule) hourly.
        if !(registered || owner_delivered) {
            trace!(target: "network-kad", %key, ?source, "unregistered key from a third party: dropped");
            return;
        }

        // Signature verify only for fresh records that passed every cheap gate.
        if let Err(e) = self.peer_record_valid_decoded(&key, record.publisher, &decoded_record) {
            let reason = RecordInvalidReason::InvalidPeerRecord(e);
            self.apply_invalid_kad_request_penalty(&source, record.publisher, &reason);
            error!(target: "network-kad", ?source, ?reason, "failed to process kad put request");
            return;
        }

        // Fresh, verified record. Only registered validators are stored and replicated: theirs
        // are the only keys anyone looks up (`find_authorities`), and registering needs the
        // allowlist and the minimum stake, so the store cannot be filled by minting keys. Every
        // other peer is identified by its connection alone (below).
        if registered {
            let publisher = record.publisher;
            let value_len = record.value.len();
            match self.swarm.behaviour_mut().kademlia.store_mut().put(record) {
                Ok(()) => trace!(target: "network-kad", %key, "stored fresh kad record"),
                Err(err) => {
                    // A store that is full or failing is this node's problem and the record
                    // still binds below. An oversize value is the record's fault: it was just
                    // penalised, and binding it would unban a committee peer the line before.
                    if !self.handle_kad_store_error(&source, publisher, value_len, err) {
                        return;
                    }
                }
            }
        } else {
            trace!(target: "network-kad", %key, ?source, "unregistered key: record not stored");
        }

        // The record is admitted whether or not it was stored. Owner-delivered, it binds: the
        // handshake authenticated `source` as the peer this record is about, which is all the
        // binding needs, so an observer is reachable while connected, holds one `known_peers`
        // entry and no store slot, and a validator stays bound even on the day the store refuses
        // it. From a third party (registered keys only, see above) it yields dial hints: the key
        // is one this node dials, and a successful store would have admitted the same hints.
        self.admit_record_info(source, key, decoded_record.info);
    }

    /// Hand a validated record's info to the peer manager. The peer id is bound to `key` only
    /// when `source`, the peer the put arrived from, *is* the peer the record is about: the
    /// connection was authenticated as that peer id by the handshake, which is the one proof of
    /// network-key ownership a record cannot forge. From anyone else -- a replicating node, or a
    /// peer naming a network key it does not hold -- the record is dial hints only; the binding
    /// is made when the named peer connects and pushes its own record.
    fn admit_record_info(&mut self, source: PeerId, key: BlsPublicKey, info: NetworkInfo) {
        let about: PeerId = info.pubkey.clone().into();
        let pm = &mut self.swarm.behaviour_mut().peer_manager;
        if source == about {
            pm.add_known_peer(key, info);
        } else {
            pm.add_known_peer_addrs(key, info);
        }
    }

    /// Process on inbound add provider request.
    ///
    /// Discovery here runs on `get_record` against the BLS key; `get_providers` is never called, so
    /// a stored provider record is never read. Persisting one only grows a never-queried table that
    /// takes unsigned writes from any non-banned peer, so foreign provider records are dropped.
    /// Sending an ADD_PROVIDER is ordinary libp2p traffic, not misbehavior, so the sender is not
    /// penalized.
    fn process_add_provider_request(&mut self, record: Option<ProviderRecord>) {
        if let Some(record) = record {
            trace!(
                target: "network-kad",
                provider = ?record.provider,
                "dropping unused inbound provider record"
            );
        }
    }

    /// Reject a put from or about a banned peer, without a publisher, or dated too far in the
    /// future, all of which are decided without touching the signature.
    fn check_kad_put_admissibility(
        &mut self,
        source: &PeerId,
        record: &kad::Record,
        decoded_record: &NodeRecord,
    ) -> Result<(), RecordInvalidReason> {
        let publisher = record.publisher.ok_or(RecordInvalidReason::MissingPublisher)?;

        let pm = &self.swarm.behaviour().peer_manager;
        if pm.peer_banned(&publisher) {
            return Err(RecordInvalidReason::PublisherBanned);
        }
        if pm.peer_banned(source) {
            return Err(RecordInvalidReason::SourceBanned);
        }

        if decoded_record.info.timestamp > now() + SECONDS_IN_FUTURE_RECORD_ALLOWANCE {
            return Err(RecordInvalidReason::TimestampTooFarInFuture);
        }

        Ok(())
    }

    /// Count an inbound PutRecord against `source`'s fixed window; `false` when over budget.
    fn within_kad_put_budget(&mut self, source: &PeerId) -> bool {
        let now = now();
        // Drop stale windows so the map tracks recent senders, not every peer ever seen.
        if self.kad_put_budget.len() > 1024 {
            self.kad_put_budget
                .retain(|_, (start, _)| now.saturating_sub(*start) <= KAD_PUT_BUDGET_WINDOW_SECS);
        }
        let (window_start, count) = self.kad_put_budget.entry(*source).or_insert((now, 0));
        if now.saturating_sub(*window_start) > KAD_PUT_BUDGET_WINDOW_SECS {
            *window_start = now;
            *count = 0;
        }
        *count += 1;
        *count <= KAD_PUT_BUDGET_MAX
    }

    /// Handle a store error from `put()`, charging the peer at fault, if any.
    ///
    /// Runs after the signature verify, so `publisher` is authenticated: an oversize
    /// value charges only its author, never an honest relayer, matching
    /// [`kad_record_fault_penalty`]. A full store is a local capacity condition and charges
    /// nobody; it is logged instead, since under the registered-only rule it should never
    /// happen (pre-upgrade records that have not expired yet, or a cap below the registry size).
    /// `KadStore::put` also reports a failed DB read or write as `ValueTooLarge`; that is told
    /// apart by `value_len`, logged, and charges nobody either.
    ///
    /// Returns whether the record may still be used for the peer binding: true for a full or
    /// failing store, false when the record itself was at fault.
    fn handle_kad_store_error(
        &mut self,
        deliverer: &PeerId,
        publisher: Option<PeerId>,
        value_len: usize,
        err: Error,
    ) -> bool {
        let penalty = match err {
            // A full store is this node's capacity, never the deliverer's fault: the record
            // that arrives after a flood is the honest one.
            Error::MaxRecords => {
                warn!(target: "network-kad", ?deliverer, "kad store full; registered record not stored");
                return true;
            }
            Error::ValueTooLarge if value_len < crate::kad::max_value_bytes() => {
                error!(target: "network-kad", ?deliverer, value_len, "kad store write failed; registered record not stored");
                return true;
            }
            Error::ValueTooLarge => (publisher == Some(*deliverer)).then_some(Penalty::Fatal),
            // `process_add_provider_request` drops provider records, so `add_provider` never runs.
            Error::MaxProvidedKeys => {
                unreachable!("provider records are dropped, so the store holds no provider keys")
            }
        };
        if let Some(penalty) = penalty {
            warn!(target: "network-kad", ?deliverer, ?err, ?penalty, "kad store rejected record");
            self.swarm.behaviour_mut().peer_manager.process_penalty(*deliverer, penalty);
        }
        false
    }

    /// Apply the penalty owed for an invalid KAD record delivered by `deliverer`.
    ///
    /// The record's fault may belong to its `publisher` rather than the deliverer; the decision is
    /// made by [`kad_record_fault_penalty`], and no penalty is applied when the deliverer is only
    /// an honest relayer of someone else's bad record.
    fn apply_invalid_kad_request_penalty(
        &mut self,
        deliverer: &PeerId,
        publisher: Option<PeerId>,
        reason: &RecordInvalidReason,
    ) {
        match kad_record_fault_penalty(deliverer, publisher, reason) {
            Some(penalty) => {
                trace!(target: "network-kad", ?deliverer, ?reason, ?penalty, "penalizing invalid kad record");
                self.swarm.behaviour_mut().peer_manager.process_penalty(*deliverer, penalty);
            }
            None => {
                trace!(target: "network-kad", ?deliverer, ?publisher, ?reason, "no penalty: kad record fault belongs to its author, not the deliverer");
            }
        }
    }

    /// Check the local kad store to compare record timestamps.
    ///
    /// This method compares timestamps for verified records to ensure the latest record
    /// is stored (prevents replay attacks). Also returns `true` if the record is not found.
    /// It is the caller's responsibility to ensure records are verified and valid.
    ///
    /// A record can be bound without being stored, so the newest info for a key may live only
    /// in `known_peers`: a record older than that binding is a replay and is refused too. One
    /// equal to it is the same record pushed again (a candidate re-publishing after it became
    /// registered) and may be stored. Configured entries carry timestamp 0 and never block.
    fn is_newer_record(
        &mut self,
        key: &RecordKey,
        bls_key: &BlsPublicKey,
        node_record: &NodeRecord,
    ) -> bool {
        let incoming = node_record.info.timestamp;
        if self
            .swarm
            .behaviour()
            .peer_manager
            .known_peer_timestamp(bls_key)
            .is_some_and(|bound| bound > incoming)
        {
            return false;
        }

        let store = self.swarm.behaviour_mut().kademlia.store_mut();
        if let Some(existing) = store.get(key) {
            match try_decode::<NodeRecord>(&existing.value) {
                Ok(existing_record) => {
                    // return true if the new record is newer
                    existing_record.info.timestamp < incoming
                }
                _ => false,
            }
        } else {
            // return true if record is not in local store
            true
        }
    }

    /// Logic to process a kad record request.
    ///
    /// This method checks:
    /// - the peer record is signed
    /// - the returned key matches the request
    /// - the latest node record is used
    fn process_kad_query_result(
        &mut self,
        query_id: &QueryId,
        record: kad::Record,
        peer: Option<PeerId>,
        is_last_step: bool,
    ) {
        // ensure returned record is valid, otherwise assess penalty
        if let Some((key, new_record)) = self.peer_record_valid(&record) {
            trace!(target: "network-kad", "Got record {key} {new_record:?}");
            // return if query id unknown - should not happen
            let Some(query) = self.kad_record_queries.get_mut(query_id) else { return };

            // ensure returned value matches request
            if query.request == key {
                match &mut query.result {
                    None => query.result = Some(new_record),
                    Some(tracked) if tracked.info.timestamp < new_record.info.timestamp => {
                        *tracked = new_record
                    }
                    Some(_) => {} // keep existing record
                }
            } else {
                // assess penalty for returning record that doesn't match key
                if let Some(peer_id) = peer {
                    trace!(target: "network-kad", ?peer_id, "processing fatal penalty for query record key mismatch");
                    self.swarm
                        .behaviour_mut()
                        .peer_manager
                        .process_penalty(peer_id, Penalty::Fatal);
                }
            }
        } else {
            // record signature invalid
            warn!(target: "network-kad", "Received invalid peer record!");

            // assess penalty against the record's author, not the responder that relayed it
            if let Some(peer_id) = peer {
                self.apply_invalid_kad_request_penalty(
                    &peer_id,
                    record.publisher,
                    &RecordInvalidReason::InvalidPeerRecord(
                        "query record failed validation".to_string(),
                    ),
                );
            }
        }

        // handle last step
        if is_last_step {
            self.close_kad_query(query_id);
        }
    }

    /// Cleanup kad record queries (called on last step).
    ///
    /// A query answer comes from whichever node stored the record, not from the peer it is
    /// about, so it yields dial addresses and no peer-id binding (see `admit_record_info`).
    fn close_kad_query(&mut self, query_id: &QueryId) {
        if let Some(query) = self.kad_record_queries.remove(query_id) {
            if let Some(node_record) = query.result {
                self.swarm
                    .behaviour_mut()
                    .peer_manager
                    .add_known_peer_addrs(query.request, node_record.info);
            }
        }
    }

    /// Load known peers from the persistent KAD store into the in-memory cache at startup.
    ///
    /// `KadStore` is persisted across restarts while the in-memory peer tables are wiped on every
    /// boot. The preload restores the dial addresses of every authority this node knew, so the
    /// committee can be redialed at once. It does not restore peer-id bindings: a stored record
    /// carries no proof of who delivered it, so the binding is re-made when each peer connects and
    /// pushes its record, which the dedup path above accepts even when the record is unchanged.
    pub(super) fn load_known_peers_from_kad_store(&mut self) {
        let local_peer_id = *self.swarm.local_peer_id();
        let records: Vec<kad::Record> = self
            .swarm
            .behaviour_mut()
            .kademlia
            .store_mut()
            .records()
            .map(|r| r.into_owned())
            .collect();
        let total = records.len();
        let mut loaded = 0_usize;

        for record in &records {
            let Some((bls_key, node_record)) = self.peer_record_valid(record) else {
                continue;
            };
            let peer_id = PeerId::from_public_key(&node_record.info.pubkey);
            if peer_id == local_peer_id {
                continue;
            }
            if self.swarm.behaviour().peer_manager.peer_banned(&peer_id) {
                continue;
            }
            self.swarm.behaviour_mut().peer_manager.add_known_peer_addrs(bls_key, node_record.info);
            loaded += 1;
        }
        info!(
            target: "network-kad",
            total,
            loaded,
            "loaded known peers from persistent kad store",
        );
    }

    /// Return a kademlia record keyed on our BlsPublicKey with our peer_id and network addresses.
    /// Return None if we don't have any confirmed external addresses yet.
    pub(super) fn get_peer_record(&self) -> kad::Record {
        let key = kad::RecordKey::new(&self.key_config.primary_public_key());
        kad::Record {
            key,
            value: encode(&self.node_record),
            publisher: Some(*self.swarm.local_peer_id()),
            expires: None, // never expire
        }
    }

    /// Publish and provide our network addresses and peer id under our BLS public key for
    /// discovery.
    ///
    /// A node always publishes, even when its advertised address is identity-only (a bare
    /// `/p2p/<peer-id>`, e.g. an outbound-only observer): the record is the peer's identity, and
    /// others rely on it to map our `peer_id -> bls` (`peer_to_bls`) so they accept our
    /// request-response traffic (serving our batch requests). An undialable advertised address is
    /// harmless -- the `add_address`/discovery transport-less filters keep it out of every dial
    /// path -- but skipping publish would leave us unidentifiable ("requesting peer unknown").
    pub(super) fn provide_our_data(&mut self) {
        let key = self.publish_our_record();
        if let Err(err) = self.swarm.behaviour_mut().kademlia.start_providing(key) {
            error!(target: "network-kad", "Failed to start providing key: {err}");
        }
    }

    /// Put our record to the DHT, without the provider announcement. Returns the record key.
    ///
    /// Used for the per-epoch republish: peers drop provider records, so the provider walk is
    /// only worth doing once at startup, not at every boundary.
    pub(super) fn publish_our_record(&mut self) -> RecordKey {
        let record = self.get_peer_record();
        info!(target: "network-kad", ?record, "Providing our record to kademlia for peer {:?}", self.swarm.local_peer_id());
        let key = record.key.clone();

        let put_record_result =
            self.swarm.behaviour_mut().kademlia.put_record(record, kad::Quorum::One);
        match put_record_result {
            Err(err) => {
                error!(target: "network-kad", "Failed to store record locally: {err}");
            }
            Ok(query_id) => {
                // if there are no peers then the PUT REQUEST to DHT is expected to fail. It will be
                // handled later on in publish_our_data_to_peer which is invoked when a peer
                // connects.
                let has_peers =
                    self.swarm.behaviour_mut().kademlia.kbuckets().any(|b| b.num_entries() > 0);
                if !has_peers {
                    self.kad_expecting_to_fail_query_ids.insert(query_id);
                }
            }
        }
        key
    }

    /// Publish our network addresses and peer id AND to the network under our BLS public key for
    /// discovery.
    pub(super) fn publish_our_data_to_peer(&mut self, peer: PeerId) {
        let record = self.get_peer_record();
        info!(target: "network-kad", "Publishing our record to kademlia. Peer: {:?}", peer);

        // Publish to the specified peer.
        let _ = self.swarm.behaviour_mut().kademlia.put_record_to(
            record.clone(),
            vec![peer].into_iter(),
            kad::Quorum::One,
        );

        // Also publish our record locally and to the network.
        if let Err(err) = self.swarm.behaviour_mut().kademlia.put_record(record, kad::Quorum::One) {
            error!(target: "network-kad", "Failed to publish record: {err}");
        }
    }
}

/// Classifies a KAD record fault into the penalty owed the delivering peer.
///
/// A record travels the DHT by replication, so its deliverer is not necessarily its author. A
/// content-integrity fault (undecodable value, malformed key, absent publisher) could never pass
/// an honest peer's validation, so no honest peer relays it: the deliverer is the origin and its
/// `publisher` is unauthenticated (bound only once the signature verifies), so it cannot excuse
/// the deliverer, which always pays. An author-attributed fault on a well-formed record (banned
/// publisher, future timestamp) can be carried by an honest relayer, so charge only the named
/// author, else one banned author becomes a ban cascade. `SourceBanned` names the deliverer
/// itself; store-capacity faults go through `handle_kad_store_error` and never reach here.
fn kad_record_fault_penalty(
    deliverer: &PeerId,
    publisher: Option<PeerId>,
    reason: &RecordInvalidReason,
) -> Option<Penalty> {
    let authored_by_deliverer = publisher == Some(*deliverer);
    match reason {
        // Content-integrity fault: never legitimately relayed, and `publisher` is unauthenticated,
        // so the deliverer is the origin and always pays regardless of the publisher field.
        RecordInvalidReason::InvalidPeerRecord(_)
        | RecordInvalidReason::InvalidKeyFormat
        | RecordInvalidReason::MissingPublisher => Some(Penalty::Fatal),
        // Author-attributed fault on a well-formed record: an honest relayer forwarding someone
        // else's record earns nothing; only the named author pays.
        RecordInvalidReason::PublisherBanned => authored_by_deliverer.then_some(Penalty::Fatal),
        RecordInvalidReason::TimestampTooFarInFuture => {
            authored_by_deliverer.then_some(Penalty::Mild)
        }
        // The source itself is banned - the deliverer is the offender, independent of authorship.
        RecordInvalidReason::SourceBanned => Some(Penalty::Fatal),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // INVARIANT: penalty attribution splits by fault class. A content-integrity fault (undecodable
    // value, malformed key, absent publisher) is never legitimately relayed and carries an
    // unauthenticated publisher, so the deliverer always pays. An author-attributed fault on a
    // well-formed record (banned publisher, future timestamp) can be honestly relayed, so only the
    // named author pays.

    /// A content-integrity fault always charges the deliverer, no matter what the (unauthenticated)
    /// publisher field says. Guards the regression where an attacker escaped by setting
    /// `publisher = None` or a stranger's id on a record that no honest peer would ever relay.
    #[test]
    fn content_integrity_fault_always_penalizes_deliverer() {
        let deliverer = PeerId::random();
        let stranger = PeerId::random();
        for reason in [
            RecordInvalidReason::InvalidPeerRecord("decoding failed".to_string()),
            RecordInvalidReason::InvalidPeerRecord("bad signature".to_string()),
            RecordInvalidReason::InvalidKeyFormat,
            RecordInvalidReason::MissingPublisher,
        ] {
            for publisher in [None, Some(stranger), Some(deliverer)] {
                assert!(
                    matches!(
                        kad_record_fault_penalty(&deliverer, publisher, &reason),
                        Some(Penalty::Fatal)
                    ),
                    "content-integrity fault must charge the deliverer Fatal for \
                     publisher={publisher:?}, reason={reason:?}"
                );
            }
        }
    }

    /// An honest relayer forwarding another author's banned or future-dated record must not move
    /// toward a ban: the author is named and authenticated, and the deliverer is a relayer.
    #[test]
    fn honest_relayer_of_author_faulted_record_is_not_penalized() {
        let deliverer = PeerId::random();
        let author = PeerId::random();
        for reason in
            [RecordInvalidReason::PublisherBanned, RecordInvalidReason::TimestampTooFarInFuture]
        {
            assert!(
                kad_record_fault_penalty(&deliverer, Some(author), &reason).is_none(),
                "relayer must not be penalized for author fault: {reason:?}"
            );
        }
    }

    /// The named author of a banned-publisher or future-timestamp record still pays, at the
    /// severity the fault carried before the authorship split.
    #[test]
    fn author_pays_for_its_own_attributed_fault() {
        let author = PeerId::random();
        assert!(matches!(
            kad_record_fault_penalty(&author, Some(author), &RecordInvalidReason::PublisherBanned),
            Some(Penalty::Fatal)
        ));
        assert!(matches!(
            kad_record_fault_penalty(
                &author,
                Some(author),
                &RecordInvalidReason::TimestampTooFarInFuture
            ),
            Some(Penalty::Mild)
        ));
    }

    /// `SourceBanned` names the deliverer itself as the offender, so it applies regardless of who
    /// authored the record (or whether a publisher is set at all).
    #[test]
    fn source_banned_always_penalizes_the_deliverer() {
        let source = PeerId::random();
        let publisher = PeerId::random();
        assert!(matches!(
            kad_record_fault_penalty(&source, Some(publisher), &RecordInvalidReason::SourceBanned),
            Some(Penalty::Fatal)
        ));
        assert!(matches!(
            kad_record_fault_penalty(&source, None, &RecordInvalidReason::SourceBanned),
            Some(Penalty::Fatal)
        ));
    }
}
