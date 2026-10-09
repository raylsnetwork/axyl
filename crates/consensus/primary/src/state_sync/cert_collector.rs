//! Collect certificates from storage for peers who are missing them.
//!
//! This module is used when retrieving certificates from local storage for peers.

use crate::{
    error::{PrimaryNetworkError, PrimaryNetworkResult},
    network::{MissingCertificatesRequest, PrimaryResponse},
};
use rayls_infrastructure_config::ConsensusConfig;
use rayls_infrastructure_storage::CertificateStore;
use rayls_infrastructure_types::{encode, AuthorityIdentifier, Certificate, Database, Round};
use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque},
    sync::LazyLock,
};
use tokio::time::Instant;
use tracing::{debug, info, warn};

/// The minimal length of a single, encoded, default [Certificate] used to set a local min for
/// message validation.
static LOCAL_MIN_REQUEST_SIZE: LazyLock<usize> =
    LazyLock::new(|| encode(&Certificate::default()).len());
/// The minimal response wrapper using a default, empty message.
///
/// Measured on an empty vector, so it accounts for a single-byte ULEB128 length. A response
/// carrying 128 or more certificates encodes a wider length prefix, so reserve the widest a
/// `usize` count can take rather than under-reserving by a few bytes at the cap.
static MESSAGE_OVERHEAD: LazyLock<usize> = LazyLock::new(|| {
    encode(&PrimaryResponse::RequestedCertificates(vec![])).len() + ULEB128_MAX_EXTRA_BYTES
});

/// Widest additional ULEB128 length prefix beyond the single byte an empty vector encodes.
const ULEB128_MAX_EXTRA_BYTES: usize = 9;

#[cfg(test)]
#[path = "../tests/cert_collector_tests.rs"]
mod cert_collector_tests;

/// Result of staging a certificate into the current round buffer.
enum Staging {
    /// Certificate staged, continue pumping the heap.
    Continue,
    /// A complete round was committed to the ready queue.
    Yield,
    /// Size limit reached, stop iteration.
    Done,
}

/// Time-bounded iterator yielding round-complete certificate batches from storage.
pub(crate) struct CertificateCollector<DB> {
    /// Priority queue for tracking the next rounds to fetch.
    fetch_queue: BinaryHeap<Reverse<(Round, AuthorityIdentifier)>>,
    /// Rounds to skip per authority.
    skip_rounds: BTreeMap<AuthorityIdentifier, BTreeSet<Round>>,
    /// Configuration for syncing behavior and access to the database.
    config: ConsensusConfig<DB>,
    /// The start time for processing the missing certificates request.
    start_time: Instant,
    /// The maximum uncompressed message size in bytes (with safety margin).
    max_message_size: usize,
    /// Committed size of certificates already moved to the ready queue.
    accumulated_size: usize,
    /// Certificates from completed rounds, ready to yield to the caller.
    ready: VecDeque<Certificate>,
    /// Certificates being collected for the current (potentially incomplete) round.
    staging: Vec<Certificate>,
    /// Encoded size of certificates in `staging`.
    staging_size: usize,
    /// The round currently being staged.
    staging_round: Option<Round>,
    /// Optional exclusive upper bound - stop fetching at or above this round.
    exclusive_upper_bound: Option<Round>,
}

impl<DB> CertificateCollector<DB>
where
    DB: Database,
{
    /// Create a new certificate collector with the given parameters.
    pub(crate) fn new(
        request: MissingCertificatesRequest,
        config: ConsensusConfig<DB>,
    ) -> PrimaryNetworkResult<Self> {
        let start_time = Instant::now();

        // assume reasonable min is 1 encoded certificate
        // NOTE: caller needs to account for cert + msg overhead
        if request.max_response_size < *LOCAL_MIN_REQUEST_SIZE {
            warn!(target: "cert-collector", "missing cert request max size too small: {}", request.max_response_size);
            return Err(PrimaryNetworkError::InvalidRequest("Request size too small".into()));
        }

        // use the min value between this node's max rpc message size and the requestor's reported
        // max message size
        //
        // NOTE: assume safe overhead is accounted for because the codec will also compress messages
        // unit tests show 318b uncompressed (response with 2 certs) -> 61b compressed
        let local_max =
            config.network_config().libp2p_config().max_rpc_message_size - *MESSAGE_OVERHEAD;
        let max_message_size = request.max_response_size.min(local_max);
        let (lower_bound, skip_rounds) = request.get_bounds()?;
        let exclusive_upper_bound = request.exclusive_upper_bound;

        // initialize the fetch queue with the first round for each authority
        let mut fetch_queue = BinaryHeap::new();
        for (origin, rounds) in &skip_rounds {
            // validate skip rounds count
            if rounds.len()
                > config.network_config().sync_config().max_skip_rounds_for_missing_certs
            {
                warn!(target: "cert-collector", "{} has sent {} rounds to skip", origin, rounds.len());

                return Err(PrimaryNetworkError::InvalidRequest(
                    "Request for rounds out of bounds".into(),
                ));
            }

            if let Some(next_round) = Self::find_next_round(
                config.node_storage(),
                origin,
                lower_bound,
                rounds,
                exclusive_upper_bound,
            )? {
                debug!(target: "cert-collector", next_round, %origin, skip_count = rounds.len(), "queuing authority for fetch");
                fetch_queue.push(Reverse((next_round, origin.clone())));
            } else {
                debug!(target: "cert-collector", %origin, lower_bound, skip_count = rounds.len(), "no rounds to fetch for authority");
            }
        }

        info!(
            target: "cert-collector",
            authorities = fetch_queue.len(),
            lower_bound,
            "Initialized fetch queue, elapsed = {}ms",
            start_time.elapsed().as_millis(),
        );

        Ok(Self {
            fetch_queue,
            skip_rounds,
            config,
            start_time,
            max_message_size,
            accumulated_size: 0,
            ready: VecDeque::new(),
            staging: Vec::new(),
            staging_size: 0,
            staging_round: None,
            exclusive_upper_bound,
        })
    }

    /// Reference to the collector's start time.
    pub(crate) fn start_time(&self) -> &Instant {
        &self.start_time
    }

    /// Find the next available round for an authority that shouldn't be skipped,
    /// respecting the optional exclusive upper bound.
    fn find_next_round(
        store: &DB,
        origin: &AuthorityIdentifier,
        current_round: Round,
        skip_rounds: &BTreeSet<Round>,
        exclusive_upper_bound: Option<Round>,
    ) -> PrimaryNetworkResult<Option<Round>> {
        let mut current_round = current_round;
        let upper_bound = exclusive_upper_bound.unwrap_or(Round::MAX);

        while let Some(round) = store.next_round_number(origin, current_round)? {
            if round >= upper_bound {
                return Ok(None);
            }
            if !skip_rounds.contains(&round) {
                return Ok(Some(round));
            }
            current_round = round;
        }

        Ok(None)
    }

    /// Try to fetch the next available certificate.
    pub(crate) fn next_certificate(&mut self) -> PrimaryNetworkResult<Option<Certificate>> {
        while let Some(Reverse((round, origin))) = self.fetch_queue.pop() {
            match self.config.node_storage().read_by_index(&origin, round)? {
                Some(cert) => {
                    // Queue up the next round for this authority if available
                    if let Some(next_round) = Self::find_next_round(
                        self.config.node_storage(),
                        &origin,
                        round,
                        self.skip_rounds.get(&origin).ok_or(PrimaryNetworkError::Internal(
                            "failed to retrieve authority from skipped rounds".to_string(),
                        ))?,
                        self.exclusive_upper_bound,
                    )? {
                        self.fetch_queue.push(Reverse((next_round, origin)));
                    }
                    return Ok(Some(cert));
                }
                None => continue,
            }
        }
        Ok(None)
    }

    /// Check if the time limit for DB reads has been reached.
    fn time_limit_reached(&self) -> bool {
        self.start_time.elapsed()
            >= self.config.network_config().sync_config().max_db_read_time_for_fetching_certificates
    }

    /// Check if adding bytes to accumulated + staging would exceed size limits.
    fn would_exceed_size_limit(&self, additional_bytes: usize) -> bool {
        self.accumulated_size + additional_bytes > self.max_message_size
    }

    /// Stage a certificate into the current round buffer.
    fn stage_certificate(&mut self, cert: Certificate, bytes: usize) -> Staging {
        let cert_round = cert.round();
        match self.staging_round {
            None => {
                if self.would_exceed_size_limit(bytes) {
                    return Staging::Done;
                }
                self.staging_round = Some(cert_round);
                self.staging.push(cert);
                self.staging_size = bytes;
                Staging::Continue
            }
            Some(r) if r == cert_round => {
                if self.would_exceed_size_limit(self.staging_size + bytes) {
                    warn!(
                        target: "cert-collector",
                        round = r, buffered = self.staging.len(),
                        "round exceeds size limit, discarding partial round"
                    );
                    self.staging.clear();
                    self.staging_size = 0;
                    return Staging::Done;
                }
                self.staging.push(cert);
                self.staging_size += bytes;
                Staging::Continue
            }
            Some(_) => {
                self.commit_staging();
                if self.would_exceed_size_limit(bytes) {
                    self.staging_round = None;
                } else {
                    self.staging_round = Some(cert_round);
                    self.staging.push(cert);
                    self.staging_size = bytes;
                }
                Staging::Yield
            }
        }
    }

    /// Flush a complete round from staging into the ready queue.
    fn commit_staging(&mut self) {
        if !self.staging.is_empty() {
            debug!(
                target: "cert-collector",
                round = ?self.staging_round,
                certs = self.staging.len(),
                accumulated_size = self.accumulated_size + self.staging_size,
                "committing complete round"
            );
            self.accumulated_size += self.staging_size;
            self.ready.extend(self.staging.drain(..));
            self.staging_size = 0;
        }
    }
}

impl<DB> Iterator for CertificateCollector<DB>
where
    DB: Database,
{
    type Item = PrimaryNetworkResult<Certificate>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(cert) = self.ready.pop_front() {
            return Some(Ok(cert));
        }

        loop {
            if self.time_limit_reached() {
                self.staging.clear();
                self.staging_size = 0;
                return None;
            }
            match self.next_certificate() {
                Ok(Some(cert)) => {
                    // Size the certificate without building the bytes: the codec encodes the
                    // response anyway, so encoding here would serialize every certificate served
                    // twice.
                    let bytes = bcs::serialized_size(&cert).unwrap_or_else(|_| encode(&cert).len());
                    match self.stage_certificate(cert, bytes) {
                        Staging::Continue => {}
                        Staging::Yield => return self.ready.pop_front().map(Ok),
                        Staging::Done => return None,
                    }
                }
                Ok(None) => {
                    self.commit_staging();
                    self.staging_round = None;
                    return self.ready.pop_front().map(Ok);
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}
