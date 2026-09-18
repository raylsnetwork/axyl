// SPDX-License-Identifier: BUSL-1.1
//! A node to inspect: a consensus database on disk, or a node's `rayls_*` RPC endpoint.
//!
//! Every report works on [`Source`]s. A database source answers everything; an RPC source answers
//! what the `rayls` namespace serves: epoch records with their certificates (`rayls_epochRecord`,
//! `rayls_epochRecordByHash`) and consensus headers (`rayls_latestHeader`,
//! `rayls_consensusHeaderByNumber`, `rayls_consensusHeaderByHash`). Batches, node-local
//! checkpoints and table statistics are not served, so `get-batch`, `get-tx` and `summary` need
//! database sources. The RPC serves only *certified* epoch records: a record stored without its
//! certificate is reported as absent by an RPC source. There is no listing of records either, so
//! the commands that need every record (`epochs --all`, `epoch-check`) probe one epoch per call.

use crate::node_db::{LiveStatus, NodeDb, OpenOptions, Position, TableStatus, Tier};
use eyre::{eyre, WrapErr as _};
use jsonrpsee::{
    core::{client::ClientT as _, ClientError},
    http_client::{HttpClient, HttpClientBuilder},
    rpc_params,
};
use rayls_infrastructure_storage::tables::EpochRecords;
use rayls_infrastructure_types::{
    BlockHash, ConsensusHeader, Epoch, EpochCertificate, EpochRecord, EpochTransitionCheckpoint,
    B256,
};
use serde::de::DeserializeOwned;
use std::{
    collections::BTreeMap,
    sync::{Mutex, OnceLock, PoisonError},
    time::{Duration, Instant},
};

/// JSON-RPC error code the `rayls` namespace uses for "not found" (shared with its other error,
/// so the message is checked too).
const NOT_FOUND: i32 = 401;
const NOT_FOUND_MESSAGE: &str = "Not Found";
/// How many epochs below the current one are probed for the latest record before giving up.
const LATEST_RECORD_PROBES: Epoch = 4;
/// Upper bound on the exponential search for the highest stored header.
const TIP_PROBE_LIMIT: u32 = 64;
/// JSON-RPC's own "method not found".
const METHOD_NOT_FOUND: i32 = -32601;

/// The node runs a version without `method`.
#[derive(Debug)]
struct Unsupported {
    label: String,
    url: String,
    method: String,
}

impl std::fmt::Display for Unsupported {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: the node at {} does not serve {}; it runs a version without that method",
            self.label, self.url, self.method
        )
    }
}

impl std::error::Error for Unsupported {}

/// One node to inspect.
// A run holds a handful of sources; the size gap between the variants does not matter.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum Source {
    /// A consensus database opened read-only.
    Db(NodeDb),
    /// A node's `rayls_*` JSON-RPC endpoint.
    Rpc(RpcNode),
}

impl Source {
    /// Opens a database source from `[LABEL=]PATH`.
    pub fn open_db(spec: &str, opts: &OpenOptions) -> eyre::Result<Self> {
        NodeDb::open(spec, opts).map(Self::Db)
    }

    /// Connects an RPC source from `[LABEL=]URL`.
    pub fn open_rpc(spec: &str, requests_per_second: u32) -> eyre::Result<Self> {
        RpcNode::connect(spec, requests_per_second).map(Self::Rpc)
    }

    pub fn label(&self) -> &str {
        match self {
            Self::Db(db) => &db.label,
            Self::Rpc(rpc) => &rpc.label,
        }
    }

    pub fn live(&self) -> LiveStatus {
        match self {
            Self::Db(db) => db.live,
            Self::Rpc(_) => LiveStatus::Rpc,
        }
    }

    /// The database behind this source, for reports that need one.
    pub fn db(&self) -> Option<&NodeDb> {
        match self {
            Self::Db(db) => Some(db),
            Self::Rpc(_) => None,
        }
    }

    pub fn position(&self) -> eyre::Result<Position> {
        match self {
            Self::Db(db) => db.position(),
            Self::Rpc(rpc) => rpc.position(),
        }
    }

    /// The epoch record for `epoch` and, if present, its certificate.
    pub fn epoch(
        &self,
        epoch: Epoch,
    ) -> eyre::Result<Option<(EpochRecord, Option<EpochCertificate>)>> {
        match self {
            Self::Db(db) => db.epoch(epoch),
            Self::Rpc(rpc) => Ok(rpc.epoch(epoch)?.map(|(r, c)| (r, Some(c)))),
        }
    }

    /// The epoch number the record with `digest` has, if the node holds it.
    pub fn epoch_by_digest(&self, digest: B256) -> eyre::Result<Option<Epoch>> {
        match self {
            Self::Db(db) => db.epoch_by_digest(digest),
            Self::Rpc(rpc) => Ok(rpc.epoch_by_digest(digest)?.map(|(r, _)| r.epoch)),
        }
    }

    /// Every epoch with a record, ascending.
    pub fn epoch_numbers(&self) -> eyre::Result<Vec<Epoch>> {
        match self {
            Self::Db(db) => db.epoch_numbers(),
            Self::Rpc(rpc) => rpc.epoch_numbers(),
        }
    }

    /// Whether the epoch-record table was never created (databases only).
    pub fn epoch_table_absent(&self) -> eyre::Result<bool> {
        match self {
            Self::Db(db) => Ok(db.table_status::<EpochRecords>()? == TableStatus::Absent),
            Self::Rpc(_) => Ok(false),
        }
    }

    /// Leftover transition checkpoint for `epoch`; node-local, so never served over RPC.
    pub fn checkpoint(&self, epoch: Epoch) -> eyre::Result<Option<EpochTransitionCheckpoint>> {
        match self {
            Self::Db(db) => db.checkpoint(epoch),
            Self::Rpc(_) => Ok(None),
        }
    }

    /// The consensus header at `number` and where it came from.
    pub fn header(&self, number: u64) -> eyre::Result<Option<(ConsensusHeader, Tier)>> {
        match self {
            Self::Db(db) => db.header(number),
            Self::Rpc(rpc) => Ok(rpc.header(number)?.map(|h| (h, Tier::Rpc))),
        }
    }

    /// The consensus number of the header with `digest`, if the node holds it.
    pub fn header_number_by_digest(&self, digest: BlockHash) -> eyre::Result<Option<u64>> {
        match self {
            Self::Db(db) => db.header_number_by_digest(digest),
            Self::Rpc(rpc) => Ok(rpc.header_by_digest(digest)?.map(|h| h.number)),
        }
    }
}

/// A node reached through its JSON-RPC endpoint. Answers are memoized for the run: a report asks
/// for the same record or header from several places.
pub struct RpcNode {
    pub label: String,
    pub url: String,
    runtime: tokio::runtime::Runtime,
    client: HttpClient,
    latest: OnceLock<ConsensusHeader>,
    tip: OnceLock<Option<u64>>,
    records: Mutex<BTreeMap<Epoch, Option<(EpochRecord, EpochCertificate)>>>,
    headers: Mutex<BTreeMap<u64, Option<ConsensusHeader>>>,
    /// Minimum spacing between requests and the earliest time for the next one; `None` when
    /// unlimited.
    pace: Option<(Duration, Mutex<Instant>)>,
}

impl std::fmt::Debug for RpcNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcNode").field("label", &self.label).field("url", &self.url).finish()
    }
}

impl RpcNode {
    /// Connects to `[LABEL=]URL`. Only builds the client; the first report call talks to the node.
    /// `requests_per_second` bounds the load this client puts on the node (0: unlimited).
    pub fn connect(spec: &str, requests_per_second: u32) -> eyre::Result<Self> {
        let (label, url) = split_label(spec);
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return Err(eyre!("{label}: RPC URL must start with http:// or https://: {url}"));
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .wrap_err("start the RPC client runtime")?;
        let client = HttpClientBuilder::default()
            .request_timeout(Duration::from_secs(20))
            .build(url)
            .map_err(|e| eyre!("{label}: RPC client for {url}: {e}"))?;
        Ok(Self {
            label,
            url: url.to_owned(),
            runtime,
            client,
            latest: OnceLock::new(),
            tip: OnceLock::new(),
            records: Mutex::new(BTreeMap::new()),
            headers: Mutex::new(BTreeMap::new()),
            pace: (requests_per_second > 0).then(|| {
                (Duration::from_secs(1) / requests_per_second, Mutex::new(Instant::now()))
            }),
        })
    }

    /// One call; `Ok(None)` when the node answers "not found", `Err` for anything else.
    fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        params: jsonrpsee::core::params::ArrayParams,
    ) -> eyre::Result<Option<T>> {
        if let Some((interval, next)) = &self.pace {
            // hold the slot while waiting so concurrent callers queue up behind it
            let mut next = next.lock().unwrap_or_else(PoisonError::into_inner);
            let now = Instant::now();
            if *next > now {
                std::thread::sleep(*next - now);
            }
            *next = Instant::now() + *interval;
        }
        let result = self.runtime.block_on(self.client.request::<T, _>(method, params));
        match result {
            Ok(value) => Ok(Some(value)),
            Err(ClientError::Call(err))
                if err.code() == NOT_FOUND && err.message().starts_with(NOT_FOUND_MESSAGE) =>
            {
                Ok(None)
            }
            Err(ClientError::Call(err)) if err.code() == METHOD_NOT_FOUND => Err(Unsupported {
                label: self.label.clone(),
                url: self.url.clone(),
                method: method.to_owned(),
            }
            .into()),
            Err(err) => Err(eyre!("{}: {method} on {}: {err}", self.label, self.url)),
        }
    }

    fn latest(&self) -> eyre::Result<&ConsensusHeader> {
        if let Some(latest) = self.latest.get() {
            return Ok(latest);
        }
        let latest: ConsensusHeader = self
            .call("rayls_latestHeader", rpc_params![])?
            .ok_or_else(|| eyre!("{}: rayls_latestHeader answered not found", self.label))?;
        Ok(self.latest.get_or_init(|| latest))
    }

    /// The latest header when it is a stored one. The watch behind `rayls_latestHeader` holds
    /// the default header (number 0) until its first update, and nothing stores a header 0, so
    /// that answer never stands in for a lookup.
    fn latest_stored(&self) -> eyre::Result<Option<&ConsensusHeader>> {
        let latest = self.latest()?;
        Ok((latest.number > 0).then_some(latest))
    }

    fn epoch(&self, epoch: Epoch) -> eyre::Result<Option<(EpochRecord, EpochCertificate)>> {
        if let Some(cached) =
            self.records.lock().unwrap_or_else(PoisonError::into_inner).get(&epoch)
        {
            return Ok(cached.clone());
        }
        let found = self.call("rayls_epochRecord", rpc_params![epoch])?;
        self.records.lock().unwrap_or_else(PoisonError::into_inner).insert(epoch, found.clone());
        Ok(found)
    }

    fn epoch_by_digest(
        &self,
        digest: B256,
    ) -> eyre::Result<Option<(EpochRecord, EpochCertificate)>> {
        self.call("rayls_epochRecordByHash", rpc_params![digest])
    }

    /// Records are probed from epoch 0 to the epoch of the tip header; the RPC has no listing.
    fn epoch_numbers(&self) -> eyre::Result<Vec<Epoch>> {
        let Some(current) = self.position()?.current_epoch else { return Ok(Vec::new()) };
        let mut found = Vec::new();
        for epoch in 0..=current {
            if self.epoch(epoch)?.is_some() {
                found.push(epoch);
            }
        }
        Ok(found)
    }

    /// The newest certified record, probing down from the current epoch and giving up after
    /// [`LATEST_RECORD_PROBES`] misses: a healthy node answers on the first or second call, and
    /// the full probe is left to the commands that need every record.
    fn latest_epoch_record(&self, current_epoch: Option<Epoch>) -> eyre::Result<Option<Epoch>> {
        let Some(current) = current_epoch else { return Ok(None) };
        let floor = current.saturating_sub(LATEST_RECORD_PROBES);
        for epoch in (floor..=current).rev() {
            if self.epoch(epoch)?.is_some() {
                return Ok(Some(epoch));
            }
        }
        Ok(None)
    }

    /// The highest consensus header the node serves. The number `rayls_latestHeader` reports is
    /// only the floor of the search: a node may hold headers above it, and the reported value
    /// can lag, so an exponential search over `rayls_consensusHeaderByNumber` is followed by a
    /// bisection.
    fn tip(&self) -> eyre::Result<Option<u64>> {
        if let Some(tip) = self.tip.get() {
            return Ok(*tip);
        }
        let floor = self.latest()?.number;
        let tip = match self.probe_tip(floor) {
            Ok(tip) => tip,
            // a node without rayls_consensusHeaderByNumber serves only its latest header, so that
            // is its tip; the commands that need another header say so
            Err(err) if err.downcast_ref::<Unsupported>().is_some() => (floor > 0).then_some(floor),
            Err(err) => return Err(err),
        };
        Ok(*self.tip.get_or_init(|| tip))
    }

    /// Exponential search up from `floor`, then a bisection between the last hit and the first
    /// miss.
    fn probe_tip(&self, floor: u64) -> eyre::Result<Option<u64>> {
        let mut known = if floor > 0 || self.header(floor)?.is_some() { Some(floor) } else { None };
        if known.is_none() && self.header(1)?.is_some() {
            known = Some(1);
        }
        Ok(match known {
            None => None,
            Some(mut lo) => {
                // grow until a miss, then bisect between the last hit and the first miss
                let mut step = 1u64;
                let mut hi = None;
                for _ in 0..TIP_PROBE_LIMIT {
                    let probe = lo.saturating_add(step);
                    if self.header(probe)?.is_some() {
                        lo = probe;
                        step = step.saturating_mul(2);
                    } else {
                        hi = Some(probe);
                        break;
                    }
                }
                if let Some(mut hi) = hi {
                    while hi - lo > 1 {
                        let mid = lo + (hi - lo) / 2;
                        if self.header(mid)?.is_some() {
                            lo = mid;
                        } else {
                            hi = mid;
                        }
                    }
                }
                Some(lo)
            }
        })
    }

    fn position(&self) -> eyre::Result<Position> {
        let tip = self.tip()?;
        let current_epoch = match tip {
            Some(number) => self.header(number)?.map(|h| h.sub_dag.leader_epoch()),
            None => None,
        };
        Ok(Position {
            latest_epoch_record: self.latest_epoch_record(current_epoch)?,
            current_epoch,
            consensus_tip: tip,
        })
    }

    fn header(&self, number: u64) -> eyre::Result<Option<ConsensusHeader>> {
        if let Some(cached) =
            self.headers.lock().unwrap_or_else(PoisonError::into_inner).get(&number)
        {
            return Ok(cached.clone());
        }
        // numbers above the canonical tip are asked for too: the node also serves headers it has
        // verified but not processed yet
        let found = match self.latest_stored()? {
            Some(latest) if latest.number == number => Some(latest.clone()),
            _ => self.call("rayls_consensusHeaderByNumber", rpc_params![number])?,
        };
        self.headers.lock().unwrap_or_else(PoisonError::into_inner).insert(number, found.clone());
        Ok(found)
    }

    fn header_by_digest(&self, digest: BlockHash) -> eyre::Result<Option<ConsensusHeader>> {
        if let Some(latest) = self.latest_stored()? {
            if latest.digest() == digest {
                return Ok(Some(latest.clone()));
            }
        }
        self.call("rayls_consensusHeaderByHash", rpc_params![digest])
    }
}

/// Splits `LABEL=URL`; a bare URL is labelled by its host.
pub(crate) fn split_label(spec: &str) -> (String, &str) {
    match spec.split_once('=') {
        Some((label, url)) if !label.is_empty() && !label.contains(['/', ':']) => {
            (label.to_owned(), url)
        }
        _ => {
            let host = spec.split("://").nth(1).unwrap_or(spec).split('/').next().unwrap_or(spec);
            (host.to_owned(), spec)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rpc_labels() {
        assert_eq!(split_label("v1=http://10.0.0.1:8545"), ("v1".into(), "http://10.0.0.1:8545"));
        assert_eq!(
            split_label("http://10.0.0.1:8545/"),
            ("10.0.0.1:8545".into(), "http://10.0.0.1:8545/")
        );
        assert!(RpcNode::connect("v1=10.0.0.1:8545", 0).is_err(), "scheme required");
    }
}
