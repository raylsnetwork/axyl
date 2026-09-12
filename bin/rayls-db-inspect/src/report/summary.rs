// SPDX-License-Identifier: BUSL-1.1
//! `summary`: a per-node overview with no cross-node verdict.

use crate::{
    node_db::{LiveStatus, NodeDb},
    view::{authority, CheckpointView},
};
use rayls_infrastructure_storage::tables::{EpochCerts, EpochRecords};
use rayls_infrastructure_types::Epoch;
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Debug, Serialize)]
pub struct SummaryReport {
    pub nodes: Vec<SummaryNodeView>,
}

#[derive(Debug, Serialize)]
pub struct SummaryNodeView {
    pub node: String,
    pub path: String,
    pub live: LiveStatus,
    pub datafile_bytes: u64,
    pub node_identity: Option<String>,
    pub first_epoch: Option<Epoch>,
    pub last_epoch: Option<Epoch>,
    pub epoch_records: usize,
    pub epoch_certs: usize,
    pub latest_consensus_number: Option<u64>,
    pub latest_cached_consensus_number: Option<u64>,
    pub cold_tier: bool,
    pub cold_high_water_mark: Option<Epoch>,
    pub leftover_checkpoints: Vec<CheckpointView>,
    pub tables: BTreeMap<String, usize>,
}

pub fn summary(nodes: &[NodeDb]) -> eyre::Result<SummaryReport> {
    let mut views = Vec::with_capacity(nodes.len());
    for node in nodes {
        let tables = node.table_counts()?;
        let epochs = node.epoch_numbers()?;
        views.push(SummaryNodeView {
            node: node.label.clone(),
            path: node.path.display().to_string(),
            live: node.live,
            datafile_bytes: node.datafile_size()?,
            node_identity: node.node_identity()?.as_ref().map(authority),
            first_epoch: epochs.first().copied(),
            last_epoch: epochs.last().copied(),
            epoch_records: tables
                .get(<EpochRecords as rayls_infrastructure_types::Table>::NAME)
                .copied()
                .unwrap_or(0),
            epoch_certs: tables
                .get(<EpochCerts as rayls_infrastructure_types::Table>::NAME)
                .copied()
                .unwrap_or(0),
            latest_consensus_number: node.latest_consensus_number()?,
            latest_cached_consensus_number: node.latest_cached_consensus_number()?,
            cold_tier: node.has_cold(),
            cold_high_water_mark: node.cold_high_water_mark()?,
            leftover_checkpoints: node.checkpoints()?.iter().map(CheckpointView::of).collect(),
            tables,
        });
    }
    Ok(SummaryReport { nodes: views })
}
