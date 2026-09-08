// SPDX-License-Identifier: BUSL-1.1
//! Fixture builder: seeds real MDBX consensus databases the way a node would write them.

#![allow(dead_code, unreachable_pub)]

use rayls_db_inspect::node_db::{NodeDb, OpenOptions};
use rayls_infrastructure_config::RaylsDirs as _;
use rayls_infrastructure_storage::{
    mem_db::MemDatabase,
    open_db,
    tables::{ConsensusBlockNumbersByDigest, ConsensusBlocks, ConsensusBlocksCache},
    CheckpointStore, DatabaseType, EpochStore,
};
use rayls_infrastructure_types::{
    BlsAggregateSignature, BlsPublicKey, BlsSignature, Certificate, CommittedSubDag,
    ConsensusHeader, Database, DbTxMut, Epoch, EpochCertificate, EpochRecord,
    EpochTransitionCheckpoint, EpochTransitionPhase, ReputationScores, B256,
};
use rayls_testing_test_utils::{AuthorityFixture, CommitteeFixture, RaylsTempDirs};
use std::num::NonZeroUsize;

/// A committee of four whose keys sign the fixture records and certificates.
pub struct Fixture {
    pub committee: CommitteeFixture<MemDatabase>,
}

impl Fixture {
    pub fn new() -> Self {
        Self::with_epoch(0)
    }

    /// A committee whose DAG headers (and so every consensus header built from them) carry
    /// `epoch`. Used to make the fixture nodes look like they are past a given epoch.
    pub fn with_epoch(epoch: Epoch) -> Self {
        let committee = CommitteeFixture::builder(MemDatabase::default)
            .randomize_ports(true)
            .committee_size(NonZeroUsize::new(4).unwrap())
            .epoch(epoch)
            .build();
        Self { committee }
    }

    /// The committee's BLS keys, sorted as `EpochRecord` stores them.
    pub fn keys(&self) -> Vec<BlsPublicKey> {
        let mut keys: Vec<BlsPublicKey> =
            self.committee.authorities().map(|a| a.primary_public_key()).collect();
        keys.sort_unstable();
        keys
    }

    pub fn authorities(&self) -> Vec<&AuthorityFixture<MemDatabase>> {
        self.committee.authorities().collect()
    }

    /// A record for `epoch` whose parent is `parent` (or zero for epoch 0).
    pub fn record(
        &self,
        epoch: Epoch,
        parent: Option<&EpochRecord>,
        boundary: B256,
    ) -> EpochRecord {
        EpochRecord {
            epoch,
            committee: self.keys(),
            next_committee: self.keys(),
            parent_hash: parent.map(|p| p.digest()).unwrap_or_default(),
            parent_state: Default::default(),
            parent_consensus: boundary,
        }
    }

    /// Aggregates votes from `signers` (indices into the sorted committee) into a certificate.
    pub fn certify(&self, record: &EpochRecord, signers: &[usize]) -> EpochCertificate {
        let keys = self.keys();
        let mut signers = signers.to_vec();
        signers.sort_unstable();
        let sigs: Vec<BlsSignature> = signers
            .iter()
            .map(|i| {
                let key = keys[*i];
                let authority = self
                    .committee
                    .authorities()
                    .find(|a| a.primary_public_key() == key)
                    .expect("signer in committee");
                record.sign_vote(authority.consensus_config().key_config()).signature
            })
            .collect();
        let signature = BlsAggregateSignature::aggregate(&sigs, true).unwrap().to_signature();
        let mut signed_authorities = roaring::RoaringBitmap::new();
        for i in signers {
            signed_authorities.push(i as u32);
        }
        EpochCertificate { epoch_hash: record.digest(), signature, signed_authorities }
    }

    /// A consensus header at `number` with one real certificate (signed by everyone) as leader.
    pub fn header(&self, number: u64, parent_hash: B256) -> ConsensusHeader {
        let committee = self.committee.committee();
        let dag_header =
            self.committee.first_authority().header_with_round(&committee, number as u32 + 1);
        let leader = self.committee.certificate(&dag_header);
        self.header_with_leader(number, parent_hash, leader)
    }

    pub fn header_with_leader(
        &self,
        number: u64,
        parent_hash: B256,
        leader: Certificate,
    ) -> ConsensusHeader {
        let sub_dag = CommittedSubDag::new(
            vec![leader.clone()],
            leader,
            number,
            ReputationScores::default(),
            None,
        );
        ConsensusHeader { parent_hash, sub_dag, number, extra: B256::default() }
    }

    /// Two leader certificates for the same DAG header signed by different quorums: identical
    /// digests, different signer sets.
    pub fn forked_leaders(&self, round: u32) -> (Certificate, Certificate) {
        let committee = self.committee.committee();
        let authorities = self.authorities();
        let header = authorities[0].header_with_round(&committee, round);
        let votes = |idx: &[usize]| {
            idx.iter()
                .map(|i| {
                    let v = authorities[*i].vote(&header);
                    (v.author().clone(), *v.signature())
                })
                .collect::<Vec<_>>()
        };
        let votes_a = votes(&[0, 1, 2]);
        let votes_b = votes(&[1, 2, 3]);
        let a = Certificate::new_unverified(&committee, header.clone(), votes_a).unwrap();
        let b = Certificate::new_unverified(&committee, header, votes_b).unwrap();
        (a, b)
    }
}

/// A seeded node datadir.
pub struct SeededNode {
    pub dirs: RaylsTempDirs,
}

impl SeededNode {
    /// Runs `seed` against a read-write database at a fresh datadir, flushes, and closes it.
    pub fn new(seed: impl FnOnce(&DatabaseType)) -> Self {
        let dirs = RaylsTempDirs::default();
        {
            let db = open_db(dirs.consensus_db_path());
            seed(&db);
            db.sync_persist().unwrap();
        }
        Self { dirs }
    }

    pub fn datadir(&self) -> String {
        self.dirs.consensus_db_path().parent().unwrap().display().to_string()
    }

    pub fn consensus_db(&self) -> String {
        self.dirs.consensus_db_path().display().to_string()
    }

    pub fn open(&self, label: &str) -> NodeDb {
        NodeDb::open(&format!("{label}={}", self.datadir()), &OpenOptions::default()).unwrap()
    }
}

/// Seeding helpers mirroring the node's own write paths.
pub fn write_epoch(db: &DatabaseType, record: &EpochRecord, cert: Option<&EpochCertificate>) {
    match cert {
        Some(cert) => db.save_epoch_record_with_cert(record, cert).unwrap(),
        None => db.save_epoch_record(record).unwrap(),
    }
}

pub fn write_header(db: &DatabaseType, header: &ConsensusHeader) {
    db.with_write_txn(|txn| {
        txn.insert::<ConsensusBlocks>(&header.number, header)?;
        txn.insert::<ConsensusBlockNumbersByDigest>(&header.digest(), &header.number)?;
        Ok(())
    })
    .unwrap();
}

pub fn write_cached_header(db: &DatabaseType, header: &ConsensusHeader) {
    db.with_write_txn(|txn| {
        txn.insert::<ConsensusBlocksCache>(&header.number, header)?;
        txn.insert::<ConsensusBlockNumbersByDigest>(&header.digest(), &header.number)?;
        Ok(())
    })
    .unwrap();
}

pub fn write_checkpoint(db: &DatabaseType, epoch: Epoch) {
    db.save_checkpoint(&EpochTransitionCheckpoint {
        epoch,
        completed_phase: EpochTransitionPhase::Draining,
        target_hash: B256::repeat_byte(0xcc),
        timestamp: 1_700_000_000,
    })
    .unwrap();
}

/// Three certified epochs (0 unsigned dummy, 1 and 2 certified) chained together, plus
/// consensus headers 0..=3 chained by parent hash. Returns the records and headers written.
pub fn seed_healthy(fx: &Fixture, db: &DatabaseType) -> (Vec<EpochRecord>, Vec<ConsensusHeader>) {
    let mut headers = Vec::new();
    let mut parent = B256::default();
    for n in 0..=3u64 {
        let h = fx.header(n, parent);
        parent = h.digest();
        write_header(db, &h);
        headers.push(h);
    }
    let r0 = fx.record(0, None, headers[0].digest());
    write_epoch(db, &r0, None);
    let r1 = fx.record(1, Some(&r0), headers[1].digest());
    write_epoch(db, &r1, Some(&fx.certify(&r1, &[0, 1, 2])));
    let r2 = fx.record(2, Some(&r1), headers[2].digest());
    write_epoch(db, &r2, Some(&fx.certify(&r2, &[0, 1, 2, 3])));
    (vec![r0, r1, r2], headers)
}
