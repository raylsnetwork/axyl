//! Module with kademlia specific extensions, like a persistant store.

use std::{
    borrow::Cow,
    fmt, iter,
    time::{Instant, SystemTime},
};

use libp2p::{
    kad::{
        store::{Error, MemoryStoreConfig, RecordStore},
        ProviderRecord, Record, RecordKey,
    },
    Multiaddr, PeerId,
};
use rayls_infrastructure_config::KeyConfig;
use rayls_infrastructure_storage::tables::{
    KadProviderRecords, KadRecords, KadWorkerProviderRecords, KadWorkerRecords,
};
use rayls_infrastructure_types::{decode, encode, BlockHash, Database, DbTx, DefaultHashFunction};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_with::{serde_as, DeserializeAs, SerializeAs};

/// A record stored in the DHT.
/// This is a "shadow" struct for a kad Record so we can serialize/deserialize
/// for peristant storage.
#[serde_as]
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct KadRecord {
    /// Key of the record.
    #[serde_as(as = "RecordKeySerde")]
    key: RecordKey,
    /// Value of the record.
    value: Vec<u8>,
    /// The (original) publisher of the record.
    publisher: Option<PeerId>,
    /// The expiration time as measured by the system clock.
    /// The original kad Record uses an Instant here but that can not
    /// be serialized or deserialized so we use SystemTime here which
    /// should be "good enough" even if lacking in precision.
    expires: Option<SystemTime>,
}

impl fmt::Debug for KadRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let key = bs58::encode(&self.key).into_string();
        let value = bs58::encode(&self.value).into_string();
        write!(
            f,
            "KadRecord {{ key: {key}, value: {value}, publisher: {:?}, expires: {:?} }}",
            self.publisher, self.expires
        )
    }
}

impl From<Record> for KadRecord {
    fn from(value: Record) -> Self {
        let expires = instant_to_system(&value.expires);

        Self { key: value.key, value: value.value, publisher: value.publisher, expires }
    }
}

impl From<KadRecord> for Record {
    fn from(value: KadRecord) -> Self {
        let expires = system_to_instant(&value.expires);
        Self { key: value.key, value: value.value, publisher: value.publisher, expires }
    }
}

/// A record stored in the DHT whose value is the ID of a peer
/// who can provide the value on-demand.
/// This is a "shadow" struct for a kad ProviderRecord so we can serialize/deserialize
/// for peristant storage.
#[serde_as]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct KadProviderRecord {
    /// The key whose value is provided by the provider.
    #[serde_as(as = "RecordKeySerde")]
    key: RecordKey,
    /// The provider of the value for the key.
    provider: PeerId,
    /// The expiration time as measured by the system clock.
    /// The original kad Record uses an Instant here but that can not
    /// be serialized or deserialized so we use SystemTime here which
    /// should be "good enough" even if lacking in precision.
    expires: Option<SystemTime>,
    /// The known addresses that the provider may be listening on.
    addresses: Vec<Multiaddr>,
}

impl From<ProviderRecord> for KadProviderRecord {
    fn from(value: ProviderRecord) -> Self {
        let expires = instant_to_system(&value.expires);

        Self { key: value.key, provider: value.provider, expires, addresses: value.addresses }
    }
}

impl From<KadProviderRecord> for ProviderRecord {
    fn from(value: KadProviderRecord) -> Self {
        let expires = system_to_instant(&value.expires);
        Self { key: value.key, provider: value.provider, expires, addresses: value.addresses }
    }
}

/// Have to crudely convert back from a SystemTime to Instant to create a Record.
fn system_to_instant(expires: &Option<SystemTime>) -> Option<Instant> {
    if let Some(expires) = expires {
        let (system_now, now) = (SystemTime::now(), Instant::now());
        // This is sloppy and imprecise to work around a raw Instant being in a kad Record
        // so just ignore an error.
        let expires = *expires;
        if expires > system_now {
            if let Ok(duration) = expires.duration_since(system_now) {
                Some(now + duration)
            } else {
                None
            }
        } else if let Ok(duration) = system_now.duration_since(expires) {
            Some(now - duration)
        } else {
            None
        }
    } else {
        None
    }
}

/// The kad Record contains an Instant which can not be serialized or deserialized.
/// We crudely convert to a SystemTime which can.  Note this can be inacurate with
/// time change, clock drift, etc but it probably the best we can do to store a record
/// given it contains an Instant...
fn instant_to_system(expires: &Option<Instant>) -> Option<SystemTime> {
    if let Some(expires) = expires {
        let (system_now, now) = (SystemTime::now(), Instant::now());
        let expires = *expires;
        if expires > now {
            Some(system_now + (expires - now))
        } else {
            Some(system_now - (now - expires))
        }
    } else {
        None
    }
}

/// The type of the Kad store, primary or worker.
#[derive(Copy, Clone, Debug)]
pub enum KadStoreType {
    /// Primary network KadStore.
    Primary,
    /// Worker network KadStore.
    Worker,
}

/// Provide a persistant store for kademlia data.
/// Wraps around the consensus DB.
#[derive(Clone, Debug)]
pub struct KadStore<DB> {
    db: DB,
    node_key: RecordKey,
    /// Provide some sanity defaults for store sizing.
    /// Not bothering to expose these as knobs currenty since they are
    /// basically just here to prevent or mitigate attacks on the Kad store.
    /// Use the same settings as a Kad Memery store.
    config: MemoryStoreConfig,
    /// Tracks to number of records in DB.
    num_records: usize,
    /// Tracks to number of provider records in DB.
    num_providers: usize,
    /// Index used for database retrieval with multiple KAD tables.
    kad_type: KadStoreType,
}

impl<DB: Database> KadStore<DB> {
    /// Create a new KadStore backed by db.
    pub fn new(db: DB, key_config: &KeyConfig, kad_type: KadStoreType) -> Self {
        let node_key = RecordKey::new(&encode(&key_config.primary_public_key()));
        // Defaults for sanity.
        let config = MemoryStoreConfig::default();
        // Count records within a properly scoped read transaction
        // to ensure MDBX can reclaim dirty pages after completion
        let (num_records, num_providers) = db
            .with_read_txn(|txn| {
                Ok(match kad_type {
                    KadStoreType::Primary => {
                        (txn.iter::<KadRecords>().count(), txn.iter::<KadProviderRecords>().count())
                    }
                    KadStoreType::Worker => (
                        txn.iter::<KadWorkerRecords>().count(),
                        txn.iter::<KadWorkerProviderRecords>().count(),
                    ),
                })
            })
            .unwrap_or((0, 0));
        Self { db, node_key, config, num_records, num_providers, kad_type }
    }

    fn key_to_hash(&self, key: &RecordKey) -> BlockHash {
        let mut h = DefaultHashFunction::new();
        h.update(encode(key).as_ref());
        BlockHash::from_slice(h.finalize().as_bytes())
    }
}

/// Iterator of KAD records.
pub struct RecordIter<'a> {
    iter: Box<dyn Iterator<Item = (BlockHash, Vec<u8>)> + 'a>,
}

impl<'a> std::fmt::Debug for RecordIter<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Record Iterator")
    }
}

impl<'a> Iterator for RecordIter<'a> {
    type Item = Cow<'a, Record>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(_, r)| {
            let r: KadRecord = decode(r.as_ref());
            Cow::Owned(r.into())
        })
    }
}

impl<DB: Database> RecordStore for KadStore<DB> {
    type RecordsIter<'a> = RecordIter<'a>;

    type ProvidedIter<'a> = iter::Map<
        std::vec::IntoIter<ProviderRecord>,
        fn(ProviderRecord) -> Cow<'a, ProviderRecord>,
    >;

    fn get(&self, k: &RecordKey) -> Option<Cow<'_, Record>> {
        let key = self.key_to_hash(k);
        let record = match self.kad_type {
            KadStoreType::Primary => self.db.get::<KadRecords>(&key).ok()?,
            KadStoreType::Worker => self.db.get::<KadWorkerRecords>(&key).ok()?,
        };
        record.map(|r| {
            let r: KadRecord = decode(r.as_ref());
            Cow::Owned(r.into())
        })
    }

    fn put(&mut self, r: Record) -> libp2p::kad::store::Result<()> {
        if r.value.len() >= self.config.max_value_bytes {
            return Err(Error::ValueTooLarge);
        }

        let key = self.key_to_hash(&r.key);
        let kr: KadRecord = r.into();
        // Are we adding a new record or replacing an existing?
        let new_record = match self.kad_type {
            KadStoreType::Primary => {
                self.db.get::<KadRecords>(&key).map_err(|_| Error::ValueTooLarge)?.is_none()
            }
            KadStoreType::Worker => {
                self.db.get::<KadWorkerRecords>(&key).map_err(|_| Error::ValueTooLarge)?.is_none()
            }
        };
        // We have a new record so go ahead and inc num_records.
        // Should be safe since a failure to insert indicates a fatal DB condition.
        if new_record {
            if self.num_records >= self.config.max_records {
                return Err(Error::MaxRecords);
            } else {
                self.num_records = self.num_records.saturating_add(1);
            }
        }
        match self.kad_type {
            KadStoreType::Primary => self
                .db
                .insert::<KadRecords>(&key, &encode(&kr))
                .map_err(|_| Error::ValueTooLarge)?,
            KadStoreType::Worker => self
                .db
                .insert::<KadWorkerRecords>(&key, &encode(&kr))
                .map_err(|_| Error::ValueTooLarge)?,
        }
        Ok(())
    }

    fn remove(&mut self, k: &RecordKey) {
        let key = self.key_to_hash(k);
        if match self.kad_type {
            KadStoreType::Primary => self.db.remove::<KadRecords>(&key),
            KadStoreType::Worker => self.db.remove::<KadWorkerRecords>(&key),
        }
        .is_ok()
        {
            // Record was removed so dec num_records.
            self.num_records = self.num_records.saturating_sub(1);
        }
    }

    fn records(&self) -> Self::RecordsIter<'_> {
        let iter = match self.kad_type {
            KadStoreType::Primary => self.db.iter::<KadRecords>(),
            KadStoreType::Worker => self.db.iter::<KadWorkerRecords>(),
        };
        RecordIter { iter }
    }

    fn add_provider(&mut self, record: ProviderRecord) -> libp2p::kad::store::Result<()> {
        if self.config.max_provided_keys == self.num_providers {
            return Err(Error::MaxProvidedKeys);
        }
        let key = self.key_to_hash(&record.key);
        let kr: KadProviderRecord = record.into();
        let mut inc_providers = false;
        let records: Vec<KadProviderRecord> = if let Ok(Some(recs)) = match self.kad_type {
            KadStoreType::Primary => self.db.get::<KadProviderRecords>(&key),
            KadStoreType::Worker => self.db.get::<KadWorkerProviderRecords>(&key),
        } {
            let mut recs: Vec<KadProviderRecord> = decode(&recs);
            let mut found = false;
            for r in recs.iter_mut() {
                if r.provider == kr.provider {
                    *r = kr.clone();
                    found = true;
                }
            }
            if !found {
                if recs.len() >= self.config.max_providers_per_key {
                    return Err(Error::MaxProvidedKeys);
                }
                recs.push(kr);
            }
            recs
        } else {
            inc_providers = true;
            vec![kr]
        };
        match self.kad_type {
            KadStoreType::Primary => self
                .db
                .insert::<KadProviderRecords>(&key, &encode(&records))
                .map_err(|_| libp2p::kad::store::Error::ValueTooLarge)?,
            KadStoreType::Worker => self
                .db
                .insert::<KadWorkerProviderRecords>(&key, &encode(&records))
                .map_err(|_| libp2p::kad::store::Error::ValueTooLarge)?,
        }
        if inc_providers {
            // If this was a new record and it was inserted then inc num_providers.
            // I.E. Don't inc if this updated an existing provider record.
            self.num_providers = self.num_providers.saturating_add(1);
        }
        Ok(())
    }

    fn providers(&self, key: &RecordKey) -> Vec<ProviderRecord> {
        let key = self.key_to_hash(key);
        if let Ok(Some(recs)) = match self.kad_type {
            KadStoreType::Primary => self.db.get::<KadProviderRecords>(&key),
            KadStoreType::Worker => self.db.get::<KadWorkerProviderRecords>(&key),
        } {
            let records: Vec<KadProviderRecord> = decode(&recs);
            let records: Vec<ProviderRecord> = records.into_iter().map(|r| r.into()).collect();
            records
        } else {
            vec![]
        }
    }

    fn provided(&self) -> Self::ProvidedIter<'_> {
        let v = self.providers(&self.node_key);

        v.into_iter().map(Cow::Owned)
    }

    fn remove_provider(&mut self, key: &RecordKey, p: &PeerId) {
        let key = self.key_to_hash(key);
        if let Ok(Some(recs)) = match self.kad_type {
            KadStoreType::Primary => self.db.get::<KadProviderRecords>(&key),
            KadStoreType::Worker => self.db.get::<KadWorkerProviderRecords>(&key),
        } {
            let records: Vec<KadProviderRecord> = decode(&recs);
            let records: Vec<KadProviderRecord> =
                records.into_iter().filter(|r| r.provider != *p).collect();
            if records.is_empty() {
                if match self.kad_type {
                    KadStoreType::Primary => self.db.remove::<KadProviderRecords>(&key),
                    KadStoreType::Worker => self.db.remove::<KadWorkerProviderRecords>(&key),
                }
                .is_ok()
                {
                    // Provider is empty and we removed it so dec num_providers.
                    self.num_providers = self.num_providers.saturating_sub(1);
                }
            } else {
                let _ = match self.kad_type {
                    KadStoreType::Primary => {
                        self.db.insert::<KadProviderRecords>(&key, &encode(&records))
                    }
                    KadStoreType::Worker => {
                        self.db.insert::<KadWorkerProviderRecords>(&key, &encode(&records))
                    }
                };
            }
        }
    }
}

struct RecordKeySerde;

impl SerializeAs<RecordKey> for RecordKeySerde {
    fn serialize_as<S>(source: &RecordKey, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let bytes = source.to_vec();

        if serializer.is_human_readable() {
            serializer.serialize_str(&bs58::encode(&bytes).into_string())
        } else {
            serializer.serialize_bytes(&bytes)
        }
    }
}

impl<'de> DeserializeAs<'de, RecordKey> for RecordKeySerde {
    fn deserialize_as<D>(deserializer: D) -> Result<RecordKey, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::*;

        struct RKVisitor;

        impl Visitor<'_> for RKVisitor {
            type Value = RecordKey;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "valid bytes")
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(RecordKey::new(&v))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                let bytes = bs58::decode(v)
                    .into_vec()
                    .map_err(|_| Error::invalid_value(Unexpected::Str(v), &self))?;
                self.visit_bytes(&bytes)
            }
        }

        if deserializer.is_human_readable() {
            deserializer.deserialize_str(RKVisitor)
        } else {
            deserializer.deserialize_bytes(RKVisitor)
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use rand::{rngs::StdRng, SeedableRng as _};
    use rayls_infrastructure_config::KeyConfig;
    use rayls_infrastructure_storage::open_db;
    use rayls_infrastructure_types::{decode, encode, BlsKeypair};
    use tempfile::TempDir;

    use super::*;

    fn test_record(expire_past: bool) -> Record {
        let key_config =
            KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::from_os_rng()));
        let key = RecordKey::new(&encode(&key_config.primary_public_key()));
        let value: Vec<u8> = vec![0, 1, 2, 3];
        let peer_id = PeerId::random();
        let expires = if expire_past {
            Instant::now().checked_sub(Duration::from_secs(60)) // Already expired
        } else {
            Instant::now().checked_add(Duration::from_secs(60 * 60 * 24)) // one day
        };
        Record { key, value: value.clone(), publisher: Some(peer_id), expires }
    }

    fn test_provider_record() -> ProviderRecord {
        let key_config =
            KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::from_os_rng()));
        let key = RecordKey::new(&encode(&key_config.primary_public_key()));
        let provider = PeerId::random();
        let expires = Instant::now().checked_add(Duration::from_secs(60 * 60 * 24)); // one day
        ProviderRecord { key, provider, expires, addresses: vec![] }
    }

    #[test]
    fn test_kad_record_future() {
        let rec = test_record(false);
        let krec: KadRecord = rec.clone().into();
        let bytes = encode(&krec);
        let krec2: KadRecord = decode(bytes.as_ref());
        let rec2: Record = krec2.into();
        assert_eq!(rec.key, rec2.key);
        assert_eq!(rec.value, rec2.value);
        assert_eq!(rec.publisher, rec2.publisher);
        let now = Instant::now();
        assert_eq!(
            rec.expires.unwrap().duration_since(now).as_secs(),
            rec2.expires.unwrap().duration_since(now).as_secs()
        );

        // Now try an already past expiration.
        let rec = test_record(true);
        let krec: KadRecord = rec.clone().into();
        let bytes = encode(&krec);
        let krec2: KadRecord = decode(bytes.as_ref());
        let rec2: Record = krec2.into();
        assert_eq!(rec.key, rec2.key);
        assert_eq!(rec.value, rec2.value);
        assert_eq!(rec.publisher, rec2.publisher);
        let now = Instant::now();
        assert_eq!(
            rec.expires.unwrap().duration_since(now).as_secs(),
            rec2.expires.unwrap().duration_since(now).as_secs()
        );

        // Now try no expiration.
        let rec =
            Record { key: rec.key, value: rec.value, publisher: rec.publisher, expires: None };
        let krec: KadRecord = rec.clone().into();
        let bytes = encode(&krec);
        let krec2: KadRecord = decode(bytes.as_ref());
        let rec2: Record = krec2.into();
        assert_eq!(rec.key, rec2.key);
        assert_eq!(rec.value, rec2.value);
        assert_eq!(rec.publisher, rec2.publisher);
        assert!(rec.expires.is_none());
        assert!(rec2.expires.is_none());
    }

    fn test_rec<DB: Database>(rec: &Record, kad_store: &KadStore<DB>) {
        let rec_get = kad_store.get(&rec.key).expect("record");
        assert_eq!(rec.key, rec_get.key);
        assert_eq!(rec.value, rec_get.value);
        assert_eq!(rec.publisher, rec_get.publisher);
        let now = Instant::now();
        assert_eq!(
            rec.expires.unwrap().duration_since(now).as_secs(),
            rec_get.expires.unwrap().duration_since(now).as_secs()
        );
    }

    #[test]
    fn test_kad_store() {
        let tmp_dir = TempDir::new().expect("temp dir");
        let db = open_db(tmp_dir.path());
        let key_config =
            KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::from_os_rng()));
        let mut kad_store = KadStore::new(db.clone(), &key_config, KadStoreType::Primary);
        let mut kad_store_worker = KadStore::new(db, &key_config, KadStoreType::Worker);

        let rec = test_record(false);
        let rec2 = test_record(false);
        let rec3 = test_record(false);
        kad_store.db.sync_persist().expect("persist");

        assert!(kad_store.get(&rec.key).is_none());
        assert_eq!(kad_store.records().count(), 0);
        kad_store.put(rec.clone()).expect("put record");
        kad_store_worker.put(rec.clone()).expect("put record");
        kad_store.db.sync_persist().expect("persist");
        test_rec(&rec, &kad_store);
        test_rec(&rec, &kad_store_worker);
        assert_eq!(kad_store.num_records, 1);
        assert_eq!(kad_store.records().count(), 1);
        assert_eq!(kad_store_worker.num_records, 1);
        assert_eq!(kad_store_worker.records().count(), 1);

        kad_store.remove(&rec.key);
        kad_store.db.sync_persist().expect("persist");
        test_rec(&rec, &kad_store_worker);
        assert_eq!(kad_store.records().count(), 0);
        assert_eq!(kad_store.num_records, 0);
        assert_eq!(kad_store_worker.records().count(), 1);
        assert_eq!(kad_store_worker.num_records, 1);
        kad_store_worker.remove(&rec.key);
        kad_store.db.sync_persist().expect("persist");
        assert!(kad_store_worker.get(&rec.key).is_none());
        assert!(kad_store.get(&rec.key).is_none());
        assert_eq!(kad_store.records().count(), 0);
        assert_eq!(kad_store_worker.records().count(), 0);

        kad_store.put(rec.clone()).expect("put record");
        kad_store_worker.put(rec.clone()).expect("put record");
        kad_store.put(rec2.clone()).expect("put record");
        kad_store.put(rec3.clone()).expect("put record");
        kad_store.db.sync_persist().expect("persist");

        assert_eq!(kad_store.num_records, 3);
        assert_eq!(kad_store_worker.num_records, 1);
        assert_eq!(kad_store.records().count(), 3);
        assert_eq!(kad_store_worker.records().count(), 1);
        test_rec(&rec, &kad_store);
        test_rec(&rec, &kad_store_worker);
        test_rec(&rec2, &kad_store);
        test_rec(&rec3, &kad_store);

        let key = RecordKey::new(&encode(&key_config.primary_public_key()));
        let provider = PeerId::random();
        let expires = Instant::now().checked_add(Duration::from_secs(60 * 60 * 24));
        // Make manually to use our node key as key.
        let provider_rec1 = ProviderRecord { key, provider, expires, addresses: vec![] };
        let provider = PeerId::random();
        let expires = Instant::now().checked_add(Duration::from_secs(60 * 60 * 24)); // one day
        let provider_rec1_1 =
            ProviderRecord { key: provider_rec1.key.clone(), provider, expires, addresses: vec![] };
        let provider = PeerId::random();
        let expires = Instant::now().checked_add(Duration::from_secs(60 * 60 * 24)); // one day
        let provider_rec1_2 =
            ProviderRecord { key: provider_rec1.key.clone(), provider, expires, addresses: vec![] };
        let provider_rec2 = test_provider_record();
        let provider_rec3 = test_provider_record();
        assert_eq!(kad_store.provided().count(), 0);
        kad_store.add_provider(provider_rec1.clone()).expect("add provider");
        kad_store.add_provider(provider_rec2.clone()).expect("add provider");
        kad_store.add_provider(provider_rec3.clone()).expect("add provider");
        assert_eq!(kad_store.num_providers, 3);
        assert_eq!(kad_store.provided().count(), 1);
        kad_store.add_provider(provider_rec1_1.clone()).expect("add provider");
        kad_store.add_provider(provider_rec1_2.clone()).expect("add provider");
        assert_eq!(kad_store.num_providers, 3);
        assert_eq!(kad_store.provided().count(), 3);
        assert_eq!(kad_store.providers(&provider_rec1.key).len(), 3);
        assert_eq!(kad_store.providers(&provider_rec2.key).len(), 1);
        assert_eq!(kad_store.providers(&provider_rec3.key).len(), 1);

        assert_eq!(kad_store_worker.num_providers, 0);
        assert_eq!(kad_store_worker.provided().count(), 0);
        kad_store_worker.add_provider(provider_rec1.clone()).expect("add provider");
        kad_store_worker.add_provider(provider_rec2.clone()).expect("add provider");
        kad_store_worker.add_provider(provider_rec3.clone()).expect("add provider");
        assert_eq!(kad_store_worker.num_providers, 3);
        assert_eq!(kad_store_worker.provided().count(), 1);
        kad_store_worker.add_provider(provider_rec1_1.clone()).expect("add provider");
        kad_store_worker.add_provider(provider_rec1_2.clone()).expect("add provider");
        assert_eq!(kad_store_worker.num_providers, 3);
        assert_eq!(kad_store_worker.provided().count(), 3);
        assert_eq!(kad_store_worker.providers(&provider_rec1.key).len(), 3);
        assert_eq!(kad_store_worker.providers(&provider_rec2.key).len(), 1);
        assert_eq!(kad_store_worker.providers(&provider_rec3.key).len(), 1);

        let recs_1 = kad_store.providers(&provider_rec1.key);
        assert_eq!(recs_1.len(), 3);
        assert_eq!(recs_1[0], provider_rec1);
        assert_eq!(recs_1[1], provider_rec1_1);
        assert_eq!(recs_1[2], provider_rec1_2);

        kad_store.remove_provider(&provider_rec1_1.key, &provider_rec1_1.provider);
        assert_eq!(kad_store.num_providers, 3);
        assert_eq!(kad_store.provided().count(), 2);
        assert_eq!(kad_store.providers(&provider_rec1.key).len(), 2);
        kad_store.add_provider(provider_rec1_1.clone()).expect("add provider");
        assert_eq!(kad_store.provided().count(), 3);
        assert_eq!(kad_store.providers(&provider_rec1.key).len(), 3);
        kad_store.add_provider(provider_rec1_1.clone()).expect("add provider");
        assert_eq!(kad_store.num_providers, 3);
        assert_eq!(kad_store.provided().count(), 3);
        assert_eq!(kad_store.providers(&provider_rec1.key).len(), 3);

        kad_store_worker.remove_provider(&provider_rec1_1.key, &provider_rec1_1.provider);
        assert_eq!(kad_store_worker.num_providers, 3);
        assert_eq!(kad_store_worker.provided().count(), 2);
        assert_eq!(kad_store_worker.providers(&provider_rec1.key).len(), 2);
        kad_store_worker.add_provider(provider_rec1_1.clone()).expect("add provider");
        assert_eq!(kad_store_worker.provided().count(), 3);
        assert_eq!(kad_store_worker.providers(&provider_rec1.key).len(), 3);
        kad_store_worker.add_provider(provider_rec1_1.clone()).expect("add provider");
        assert_eq!(kad_store_worker.num_providers, 3);
        assert_eq!(kad_store_worker.provided().count(), 3);
        assert_eq!(kad_store_worker.providers(&provider_rec1.key).len(), 3);

        kad_store.remove_provider(&provider_rec1.key, &provider_rec1.provider);
        assert_eq!(kad_store.num_providers, 3);
        kad_store.remove_provider(&provider_rec1_1.key, &provider_rec1_1.provider);
        assert_eq!(kad_store.num_providers, 3);
        kad_store.remove_provider(&provider_rec1_2.key, &provider_rec1_2.provider);
        kad_store.db.sync_persist().expect("persist");

        assert_eq!(kad_store.num_providers, 2);
        assert_eq!(kad_store.provided().count(), 0);
        assert_eq!(kad_store.providers(&provider_rec1.key).len(), 0);
        kad_store.remove_provider(&provider_rec2.key, &provider_rec2.provider);
        kad_store.db.sync_persist().expect("persist");

        assert_eq!(kad_store.num_providers, 1);
        assert_eq!(kad_store.providers(&provider_rec2.key).len(), 0);

        kad_store_worker.remove_provider(&provider_rec1.key, &provider_rec1.provider);
        assert_eq!(kad_store_worker.num_providers, 3);
        kad_store_worker.remove_provider(&provider_rec1_1.key, &provider_rec1_1.provider);
        assert_eq!(kad_store_worker.num_providers, 3);
        kad_store_worker.remove_provider(&provider_rec1_2.key, &provider_rec1_2.provider);
        assert_eq!(kad_store_worker.num_providers, 2);
        assert_eq!(kad_store_worker.provided().count(), 0);
        assert_eq!(kad_store_worker.providers(&provider_rec1.key).len(), 0);
        kad_store_worker.remove_provider(&provider_rec2.key, &provider_rec2.provider);
        assert_eq!(kad_store_worker.num_providers, 1);
        assert_eq!(kad_store_worker.providers(&provider_rec2.key).len(), 0);

        // Bogus remove, mismatches key and provider.
        kad_store.remove_provider(&provider_rec3.key, &provider_rec2.provider);
        kad_store.db.sync_persist().expect("persist");

        assert_eq!(kad_store.num_providers, 1);
        kad_store.remove_provider(&provider_rec3.key, &provider_rec3.provider);
        kad_store.db.sync_persist().expect("persist");

        assert_eq!(kad_store.num_providers, 0);
        kad_store_worker.remove_provider(&provider_rec3.key, &provider_rec2.provider);
        assert_eq!(kad_store_worker.num_providers, 1);
        kad_store_worker.remove_provider(&provider_rec3.key, &provider_rec3.provider);
        assert_eq!(kad_store_worker.num_providers, 0);
    }

    /// Test that we do not count duplicate puts against our max records.
    #[test]
    fn test_kad_put_limit() {
        let tmp_dir = TempDir::new().expect("temp dir");
        let db = open_db(tmp_dir.path());
        let key_config =
            KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::from_os_rng()));
        let mut kad_store = KadStore::new(db.clone(), &key_config, KadStoreType::Primary);
        let mut kad_store_worker = KadStore::new(db, &key_config, KadStoreType::Worker);

        let config = MemoryStoreConfig::default();

        // Almost fill up the stores.
        for _ in 0..config.max_records - 1 {
            let rec = test_record(false);
            kad_store.put(rec.clone()).expect("put record");
            kad_store_worker.put(rec).expect("put record");
        }
        let rec = test_record(false);
        // These should all work because they are overwrites not new records.
        for _ in 0..10 {
            kad_store.put(rec.clone()).expect("put record");
            kad_store_worker.put(rec.clone()).expect("put record");
        }
        let rec = test_record(false);
        // Should be full now so get max errors.
        assert!(matches!(kad_store.put(rec.clone()), Err(Error::MaxRecords)));
        assert!(matches!(kad_store_worker.put(rec.clone()), Err(Error::MaxRecords)));
    }
}
