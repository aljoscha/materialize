// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Abstraction for reading and updating durable coordinator state to/from a log (a persist shard,
//! in our case).
//!
//! ## TODO
//!  - Maybe allow "read" methods on DurableCoordState itself, instead of forcing users to create a
//!  "read transaction"?
//!  - As of right now, creating a "read transaction" requires mutable access to DurableCoordState.

use std::collections::BTreeMap;
use std::sync::Arc;

use differential_dataflow::consolidation;
use differential_dataflow::lattice::Lattice;
use itertools::Itertools;
use serde::Deserialize;
use serde::Serialize;
use timely::progress::Antichain;
use tracing::debug;

use mz_persist_client::error::UpperMismatch;
use mz_persist_client::read::Listen;
use mz_persist_client::read::ListenEvent;
use mz_persist_client::write::WriteHandle;
use mz_persist_client::PersistClient;
use mz_persist_client::ShardId;
use mz_persist_types::codec_impls::TodoSchema;
use mz_persist_types::codec_impls::UnitSchema;
use mz_persist_types::Codec;
use mz_storage_client::types::sources::Timeline;

use crate::catalog::storage::ConfigValue;
use crate::catalog::storage::ItemKey;
use crate::catalog::storage::ItemValue;
use crate::catalog::storage::TimestampKey;
use crate::catalog::storage::TimestampValue;

#[derive(Debug)]
pub struct DurableCoordState {
    // TODO(aljoscha): We should keep these collections as a _trace_, with the
    // full diffs. And only collapse them down to a map when needed. And also
    // ensure that there is only one entry for a given `GlobalId` then.
    timestamps: BTreeMap<TimestampKey, TimestampValue>,
    items: BTreeMap<ItemKey, ItemValue>,
    config: BTreeMap<String, ConfigValue>,

    trace: Vec<(StateUpdate, u64, i64)>,

    /// Updates that have been committed but not yet applied by whoever is driving around this
    /// [`DurableCoordState`]. All holders of a durable state should periodically drain this and
    /// apply the updates to their in-memory state.
    pending_updates: Vec<(StateUpdate, u64, i64)>,

    upper: u64,
    listen: Listen<StateUpdate, (), u64, i64>,
    write_handle: WriteHandle<StateUpdate, (), u64, i64>,
}

/// An transaction for updating [`DurableCoordState`].
///
/// This contains a copy of state as it was when the transaction was created. You also need to use
/// this when querying state.
#[derive(Debug)]
pub struct Transaction<'a> {
    // NOTE: We keep a copy of the state here and mutate it when preparing state updates, to make
    // sure that we always have an up-to-date view and can generate the correct differential state
    // updates.
    timestamps: BTreeMap<TimestampKey, TimestampValue>,
    items: BTreeMap<ItemKey, ItemValue>,
    config: BTreeMap<String, ConfigValue>,

    updates: Vec<(StateUpdate, i64)>,

    durable_coord_state: &'a mut DurableCoordState,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum StateUpdate {
    Timestamp(TimestampKey, TimestampValue),
    Item(ItemKey, ItemValue),
    Config(String, ConfigValue),
}

impl Codec for StateUpdate {
    type Schema = TodoSchema<StateUpdate>;

    fn codec_name() -> String {
        "StateUpdateSerde".into()
    }

    fn encode<B>(&self, buf: &mut B)
    where
        B: bytes::BufMut,
    {
        let bytes = serde_json::to_vec(&self).expect("failed to encode StateUpdate");
        buf.put(bytes.as_slice());
    }

    fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
        serde_json::from_slice(buf).map_err(|err| err.to_string())
    }
}

impl DurableCoordState {
    pub async fn new(shard_id: ShardId, persist_client: PersistClient) -> DurableCoordState {
        let purpose = "DurableCoordState".to_string();
        let (mut write_handle, mut read_handle) = persist_client
            .open(
                shard_id,
                &purpose,
                Arc::new(TodoSchema::default()),
                Arc::new(UnitSchema),
            )
            .await
            .expect("invalid usage");

        let since = read_handle.since().clone();
        let upper: u64 = write_handle
            .upper()
            .as_option()
            .cloned()
            .expect("we use a totally ordered time and never finalize the shard");

        let upper = if upper == 0 {
            // Get around the fact that we can't get a snapshot and listen when
            // the upper is 0.
            //
            // And yes, we should probably not do this in a production impl.
            // const EMPTY_UPDATES: &[((StateUpdate<T>, ()), u64, i64)] = &[];
            let batch_builder = write_handle.builder(Antichain::from_elem(0));
            let mut batch = batch_builder
                .finish(Antichain::from_elem(1))
                .await
                .expect("invalid usage");

            let _res = write_handle
                .compare_and_append_batch(
                    &mut [&mut batch],
                    Antichain::from_elem(0),
                    Antichain::from_elem(1),
                )
                .await
                .expect("unvalid usage");
            1
        } else {
            upper
        };

        let mut restart_as_of = upper.saturating_sub(1);
        restart_as_of.advance_by(since.borrow());

        // Always true, currently!
        let initial_snapshot = if restart_as_of > 0 {
            read_handle
                .snapshot_and_fetch(Antichain::from_elem(restart_as_of))
                .await
                .expect("we have advanced the restart_as_of by the since")
        } else {
            Vec::new()
        };

        let initial_snapshot = initial_snapshot
            .into_iter()
            .map(|((key, _unit), ts, diff)| (key.expect("key decoding error"), ts, diff))
            .collect_vec();

        debug!("initial snapshot: {:?}", initial_snapshot);

        debug!("creating listen at {:?}", restart_as_of);
        let listen = read_handle
            .listen(Antichain::from_elem(restart_as_of))
            .await
            .expect("invalid usage");

        let mut this = Self {
            timestamps: BTreeMap::new(),
            items: BTreeMap::new(),
            config: BTreeMap::new(),
            trace: Vec::new(),
            upper: restart_as_of,
            pending_updates: Vec::new(),
            listen,
            write_handle,
        };

        this.apply_updates(initial_snapshot);

        debug!("syncing to {:?}", upper);
        this.sync(upper).await;

        // Drain away the updates that we know already!
        let _updates = this.drain_pending_updates();

        this
    }

    pub async fn begin_transaction(&mut self) -> Transaction {
        self.sync_to_recent_upper().await;
        Transaction::new(self)
    }

    pub(crate) async fn commit_updates(
        &mut self,
        updates: Vec<(StateUpdate, i64)>,
    ) -> Result<(), UpperMismatch<u64>> {
        debug!("commit_updates {:?}", updates);

        let current_upper = self.upper.clone();
        // Yeah yeah, need to make this better...
        let next_upper = current_upper + 1;

        let mut batch_builder = self
            .write_handle
            .builder(Antichain::from_elem(current_upper));

        for (state_update, diff) in updates.into_iter() {
            batch_builder
                .add(&state_update, &(), &current_upper, &diff)
                .await
                .expect("invalid usage");
        }

        let mut batch = batch_builder
            .finish(Antichain::from_elem(next_upper))
            .await
            .expect("invalid usage");

        let res = self
            .write_handle
            .compare_and_append_batch(
                &mut [&mut batch],
                Antichain::from_elem(current_upper),
                Antichain::from_elem(next_upper),
            )
            .await
            .expect("invalid usage");

        match res {
            Ok(()) => {
                // All's well! We can sync back our updates to update our
                // in-memory maps/traces.
                self.sync(next_upper).await;
            }
            Err(upper_mismatch) => return Err(upper_mismatch),
        }

        Ok(())
    }

    pub async fn sync_to_recent_upper(&mut self) {
        let upper = self.write_handle.fetch_recent_upper().await;
        let upper = upper.as_option().cloned().expect("closed out shard");
        self.sync(upper).await
    }

    async fn sync(&mut self, target_upper: u64) {
        let mut updates: Vec<(StateUpdate, u64, i64)> = Vec::new();

        // Tail the log until we reach the target upper. Note that, in the
        // common case, we are also the writer, so we are waiting to read back
        // what we wrote.
        while self.upper < target_upper {
            let listen_events = self.listen.fetch_next().await;

            for listen_event in listen_events.into_iter() {
                match listen_event {
                    ListenEvent::Progress(upper) => {
                        debug!("sync; progress: {:?}", upper);
                        self.upper = upper
                            .as_option()
                            .cloned()
                            .expect("we use a totally ordered time and never finalize the shard");
                    }
                    ListenEvent::Updates(batch_updates) => {
                        debug!("sync; updates: {:?}", batch_updates);
                        let mut batch_updates = batch_updates
                            .into_iter()
                            .map(|((key, _unit), ts, diff)| {
                                (key.expect("key decoding error"), ts, diff)
                            })
                            .collect_vec();

                        updates.append(&mut batch_updates);
                    }
                }
            }
        }

        self.apply_updates(updates);
    }

    // TODO(aljoscha): We should keep the collections as a _trace_, with the
    // full diffs. And only collapse them down to a map when needed. And also
    // ensure that there is only one entry for a given `GlobalId` then.
    fn apply_updates(&mut self, mut updates: Vec<(StateUpdate, u64, i64)>) {
        self.pending_updates.extend(updates.iter().cloned());
        self.trace.append(&mut updates);

        let upper_frontier = Antichain::from_elem(self.upper);
        for (_update, ts, _diff) in self.trace.iter_mut() {
            ts.advance_by(upper_frontier.borrow());
        }

        // TODO(aljoscha): Make sure we reduce the capacity of `self.trace` if
        // it grows too much.
        consolidation::consolidate_updates(&mut self.trace);

        // TODO(aljoscha): This is very inefficient, we're rebuilding the
        // in-memory cash of every type of collection every time we're applying
        // updates. It's easy and correct, though.
        self.timestamps = BTreeMap::new();
        self.items = BTreeMap::new();
        self.config = BTreeMap::new();

        for update in self.trace.iter() {
            match update {
                (state_update, _ts, 1) => match state_update {
                    StateUpdate::Timestamp(timeline, timestamp) => {
                        self.timestamps.insert(timeline.clone(), timestamp.clone());
                    }
                    StateUpdate::Item(key, item) => {
                        self.items.insert(key.clone(), item.clone());
                    }
                    StateUpdate::Config(key, value) => {
                        self.config.insert(key.clone(), value.clone());
                    }
                },
                invalid_update => {
                    panic!("invalid update in consolidated trace: {:?}", invalid_update);
                }
            }
        }
    }

    /// Updates that have been committed but not yet applied by whoever is driving around this
    /// [`DurableCoordState`]. All holders of a durable state should periodically drain this and
    /// apply the updates to their in-memory state.
    pub fn drain_pending_updates(&mut self) -> Vec<(StateUpdate, u64, i64)> {
        // TODO(aljoscha): Also won't win prices for efficiency!
        let updates = std::mem::replace(&mut self.pending_updates, Vec::new());
        updates
    }
}

impl<'a> Transaction<'a> {
    fn new(durable_coord_state: &mut DurableCoordState) -> Transaction {
        Transaction {
            timestamps: durable_coord_state.timestamps.clone(),
            items: durable_coord_state.items.clone(),
            config: durable_coord_state.config.clone(),
            updates: Vec::new(),
            durable_coord_state,
        }
    }

    pub fn get_timestamp(&self, timeline: &Timeline) -> Option<mz_repr::Timestamp> {
        let key = TimestampKey {
            id: timeline.to_string(),
        };
        let timestamp = self.timestamps.get(&key);
        timestamp.map(|ts| ts.ts)
    }

    pub fn get_all_timestamps(&self) -> Vec<(Timeline, mz_repr::Timestamp)> {
        self.timestamps
            .iter()
            .map(|(timeline, ts)| {
                (
                    timeline.id.parse().expect("invalid timeline persisted"),
                    ts.ts,
                )
            })
            .collect_vec()
    }

    pub fn get_item(&self, key: &ItemKey) -> Option<ItemValue> {
        self.items.get(key).cloned()
    }

    pub fn get_all_items(&self) -> BTreeMap<ItemKey, ItemValue> {
        self.items.clone()
    }

    pub fn get_config(&self, key: &String) -> Option<ConfigValue> {
        self.config.get(key).cloned()
    }

    // TODO(aljoscha): We can probably factor out quite a bit of common functionality.

    pub fn upsert_timestamp(
        &mut self,
        timeline: Timeline,
        timestamp: mz_repr::Timestamp,
    ) -> Option<mz_repr::Timestamp> {
        let key = TimestampKey {
            id: timeline.to_string(),
        };

        let current_timestamp = self.timestamps.get(&key).cloned();

        match current_timestamp {
            Some(current_timestamp) if current_timestamp.ts == timestamp => {
                // No need to change anything!
                Some(current_timestamp.ts)
            }
            Some(current_timestamp) => {
                // Need to retract the old mapping and insert a new mapping.
                self.updates.push((
                    StateUpdate::Timestamp(key.clone(), current_timestamp.clone()),
                    -1,
                ));
                let timestamp_value = TimestampValue { ts: timestamp };
                self.updates.push((
                    StateUpdate::Timestamp(key.clone(), timestamp_value.clone()),
                    1,
                ));
                self.timestamps.insert(key, timestamp_value);
                Some(current_timestamp.ts)
            }
            None => {
                let timestamp_value = TimestampValue { ts: timestamp };
                // Only need to add the new mapping.
                self.updates.push((
                    StateUpdate::Timestamp(key.clone(), timestamp_value.clone()),
                    1,
                ));
                self.timestamps.insert(key, timestamp_value);
                None
            }
        }
    }

    // We currently never remove timestamps, but ... :shrug:.
    #[allow(unused)]
    pub fn remove_timestamp(&mut self, timeline: Timeline) {
        let key = TimestampKey {
            id: timeline.to_string(),
        };

        let current_timestamp = self.timestamps.get(&key);

        match current_timestamp {
            Some(current_timestamp) => {
                // Need to retract the mapping.
                self.updates.push((
                    StateUpdate::Timestamp(key.clone(), current_timestamp.clone()),
                    -1,
                ));
                self.timestamps.remove(&key);
            }
            None => {
                // Nothing to do!
            }
        }
    }

    pub fn upsert_item(&mut self, key: ItemKey, item: ItemValue) -> Option<ItemValue> {
        let current_item = self.items.get(&key).cloned();

        match current_item {
            Some(current_item) if current_item == item => {
                // No need to change anything!
                Some(current_item)
            }
            Some(current_item) => {
                // Need to retract the old mapping and insert a new mapping.
                self.updates
                    .push((StateUpdate::Item(key.clone(), current_item.clone()), -1));
                self.updates
                    .push((StateUpdate::Item(key.clone(), item.clone()), 1));
                self.items.insert(key, item);
                Some(current_item)
            }
            None => {
                // Only need to add the new mapping.
                self.updates
                    .push((StateUpdate::Item(key.clone(), item.clone()), 1));
                self.items.insert(key, item);
                None
            }
        }
    }

    #[allow(unused)]
    pub fn remove_item(&mut self, key: ItemKey) {
        let current_item = self.items.get(&key);

        match current_item {
            Some(current_item) => {
                // Need to retract the mapping.
                self.updates
                    .push((StateUpdate::Item(key.clone(), current_item.clone()), -1));

                self.items.remove(&key);
            }
            None => {
                // Nothing to do!
            }
        }
    }

    pub fn upsert_config(&mut self, key: String, config: ConfigValue) -> Option<ConfigValue> {
        let current_config = self.config.get(&key).cloned();

        match current_config {
            Some(current_config) if current_config == config => {
                // No need to change anything!
                Some(current_config)
            }
            Some(current_config) => {
                // Need to retract the old mapping and insert a new mapping.
                self.updates
                    .push((StateUpdate::Config(key.clone(), current_config.clone()), -1));
                self.updates
                    .push((StateUpdate::Config(key.clone(), config.clone()), 1));
                self.config.insert(key, config);
                Some(current_config)
            }
            None => {
                // Only need to add the new mapping.
                self.updates
                    .push((StateUpdate::Config(key.clone(), config.clone()), 1));
                self.config.insert(key, config);
                None
            }
        }
    }

    #[allow(unused)]
    pub fn remove_config(&mut self, key: String) {
        let current_config = self.config.get(&key);

        match current_config {
            Some(current_config) => {
                // Need to retract the mapping.
                self.updates
                    .push((StateUpdate::Config(key.clone(), current_config.clone()), -1));

                self.config.remove(&key);
            }
            None => {
                // Nothing to do!
            }
        }
    }

    pub async fn commit(self) -> Result<(), UpperMismatch<u64>> {
        self.durable_coord_state.commit_updates(self.updates).await
    }

    pub async fn commit_and_drain_updates(
        self,
    ) -> Result<Vec<(StateUpdate, u64, i64)>, UpperMismatch<u64>> {
        self.durable_coord_state
            .commit_updates(self.updates)
            .await?;
        Ok(self.durable_coord_state.drain_pending_updates())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::time::Duration;

    use once_cell::sync::Lazy;

    use mz_build_info::DUMMY_BUILD_INFO;
    use mz_ore::metrics::MetricsRegistry;
    use mz_ore::now::SYSTEM_TIME;
    use mz_persist_client::cache::PersistClientCache;
    use mz_persist_client::cfg::PersistConfig;
    use mz_persist_client::{PersistLocation, ShardId};

    // 15 minutes
    static PERSIST_READER_LEASE_TIMEOUT_MS: Duration = Duration::from_secs(60 * 15);

    static PERSIST_CACHE: Lazy<Arc<PersistClientCache>> = Lazy::new(|| {
        let mut persistcfg = PersistConfig::new(&DUMMY_BUILD_INFO, SYSTEM_TIME.clone());
        persistcfg.reader_lease_duration = PERSIST_READER_LEASE_TIMEOUT_MS;
        Arc::new(PersistClientCache::new(persistcfg, &MetricsRegistry::new()))
    });

    async fn make_test_state(shard_id: ShardId) -> DurableCoordState {
        let persist_location = PersistLocation {
            blob_uri: "mem://".to_owned(),
            consensus_uri: "mem://".to_owned(),
        };

        let persist_client = PERSIST_CACHE.open(persist_location).await.unwrap();

        let durable_storage_state = DurableCoordState::new(shard_id, persist_client).await;

        durable_storage_state
    }

    #[tokio::test]
    async fn test_basic_usage() {
        let log_shard_id = ShardId::new();

        let mut durable_state = make_test_state(log_shard_id).await;

        let timeline = Timeline::EpochMilliseconds;
        let timestamp = mz_repr::Timestamp::new(0);

        let mut tx = durable_state.begin_transaction().await;

        tx.upsert_timestamp(timeline.clone(), timestamp);

        let res = tx.commit().await;
        assert!(matches!(res, Ok(())));

        let read_tx = durable_state.begin_transaction().await;
        let stored_timestamp = read_tx.get_timestamp(&timeline);
        assert_eq!(stored_timestamp, Some(timestamp));

        // Re-create our durable state.
        let mut durable_state = make_test_state(log_shard_id).await;

        let read_tx = durable_state.begin_transaction().await;
        let stored_timestamp = read_tx.get_timestamp(&timeline);
        assert_eq!(stored_timestamp, Some(timestamp));

        // Do something that causes a retraction and update.

        let new_timestamp = mz_repr::Timestamp::new(1);

        let mut tx = durable_state.begin_transaction().await;
        tx.upsert_timestamp(timeline.clone(), new_timestamp);

        let res = tx.commit().await;
        assert!(matches!(res, Ok(())));

        // Re-create our durable state.
        let mut durable_state = make_test_state(log_shard_id).await;

        let read_tx = durable_state.begin_transaction().await;
        let stored_timestamp = read_tx.get_timestamp(&timeline);
        assert_eq!(stored_timestamp, Some(new_timestamp));
    }
}
