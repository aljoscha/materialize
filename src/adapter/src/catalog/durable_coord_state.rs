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
//!  - Add a transaction abstraction:
//!   - you start a transaction
//!   - you can read state as of that transaction and prepare updates
//!   - you can commit a transaction, which might fail
//!   - we currently make it heard to build a valid transaction and to iterate over state as of the
//!    transaction, along with already prepared updates
//!   - all the manual handling of state updates is also a big footgun!

use std::collections::BTreeMap;
use std::sync::Arc;

use differential_dataflow::consolidation;
use differential_dataflow::lattice::Lattice;
use itertools::Itertools;
use serde::Deserialize;
use serde::Serialize;
use timely::progress::Antichain;

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

    trace: Vec<(StateUpdate, u64, i64)>,

    upper: u64,
    listen: Listen<StateUpdate, (), u64, i64>,
    write_handle: WriteHandle<StateUpdate, (), u64, i64>,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum StateUpdate {
    Timestamp(TimestampKey, TimestampValue),
    Item(ItemKey, ItemValue),
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

        println!("initial snapshot: {:?}", initial_snapshot);

        println!("creating listen at {:?}", restart_as_of);
        let listen = read_handle
            .listen(Antichain::from_elem(restart_as_of))
            .await
            .expect("invalid usage");

        let mut this = Self {
            timestamps: BTreeMap::new(),
            items: BTreeMap::new(),
            trace: Vec::new(),
            upper: restart_as_of,
            listen,
            write_handle,
        };

        this.apply_updates(initial_snapshot);

        println!("syncing to {:?}", upper);
        this.sync(upper).await;

        this
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

    // TODO(aljoscha): These `prepare_*` updates are very inefficient. Also, we
    // can probably factor out quite a bit of common functionality.
    //
    // Also, the API for these is currently a big footgun! If we do two
    // `prepare_*` calls for the same global ID, both will emit updates because
    // we don't "remember" an update that we already emitted.

    pub fn prepare_upsert_timestamp(
        &self,
        timeline: Timeline,
        timestamp: mz_repr::Timestamp,
    ) -> Vec<(StateUpdate, i64)> {
        let key = TimestampKey {
            id: timeline.to_string(),
        };

        let mut updates = Vec::new();

        let current_timestamp = self.timestamps.get(&key);

        match current_timestamp {
            Some(current_timestamp) if current_timestamp.ts == timestamp => {
                // No need to change anything!
            }
            Some(current_timestamp) => {
                // Need to retract the old mapping and insert a new mapping.
                updates.push((
                    StateUpdate::Timestamp(key.clone(), current_timestamp.clone()),
                    -1,
                ));
                let timestamp_value = TimestampValue { ts: timestamp };
                updates.push((StateUpdate::Timestamp(key.clone(), timestamp_value), 1));
            }
            None => {
                let timestamp_value = TimestampValue { ts: timestamp };
                // Only need to add the new mapping.
                updates.push((StateUpdate::Timestamp(key, timestamp_value), 1));
            }
        }

        updates
    }

    // We currently never remove timestamps, but ... :shrug:.
    #[allow(unused)]
    pub fn prepare_remove_timestamp(&self, timeline: Timeline) -> Vec<(StateUpdate, i64)> {
        let key = TimestampKey {
            id: timeline.to_string(),
        };

        let mut updates = Vec::new();

        let current_timestamp = self.timestamps.get(&key);

        match current_timestamp {
            Some(current_timestamp) => {
                // Need to retract the mapping.
                updates.push((
                    StateUpdate::Timestamp(key.clone(), current_timestamp.clone()),
                    -1,
                ));
            }
            None => {
                // Nothing to do!
            }
        }

        updates
    }

    pub fn prepare_upsert_item(&self, key: ItemKey, item: ItemValue) -> Vec<(StateUpdate, i64)> {
        let mut updates = Vec::new();

        let current_item = self.items.get(&key);

        match current_item {
            Some(current_item) if current_item == &item => {
                // No need to change anything!
            }
            Some(current_item) => {
                // Need to retract the old mapping and insert a new mapping.
                updates.push((StateUpdate::Item(key.clone(), current_item.clone()), -1));
                updates.push((StateUpdate::Item(key.clone(), item), 1));
            }
            None => {
                // Only need to add the new mapping.
                updates.push((StateUpdate::Item(key, item), 1));
            }
        }

        updates
    }

    #[allow(unused)]
    pub fn prepare_remove_item(&self, key: ItemKey) -> Vec<(StateUpdate, i64)> {
        let mut updates = Vec::new();

        let current_item = self.items.get(&key);

        match current_item {
            Some(current_item) => {
                // Need to retract the mapping.
                updates.push((StateUpdate::Item(key, current_item.clone()), -1));
            }
            None => {
                // Nothing to do!
            }
        }

        updates
    }

    pub async fn commit_updates(
        &mut self,
        updates: Vec<(StateUpdate, i64)>,
    ) -> Result<(), UpperMismatch<u64>> {
        println!("committing {:?}", updates);

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
                        println!("progress: {:?}", upper);
                        self.upper = upper
                            .as_option()
                            .cloned()
                            .expect("we use a totally ordered time and never finalize the shard");
                    }
                    ListenEvent::Updates(batch_updates) => {
                        println!("updates: {:?}", batch_updates);
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

        for update in self.trace.iter() {
            match update {
                (state_update, _ts, 1) => match state_update {
                    StateUpdate::Timestamp(timeline, timestamp) => {
                        self.timestamps.insert(timeline.clone(), timestamp.clone());
                    }
                    StateUpdate::Item(key, item) => {
                        self.items.insert(key.clone(), item.clone());
                    }
                },
                invalid_update => {
                    panic!("invalid update in consolidated trace: {:?}", invalid_update);
                }
            }
        }
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

        let mut updates = Vec::new();

        let mut new_updates = durable_state.prepare_upsert_timestamp(timeline.clone(), timestamp);
        updates.append(&mut new_updates);

        let res = durable_state.commit_updates(updates).await;
        assert!(matches!(res, Ok(())));

        let stored_timestamp = durable_state.get_timestamp(&timeline);
        assert_eq!(stored_timestamp, Some(timestamp));

        println!("re-creating!");

        // Re-create our durable state.
        let mut durable_state = make_test_state(log_shard_id).await;

        let stored_timestamp = durable_state.get_timestamp(&timeline);
        assert_eq!(stored_timestamp, Some(timestamp));

        // Do something that causes a retraction and update.
        let mut updates = Vec::new();

        let new_timestamp = mz_repr::Timestamp::new(1);

        let mut new_updates =
            durable_state.prepare_upsert_timestamp(timeline.clone(), new_timestamp);
        updates.append(&mut new_updates);

        let res = durable_state.commit_updates(updates).await;
        assert!(matches!(res, Ok(())));

        // Re-create our durable state.
        let durable_state = make_test_state(log_shard_id).await;

        let stored_timestamp = durable_state.get_timestamp(&timeline);
        assert_eq!(stored_timestamp, Some(new_timestamp));
    }
}
