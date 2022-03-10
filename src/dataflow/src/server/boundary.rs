// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Traits and types for capturing and replaying collections of data.
use std::any::Any;
use std::cell::RefCell;
use std::rc::Rc;

use differential_dataflow::Collection;
use timely::dataflow::Scope;

use mz_dataflow_types::DataflowError;
use mz_dataflow_types::SourceInstanceKey;
use mz_expr::GlobalId;
use mz_repr::{Diff, Row};

/// A type that can capture a specific source.
pub trait StorageCapture {
    /// Captures the source and binds to `id`.
    fn capture<G: Scope<Timestamp = mz_repr::Timestamp>>(
        &mut self,
        id: SourceInstanceKey,
        ok: Collection<G, Row, Diff>,
        err: Collection<G, DataflowError, Diff>,
        token: Rc<dyn Any>,
        name: &str,
        dataflow_id: GlobalId,
    );
}

impl<SC: StorageCapture> StorageCapture for Rc<RefCell<SC>> {
    fn capture<G: Scope<Timestamp = mz_repr::Timestamp>>(
        &mut self,
        id: SourceInstanceKey,
        ok: Collection<G, Row, Diff>,
        err: Collection<G, DataflowError, Diff>,
        token: Rc<dyn Any>,
        name: &str,
        dataflow_id: GlobalId,
    ) {
        self.borrow_mut()
            .capture(id, ok, err, token, name, dataflow_id)
    }
}

impl<CR: ComputeReplay> ComputeReplay for Rc<RefCell<CR>> {
    fn replay<G: Scope<Timestamp = mz_repr::Timestamp>>(
        &mut self,
        id: SourceInstanceKey,
        scope: &mut G,
        name: &str,
        dataflow_id: GlobalId,
    ) -> (
        Collection<G, Row, Diff>,
        Collection<G, DataflowError, Diff>,
        Rc<dyn Any>,
    ) {
        self.borrow_mut().replay(id, scope, name, dataflow_id)
    }
}

/// A type that can replay specific sources
pub trait ComputeReplay {
    /// Replays the source bound to `id`.
    ///
    /// `None` is returned if the source does not exist, either because
    /// it was not there to begin, or has already been replayed. Either
    /// case is likely an error.
    fn replay<G: Scope<Timestamp = mz_repr::Timestamp>>(
        &mut self,
        id: SourceInstanceKey,
        scope: &mut G,
        name: &str,
        dataflow_id: GlobalId,
    ) -> (
        Collection<G, Row, Diff>,
        Collection<G, DataflowError, Diff>,
        Rc<dyn Any>,
    );
}

/// A boundary implementation that panics on use.
pub struct DummyBoundary;

impl ComputeReplay for DummyBoundary {
    fn replay<G: Scope<Timestamp = mz_repr::Timestamp>>(
        &mut self,
        _id: SourceInstanceKey,
        _scope: &mut G,
        _name: &str,
        _dataflow_id: GlobalId,
    ) -> (
        Collection<G, Row, Diff>,
        Collection<G, DataflowError, Diff>,
        Rc<dyn Any>,
    ) {
        panic!("DummyBoundary cannot replay")
    }
}

impl StorageCapture for DummyBoundary {
    fn capture<G: Scope<Timestamp = mz_repr::Timestamp>>(
        &mut self,
        _id: SourceInstanceKey,
        _ok: Collection<G, Row, Diff>,
        _err: Collection<G, DataflowError, Diff>,
        _token: Rc<dyn Any>,
        _name: &str,
        _dataflow_id: GlobalId,
    ) {
        panic!("DummyBoundary cannot capture")
    }
}

pub use event_link::EventLinkBoundary;

/// A simple boundary that uses activated event linked lists.
mod event_link {

    use std::any::Any;
    use std::collections::BTreeMap;
    use std::rc::Rc;
    use std::time::Duration;

    use differential_dataflow::AsCollection;
    use differential_dataflow::Collection;
    use mz_dataflow_types::DecodeError;
    use mz_dataflow_types::SourceError;
    use mz_dataflow_types::SourceErrorDetails;
    use mz_expr::SourceInstanceId;
    use timely::dataflow::operators::capture::EventLink;
    use timely::dataflow::operators::Concat;
    use timely::dataflow::operators::Map;
    use timely::dataflow::operators::OkErr;
    use timely::dataflow::Scope;
    use timely::progress::Antichain;
    use tracing::info;

    use mz_dataflow_types::DataflowError;
    use mz_dataflow_types::SourceInstanceKey;
    use mz_expr::GlobalId;
    use mz_persist::client::RuntimeClient;
    use mz_persist::operators::source::PersistedSource;
    use mz_repr::{Diff, Row};

    use crate::activator::RcActivator;
    use crate::replay::MzReplay;
    use crate::server::ActivatedEventPusher;

    use super::{ComputeReplay, StorageCapture};

    /// A simple boundary that uses activated event linked lists.
    pub struct EventLinkBoundary {
        send: BTreeMap<SourceInstanceKey, crossbeam_channel::Sender<SourceBoundary>>,
        recv: BTreeMap<SourceInstanceKey, crossbeam_channel::Receiver<SourceBoundary>>,
        persist: Option<RuntimeClient>,
    }

    impl EventLinkBoundary {
        /// Create a new boundary, initializing the state to be empty.
        pub fn new(persist: Option<RuntimeClient>) -> Self {
            Self {
                send: BTreeMap::new(),
                recv: BTreeMap::new(),
                persist,
            }
        }
    }

    impl StorageCapture for EventLinkBoundary {
        fn capture<G: Scope<Timestamp = mz_repr::Timestamp>>(
            &mut self,
            id: SourceInstanceKey,
            ok: Collection<G, Row, Diff>,
            err: Collection<G, DataflowError, Diff>,
            token: Rc<dyn Any>,
            name: &str,
            _dataflow_id: GlobalId,
        ) {
            let boundary = SourceBoundary::new(name, token);

            // Ensure that a channel pair exists.
            if !self.send.contains_key(&id) {
                let (send, recv) = crossbeam_channel::unbounded();
                self.send.insert(id.clone(), send);
                self.recv.insert(id.clone(), recv);
            }

            self.send[&id]
                .send(boundary.clone())
                .expect("Failed to transmit source");

            use timely::dataflow::operators::Capture;
            ok.inner.capture_into(boundary.ok);
            err.inner.capture_into(boundary.err);
        }
    }

    impl ComputeReplay for EventLinkBoundary {
        fn replay<G: Scope<Timestamp = mz_repr::Timestamp>>(
            &mut self,
            id: SourceInstanceKey,
            scope: &mut G,
            name: &str,
            _dataflow_id: GlobalId,
        ) -> (
            Collection<G, Row, Diff>,
            Collection<G, DataflowError, Diff>,
            Rc<dyn Any>,
        ) {
            if let Some(persist_desc) = id.persist.clone() {
                info!("Replaying persistent source {:?}", id);

                let persist = self.persist.as_mut().expect("missing persist runtime");

                match persist_desc.envelope_desc {
                    mz_dataflow_types::sources::persistence::EnvelopePersistDesc::Upsert => {
                        todo!("envelope UPSERT replay not yet implemented")
                    }
                    mz_dataflow_types::sources::persistence::EnvelopePersistDesc::None => {
                        let stream_name = persist_desc.primary_stream;

                        let (_write, read) =
                            persist.create_or_load::<Result<Row, DecodeError>, ()>(&stream_name);

                        let (persist_ok_stream, persist_err_stream) = scope
                            .persisted_source(read, &Antichain::from_elem(persist_desc.since_ts))
                            .ok_err(|x| match x {
                                (Ok(kv), ts, diff) => Ok((kv, ts, diff)),
                                (Err(err), ts, diff) => Err((err, ts, diff)),
                            });

                        let (persist_ok_stream, decode_err_stream) =
                            persist_ok_stream.ok_err(|((row, ()), ts, diff)| match row {
                                Ok(row) => Ok((row, ts, diff)),
                                Err(e) => Err((e.into(), ts, diff)),
                            });

                        let source_instance_id = SourceInstanceId {
                            source_id: id.identifier.clone(),
                            dataflow_id: 0,
                        };

                        let persist_err_collection = persist_err_stream
                            .map(move |(err, ts, diff)| {
                                let err = SourceError::new(
                                    source_instance_id.clone(),
                                    SourceErrorDetails::Persistence(err),
                                );
                                (err.into(), ts, diff)
                            })
                            .concat(&decode_err_stream)
                            .as_collection();

                        (
                            persist_ok_stream.as_collection(),
                            persist_err_collection,
                            Rc::new(()),
                        )
                    }
                }
            } else {
                // Ensure that a channel pair exists.
                if !self.send.contains_key(&id) {
                    let (send, recv) = crossbeam_channel::unbounded();
                    self.send.insert(id.clone(), send);
                    self.recv.insert(id.clone(), recv);
                }

                let source = self.recv[&id].recv().expect("Unable to acquire source");

                let ok = Some(source.ok.inner)
                    .mz_replay(
                        scope,
                        &format!("{name}-ok"),
                        Duration::MAX,
                        source.ok.activator,
                    )
                    .as_collection();
                let err = Some(source.err.inner)
                    .mz_replay(
                        scope,
                        &format!("{name}-err"),
                        Duration::MAX,
                        source.err.activator,
                    )
                    .as_collection();

                (ok, err, source.token)
            }
        }
    }

    /// Information about each source that must be communicated between storage and compute layers.
    #[derive(Clone)]
    pub struct SourceBoundary {
        /// Captured `row` updates representing a differential collection.
        pub ok: ActivatedEventPusher<
            Rc<EventLink<mz_repr::Timestamp, (Row, mz_repr::Timestamp, mz_repr::Diff)>>,
        >,
        /// Captured error updates representing a differential collection.
        pub err: ActivatedEventPusher<
            Rc<EventLink<mz_repr::Timestamp, (DataflowError, mz_repr::Timestamp, mz_repr::Diff)>>,
        >,
        /// A token that should be dropped to terminate the source.
        pub token: Rc<dyn std::any::Any>,
    }

    impl SourceBoundary {
        /// Create a new boundary, from a name and a token.
        fn new(name: &str, token: Rc<dyn Any>) -> Self {
            let ok_activator = RcActivator::new(format!("{name}-ok"), 1);
            let err_activator = RcActivator::new(format!("{name}-err"), 1);
            let ok_handle = ActivatedEventPusher::new(Rc::new(EventLink::new()), ok_activator);
            let err_handle = ActivatedEventPusher::new(Rc::new(EventLink::new()), err_activator);
            SourceBoundary {
                ok: ActivatedEventPusher::<_>::clone(&ok_handle),
                err: ActivatedEventPusher::<_>::clone(&err_handle),
                token,
            }
        }
    }
}
