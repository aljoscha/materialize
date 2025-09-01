// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Traits and types for table write operations.

use std::fmt::Debug;
use std::sync::Arc;

use mz_repr::GlobalId;
use mz_storage_types::controller::StorageError;
use timely::progress::Timestamp;
use tokio::sync::oneshot;

use crate::client::TableData;

/// A worker that can perform table write operations.
pub trait TableWriteWorker<T: Timestamp>: Debug + Send + Sync + 'static {
    /// Append data to tables at the given timestamp and advance the upper
    /// frontier.
    ///
    /// Returns a receiver that will indicate success or failure of the append
    /// operation.
    fn append(
        &self,
        write_ts: T,
        advance_to: T,
        updates: Vec<(GlobalId, Vec<TableData>)>,
    ) -> oneshot::Receiver<Result<(), StorageError<T>>>;
}

/// A cloneable wrapper around a table write worker.
///
/// This allows for cheap sharing across async tasks while maintaining the
/// ability to perform table write operations with retry logic.
#[derive(Debug, Clone)]
pub struct CloneableTableWriteWorker<T: Timestamp> {
    inner: Arc<dyn TableWriteWorker<T>>,
}

impl<T: Timestamp> CloneableTableWriteWorker<T> {
    /// Create a new cloneable table write worker from any type that implements
    /// TableWriteWorker.
    pub fn new(worker: Arc<dyn TableWriteWorker<T>>) -> Self {
        Self { inner: worker }
    }

    /// Append data to tables at the given timestamp and advance the upper
    /// frontier.
    ///
    /// Returns a receiver that will indicate success or failure of the append
    /// operation. This operation should be retryable on failure.
    pub fn append(
        &self,
        write_ts: T,
        advance_to: T,
        updates: Vec<(GlobalId, Vec<TableData>)>,
    ) -> oneshot::Receiver<Result<(), StorageError<T>>> {
        self.inner.append(write_ts, advance_to, updates)
    }
}
