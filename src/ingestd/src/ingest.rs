// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::{Duration, Instant};

use tokio::sync::oneshot;

/// Serves the ingester based on the provided configuration.
pub async fn serve(_config: Config) -> Result<Handle, anyhow::Error> {
    let mut update_interval = tokio::time::interval(Duration::from_millis(1000));
    let (drain_trigger, mut drain_tripwire) = oneshot::channel();

    let _ingester = Ingester {};

    tokio::task::spawn(async move {
        loop {
            tokio::select! {
                _ = update_interval.tick() => {
                    println!("Ingesting...")
                },
                _ = &mut drain_tripwire => {
                    println!("Ingester got shutdown signal...");
                    break;
                }
            }
        }
    });

    let start_instant = Instant::now();
    let handle = Handle {
        start_instant,
        // _thread: thread.join_on_drop(),
        _drain_trigger: drain_trigger,
    };

    Ok(handle)
}

/// Configures a ingester.
pub struct Config {}

/// Ingester service.
pub struct Ingester {}

/// A handle to a running ingester.
///
/// The ingester runs on its own thread. Dropping the handle will wait for
/// the ingester's thread to exit, which will only occur after all
/// outstanding [`Client`]s for the ingester have dropped.
pub struct Handle {
    pub(crate) start_instant: Instant,
    pub(crate) _drain_trigger: oneshot::Sender<()>,
}

impl Handle {
    /// Returns the instant at which the ingester booted.
    pub fn start_instant(&self) -> Instant {
        self.start_instant
    }
}
