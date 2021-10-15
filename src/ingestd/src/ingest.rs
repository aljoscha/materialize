// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::thread;
use std::time::{Duration, Instant};

use tokio::runtime::Handle as TokioHandle;

use ore::thread::{JoinHandleExt as _, JoinOnDropHandle};

/// Serves the ingester based on the provided configuration.
pub async fn serve(_config: Config) -> Result<Handle, anyhow::Error> {
    let handle = TokioHandle::current();
    let thread = thread::Builder::new()
        .name("ingester".to_string())
        .spawn(move || {
            let _ingest = Ingester {};
            handle.block_on(async {
                loop {
                    tokio::time::sleep(Duration::from_millis(1000)).await;
                    println!("Ingesting...");
                }
            })
        })
        .unwrap();

    let start_instant = Instant::now();
    let handle = Handle {
        start_instant,
        _thread: thread.join_on_drop(),
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
    pub(crate) _thread: JoinOnDropHandle<()>,
}

impl Handle {
    /// Returns the instant at which the ingester booted.
    pub fn start_instant(&self) -> Instant {
        self.start_instant
    }
}
