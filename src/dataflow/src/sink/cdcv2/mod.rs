// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod kafka;

use std::time::Duration;

use repr::{Diff, Row, Timestamp};

/// A trait for objects which write CDCv2 updates to an external system.
pub trait CdcV2SinkWrite {
    fn write_updates(
        &mut self,
        updates: &[(&Row, &Timestamp, &Diff)],
    ) -> Result<Option<Duration>, anyhow::Error>;

    fn write_progress(
        &mut self,
        lower: &[Timestamp],
        upper: &[Timestamp],
        counts: &[(Timestamp, i64)],
    ) -> Result<Option<Duration>, anyhow::Error>;

    fn errors(&mut self) -> Option<Vec<anyhow::Error>>;

    fn done(&self) -> bool;
}
