// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A dedicated timestamp oracle gRPC service (`tsoracled`).
//!
//! This crate provides the `tsoracled` binary, which serves timestamps from
//! memory and only persists to CRDB periodically via a pre-allocation window.
//! This eliminates most CRDB round-trips compared to the direct
//! `PostgresTimestampOracle`.

pub use mz_timestamp_oracle::grpc_server::TsoracledServer;
pub use mz_timestamp_oracle::service::proto_ts_oracle_server::ProtoTsOracleServer;
