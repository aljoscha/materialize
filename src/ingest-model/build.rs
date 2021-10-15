// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = env::var("OUT_DIR")?;
    let descriptor_path = PathBuf::from(out_dir.clone()).join("descriptor.bin");

    tonic_build::configure()
        .out_dir(&out_dir)
        .file_descriptor_set_path(&descriptor_path)
        .compile(&["src/proto/ingest.proto"], &["src/proto"])?;
    Ok(())
}
