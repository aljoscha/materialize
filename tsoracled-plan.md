# Plan: `tsoracled` — Timestamp Oracle gRPC Service

## Context

Every `read_ts` call currently hits CRDB over the network via `PostgresTimestampOracle`.
The `BatchingTimestampOracle` helps by coalescing concurrent callers, but each batch
still requires a CRDB round-trip. A dedicated timestamp oracle service (`tsoracled`)
eliminates most CRDB round-trips by serving timestamps from memory and only persisting
to CRDB periodically (pre-allocation window), similar to TiKV's TSO.

**Correctness**: tsoracled IS the authoritative oracle — every gRPC call is a real
oracle call made during the query's real-time interval. Multiple environmentd nodes
all go through the same tsoracled, so distributed correctness is maintained. This is
NOT caching (which GUIDE.md rightly rejects).

---

## Crate Structure

### Problem: circular dependency
- `tsoracled` (server) needs `timestamp-oracle` for `PostgresTimestampOracleConfig`
- `timestamp-oracle` (client) would need `tsoracled` for generated proto types

### Solution: proto + client live in `timestamp-oracle` (feature-gated)

The proto file and generated tonic stubs (both client AND server) go in
`src/timestamp-oracle/` behind a `grpc` feature flag. The `tsoracled` binary crate
depends on `timestamp-oracle` with `features = ["grpc"]` and implements the server
using the generated server trait. No circular dependency.

```
src/timestamp-oracle/          # existing crate, extended
  build.rs                     # NEW — tonic_prost_build (only when "grpc" feature)
  src/
    service.proto              # NEW — gRPC service definition
    lib.rs                     # add `pub mod grpc_oracle;` (feature-gated)
    grpc_oracle.rs             # NEW — GrpcTimestampOracle client (TimestampOracle impl)
    grpc_server.rs             # NEW — TsoracledServer (tonic service impl)
    ...existing modules...

src/tsoracled/                 # NEW binary crate
  Cargo.toml
  src/
    bin/
      tsoracled.rs             # Binary entry point (clap Args, main)
    lib.rs                     # Re-exports from timestamp-oracle grpc modules
```

---

## Proto Service Definition

File: `src/timestamp-oracle/src/service.proto`

```protobuf
syntax = "proto3";
package mz_timestamp_oracle.service;

message ProtoTimeline {
  string timeline = 1;
}

message ProtoWriteTimestamp {
  uint64 timestamp = 1;
  uint64 advance_to = 2;
}

message ProtoTimestamp {
  uint64 timestamp = 1;
}

message ProtoApplyWriteRequest {
  string timeline = 1;
  uint64 lower_bound = 2;
}

message ProtoOpenTimelineRequest {
  string timeline = 1;
  uint64 initially = 2;
  bool read_only = 3;
}

message ProtoOpenTimelineResponse {}

message ProtoGetAllTimelinesRequest {}

message ProtoTimelineTimestamp {
  string timeline = 1;
  uint64 timestamp = 2;
}

message ProtoGetAllTimelinesResponse {
  repeated ProtoTimelineTimestamp timelines = 1;
}

message ProtoEmpty {}

service ProtoTsOracle {
  rpc WriteTs(ProtoTimeline) returns (ProtoWriteTimestamp);
  rpc PeekWriteTs(ProtoTimeline) returns (ProtoTimestamp);
  rpc ReadTs(ProtoTimeline) returns (ProtoTimestamp);
  rpc ApplyWrite(ProtoApplyWriteRequest) returns (ProtoEmpty);
  rpc OpenTimeline(ProtoOpenTimelineRequest) returns (ProtoOpenTimelineResponse);
  rpc GetAllTimelines(ProtoGetAllTimelinesRequest) returns (ProtoGetAllTimelinesResponse);
}
```

Timestamps are `uint64` (matching `mz_repr::Timestamp`), not DECIMAL strings.

---

## Server Design (`grpc_server.rs` + `tsoracled` binary)

### Core Architecture

```
TsoracledServer
  ├── timelines: RwLock<HashMap<String, Arc<TimelineState>>>
  ├── pg_config: PostgresTimestampOracleConfig   (for CRDB persistence)
  └── metrics: Arc<Metrics>

TimelineState
  └── inner: tokio::sync::Mutex<TimelineInner>

TimelineInner
  ├── read_ts: Timestamp
  ├── write_ts: Timestamp
  ├── persisted_upper: Timestamp   (invariant: >= write_ts)
  ├── window_size: u64             (default: 1000 = 1 second)
  ├── now_fn: NowFn
  ├── read_only: bool
  └── pg_client: Arc<PostgresClient>  (shared CRDB connection pool)
```

### Pre-allocation Window Algorithm

1. **`open_timeline(timeline, initially)`**:
   - Read existing state from CRDB (same `SELECT` as `PostgresTimestampOracle`)
   - Initialize `read_ts = max(initially, crdb_read_ts)`, `write_ts = max(initially, crdb_write_ts)`
   - Persist `write_ts + window_size` to CRDB
   - Set `persisted_upper = write_ts + window_size`

2. **`write_ts(timeline)`** — may or may not hit CRDB:
   - Lock `TimelineInner`
   - `proposed = max(write_ts + 1, now_fn())`
   - If `proposed < persisted_upper`: serve from memory, `write_ts = proposed`
   - If `proposed >= persisted_upper`: extend window — persist `proposed + window_size` to CRDB, update `persisted_upper`
   - Return `WriteTimestamp { timestamp: write_ts, advance_to: write_ts + 1 }`

3. **`read_ts(timeline)`** — pure memory, no CRDB (main perf win):
   - Lock `TimelineInner`, return `read_ts`

4. **`apply_write(timeline, lower_bound)`** — may hit CRDB:
   - Lock `TimelineInner`
   - `read_ts = max(read_ts, lower_bound)`
   - `write_ts = max(write_ts, lower_bound)`
   - If `write_ts >= persisted_upper`: extend window and persist

5. **`peek_write_ts(timeline)`** — pure memory:
   - Lock `TimelineInner`, return `write_ts`

### Crash Recovery

On restart, tsoracled reads from CRDB. The `persisted_upper` is the recovery point.
Timestamps between last-served and `persisted_upper` are "wasted" (gaps), which is
fine — timestamps are opaque, not dense.

### Concurrency

- Per-timeline `tokio::sync::Mutex` serializes operations (required by oracle contract)
- `RwLock<HashMap>` on the timeline map: read lock for lookups (hot), write lock for `open_timeline` (cold)
- Different timelines proceed independently

### CRDB Schema

Reuse the **existing** `timestamp_oracle` table schema (same SQL in `postgres_oracle.rs`):
```sql
CREATE TABLE IF NOT EXISTS timestamp_oracle (
    timeline text NOT NULL,
    read_ts DECIMAL(20,0) NOT NULL,
    write_ts DECIMAL(20,0) NOT NULL,
    PRIMARY KEY(timeline)
);
```

This allows seamless migration — tsoracled can take over from the direct Postgres oracle
with no schema changes.

---

## Client Design (`grpc_oracle.rs`)

### Structure

```rust
#[derive(Debug)]
pub struct GrpcTimestampOracle {
    timeline: String,
    client: ProtoTsOracleClient<tonic::transport::Channel>,
    metrics: Arc<Metrics>,
}
```

### Construction

```rust
pub struct GrpcTimestampOracleConfig {
    pub url: String,  // e.g., "http://127.0.0.1:6880"
    pub metrics: Arc<Metrics>,
}

impl GrpcTimestampOracle {
    pub async fn open(
        config: GrpcTimestampOracleConfig,
        timeline: String,
        initially: Timestamp,
        now_fn: NowFn,
        read_only: bool,
    ) -> Self {
        let channel = Endpoint::from_shared(config.url)
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(5))
            .http2_keep_alive_interval(Duration::from_secs(10))
            .connect().await.expect("connect to tsoracled");

        let mut client = ProtoTsOracleClient::new(channel);
        client.open_timeline(ProtoOpenTimelineRequest { ... }).await;
        Self { timeline, client, metrics }
    }
}
```

### TimestampOracle Implementation

Each method calls the corresponding gRPC RPC with retry logic (using the existing
`retry.rs` patterns — exponential backoff with jitter). The `read_ts` hot path is
kept minimal (no tracing span, matching the postgres oracle optimization).

### Connection to BatchingTimestampOracle

The `GrpcTimestampOracle` is wrapped in `BatchingTimestampOracle` just like other
backends — this provides additional client-side batching on top of the fast gRPC calls.

---

## Integration with `TimestampOracleConfig`

### New variant in config.rs

```rust
pub enum TimestampOracleConfig {
    Postgres(PostgresTimestampOracleConfig),
    #[cfg(feature = "foundationdb")]
    Fdb(FdbTimestampOracleConfig),
    #[cfg(feature = "grpc")]
    Grpc(GrpcTimestampOracleConfig),
}
```

Update `open()`, `get_all_timelines()`, `metrics()`, and `apply_parameters()` with
the new arm. No changes needed in `src/adapter/src/coord/timeline.rs` — it already
dispatches through `TimestampOracleConfig::open()`.

---

## Integration with `environmentd`

### New CLI Arg (in `src/environmentd/src/environmentd/main.rs`)

```rust
/// URL of the tsoracled gRPC service for timestamp oracle.
/// If set, uses the gRPC timestamp oracle instead of direct Postgres.
#[clap(long, env = "TSORACLED_URL")]
tsoracled_url: Option<String>,
```

### Config Wiring (in `main.rs` run function, ~line 1019)

If `--tsoracled-url` is provided, create a `GrpcTimestampOracleConfig` and pass it
as `TimestampOracleConfig::Grpc` instead of the `Postgres` variant. The
`--timestamp-oracle-url` remains required — tsoracled needs it for its CRDB backing
store; environmentd passes it through when spawning tsoracled.

```rust
let timestamp_oracle_config = if let Some(tsoracled_url) = args.tsoracled_url {
    TimestampOracleConfig::Grpc(GrpcTimestampOracleConfig::new(&tsoracled_url, &metrics_registry))
} else {
    // existing Postgres path
    TimestampOracleConfig::Postgres(pg_timestamp_oracle_config)
};
```

### Startup: `bin/environmentd` (via `run.py`) starts tsoracled as a sibling process

The `misc/python/materialize/cli/run.py` script already has infrastructure for
building and managing sibling services. Changes needed:

1. **Add to `REQUIRED_SERVICES`** (line 41):
   ```python
   REQUIRED_SERVICES = ["clusterd", "tsoracled"]
   ```
   This ensures `tsoracled` is built alongside environmentd and that
   `_handle_lingering_services` cleans up stale tsoracled processes.

2. **Start tsoracled before environmentd** (after line 311, in the `if args.program == "environmentd"` block):
   ```python
   # Start tsoracled as a sibling process
   tsoracled_binary = binaries_dir / "tsoracled"
   tsoracled_proc = subprocess.Popen([
       str(tsoracled_binary),
       f"--timestamp-oracle-url={args.postgres}?options=--search_path=tsoracle",
       "--listen-addr=127.0.0.1:6880",
   ], env=env)
   atexit.register(lambda: tsoracled_proc.terminate())
   time.sleep(0.5)  # Brief wait for tsoracled to start listening
   ```

3. **Pass `--tsoracled-url` to environmentd** (add to command list ~line 316):
   ```python
   f"--tsoracled-url=http://127.0.0.1:6880",
   ```

In production, tsoracled is deployed as a standalone service. The `--tsoracled-url`
flag on environmentd points to it.

### `tsoracled` Binary

`src/tsoracled/src/bin/tsoracled.rs`:
```rust
#[derive(clap::Parser)]
struct Args {
    /// Address to listen on for gRPC connections.
    #[clap(long, default_value = "127.0.0.1:6880")]
    listen_addr: SocketAddr,

    /// Postgres/CRDB URL for durable timestamp storage.
    #[clap(long, env = "TIMESTAMP_ORACLE_URL")]
    timestamp_oracle_url: SensitiveUrl,

    /// Pre-allocation window size in timestamp units (ms for EpochMilliseconds).
    #[clap(long, default_value = "1000")]
    window_size: u64,
}
```

Port 6880 (next after persist pubsub's 6879).

---

## Dependency Changes

### `src/timestamp-oracle/Cargo.toml` — new deps (feature-gated)
```toml
[dependencies]
prost = { version = "0.14.3", optional = true }
tonic = { version = "0.14.2", optional = true }
tonic-prost = { version = "0.14.2", optional = true }

[build-dependencies]
mz-build-tools = { path = "../build-tools", default-features = false, features = ["protobuf-src"] }
prost-build = { version = "0.14.3", optional = true }
tonic-prost-build = { version = "0.14.2", optional = true }

[features]
grpc = ["prost", "tonic", "tonic-prost", "prost-build", "tonic-prost-build"]
```

### `src/tsoracled/Cargo.toml` — new crate
```toml
[dependencies]
mz-timestamp-oracle = { path = "../timestamp-oracle", features = ["grpc"] }
mz-ore = { path = "../ore", features = ["cli", "async", "tracing"] }
mz-build-info = { path = "../build-info" }
mz-repr = { path = "../repr" }
clap, tokio, tonic, tracing, anyhow
```

### `src/environmentd/Cargo.toml` — add dep
```toml
mz-timestamp-oracle = { path = "../timestamp-oracle", features = ["grpc"] }
```

### Workspace `Cargo.toml` — register new crate
Add `"src/tsoracled"` to `members` and `default-members`.

### After dependency changes: `cargo hakari generate && cargo hakari manage-deps`

---

## Files to Create/Modify (summary)

| Action | File |
|--------|------|
| CREATE | `src/timestamp-oracle/src/service.proto` |
| CREATE | `src/timestamp-oracle/build.rs` |
| CREATE | `src/timestamp-oracle/src/grpc_oracle.rs` |
| CREATE | `src/timestamp-oracle/src/grpc_server.rs` |
| MODIFY | `src/timestamp-oracle/src/lib.rs` — add modules |
| MODIFY | `src/timestamp-oracle/src/config.rs` — add Grpc variant |
| MODIFY | `src/timestamp-oracle/Cargo.toml` — add grpc deps |
| CREATE | `src/tsoracled/Cargo.toml` |
| CREATE | `src/tsoracled/src/lib.rs` |
| CREATE | `src/tsoracled/src/bin/tsoracled.rs` |
| MODIFY | `src/environmentd/src/environmentd/main.rs` — CLI arg + config wiring |
| MODIFY | `src/environmentd/src/lib.rs` — config field |
| MODIFY | `src/environmentd/Cargo.toml` — add dep |
| MODIFY | `Cargo.toml` (workspace) — register crate |
| MODIFY | `misc/python/materialize/cli/run.py` — build + start tsoracled as sibling |

---

## Testing

1. **Shared oracle test harness**: Run `crate::tests::timestamp_oracle_impl_test`
   against `GrpcTimestampOracle` (start in-process tsoracled on ephemeral port)

2. **Batching integration**: Test `BatchingTimestampOracle<GrpcTimestampOracle>`

3. **Server unit tests**: Test `TimelineState` pre-allocation window logic,
   monotonicity, crash recovery (mock CRDB persistence)

4. **End-to-end**: Run existing sqllogictest suites with tsoracled enabled

---

## Verification

1. `cargo build -p mz-tsoracled` — binary compiles
2. `cargo test -p mz-timestamp-oracle --features grpc` — oracle impl tests pass
3. `bin/environmentd --reset -- --all-features --unsafe-mode` with tsoracled enabled — coordinator boots and serves queries
4. `psql -h localhost -p 6875 -U materialize -c "SELECT 1"` — basic query works
5. Compare `read_ts` latency: gRPC oracle vs direct Postgres oracle via prometheus metrics
