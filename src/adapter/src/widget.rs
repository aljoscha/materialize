// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Widget system for semi-autonomous extensions within environmentd.
//!
//! A widget is a semi-autonomous component that:
//! - Runs in its own async task, isolated from the coordinator main loop
//! - Receives periodic ticks and system events
//! - Can observe system state (read catalog, query builtin tables)
//! - Proposes actions through a channel (DDL operations, state updates)
//! - Persists all state in builtin tables (no other coordinator state)
//!
//! The isolation is important: widget bugs or slow operations should not block
//! critical coordinator functions.

use std::collections::BTreeMap;
use std::time::Duration;

use async_trait::async_trait;
use mz_controller_types::ClusterId;
use mz_ore::now::NowFn;
use mz_ore::task::spawn;
use mz_repr::GlobalId;
use thiserror::Error;
use tokio::select;
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;
use tracing::{debug, error, info, warn};

/// A semi-autonomous extension that runs within environmentd.
///
/// Each widget runs in its own async task. The runtime guarantees that
/// `initialize`, `tick`, `on_event`, and `shutdown` are never executed
/// concurrently for a given widget instance—all are called from a single
/// async task using a select! loop.
#[async_trait]
pub trait Widget: Send + Sync + 'static {
    /// Unique identifier for this widget type.
    fn name(&self) -> &'static str;

    /// The interval between periodic ticks for this widget.
    /// This is static for the lifetime of the widget.
    fn tick_interval(&self) -> Duration;

    /// Called during coordinator startup to initialize the widget.
    /// Validates configuration and prepares for operation. Should NOT cache
    /// state in memory—state is read fresh from builtin tables on each tick.
    /// If this fails, the widget is disabled but environmentd continues.
    async fn initialize(&mut self, ctx: &WidgetContext) -> Result<(), WidgetError>;

    /// Called periodically to perform widget logic.
    /// Each tick reads current state from builtin tables (source of truth),
    /// collects signals, makes decisions, executes actions, and writes state.
    /// Events may trigger earlier evaluation but must respect cooldowns.
    async fn tick(&mut self, ctx: &WidgetContext) -> Result<Vec<WidgetAction>, WidgetError>;

    /// Called when relevant system events occur.
    async fn on_event(
        &mut self,
        event: WidgetEvent,
        ctx: &WidgetContext,
    ) -> Result<Vec<WidgetAction>, WidgetError>;

    /// Called during coordinator shutdown with a timeout for graceful cleanup.
    async fn shutdown(&mut self, ctx: &WidgetContext) -> Result<(), WidgetError>;
}

/// Read-only context provided to widgets for observing the system.
///
/// Widgets use this to read from the catalog and builtin tables.
/// All writes must go through `WidgetAction` to be processed by the coordinator.
#[derive(Clone)]
pub struct WidgetContext {
    /// Current wall-clock time (milliseconds since epoch).
    pub now_millis: u64,
    /// Whether the system is in read-only mode.
    pub read_only: bool,
}

impl WidgetContext {
    /// Create a new widget context.
    pub fn new(now_millis: u64, read_only: bool) -> Self {
        Self {
            now_millis,
            read_only,
        }
    }
}

/// Events that widgets can react to.
#[derive(Debug, Clone)]
pub enum WidgetEvent {
    /// Replicas in a cluster have changed (created, dropped, or status changed).
    ClusterReplicasChanged { cluster_id: ClusterId },
    /// Hydration status changed for a replica.
    ReplicaHydrationChanged {
        cluster_id: ClusterId,
        replica_id: GlobalId,
    },
    /// A replica crashed.
    ReplicaCrashed {
        cluster_id: ClusterId,
        replica_id: GlobalId,
        reason: CrashReason,
    },
    /// Activity was detected on a cluster (query executed).
    ClusterActivity { cluster_id: ClusterId },
    /// Widget configuration changed.
    ConfigurationChanged {
        widget_name: String,
        cluster_id: Option<ClusterId>,
    },
}

/// Reason for a replica crash.
#[derive(Debug, Clone)]
pub enum CrashReason {
    /// Out of memory.
    Oom,
    /// Other crash reason.
    Other(String),
}

/// Actions that widgets request from the coordinator.
#[derive(Debug, Clone)]
pub enum WidgetAction {
    /// Execute a DDL statement (e.g., CREATE/DROP CLUSTER REPLICA).
    ExecuteDdl {
        /// The DDL SQL statement to execute.
        sql: String,
        /// Human-readable reason for this action.
        reason: String,
    },
    /// Write rows to a builtin table.
    WriteToBuiltinTable {
        /// The global ID of the builtin table.
        table_id: GlobalId,
        /// Rows to write (with diffs).
        rows: Vec<(mz_repr::Row, mz_repr::Diff)>,
    },
    /// Delete rows from a builtin table.
    DeleteFromBuiltinTable {
        /// The global ID of the builtin table.
        table_id: GlobalId,
        /// Filter for rows to delete (represented as a Row pattern).
        filter: mz_repr::Row,
    },
}

/// Error types for widget operations.
#[derive(Debug, Error)]
pub enum WidgetError {
    /// Configuration error.
    #[error("configuration error: {0}")]
    Config(String),
    /// Signal collection failed.
    #[error("signal collection failed: {0}")]
    SignalCollection(String),
    /// DDL execution failed.
    #[error("DDL execution failed: {0}")]
    DdlExecution(String),
    /// State persistence failed.
    #[error("state persistence failed: {0}")]
    StatePersistence(String),
    /// Internal error.
    #[error("internal error: {0}")]
    Internal(String),
}

// ============================================================================
// Widget Runtime
// ============================================================================

/// Configuration for the widget runtime.
#[derive(Clone)]
pub struct WidgetRuntimeConfig {
    /// Function to get the current time in milliseconds since epoch.
    pub now: NowFn,
    /// Whether the system is in read-only mode.
    pub read_only: bool,
}

/// Handle to send events to a widget.
#[derive(Clone)]
pub struct WidgetHandle {
    event_tx: mpsc::UnboundedSender<WidgetEvent>,
    name: &'static str,
}

impl WidgetHandle {
    /// Send an event to the widget.
    pub fn send_event(&self, event: WidgetEvent) {
        if let Err(e) = self.event_tx.send(event) {
            debug!(
                widget = self.name,
                "failed to send event to widget (shutdown?): {}", e
            );
        }
    }
}

/// Message from a widget to the coordinator.
#[derive(Debug)]
pub enum WidgetMessage {
    /// Widget produced actions that should be executed.
    Actions {
        widget_name: &'static str,
        actions: Vec<WidgetAction>,
    },
    /// Widget encountered an error.
    Error {
        widget_name: &'static str,
        error: WidgetError,
    },
}

/// Runtime that manages widget tasks.
pub struct WidgetRuntime {
    /// Configuration for the runtime.
    config: WidgetRuntimeConfig,
    /// Handles to send events to widgets.
    handles: BTreeMap<&'static str, WidgetHandle>,
    /// Channel to send shutdown signals to widgets.
    shutdown_txs: BTreeMap<&'static str, mpsc::Sender<()>>,
}

impl WidgetRuntime {
    /// Create a new widget runtime.
    pub fn new(config: WidgetRuntimeConfig) -> Self {
        Self {
            config,
            handles: BTreeMap::new(),
            shutdown_txs: BTreeMap::new(),
        }
    }

    /// Spawn a widget task and register it with the runtime.
    ///
    /// Returns a handle that can be used to send events to the widget.
    ///
    /// The `message_tx` channel is used by the widget to send messages (actions,
    /// errors) back to the coordinator.
    pub fn spawn_widget(
        &mut self,
        mut widget: Box<dyn Widget>,
        message_tx: mpsc::UnboundedSender<WidgetMessage>,
    ) -> WidgetHandle {
        let name = widget.name();
        let tick_interval = widget.tick_interval();
        let now = self.config.now.clone();
        let read_only = self.config.read_only;

        // Channel for sending events to the widget
        let (event_tx, mut event_rx) = mpsc::unbounded_channel::<WidgetEvent>();

        // Channel for shutdown signal
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);

        let handle = WidgetHandle {
            event_tx: event_tx.clone(),
            name,
        };

        // Spawn the widget task
        spawn(|| format!("widget:{}", name), async move {
            // Create initial context
            let ctx = WidgetContext::new(now(), read_only);

            // Initialize the widget
            info!(widget = name, "initializing widget");
            if let Err(e) = widget.initialize(&ctx).await {
                error!(widget = name, error = %e, "widget initialization failed, disabling");
                let _ = message_tx.send(WidgetMessage::Error {
                    widget_name: name,
                    error: e,
                });
                return;
            }
            info!(widget = name, "widget initialized");

            // Set up tick timer
            let mut tick_timer = tokio::time::interval(tick_interval);
            tick_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                select! {
                    biased;

                    // Shutdown signal has highest priority
                    _ = shutdown_rx.recv() => {
                        info!(widget = name, "widget received shutdown signal");
                        let ctx = WidgetContext::new(now(), read_only);
                        if let Err(e) = widget.shutdown(&ctx).await {
                            warn!(widget = name, error = %e, "widget shutdown failed");
                        }
                        break;
                    }

                    // Process events
                    Some(event) = event_rx.recv() => {
                        let ctx = WidgetContext::new(now(), read_only);
                        debug!(widget = name, event = ?event, "widget processing event");
                        match widget.on_event(event, &ctx).await {
                            Ok(actions) => {
                                if !actions.is_empty() {
                                    let _ = message_tx.send(WidgetMessage::Actions {
                                        widget_name: name,
                                        actions,
                                    });
                                }
                            }
                            Err(e) => {
                                warn!(widget = name, error = %e, "widget event handling failed");
                                let _ = message_tx.send(WidgetMessage::Error {
                                    widget_name: name,
                                    error: e,
                                });
                            }
                        }
                    }

                    // Periodic tick
                    _ = tick_timer.tick() => {
                        let ctx = WidgetContext::new(now(), read_only);
                        debug!(widget = name, "widget tick");
                        match widget.tick(&ctx).await {
                            Ok(actions) => {
                                if !actions.is_empty() {
                                    debug!(widget = name, num_actions = actions.len(), "widget produced actions");
                                    let _ = message_tx.send(WidgetMessage::Actions {
                                        widget_name: name,
                                        actions,
                                    });
                                }
                            }
                            Err(e) => {
                                warn!(widget = name, error = %e, "widget tick failed");
                                let _ = message_tx.send(WidgetMessage::Error {
                                    widget_name: name,
                                    error: e,
                                });
                            }
                        }
                    }
                }
            }

            info!(widget = name, "widget task exiting");
        });

        self.handles.insert(name, handle.clone());
        self.shutdown_txs.insert(name, shutdown_tx);

        handle
    }

    /// Get a handle to a widget by name.
    pub fn get_handle(&self, name: &str) -> Option<&WidgetHandle> {
        self.handles.get(name)
    }

    /// Shutdown all widgets gracefully.
    ///
    /// This sends shutdown signals to all widgets and waits for them to complete
    /// (with a timeout).
    pub async fn shutdown_all(&mut self, timeout: Duration) {
        info!("shutting down all widgets");

        // Send shutdown signals to all widgets
        let shutdown_txs = std::mem::take(&mut self.shutdown_txs);
        for (name, tx) in shutdown_txs {
            debug!(widget = name, "sending shutdown signal");
            let _ = tx.send(()).await;
        }

        // Wait for widgets to shutdown (with timeout)
        // Note: In a real implementation, we'd track the join handles and wait
        // for them. For now, we just sleep to give widgets time to shutdown.
        tokio::time::sleep(timeout.min(Duration::from_secs(5))).await;

        self.handles.clear();
        info!("all widgets shut down");
    }

    /// Broadcast an event to all widgets.
    pub fn broadcast_event(&self, event: WidgetEvent) {
        for handle in self.handles.values() {
            handle.send_event(event.clone());
        }
    }
}

// ============================================================================
// Dummy Widget for Testing
// ============================================================================

/// A dummy widget for testing the widget runtime and coordinator communication.
///
/// This widget logs on every tick and event, and periodically produces test
/// actions to verify the action handling pipeline.
pub struct DummyWidget {
    tick_count: u64,
}

impl DummyWidget {
    /// Create a new dummy widget.
    pub fn new() -> Self {
        Self { tick_count: 0 }
    }
}

impl Default for DummyWidget {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Widget for DummyWidget {
    fn name(&self) -> &'static str {
        "dummy_widget"
    }

    fn tick_interval(&self) -> Duration {
        // Tick every 10 seconds for testing
        Duration::from_secs(10)
    }

    async fn initialize(&mut self, ctx: &WidgetContext) -> Result<(), WidgetError> {
        info!(
            widget = self.name(),
            now_millis = ctx.now_millis,
            read_only = ctx.read_only,
            "DummyWidget initialized"
        );
        Ok(())
    }

    async fn tick(&mut self, ctx: &WidgetContext) -> Result<Vec<WidgetAction>, WidgetError> {
        self.tick_count += 1;
        info!(
            widget = self.name(),
            tick_count = self.tick_count,
            now_millis = ctx.now_millis,
            "DummyWidget tick"
        );

        // Every 3rd tick, produce a test action to verify the pipeline
        if self.tick_count % 3 == 0 {
            info!(
                widget = self.name(),
                tick_count = self.tick_count,
                "DummyWidget producing test DDL action"
            );
            Ok(vec![WidgetAction::ExecuteDdl {
                sql: format!(
                    "-- DummyWidget test action (tick {}), not actually executed",
                    self.tick_count
                ),
                reason: format!("Test action from DummyWidget tick {}", self.tick_count),
            }])
        } else {
            Ok(vec![])
        }
    }

    async fn on_event(
        &mut self,
        event: WidgetEvent,
        ctx: &WidgetContext,
    ) -> Result<Vec<WidgetAction>, WidgetError> {
        info!(
            widget = self.name(),
            event = ?event,
            now_millis = ctx.now_millis,
            "DummyWidget received event"
        );
        Ok(vec![])
    }

    async fn shutdown(&mut self, ctx: &WidgetContext) -> Result<(), WidgetError> {
        info!(
            widget = self.name(),
            tick_count = self.tick_count,
            now_millis = ctx.now_millis,
            "DummyWidget shutting down"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    struct TestWidget {
        tick_count: u32,
        event_count: u32,
    }

    impl TestWidget {
        fn new() -> Self {
            Self {
                tick_count: 0,
                event_count: 0,
            }
        }
    }

    #[async_trait]
    impl Widget for TestWidget {
        fn name(&self) -> &'static str {
            "test_widget"
        }

        fn tick_interval(&self) -> Duration {
            Duration::from_millis(100)
        }

        async fn initialize(&mut self, _ctx: &WidgetContext) -> Result<(), WidgetError> {
            Ok(())
        }

        async fn tick(&mut self, _ctx: &WidgetContext) -> Result<Vec<WidgetAction>, WidgetError> {
            self.tick_count += 1;
            Ok(vec![])
        }

        async fn on_event(
            &mut self,
            _event: WidgetEvent,
            _ctx: &WidgetContext,
        ) -> Result<Vec<WidgetAction>, WidgetError> {
            self.event_count += 1;
            Ok(vec![])
        }

        async fn shutdown(&mut self, _ctx: &WidgetContext) -> Result<(), WidgetError> {
            Ok(())
        }
    }

    #[mz_ore::test(tokio::test)]
    async fn test_widget_runtime_basic() {
        let config = WidgetRuntimeConfig {
            now: Arc::new(|| 0),
            read_only: false,
        };
        let mut runtime = WidgetRuntime::new(config);
        let (message_tx, _message_rx) = mpsc::unbounded_channel();

        let widget = Box::new(TestWidget::new());
        let handle = runtime.spawn_widget(widget, message_tx);

        assert_eq!(handle.name, "test_widget");

        // Give the widget time to initialize and tick a few times
        tokio::time::sleep(Duration::from_millis(350)).await;

        // Shutdown
        runtime.shutdown_all(Duration::from_secs(1)).await;
    }
}
