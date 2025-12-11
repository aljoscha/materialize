// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Widget spawning and management for the coordinator.

use tokio::sync::mpsc;

use crate::coord::{Coordinator, Message};
use crate::widget::{DummyWidget, WidgetMessage, WidgetRuntime, WidgetRuntimeConfig};

impl Coordinator {
    /// Spawn the widget runtime and register widgets.
    ///
    /// This creates a widget runtime, spawns the dummy widget for testing,
    /// and sets up message forwarding to the coordinator's internal command channel.
    pub(crate) fn spawn_widget_runtime(&self) {
        let internal_cmd_tx = self.internal_cmd_tx.clone();
        let read_only = self.controller.read_only();
        let now = self.catalog().config().now.clone();

        // Create channels for widget messages
        let (widget_msg_tx, mut widget_msg_rx) = mpsc::unbounded_channel::<WidgetMessage>();

        // Create the widget runtime
        let config = WidgetRuntimeConfig { now, read_only };
        let mut runtime = WidgetRuntime::new(config);

        // Spawn the dummy widget for testing
        let dummy_widget = Box::new(DummyWidget::new());
        let _handle = runtime.spawn_widget(dummy_widget, widget_msg_tx);

        tracing::info!("Widget runtime started with dummy_widget");

        // Spawn a task to forward widget messages to the coordinator
        mz_ore::task::spawn(|| "widget_message_forwarder", async move {
            // Keep the runtime alive in this task
            let _runtime = runtime;

            while let Some(msg) = widget_msg_rx.recv().await {
                tracing::debug!("Forwarding widget message to coordinator");
                if let Err(e) = internal_cmd_tx.send(Message::Widget(msg)) {
                    tracing::warn!("Failed to forward widget message to coordinator: {}", e);
                    break;
                }
            }
            tracing::info!("Widget message forwarder exiting");
        });
    }
}
