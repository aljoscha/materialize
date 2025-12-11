// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Widget spawning and management for the coordinator.

use std::sync::Arc;

use tokio::sync::mpsc;

use crate::coord::{Coordinator, Message};
use crate::widget::auto_scaling::AutoScalingWidget;
use crate::widget::{WidgetDataRequest, WidgetMessage, WidgetRuntime, WidgetRuntimeConfig};

impl Coordinator {
    /// Spawn the widget runtime and register widgets.
    ///
    /// This creates a widget runtime, spawns widgets, and sets up message
    /// forwarding to the coordinator's internal command channel.
    pub(crate) fn spawn_widget_runtime(&self) {
        if !self.catalog().system_config().enable_auto_scaling() {
            tracing::info!("Widget runtime not started: enable_auto_scaling feature flag is off");
            return;
        }

        let internal_cmd_tx = self.internal_cmd_tx.clone();
        let read_only = self.controller.read_only();
        let now = self.catalog().config().now.clone();
        let catalog = Arc::clone(&self.catalog);

        // Create channels for widget messages (actions going to coordinator)
        let (widget_msg_tx, mut widget_msg_rx) = mpsc::unbounded_channel::<WidgetMessage>();

        // Create channel for widget data requests (queries from widget to coordinator)
        // These are forwarded to the coordinator's main message loop for handling.
        let (request_tx, mut request_rx) = mpsc::unbounded_channel::<WidgetDataRequest>();

        // Create the widget runtime
        let config = WidgetRuntimeConfig {
            now,
            read_only,
            catalog: Arc::clone(&catalog),
            request_tx,
        };
        let mut runtime = WidgetRuntime::new(config);

        // Spawn the auto-scaling widget
        let auto_scaling_widget = Box::new(AutoScalingWidget::new());
        let _auto_scaling_handle = runtime.spawn_widget(auto_scaling_widget, widget_msg_tx);

        tracing::info!("Widget runtime started with auto_scaling widget");

        // Spawn a task to forward widget messages to the coordinator
        let internal_cmd_tx_for_actions = internal_cmd_tx.clone();
        mz_ore::task::spawn(|| "widget_message_forwarder", async move {
            // Keep the runtime alive in this task
            let _runtime = runtime;

            while let Some(msg) = widget_msg_rx.recv().await {
                tracing::debug!("Forwarding widget message to coordinator");
                if let Err(e) = internal_cmd_tx_for_actions.send(Message::Widget(msg)) {
                    tracing::warn!("Failed to forward widget message to coordinator: {}", e);
                    break;
                }
            }
            tracing::info!("Widget message forwarder exiting");
        });

        // Spawn a task to forward widget data requests to the coordinator
        // Data requests go through the main message loop so the coordinator can
        // execute queries against storage.
        mz_ore::task::spawn(|| "widget_request_forwarder", async move {
            while let Some(request) = request_rx.recv().await {
                tracing::debug!("Forwarding widget data request to coordinator");
                if let Err(e) = internal_cmd_tx.send(Message::WidgetDataRequest(request)) {
                    tracing::warn!(
                        "Failed to forward widget data request to coordinator: {}",
                        e
                    );
                    break;
                }
            }
            tracing::info!("Widget request forwarder exiting");
        });
    }
}
