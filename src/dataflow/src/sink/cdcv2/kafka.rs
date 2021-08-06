// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use dataflow_types::KafkaSinkConnector;
use interchange::avro::cdc_v2::{AvroCdcV2Encoder, AvroCdcV2SchemaGenerator};
use interchange::avro::GenerateAvroSchema;
use log::error;
use ore::metrics::{CounterVecExt, DeleteOnDropCounter, DeleteOnDropGauge, GaugeVecExt};
use prometheus::core::{AtomicI64, AtomicU64};
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Message, ToBytes};
use rdkafka::producer::{BaseRecord, DeliveryResult, Producer, ProducerContext, ThreadedProducer};

use expr::GlobalId;
use repr::{Diff, Row, Timestamp};

use crate::sink::KafkaBaseMetrics;

use super::CdcV2SinkWrite;

/// A Kafka sink that implements [`CdcV2SinkWrite`].
pub struct KafkaCdcV2SinkWriter {
    name: String,
    topic: String,
    encoder: AvroCdcV2Encoder,
    fatal_errors: Arc<Mutex<Vec<KafkaError>>>,
    producer: ThreadedProducer<SinkProducerContext>,
    metrics: SinkMetrics,
}

impl CdcV2SinkWrite for KafkaCdcV2SinkWriter {
    fn write_updates(
        &mut self,
        updates: &[(&Row, &Timestamp, &Diff)],
    ) -> Result<Option<Duration>, anyhow::Error> {
        let encoded = self.encoder.encode_updates(updates);
        let record = BaseRecord::<[u8], _>::to(&self.topic).payload(&encoded);

        let send_result = self.send(record)?;
        Ok(send_result)
    }

    fn write_progress(
        &mut self,
        lower: &[Timestamp],
        upper: &[Timestamp],
        counts: &[(Timestamp, i64)],
    ) -> Result<Option<Duration>, anyhow::Error> {
        let encoded = self.encoder.encode_progress(lower, upper, counts);
        let record = BaseRecord::<[u8], _>::to(&self.topic).payload(&encoded);

        let send_result = self.send(record)?;
        Ok(send_result)
    }

    fn errors(&mut self) -> Option<Vec<anyhow::Error>> {
        let mut fatal_errors = self.fatal_errors.lock().expect("lock poisoned");
        if fatal_errors.is_empty() {
            None
        } else {
            Some(
                fatal_errors
                    .drain(..)
                    .map(|kafka_error| anyhow::anyhow!("{}", kafka_error))
                    .collect(),
            )
        }
    }

    fn done(&self) -> bool {
        // TODO(aljoscha): double-check if this is the right contract. Maybe we don't have to make
        // sure that there are no errors.
        //
        // The reasoning is that we want to a) make sure there are no in-flight records, and b)
        // make sure we didn't get any error messages that could indicate that sending some records
        // failed.
        let fatal_errors = self.fatal_errors.lock().expect("lock poisoned");
        self.producer.in_flight_count() == 0 && fatal_errors.is_empty()
    }
}

impl KafkaCdcV2SinkWriter {
    pub fn new(
        connector: KafkaSinkConnector,
        sink_name: String,
        sink_id: &GlobalId,
        worker_id: String,
        metrics: &KafkaBaseMetrics,
    ) -> Self {
        let config = Self::create_producer_config(&connector);

        let fatal_errors = Arc::new(Mutex::new(Vec::new()));

        let context_metrics =
            SinkMetrics::new(metrics, &connector.topic, &sink_id.to_string(), &worker_id);
        let producer = config
            .create_with_context::<_, ThreadedProducer<_>>(SinkProducerContext::new(
                context_metrics,
                Arc::clone(&fatal_errors),
            ))
            .expect("creating kafka producer for Kafka sink failed");

        let value_schema_id = connector
            .published_schema_info
            .map(|schem_info| schem_info.value_schema_id)
            .expect("missing Avro schema id");

        let schema_generator = <AvroCdcV2SchemaGenerator as GenerateAvroSchema>::new(
            None,
            connector.value_desc,
            false,
        );
        let encoder = AvroCdcV2Encoder::new(schema_generator, value_schema_id);

        let metrics = SinkMetrics::new(metrics, &connector.topic, &sink_id.to_string(), &worker_id);
        KafkaCdcV2SinkWriter {
            name: sink_name,
            topic: connector.topic,
            encoder,
            fatal_errors,
            producer,
            metrics,
        }
    }

    fn create_producer_config(connector: &KafkaSinkConnector) -> ClientConfig {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", &connector.addrs.to_string());

        for (k, v) in connector.config_options.iter() {
            // We explicitly reject `statistics.interval.ms` here so that we don't
            // flood the INFO log with statistics messages.
            // TODO: properly support statistics on Kafka sinks
            // We explicitly reject 'isolation.level' as it's a consumer property
            // and, while benign, will fill the log with WARN messages
            if k != "statistics.interval.ms" && k != "isolation.level" {
                config.set(k, v);
            }
        }

        config
    }

    fn send<K, P>(&self, record: BaseRecord<K, P>) -> Result<Option<Duration>, KafkaError>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
    {
        if let Err((e, _)) = self.producer.send(record) {
            error!("unable to produce message in {}: {}", self.name, e);
            self.metrics.message_send_errors_counter.inc();

            if let KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull) = e {
                // TODO(aljoscha) figure out a better backoff time here
                Ok(Some(Duration::from_secs(1)))
            } else {
                Err(e)
            }
        } else {
            self.metrics.messages_sent_counter.inc();
            Ok(None)
        }
    }
}

/// Metrics for [`KafkaCdcV2SinkWriter`].
struct SinkMetrics {
    messages_sent_counter: DeleteOnDropCounter<'static, AtomicI64, Vec<String>>,
    message_send_errors_counter: DeleteOnDropCounter<'static, AtomicI64, Vec<String>>,
    message_delivery_errors_counter: DeleteOnDropCounter<'static, AtomicI64, Vec<String>>,
    rows_queued: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    messages_in_flight: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
}

impl SinkMetrics {
    fn new(
        base: &KafkaBaseMetrics,
        topic_name: &str,
        sink_id: &str,
        worker_id: &str,
    ) -> SinkMetrics {
        let labels = vec![
            topic_name.to_string(),
            sink_id.to_string(),
            worker_id.to_string(),
        ];
        SinkMetrics {
            messages_sent_counter: base
                .messages_sent_counter
                .get_delete_on_drop_counter(labels.clone()),
            message_send_errors_counter: base
                .message_send_errors_counter
                .get_delete_on_drop_counter(labels.clone()),
            message_delivery_errors_counter: base
                .message_delivery_errors_counter
                .get_delete_on_drop_counter(labels.clone()),
            rows_queued: base.rows_queued.get_delete_on_drop_gauge(labels.clone()),
            messages_in_flight: base.messages_in_flight.get_delete_on_drop_gauge(labels),
        }
    }
}

struct SinkProducerContext {
    metrics: SinkMetrics,
    fatal_errors: Arc<Mutex<Vec<KafkaError>>>,
}

impl SinkProducerContext {
    pub fn new(metrics: SinkMetrics, fatal_errors: Arc<Mutex<Vec<KafkaError>>>) -> Self {
        SinkProducerContext {
            metrics,
            fatal_errors,
        }
    }
}

impl ClientContext for SinkProducerContext {}
impl ProducerContext for SinkProducerContext {
    type DeliveryOpaque = ();

    fn delivery(&self, result: &DeliveryResult, _: Self::DeliveryOpaque) {
        match result {
            Ok(_) => (),
            Err((e, msg)) => {
                self.metrics.message_delivery_errors_counter.inc();
                error!(
                    "received error while writing to kafka sink topic {}: {}",
                    msg.topic(),
                    e
                );
                let mut fatal_errors = self.fatal_errors.lock().expect("lock poisoned");
                fatal_errors.push(e.clone());
            }
        }
    }
}
