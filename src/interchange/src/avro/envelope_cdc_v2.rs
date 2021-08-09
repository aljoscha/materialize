// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic for the Avro representation of the CDCv2 protocol.

use byteorder::{NetworkEndian, WriteBytesExt};
use mz_avro::schema::{FullName, SchemaNode};
use repr::{ColumnName, ColumnType, Diff, RelationDesc, Row, Timestamp};
use serde_json::json;

use anyhow::anyhow;
use avro_derive::AvroDecodable;
use differential_dataflow::capture::{Message, Progress};
use mz_avro::error::{DecodeError, Error as AvroError};
use mz_avro::schema::Schema;
use mz_avro::types::Value;
use mz_avro::{
    define_unexpected, ArrayAsVecDecoder, AvroDecodable, AvroDecode, AvroDeserializer, AvroRead,
    StatefulAvroDecodable,
};
use std::fmt;
use std::{cell::RefCell, rc::Rc};

use crate::encode::column_names_and_types;

use super::{GenerateAvroSchema, RowWrapper};

pub fn extract_data_columns<'a>(schema: &'a Schema) -> anyhow::Result<SchemaNode<'a>> {
    let data_name = FullName::from_parts("data", Some("com.materialize.cdc"), "");
    let data_schema = &schema
        .try_lookup_name(&data_name)
        .ok_or_else(|| anyhow!("record not found: {}", data_name))?
        .piece;
    Ok(SchemaNode {
        root: &schema,
        inner: data_schema,
        name: None,
    })
}

/// Generates key and value Avro schemas for the CDCv2 envelope.
pub struct AvroCdcV2SchemaGenerator {
    columns: Vec<(ColumnName, ColumnType)>,
    schema: Schema,
}

impl GenerateAvroSchema for AvroCdcV2SchemaGenerator {
    fn new(
        key_desc: Option<RelationDesc>,
        value_desc: RelationDesc,
        include_transaction: bool,
    ) -> Self {
        assert!(key_desc.is_none(), "cannot have a key with CDCv2");
        assert!(
            !include_transaction,
            "cannot include transaction information with CDCv2"
        );

        let columns = column_names_and_types(value_desc);
        let row_schema = super::build_row_schema_json(&columns, "data");
        let schema = build_schema(row_schema);
        Self { columns, schema }
    }

    fn value_writer_schema(&self) -> &Schema {
        &self.schema
    }

    fn value_columns(&self) -> &[(ColumnName, ColumnType)] {
        &self.columns
    }

    fn key_writer_schema(&self) -> Option<&Schema> {
        None
    }

    fn key_columns(&self) -> Option<&[(ColumnName, ColumnType)]> {
        None
    }
}

impl fmt::Debug for AvroCdcV2SchemaGenerator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Cdcv2SchemaGenerator")
            .field("schema", &self.schema)
            .finish()
    }
}

/// Collected state to encode update batches and progress statements.
#[derive(Debug)]
pub struct AvroCdcV2Encoder {
    schema_generator: AvroCdcV2SchemaGenerator,
    schema_id: i32,
}

impl AvroCdcV2Encoder {
    /// Creates a new [`AvroCdcV2Encoder`] that uses the schema from the given `schema_generator`.
    pub fn new(schema_generator: AvroCdcV2SchemaGenerator, schema_id: i32) -> Self {
        Self {
            schema_generator,
            schema_id,
        }
    }

    /// Encodes a batch of updates as an Avro value.
    pub fn encode_updates(&self, updates: &[(&Row, &Timestamp, &Diff)]) -> Vec<u8> {
        let mut enc_updates = Vec::new();
        for (data, time, diff) in updates {
            let enc_data = super::encode_datums_as_avro(*data, &self.schema_generator.columns);
            let enc_time = Value::Long(*time.clone() as i64);
            let enc_diff = Value::Long(*diff.clone() as i64);
            enc_updates.push(Value::Record(vec![
                ("data".to_string(), enc_data),
                ("time".to_string(), enc_time),
                ("diff".to_string(), enc_diff),
            ]));
        }
        let avro_value = Value::Union {
            index: 0,
            inner: Box::new(Value::Array(enc_updates)),
            n_variants: 2,
            null_variant: None,
        };

        encode_message_unchecked(
            self.schema_id,
            &avro_value,
            self.schema_generator.value_writer_schema(),
        )
    }

    /// Encodes the contents of a progress statement as an Avro value.
    pub fn encode_progress(
        &self,
        lower: &[Timestamp],
        upper: &[Timestamp],
        counts: &[(Timestamp, i64)],
    ) -> Vec<u8> {
        let enc_lower = Value::Array(
            lower
                .iter()
                .cloned()
                .map(|ts| Value::Long(ts as i64))
                .collect(),
        );
        let enc_upper = Value::Array(
            upper
                .iter()
                .cloned()
                .map(|ts| Value::Long(ts as i64))
                .collect(),
        );
        let enc_counts = Value::Array(
            counts
                .iter()
                .map(|(time, count)| {
                    Value::Record(vec![
                        ("time".to_string(), Value::Long(time.clone() as i64)),
                        ("count".to_string(), Value::Long(count.clone())),
                    ])
                })
                .collect(),
        );
        let enc_progress = Value::Record(vec![
            ("lower".to_string(), enc_lower),
            ("upper".to_string(), enc_upper),
            ("counts".to_string(), enc_counts),
        ]);

        let avro_value = Value::Union {
            index: 1,
            inner: Box::new(enc_progress),
            n_variants: 2,
            null_variant: None,
        };

        encode_message_unchecked(
            self.schema_id,
            &avro_value,
            self.schema_generator.value_writer_schema(),
        )
    }
}

fn encode_avro_header(buf: &mut Vec<u8>, schema_id: i32) {
    // The first byte is a magic byte (0) that indicates the Confluent
    // serialization format version, and the next four bytes are a
    // 32-bit schema ID.
    //
    // https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
    buf.write_u8(0).expect("writing to vec cannot fail");
    buf.write_i32::<NetworkEndian>(schema_id)
        .expect("writing to vec cannot fail");
}

fn encode_message_unchecked(schema_id: i32, value: &Value, schema: &Schema) -> Vec<u8> {
    let mut buf = vec![];
    encode_avro_header(&mut buf, schema_id);
    mz_avro::encode_unchecked(value, schema, &mut buf);
    buf
}

#[derive(AvroDecodable)]
#[state_type(Rc<RefCell<Row>>, Rc<RefCell<Vec<u8>>>)]
struct MyUpdate {
    #[state_expr(self._STATE.0.clone(), self._STATE.1.clone())]
    data: RowWrapper,
    time: Timestamp,
    diff: Diff,
}

#[derive(AvroDecodable)]
struct Count {
    time: Timestamp,
    count: usize,
}

fn make_counts_decoder() -> impl AvroDecode<Out = Vec<(Timestamp, usize)>> {
    ArrayAsVecDecoder::new(|| {
        <Count as AvroDecodable>::new_decoder().map_decoder(|ct| Ok((ct.time, ct.count)))
    })
}

#[derive(AvroDecodable)]
struct MyProgress {
    lower: Vec<Timestamp>,
    upper: Vec<Timestamp>,
    #[decoder_factory(make_counts_decoder)]
    counts: Vec<(Timestamp, usize)>,
}

impl AvroDecode for Decoder {
    type Out = Message<Row, Timestamp, Diff>;
    fn union_branch<'a, R: AvroRead, D: AvroDeserializer>(
        self,
        idx: usize,
        _n_variants: usize,
        _null_variant: Option<usize>,
        deserializer: D,
        r: &'a mut R,
    ) -> Result<Self::Out, AvroError> {
        match idx {
            0 => {
                let packer = Rc::new(RefCell::new(Row::default()));
                let buf = Rc::new(RefCell::new(vec![]));
                let d = ArrayAsVecDecoder::new(|| {
                    <MyUpdate as StatefulAvroDecodable>::new_decoder((packer.clone(), buf.clone()))
                        .map_decoder(|update| Ok((update.data.0, update.time, update.diff)))
                });
                let updates = deserializer.deserialize(r, d)?;
                Ok(Message::Updates(updates))
            }
            1 => {
                let progress =
                    deserializer.deserialize(r, <MyProgress as AvroDecodable>::new_decoder())?;
                let progress = Progress {
                    lower: progress.lower,
                    upper: progress.upper,
                    counts: progress.counts,
                };
                Ok(Message::Progress(progress))
            }

            other => Err(DecodeError::Custom(format!(
                "Unrecognized union variant in CDCv2 decoder: {}",
                other
            ))
            .into()),
        }
    }
    define_unexpected! {
        record, array, map, enum_variant, scalar, decimal, bytes, string, json, uuid, fixed
    }
}

/// Collected state to decode update batches and progress statements.
#[derive(Debug)]
pub struct Decoder;

/// Construct the schema for the CDC V2 protocol.
pub fn build_schema(row_schema: serde_json::Value) -> Schema {
    let updates_schema = json!({
        "type": "array",
        "items": {
            "name" : "update",
            "type" : "record",
            "namespace": "com.materialize.cdc",
            "fields" : [
                {
                    "name": "data",
                    "type": row_schema,
                },
                {
                    "name" : "time",
                    "type" : "long",
                },
                {
                    "name" : "diff",
                    "type" : "long",
                },
            ],
        },
    });

    let progress_schema = json!({
        "name" : "progress",
        "type" : "record",
        "fields" : [
            {
                "name": "lower",
                "type": {
                    "type": "array",
                    "items": "long"
                }
            },
            {
                "name": "upper",
                "type": {
                    "type": "array",
                    "items": "long"
                }
            },
            {
                "name": "counts",
                "type": {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "counts",
                        "fields": [
                            {
                                "name": "time",
                                "type": "long",
                            },
                            {
                                "name": "count",
                                "type": "long",
                            },
                        ],
                    }
                }
            },
        ],
    });
    let message_schema = json!([updates_schema, progress_schema,]);

    Schema::parse(&message_schema).expect("schema constrution failed")
}

#[cfg(test)]
mod tests {

    use super::*;
    use mz_avro::AvroDeserializer;
    use mz_avro::GeneralDeserializer;
    use repr::ScalarType;

    use crate::avro::build_row_schema_json;

    #[test]
    fn test_roundtrip() {
        let desc = RelationDesc::empty()
            .with_named_column("id", ScalarType::Int64.nullable(false))
            .with_named_column("price", ScalarType::Float64.nullable(true));

        let schema_generator = AvroCdcV2SchemaGenerator::new(None, desc.clone(), false);
        let encoder = AvroCdcV2Encoder::new(schema_generator, 0);

        let row_schema =
            build_row_schema_json(&crate::encode::column_names_and_types(desc), "data");
        let schema = build_schema(row_schema);

        let mut encoded = vec![
            encoder.encode_updates(&[]),
            encoder.encode_progress(&[0], &[3], &[]),
            encoder.encode_progress(&[3], &[], &[]),
        ];

        let g = GeneralDeserializer {
            schema: schema.top_node(),
        };
        assert!(matches!(
            g.deserialize(&mut &encoded.remove(0)[..], Decoder).unwrap(),
            Message::Updates(_)
        ),);
        assert!(matches!(
            g.deserialize(&mut &encoded.remove(0)[..], Decoder).unwrap(),
            Message::Progress(_)
        ),);
        assert!(matches!(
            g.deserialize(&mut &encoded.remove(0)[..], Decoder).unwrap(),
            Message::Progress(_)
        ),);
    }
}
