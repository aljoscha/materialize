# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set schema={
    "type": "record",
    "name": "envelope",
    "fields": [
      {
        "name": "before",
        "type": [
          {
            "name": "row",
            "type": "record",
            "fields": [
              {"name": "id", "type": "string"},
              {"name": "a", "type": "long"},
              {"name": "b", "type": "long"}
            ]
          },
          "null"
        ]
      },
      { "name": "after", "type": ["row", "null"] }
    ]
  }

$ kafka-create-topic topic=input

$ kafka-ingest format=avro topic=input schema=${schema} timestamp=1
{"before": null, "after": {"row":{"id": "valid1", "a": 2, "b": 1}}}
{"before": null, "after": {"row":{"id": "valid2", "a": 17, "b": 5}}}

> CREATE SOURCE source
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-input-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE DEBEZIUM

> CREATE MATERIALIZED VIEW multiply AS SELECT id, a * b AS product FROM source

> CREATE MATERIALIZED VIEW divide AS SELECT id, a / b AS quotient FROM source

> CREATE MATERIALIZED VIEW both AS
  SELECT * FROM multiply NATURAL JOIN divide

> CREATE SINK sink FROM divide
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> SELECT * FROM both
valid1  2   2
valid2  85  3

$ kafka-ingest format=avro topic=input schema=${schema} timestamp=2
{"before": null, "after": {"row": {"id": "bad1", "a": 7, "b": 0}}}

> SELECT * FROM multiply
valid1  2
valid2  85
bad1    0

! SELECT * FROM divide
division by zero

! SELECT * FROM both
division by zero

> SELECT i.name, e.error from mz_indexes i, mz_index_errors e where i.id = e.id;
name error
-----------
both_primary_idx "Evaluation error: division by zero"
divide_primary_idx "Evaluation error: division by zero"

> SELECT s.name, e.error from mz_sinks s, mz_sink_errors e where s.id = e.id;
name error
-----------
sink "Evaluation error: division by zero"

$ kafka-ingest format=avro topic=input schema=${schema} timestamp=3
{"before": {"row": {"id": "bad1", "a": 7, "b": 0}}, "after": null}

> SELECT * FROM both
valid1  2   2
valid2  85  3

> SELECT i.name, e.error from mz_indexes i, mz_index_errors e where i.id = e.id;
name error
-----------

> SELECT s.name, e.error from mz_sinks s, mz_index_errors e where s.id = e.id;
name error
-----------

$ kafka-ingest format=avro topic=input schema=${schema} timestamp=2
{"before": null, "after": {"row": {"id": "bad2", "a": 7, "b": 0}}}

> SELECT i.name, e.error from mz_indexes i, mz_index_errors e where i.id = e.id;
name error
-----------
both_primary_idx "Evaluation error: division by zero"
divide_primary_idx "Evaluation error: division by zero"

# Dropping an index also retracts outstanding errors

> DROP VIEW both

> SELECT i.name, e.error from mz_indexes i, mz_index_errors e where i.id = e.id;
name error
-----------
divide_primary_idx "Evaluation error: division by zero"

# Dropping a sink also retracts outstanding errors

> DROP SINK sink

> SELECT s.name, e.error from mz_sinks s, mz_sink_errors e where s.id = e.id;
name error
-----------
