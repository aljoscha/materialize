# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Source with new-style three-valued "snapshot".
$ set new-dbz-schema={
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
              {"name": "a", "type": "long"},
              {"name": "b", "type": "long"}
            ]
          },
          "null"
        ]
      },
      { "name": "after", "type": ["row", "null"] },
      {
        "name": "source",
        "type": {
          "type": "record",
          "name": "Source",
          "namespace": "io.debezium.connector.mysql",
          "fields": [
            {
              "name": "snapshot",
              "type": [
                {
                  "type": "string",
                  "connect.version": 1,
                  "connect.parameters": {
                    "allowed": "true,last,false"
                  },
                  "connect.default": "false",
                  "connect.name": "io.debezium.data.Enum"
                },
                "null"
              ],
              "default": "false"
            },
            {
              "name": "file",
              "type": "string"
            },
            {
              "name": "pos",
              "type": "long"
            },
            {
              "name": "row",
              "type": "int"
            }
          ],
          "connect.name": "io.debezium.connector.mysql.Source"
        }
      },
      {
        "name": "transaction",
        "type": {
          "type": "record",
          "name": "Transaction",
          "namespace": "whatever",
          "fields": [
            {
              "name": "total_order",
              "type": ["long", "null"]
            }
          ]
        }
      }
    ]
  }

# Test that deduplication=full accepts mis-ordered Debezium data.

$ kafka-create-topic topic=misordered-dbz-data

$ set new-dbz-key-schema={"type": "record", "name": "row", "fields": [{"name": "a", "type": "long"}]}

$ kafka-ingest format=avro key-format=avro topic=misordered-dbz-data schema=${new-dbz-schema} key-schema=${new-dbz-key-schema} timestamp=1 publish=true
{"a": 5} {"before": null, "after": {"row":{"a": 5, "b": 3}}, "source": {"file": "binlog", "pos": 1, "row": 0, "snapshot": {"string": "true"}}, "transaction": {"total_order": null}}
{"a": 5} {"before": null, "after": {"row":{"a": 5, "b": 3}}, "source": {"file": "binlog", "pos": 1, "row": 0, "snapshot": {"string": "true"}}, "transaction": {"total_order": null}}
{"a": 1} {"before": null, "after": {"row":{"a": 1, "b": 1}}, "source": {"file": "binlog", "pos": 2, "row": 0, "snapshot": {"string": "false"}}, "transaction": {"total_order": null}}
{"a": 2} {"before": null, "after": {"row":{"a": 2, "b": 3}}, "source": {"file": "binlog", "pos": 1, "row": 0, "snapshot": {"string": "false"}}, "transaction": {"total_order": null}}
{"a": -1} {"before": null, "after": {"row":{"a": -1, "b": 7}}, "source": {"file": "binlog", "pos": 1, "row": 1, "snapshot": {"string": "false"}}, "transaction": {"total_order": null}}
{"a": 2} {"before": null, "after": {"row":{"a": 2, "b": 3}}, "source": {"file": "binlog", "pos": 1, "row": 0, "snapshot": {"string": "false"}}, "transaction": {"total_order": null}}
{"a": 1} {"before": null, "after": {"row":{"a": 1, "b": 1}}, "source": {"file": "binlog", "pos": 2, "row": 0, "snapshot": {"string": "false"}}, "transaction": {"total_order": null}}
{"a": 3} {"before": null, "after": {"row":{"a": 3, "b": 4}}, "source": {"file": "binlog2", "pos": 2, "row": 0, "snapshot": {"string": "false"}}, "transaction": {"total_order": null}}

> CREATE MATERIALIZED SOURCE misordered_dbz
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-misordered-dbz-data-${testdrive.seed}'
  WITH (deduplication = 'full')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM

> SELECT * FROM misordered_dbz
a b
---
1 1
2 3
-1 7
3 4
5 3
