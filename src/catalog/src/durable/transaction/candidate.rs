// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;
use crate::durable::objects::state_update::{StateUpdateKind as Kind, StateUpdateKindJson};
use mz_repr::adt::jsonb::JsonbRef;
use mz_repr::{ReprScalarType, Row};
use mz_storage_types::sources::SourceData;

fn invalid(reason: impl Into<String>) -> CatalogError {
    DurableCatalogError::Internal(reason.into()).into()
}

fn decode(row: Row, diff: Diff) -> Result<Kind, CatalogError> {
    if diff != Diff::ONE && diff != Diff::MINUS_ONE {
        return Err(invalid("catalog diff must be +1 or -1"));
    }
    let datum = row
        .iter()
        .exactly_one()
        .ok()
        .filter(|datum| datum.is_instance_of(&ReprScalarType::Jsonb.nullable(false)))
        .ok_or_else(|| invalid("catalog row must contain a single JSONB datum"))?;
    // The persist reader assumes trusted JSONB. Parse candidate data fallibly
    // before using the same durable proto codec as recovery.
    let mut bytes = Vec::new();
    JsonbRef::from_datum(datum)
        .to_writer(&mut bytes)
        .map_err(|err| invalid(err.to_string()))?;
    let proto = serde_json::from_slice(&bytes).map_err(|err| invalid(err.to_string()))?;
    let kind = Kind::from_proto(proto)?;
    if SourceData::from(StateUpdateKindJson::from(kind.clone())).0 != Ok(row) {
        return Err(invalid("incomplete or noncanonical durable catalog row"));
    }
    Ok(kind)
}

fn from_proto<R: RustType<P>, P: PartialEq + Clone>(proto: P) -> Result<R, CatalogError> {
    let rust = R::from_proto(proto.clone())?;
    if rust.into_proto() != proto {
        return Err(invalid("lossy durable catalog row conversion"));
    }
    Ok(rust)
}

impl Transaction<'_> {
    /// Stage complete, consolidated catalog row diffs against this transaction's snapshot.
    /// False indicates a stale before-image. Drop the transaction on false or error.
    pub fn stage_catalog_diffs(&mut self, rows: Vec<(Row, Diff)>) -> Result<bool, CatalogError> {
        let audit_key = IdAllocKey {
            name: AUDIT_LOG_ID_ALLOC_KEY.into(),
        };
        let audit_start = self.id_allocator.get(&audit_key).map(|v| v.next_id);
        let mut audits = BTreeMap::new();

        macro_rules! stage {
            ($($variant:ident => $field:ident),+ $(,)?) => {{
                $(let mut $field = Vec::new();)+
                for (row, diff) in rows {
                    match decode(row, diff)? {
                        $(Kind::$variant(k, v) => {
                            $field.push((from_proto(k)?, from_proto(v)?, diff));
                        })+
                        Kind::AuditLog(k, ()) => {
                            let key: AuditLogKey = from_proto(k)?;
                            let prev = audits.insert(key.event.sortable_id(), key.event);
                            if diff != Diff::ONE || prev.is_some() {
                                return Err(invalid("audit updates must append distinct event IDs"));
                            }
                        }
                        Kind::FenceToken(_) | Kind::TxnWalShard(..) => {
                            return Err(invalid("cannot change catalog ownership in a candidate"));
                        }
                    }
                }
                $(if !self.$field.stage_candidate($field, self.op_id)? {
                    return Ok(false);
                })+
            }}
        }
        stage!(
            Cluster => clusters,
            ClusterReplica => cluster_replicas,
            ClusterReplicaSize => cluster_replica_sizes,
            Comment => comments,
            Config => configs,
            Database => databases,
            DefaultPrivilege => default_privileges,
            IdAllocator => id_allocator,
            IndexColumns => index_columns,
            IntrospectionSourceIndex => introspection_sources,
            Item => items,
            ItemColumns => item_columns,
            NetworkPolicy => network_policies,
            QueryPolicy => query_policies,
            Role => roles,
            RoleAuth => role_auth,
            Schema => schemas,
            Setting => settings,
            SourceReferences => source_references,
            SystemConfiguration => system_configurations,
            ClusterSystemConfiguration => cluster_system_configurations,
            ReplicaSystemConfiguration => replica_system_configurations,
            SystemObjectMapping => system_gid_mapping,
            SystemPrivilege => system_privileges,
            StorageCollectionMetadata => storage_collection_metadata,
            UnfinalizedShard => unfinalized_shards,
        );
        // Audit history isn't cached. Its monotone allocator is the before-image
        // that proves new IDs have never been used, without scanning history.
        let audit_end = self.id_allocator.get(&audit_key).map(|v| v.next_id);
        if audit_start != audit_end || !audits.is_empty() {
            let valid = audit_start.zip(audit_end).is_some_and(|(start, end)| {
                end.checked_sub(start) == Some(usize_to_u64(audits.len()))
                    && (start..end).eq(audits.keys().copied())
            });
            if !valid {
                return Err(invalid(
                    "audit IDs do not match the supplied allocator update",
                ));
            }
        }
        self.insert_audit_log_events(audits.into_values());
        Ok(true)
    }
}

impl<K, V> TableTransaction<K, V>
where
    K: Ord + Clone + Debug,
    V: Ord + Clone + Debug + UniqueName,
{
    fn stage_candidate(
        &mut self,
        rows: Vec<(K, V, Diff)>,
        ts: Timestamp,
    ) -> Result<bool, CatalogError> {
        let mut changes = BTreeMap::<K, (Option<V>, Option<V>)>::new();
        for (key, value, diff) in rows {
            let (before, after) = changes.entry(key).or_default();
            let slot = if diff == Diff::MINUS_ONE {
                before
            } else {
                after
            };
            if slot.replace(value).is_some() {
                return Err(invalid(
                    "multiple before- or after-images for a catalog key",
                ));
            }
        }
        let mut next = BTreeMap::new();
        for (key, (before, after)) in changes {
            if self.get(&key) != before.as_ref() {
                return Ok(false);
            }
            if before != after {
                next.insert(key, after);
            }
        }
        self.set_many(next, ts)?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_repr::Datum;
    use mz_repr::adt::jsonb::Jsonb;

    #[mz_ore::test]
    fn catalog_rows_require_canonical_image_and_unit_diff() {
        let row = |json: &str| json.parse::<Jsonb>().expect("valid JSON").into_row();
        let complete = row(r#"{"kind":"Setting","key":{"name":"s"},"value":{"value":"v"}}"#);
        assert!(matches!(
            decode(complete.clone(), Diff::ONE),
            Ok(Kind::Setting(..))
        ));
        assert!(decode(complete, Diff::from(2)).is_err());
        assert!(decode(Row::pack_slice(&[Datum::Int32(1)]), Diff::ONE).is_err());
        assert!(decode(Row::pack_slice(&[Datum::JsonNull; 2]), Diff::ONE).is_err());
        for bad in [
            r#"{"kind":"Unrecognized"}"#,
            r#"{"kind":"Setting","key":{"name":"s"}}"#,
            r#"{"kind":"Setting","key":{"name":"s"},"value":{"value":"v","extra":true}}"#,
        ] {
            assert!(decode(row(bad), Diff::ONE).is_err());
        }
    }

    #[mz_ore::test]
    fn compare_before_images_before_staging_replacements() {
        let mut table = TableTransaction {
            initial: BTreeMap::from([(1, String::from("current"))]),
            pending: BTreeMap::new(),
            uniqueness_check: None,
        };
        let after = (1, String::from("updated"), Diff::ONE);
        let before = (1, String::from("current"), Diff::MINUS_ONE);
        let stale = (1, String::from("stale"), Diff::MINUS_ONE);

        assert!(
            !table
                .stage_candidate(vec![after.clone(), stale], 0)
                .unwrap()
        );
        assert!(!table.stage_candidate(vec![after.clone()], 0).unwrap());
        assert!(
            table
                .stage_candidate(vec![before.clone(), after.clone(), after.clone()], 0)
                .is_err()
        );
        assert!(table.pending.is_empty());
        assert!(table.stage_candidate(vec![after, before], 0).unwrap());
        assert_eq!(table.get(&1).map(String::as_str), Some("updated"));
    }
}
