// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::now::SYSTEM_TIME;
use mz_persist_client::PersistLocation;
use mz_persist_client::cache::PersistClientCache;
use uuid::Uuid;

use crate::durable::persist::{CATALOG_SEED, fetch_catalog_shard_version, shard_id};
use crate::durable::{DurableCatalogError, TestCatalogStateBuilder, test_bootstrap_args};

/// Test that the catalog forces users to upgrade one major version at a time.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_version_step() {
    let first_version = semver::Version::parse("0.147.0").expect("failed to parse version");
    let second_version = semver::Version::parse("26.0.0").expect("failed to parse version");
    let second_dev_version =
        semver::Version::parse("26.0.0-dev.0").expect("failed to parse version");
    let third_version = semver::Version::parse("27.1.0").expect("failed to parse version");
    let organization_id = Uuid::new_v4();
    let deploy_generation = 0;
    let mut persist_cache = PersistClientCache::new_no_metrics();
    let catalog_shard_id = shard_id(organization_id, CATALOG_SEED);

    persist_cache.cfg.build_version = first_version.clone();
    let persist_client = persist_cache
        .open(PersistLocation::new_in_mem())
        .await
        .expect("in-mem location is valid");

    assert_eq!(
        None,
        fetch_catalog_shard_version(&persist_client, catalog_shard_id).await
    );

    let persist_openable_state = TestCatalogStateBuilder::new(persist_client.clone())
        .with_organization_id(organization_id)
        .with_deploy_generation(deploy_generation)
        .with_version(first_version.clone())
        .expect_build("failed to create persist catalog")
        .await;
    let mut persist_state = persist_openable_state
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .expect("failed to open persist catalog");
    persist_state.mark_bootstrap_complete().await;

    assert_eq!(
        Some(first_version.clone()),
        fetch_catalog_shard_version(&persist_client, catalog_shard_id).await,
        "writable open + bootstrap should set the catalog shard version"
    );

    persist_cache.cfg.build_version = third_version.clone();
    let persist_client = persist_cache
        .open(PersistLocation::new_in_mem())
        .await
        .expect("in-mem location is valid");
    let err = TestCatalogStateBuilder::new(persist_client.clone())
        .with_organization_id(organization_id)
        .with_deploy_generation(deploy_generation)
        .with_version(third_version.clone())
        .build()
        .await
        .expect_err("skipping versions should error");
    assert!(
        matches!(
            &err,
            DurableCatalogError::IncompatiblePersistVersion {
                found_version,
                catalog_version
            }
            if found_version == &first_version && catalog_version == &third_version
        ),
        "Unexpected error: {err:?}"
    );

    persist_cache.cfg.build_version = second_dev_version.clone();
    let persist_client = persist_cache
        .open(PersistLocation::new_in_mem())
        .await
        .expect("in-mem location is valid");
    TestCatalogStateBuilder::new(persist_client.clone())
        .with_organization_id(organization_id)
        .with_deploy_generation(deploy_generation)
        .with_version(second_dev_version.clone())
        .expect_build("failed to create persist catalog")
        .await;

    persist_cache.cfg.build_version = second_version.clone();
    let persist_client = persist_cache
        .open(PersistLocation::new_in_mem())
        .await
        .expect("in-mem location is valid");
    let state_builder = TestCatalogStateBuilder::new(persist_client.clone())
        .with_organization_id(organization_id)
        .with_deploy_generation(deploy_generation)
        .with_version(second_version.clone());
    let persist_openable_state = state_builder
        .clone()
        .expect_build("failed to create persist catalog")
        .await;
    let _persist_state = persist_openable_state
        .open_savepoint(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .expect("failed to open savepoint persist catalog");

    assert_eq!(
        Some(first_version.clone()),
        fetch_catalog_shard_version(&persist_client, catalog_shard_id).await,
        "opening a savepoint catalog should not increment the catalog shard version"
    );

    let persist_openable_state = state_builder
        .clone()
        .expect_build("failed to create persist catalog")
        .await;
    let _persist_state = persist_openable_state
        .open_read_only(&test_bootstrap_args())
        .await
        .expect("failed to open readonly persist catalog");

    assert_eq!(
        Some(first_version),
        fetch_catalog_shard_version(&persist_client, catalog_shard_id).await,
        "opening a readonly catalog should not increment the catalog shard version"
    );

    let persist_openable_state = state_builder
        .expect_build("failed to create persist catalog")
        .await;
    let mut persist_state = persist_openable_state
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .expect("failed to open persist catalog");
    persist_state.mark_bootstrap_complete().await;

    assert_eq!(
        Some(second_version),
        fetch_catalog_shard_version(&persist_client, catalog_shard_id).await,
        "writable open + bootstrap should increment the catalog shard version"
    );
}

#[mz_ore::test(tokio::test)]
async fn demo_wal_recovery_and_fencing() {
    use super::*;
    use crate::durable::objects::serialization::proto;
    use mz_ore::metrics::MetricsRegistry;
    use mz_repr::{Datum, RelationDesc, Row, SqlScalarType};

    let mut cache = PersistClientCache::new_no_metrics();
    cache.cfg.configs = Arc::new(mz_txn_wal::all_dyncfgs((*cache.cfg.configs).clone()));
    let client = cache.open(PersistLocation::new_in_mem()).await.unwrap();
    let org = Uuid::new_v4();
    let direct_org = Uuid::new_v4();
    let open = |org, demo| {
        UnopenedPersistCatalogState::new_with_ownership(
            client.clone(),
            org,
            cache.cfg.build_version.clone(),
            Some(1),
            Arc::new(Metrics::new(&MetricsRegistry::new())),
            demo,
        )
    };
    drop(open(direct_org, false).await.unwrap());
    assert!(open(direct_org, true).await.is_err());

    let unopened = open(org, true).await.unwrap();
    assert!(open(org, false).await.is_err());
    let catalog_id = unopened.shard_id;
    let mut first = unopened
        .open_inner(Mode::Writable, Timestamp::MIN, &test_bootstrap_args())
        .await
        .unwrap();
    first.sync_to_current_updates().await.unwrap();

    let table_id = ShardId::new();
    let desc = RelationDesc::builder()
        .with_column("v", SqlScalarType::Int64.nullable(false))
        .finish();
    let (writer, mut reader) = client
        .open(
            table_id,
            Arc::new(desc),
            Arc::new(UnitSchema),
            Diagnostics::from_purpose("demo participant"),
            false,
        )
        .await
        .unwrap();
    let row = Row::pack_slice(&[Datum::Int64(7)]);
    let mut peer = open(org, true).await.unwrap();
    let wal = &mut peer.wal.as_mut().unwrap().handle;
    let registration = wal.current_upper().await;
    wal.register(registration, [writer]).await.unwrap();
    let ts = wal.current_upper().await;
    let mut invalid = first.transaction().await.unwrap();
    invalid
        .set_setting("rejected".into(), Some("bad".into()))
        .unwrap();
    let _ = invalid.get_and_commit_op_updates();
    let err = invalid
        .commit_with_table_diffs(
            ts,
            TableDiffs::from([(ShardId::new(), vec![(row.clone(), Diff::ONE)])]),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        CatalogError::Durable(DurableCatalogError::NotWritable(_))
    ));
    assert_eq!(wal.current_upper().await, ts);
    let mut txn = first.transaction().await.unwrap();
    txn.set_setting("mixed".into(), Some("one".into())).unwrap();
    let _ = txn.get_and_commit_op_updates();
    let upper = txn
        .commit_with_table_diffs(
            ts,
            TableDiffs::from([(table_id, vec![(row.clone(), Diff::ONE)])]),
        )
        .await
        .unwrap();
    assert_eq!(upper, ts.step_forward());
    assert_eq!(
        reader
            .snapshot_and_fetch(Antichain::from_elem(ts))
            .await
            .unwrap(),
        vec![((SourceData(Ok(row.clone())), ()), ts, 1)]
    );
    let stale = first.transaction().await.unwrap();

    // Model a crash just after the durable WAL decision: neither data shard
    // has been applied and no tidy token is available to the new process.
    let mut incomplete = wal.begin();
    let key: StateUpdateKindJson = StateUpdateKind::Setting(
        proto::SettingKey {
            name: "recovered".into(),
        },
        proto::SettingValue {
            value: "two".into(),
        },
    )
    .into();
    incomplete
        .write(&catalog_id, SourceData::from(key), (), 1)
        .await;
    incomplete
        .write(&table_id, SourceData(Ok(row.clone())), (), -1)
        .await;
    incomplete
        .compare_and_commit_at(wal, upper, upper)
        .await
        .unwrap();
    drop(peer);

    let mut reopened = open(org, true)
        .await
        .unwrap()
        .open_inner(Mode::Writable, upper, &test_bootstrap_args())
        .await
        .unwrap();
    reopened.sync_to_current_updates().await.unwrap();
    let txn = reopened.transaction().await.unwrap();
    assert_eq!(txn.get_setting("rejected".into()), None);
    assert_eq!(txn.get_setting("mixed".into()), Some("one"));
    assert_eq!(txn.get_setting("recovered".into()), Some("two"));
    assert_eq!(txn.get_txn_wal_shard(), Some(shard_id(org, DEMO_WAL_SEED)));
    assert!(
        reader
            .snapshot_and_fetch(Antichain::from_elem(upper))
            .await
            .unwrap()
            .is_empty()
    );

    // The requested timestamp is deliberately *after* the new owner's fence.
    let fenced_upper = reopened.current_upper().await;
    let future = upper.step_forward_by(&Timestamp::from(1000));
    let err = stale
        .commit_with_table_diffs(
            future,
            TableDiffs::from([(table_id, vec![(row, Diff::ONE)])]),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        CatalogError::Durable(DurableCatalogError::Fence(_))
    ));
    assert_eq!(reopened.current_upper().await, fenced_upper);
}
