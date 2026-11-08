//! Projection and projector tests over the test projections.

mod next_tests;

#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
mod tests {
    use crate::{
        ReadTransactionExt,
        testsupport::{
            NewTodoV1, NewUserV3, Todo, TodoDeletedV1, TodoId, User, UserId, event, inmemory,
            tenant, tenant_id,
        },
    };
    use nvzn_projector::*;
    use nvzn_store::*;
    use nvzn_types::*;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;

    /// `Todo::BY_USER`'s id.
    const IID: IndexId = IndexId::from_u32(1);

    fn todo(id: u128, user_id: u128, title: &str) -> NewTodoV1 {
        NewTodoV1 {
            id: TodoId(id),
            user_id: UserId(user_id),
            title: title.to_string(),
        }
    }

    fn rig() -> (Arc<dyn crate::Store>, ProjectorManager<event::Event>) {
        let mut manager = ProjectorManager::<event::Event>::new(tenant());
        manager.register::<Todo>().unwrap();
        (inmemory(), manager)
    }

    fn handle(
        store: &Arc<dyn crate::Store>,
        manager: &ProjectorManager<event::Event>,
        events: &[event::Event],
    ) {
        let mut txn = store.write().unwrap();

        for event in events {
            fold_event(&manager.projectors, tenant(), event, &mut txn).unwrap();
        }

        txn.commit(crate::Watermark::default()).unwrap();
    }

    fn indexed_under(store: &Arc<dyn crate::Store>, user_id: u128) -> Vec<ProjectionId> {
        let txn = store.read().unwrap();
        txn.scan_index(
            tenant_id(),
            Todo::TYPE_ID,
            IID,
            ProjectionId::from_u128(user_id),
        )
        .map(|r| r.unwrap())
        .collect()
    }

    /// The wiring, not just the helper: a fold through a real projector must
    /// label the row counter with this tenant and this projection type.
    #[test]
    fn project_counts_rows_by_tenant_and_projection() {
        use metrics_util::debugging::DebuggingRecorder;

        let store = inmemory();
        let mut manager = ProjectorManager::<event::Event>::new(tenant());
        manager.register::<Todo>().unwrap();

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let mut txn = store.write().unwrap();
        // `crate::metrics` shadows the facade in this module.
        ::metrics::with_local_recorder(&recorder, || {
            for event in [
                event::Event::NewTodo(todo(1, 1, "t")),
                event::Event::TodoDeleted(TodoDeletedV1 { id: TodoId(1) }),
            ] {
                fold_event(&manager.projectors, tenant(), &event, &mut txn).unwrap();
            }
        });

        let snapshot = snapshotter.snapshot().into_hashmap();
        let ops: Vec<String> = snapshot
            .keys()
            .filter(|k| k.key().name() == "nvzn_projection_rows_total")
            .map(|k| {
                let mut ls: Vec<String> = k
                    .key()
                    .labels()
                    .map(|l| format!("{}={}", l.key(), l.value()))
                    .collect();
                ls.sort();
                ls.join(",")
            })
            .collect();
        assert!(
            ops.contains(&"op=upsert,projection=Todo,tenant=test".to_string()),
            "no upsert row counted: {ops:?}"
        );
        assert!(
            ops.contains(&"op=delete,projection=Todo,tenant=test".to_string()),
            "no delete row counted: {ops:?}"
        );
    }

    /// A fold that stores the row exactly as it is stored writes nothing:
    /// no row, no index entry, no counted upsert.
    #[test]
    fn identical_write_is_skipped() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        let (store, manager) = rig();
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let mut txn = store.write().unwrap();
        ::metrics::with_local_recorder(&recorder, || {
            for _ in 0..2 {
                let event = event::Event::NewTodo(todo(1, 1, "t"));
                fold_event(&manager.projectors, tenant(), &event, &mut txn).unwrap();
            }
        });
        let upserts: u64 = snapshotter
            .snapshot()
            .into_hashmap()
            .into_iter()
            .filter(|(k, _)| {
                k.key().name() == "nvzn_projection_rows_total"
                    && k.key().labels().any(|l| l.value() == "upsert")
            })
            .map(|(_, (_, _, v))| match v {
                DebugValue::Counter(n) => n,
                _ => 0,
            })
            .sum();
        assert_eq!(upserts, 1, "the second, identical write is a no-op");
    }

    #[test]
    fn register_sets_and_indexes() {
        let (store, manager) = rig();
        handle(&store, &manager, &[event::Event::NewTodo(todo(1, 42, "t"))]);

        let txn = store.read().unwrap();
        let got = ReadTransactionExt::get::<Todo>(&*txn, tenant_id(), Ref::new(TodoId(1).into()))
            .unwrap();
        assert_eq!(got.unwrap().title, "t");
        assert_eq!(indexed_under(&store, 42), vec![ProjectionId::from_u128(1)]);
    }

    #[test]
    fn projector_rekey_moves_index() {
        let (store, manager) = rig();
        handle(&store, &manager, &[event::Event::NewTodo(todo(1, 42, "t"))]);
        handle(
            &store,
            &manager,
            &[event::Event::NewTodo(todo(1, 43, "t2"))],
        );

        assert!(indexed_under(&store, 42).is_empty());
        assert_eq!(indexed_under(&store, 43), vec![ProjectionId::from_u128(1)]);
    }

    #[test]
    fn projector_delete_removes_index_and_tombs() {
        let (store, manager) = rig();
        handle(&store, &manager, &[event::Event::NewTodo(todo(1, 42, "t"))]);
        handle(
            &store,
            &manager,
            &[event::Event::TodoDeleted(TodoDeletedV1 { id: TodoId(1) })],
        );

        let txn = store.read().unwrap();
        let got = ReadTransactionExt::get::<Todo>(&*txn, tenant_id(), Ref::new(TodoId(1).into()))
            .unwrap();
        assert!(got.is_none());
        assert!(indexed_under(&store, 42).is_empty());
    }

    #[test]
    fn projector_reapply_is_idempotent() {
        let (store, manager) = rig();
        let ev = event::Event::NewTodo(todo(1, 42, "t"));
        handle(&store, &manager, &[ev.clone(), ev]);

        let txn = store.read().unwrap();
        let got = ReadTransactionExt::get::<Todo>(&*txn, tenant_id(), Ref::new(TodoId(1).into()))
            .unwrap();
        assert_eq!(got.unwrap().title, "t");
        assert_eq!(indexed_under(&store, 42), vec![ProjectionId::from_u128(1)]);
    }

    #[test]
    fn manager_duplicate_type_id_rejected() {
        let mut manager = ProjectorManager::<event::Event>::new(tenant());
        manager.register::<Todo>().unwrap();
        let err = manager.register::<Todo>().expect_err("duplicate must err");
        assert!(matches!(err, crate::Error::ProjectionAlreadyRegistered(_)));
    }

    #[test]
    fn projector_ignores_events_outside_projection() {
        let (store, manager) = rig();
        let user_event = event::Event::NewUser(NewUserV3 {
            id: UserId(1),
            name: "holden".to_string(),
            address: None,
            email: None,
        });
        handle(&store, &manager, &[user_event]);

        let txn = store.read().unwrap();
        let got = ReadTransactionExt::get::<Todo>(&*txn, tenant_id(), Ref::new(TodoId(1).into()))
            .unwrap();
        assert!(got.is_none());
        assert!(indexed_under(&store, 1).is_empty());
    }

    fn rig_both() -> (Arc<dyn crate::Store>, ProjectorManager<event::Event>) {
        let mut manager = ProjectorManager::<event::Event>::new(tenant());
        manager.register::<Todo>().unwrap();
        manager.register::<User>().unwrap();
        (inmemory(), manager)
    }

    #[test]
    fn projector_recreate_after_delete_in_one_batch() {
        let (store, manager) = rig();
        handle(
            &store,
            &manager,
            &[
                event::Event::NewTodo(todo(1, 42, "first")),
                event::Event::TodoDeleted(TodoDeletedV1 { id: TodoId(1) }),
                event::Event::NewTodo(todo(1, 43, "second")),
            ],
        );

        let txn = store.read().unwrap();
        let got = ReadTransactionExt::get::<Todo>(&*txn, tenant_id(), Ref::new(TodoId(1).into()))
            .unwrap();
        assert_eq!(got.unwrap().title, "second");
        drop(txn);

        assert!(indexed_under(&store, 42).is_empty());
        assert_eq!(indexed_under(&store, 43), vec![ProjectionId::from_u128(1)]);
    }

    #[test]
    fn projector_handles_two_projection_types_in_one_batch() {
        let (store, manager) = rig_both();
        handle(
            &store,
            &manager,
            &[
                event::Event::NewUser(NewUserV3 {
                    id: UserId(42),
                    name: "holden".to_string(),
                    address: None,
                    email: None,
                }),
                event::Event::NewTodo(todo(1, 42, "t")),
            ],
        );

        let txn = store.read().unwrap();
        assert_eq!(
            ReadTransactionExt::get::<User>(&*txn, tenant_id(), Ref::new(UserId(42).into()))
                .unwrap()
                .unwrap()
                .name,
            "holden"
        );
        assert_eq!(
            ReadTransactionExt::get::<Todo>(&*txn, tenant_id(), Ref::new(TodoId(1).into()))
                .unwrap()
                .unwrap()
                .title,
            "t"
        );
        assert_eq!(
            ReadTransactionExt::scan::<Todo>(&*txn, tenant_id())
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
                .len(),
            1,
            "a Todo scan must not see the User row"
        );
        drop(txn);

        assert_eq!(indexed_under(&store, 42), vec![ProjectionId::from_u128(1)]);
    }

    #[test]
    fn projector_index_moves_and_moves_back_in_one_batch() {
        let (store, manager) = rig();
        handle(
            &store,
            &manager,
            &[
                event::Event::NewTodo(todo(1, 42, "a")),
                event::Event::NewTodo(todo(1, 43, "b")),
                event::Event::NewTodo(todo(1, 42, "c")),
            ],
        );

        assert_eq!(indexed_under(&store, 42), vec![ProjectionId::from_u128(1)]);
        assert!(indexed_under(&store, 43).is_empty());
    }

    /// A user row indexed under its own id, with `Todo::BY_USER`'s index id.
    #[derive(Debug, Serialize, Deserialize)]
    struct UserById {
        id: UserId,
    }

    impl Projection for UserById {
        type Event<'a> = event::UserEvent<'a>;
        const TYPE_ID: ProjectionTypeId = ProjectionTypeId::from_u32(0x1D);
        const INDICES: Indices<Self> = &[&Index::new(IID, UserById::id)];

        fn id(&self) -> Ref<Self> {
            Ref::new(self.id.into())
        }

        fn next(ev: &event::UserEvent, _: &View<'_>) -> Result<Vec<Write<Self>>, Error> {
            match ev {
                event::UserEvent::NewUser(n) => Write::set(UserById { id: n.id }),
                event::UserEvent::Pinged(_) => Ok(vec![]),
            }
        }
    }

    #[test]
    fn index_ids_may_repeat_across_projections() {
        let mut manager = ProjectorManager::<event::Event>::new(tenant());
        manager.register::<Todo>().unwrap();
        manager.register::<UserById>().unwrap();
        let store = inmemory();

        handle(
            &store,
            &manager,
            &[
                event::Event::NewUser(NewUserV3 {
                    id: UserId(42),
                    name: "holden".to_string(),
                    address: None,
                    email: None,
                }),
                event::Event::NewTodo(todo(7, 42, "t")),
            ],
        );

        let txn = store.read().unwrap();
        let under = |ptid| {
            txn.scan_index(tenant_id(), ptid, IID, ProjectionId::from_u128(42))
                .map(Result::unwrap)
                .collect::<Vec<_>>()
        };
        assert_eq!(under(Todo::TYPE_ID), vec![ProjectionId::from_u128(7)]);
        assert_eq!(under(UserById::TYPE_ID), vec![ProjectionId::from_u128(42)]);
    }

    /// Lists two indices under one id.
    #[derive(Debug, Serialize, Deserialize)]
    struct TwoUnderOneId {
        id: TodoId,
        user_id: UserId,
    }

    impl Projection for TwoUnderOneId {
        type Event<'a> = event::TodoEvent<'a>;
        const TYPE_ID: ProjectionTypeId = ProjectionTypeId::from_u32(0xD0);
        const INDICES: Indices<Self> = &[
            &Index::<Self, User>::new(IID, |t| Ref::new(t.user_id.into())),
            &Index::<Self, Self>::new(IID, TwoUnderOneId::id),
        ];

        fn id(&self) -> Ref<Self> {
            Ref::new(self.id.into())
        }

        fn next(_: &event::TodoEvent, _: &View<'_>) -> Result<Vec<Write<Self>>, Error> {
            Ok(vec![])
        }
    }

    #[test]
    fn register_rejects_two_indices_under_one_id() {
        let mut manager = ProjectorManager::<event::Event>::new(tenant());
        let err = manager
            .register::<TwoUnderOneId>()
            .expect_err("duplicate index must err");
        assert!(
            matches!(err, crate::Error::IndexAlreadyRegistered(1)),
            "{err:?}"
        );
        assert!(manager.projectors.is_empty(), "nothing half-registered");
    }
}
