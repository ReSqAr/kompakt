#![allow(unused_crate_dependencies, reason = "example code")]

use futures::FutureExt;
use nvzn::{
    Dispatcher, Index, IndexId, Indices, Projection, ProjectionId, ProjectionTypeId, Ref,
    StoreConfig, Tenant, View, Write, testing::TestLog,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::PathBuf;
use tokio_util::sync::CancellationToken;
use zslg::{ObjectStoreConfig, PeerAddr};

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
struct UserId(u128);

impl From<UserId> for ProjectionId {
    fn from(val: UserId) -> Self {
        ProjectionId::from_u128(val.0)
    }
}

impl From<UserId> for Ref<User> {
    fn from(val: UserId) -> Self {
        Ref::new(val.into())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct NewUserV1 {
    id: UserId,
    name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct NewUserV2 {
    id: UserId,
    name: String,
    address: Option<String>,
}

impl From<NewUserV1> for NewUserV2 {
    fn from(NewUserV1 { id, name }: NewUserV1) -> Self {
        Self {
            id,
            name,
            address: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct NewUserV3 {
    id: UserId,
    name: String,
    address: Option<String>,
    email: Option<String>,
}

impl From<NewUserV2> for NewUserV3 {
    fn from(NewUserV2 { id, name, address }: NewUserV2) -> Self {
        Self {
            id,
            name,
            address,
            email: None,
        }
    }
}

nvzn::chained_from!(from: NewUserV1, to: NewUserV3, via: NewUserV2);

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Ord, PartialOrd)]
struct TodoId(u128);

impl From<TodoId> for ProjectionId {
    fn from(val: TodoId) -> Self {
        ProjectionId::from_u128(val.0)
    }
}

impl From<TodoId> for Ref<Todo> {
    fn from(val: TodoId) -> Self {
        Ref::new(val.into())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct NewTodoV1 {
    id: TodoId,
    user_id: UserId,
    title: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct UserDeletedV1 {
    id: UserId,
}

nvzn::define_events! {
    event::Event = {
        event 1 := NewUser {
            version 3 => NewUserV3
        } upgrades from {
            version 1 => NewUserV1,
            version 2 => NewUserV2,
        }

        event 2 := NewTodo {
            version 1 => NewTodoV1
        }

        event 3 := UserDeleted {
            version 1 => UserDeletedV1
        }
    } subsets {
        UserEvent := NewUser | UserDeleted,
        TodoEvent := NewTodo | UserDeleted,
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct User {
    id: UserId,
    name: String,
    address: Option<String>,
    email: Option<String>,
}

/// One row per event: each arm is one `Write` helper.
impl Projection for User {
    type Event<'a> = event::UserEvent<'a>;
    const TYPE_ID: ProjectionTypeId = ProjectionTypeId::from_string_hash("User", 0);

    fn id(&self) -> Ref<Self> {
        Ref::new(self.id.into())
    }

    fn next(event: &event::UserEvent, view: &View<'_>) -> Result<Vec<Write<Self>>, nvzn::Error> {
        match event {
            event::UserEvent::NewUser(n) => Write::insert(view, n.id, || User {
                id: n.id,
                name: n.name.clone(),
                address: n.address.clone(),
                email: n.email.clone(),
            }),
            event::UserEvent::UserDeleted(u) => Write::delete(u.id),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Todo {
    id: TodoId,
    user_id: UserId,
    title: String,
}

impl Todo {
    #[expect(dead_code, reason = "example code")]
    fn id(&self) -> TodoId {
        self.id
    }

    const BY_USER: Index<Todo, User> =
        Index::new(IndexId::from_string_hash("todo->user", 0), Todo::user);

    fn user(&self) -> Ref<User> {
        Ref::new(self.user_id.into())
    }
}

/// Deleting a user deletes their todos, which no single-row shorthand can
/// say: `next` reads the reverse index and returns every row it changes.
impl Projection for Todo {
    type Event<'a> = event::TodoEvent<'a>;
    const TYPE_ID: ProjectionTypeId = ProjectionTypeId::from_string_hash("Todo", 0);
    const INDICES: Indices<Self> = &[&Todo::BY_USER];

    fn id(&self) -> Ref<Self> {
        Ref::new(self.id.into())
    }

    fn next(event: &event::TodoEvent, view: &View<'_>) -> Result<Vec<Write<Self>>, nvzn::Error> {
        match event {
            event::TodoEvent::NewTodo(n) if view.get(Ref::<Todo>::from(n.id))?.is_some() => {
                Ok(vec![])
            }
            event::TodoEvent::NewTodo(n) => Ok(vec![Write::Set(Todo {
                id: n.id,
                user_id: n.user_id,
                title: n.title.clone(),
            })]),
            event::TodoEvent::UserDeleted(u) => view
                .lookup(Todo::BY_USER, u.id)
                .map(|todo| todo.map(Write::Delete))
                .collect(),
        }
    }
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

/// Until `live` serves the write `d` (it starts once the fold has caught up).
async fn seen(live: &nvzn::Live<event::Event>, d: zslg::DataId) {
    loop {
        match live.wait_for(d, std::time::Duration::from_secs(5)).await {
            nvzn::Wait::Seen => return,
            nvzn::Wait::NotStarted => {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await
            }
            other => panic!("the live view did not see the write: {other:?}"),
        }
    }
}

#[tokio::main]
async fn main() {
    init_tracing();
    let tmpdir = tempfile::tempdir().expect("tempdir");

    let tenant = Tenant::new("todo", 0);
    let config = StoreConfig::new(tmpdir.path().join("rocksdb"));

    let db = Db::Zslg(tmpdir.path().join("zslg"));
    //let db = Db::InMemory;

    let (mut dispatcher, rig) = setup_db(db, &config).boxed().await;
    let mut manager = dispatcher.new_handler(tenant);

    manager.register::<User>().expect("registration failed");
    manager.register::<Todo>().expect("registration failed");

    let (writer, reader) = manager.finalise().expect("registration failed");

    let handle = dispatcher.finalise().await.expect("handle");
    // Every projection live: the example reads its own writes at once.
    let live = reader.live::<event::Event>().expect("live view");

    let user = NewUserV3 {
        id: UserId(0),
        name: "James Holden".to_string(),
        address: Some(String::from("Rocinante")),
        email: None,
    };
    let todo1 = NewTodoV1 {
        id: TodoId(0),
        user_id: UserId(0),
        title: "Remember the Cant".to_string(),
    };
    let todo2 = NewTodoV1 {
        id: TodoId(1),
        user_id: UserId(0),
        title: "Save the world".to_string(),
    };

    let written = writer
        .write(&[user.into(), todo1.into(), todo2.into()])
        .await
        .expect("encryption failed");

    seen(&live, written).await;
    let snapshot = live.snapshot().expect("snapshot failed");
    let all_users = snapshot.select::<User>();
    let all_todos = snapshot.select::<Todo>();

    let users_with_todos = all_users.join_idx(Todo::BY_USER).on_collected(User::id);
    let users_with_todos = users_with_todos
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("users failed");
    println!("Users with todos: {:?}", users_with_todos);

    assert_eq!(users_with_todos.len(), 1);
    let (user0, todos_of_user0) = users_with_todos.first().unwrap();
    assert_eq!(user0.id, UserId(0));
    assert_eq!(todos_of_user0.len(), 2);

    let todos_with_users = all_todos.join_pk(Todo::user);
    let todos_with_users = todos_with_users.collect().expect("collect failed");
    println!("Todos with users: {:?}", todos_with_users);

    assert_eq!(todos_with_users.len(), 2);
    for (_, u) in &todos_with_users {
        assert_eq!(u.id, UserId(0));
    }
    let t_ids = todos_with_users
        .into_iter()
        .map(|(t, _u)| t.id)
        .collect::<BTreeSet<TodoId>>();
    assert_eq!(t_ids, BTreeSet::from_iter([TodoId(0), TodoId(1)]));

    let user0 = snapshot.with_iter([UserId(0)]);
    let user0_with_todos = user0
        .join_pk(|uid| Ref::<User>::new((*uid).into()))
        .join_idx(Todo::BY_USER)
        .on(|(uid, _)| Ref::<User>::new((*uid).into()))
        .try_map(|((uid, u), ts)| Ok((uid, u, ts.collect::<Result<Vec<_>, _>>()?)))
        .collect()
        .expect("collect failed");
    println!("User0 with todos: {:?}", user0_with_todos);

    assert_eq!(user0_with_todos.len(), 1);
    let (uid0, u0, ts) = user0_with_todos.into_iter().next().expect("no error");
    assert_eq!(uid0, UserId(0));
    assert_eq!(u0.id, UserId(0));
    assert_eq!(ts.len(), 2);

    let deleted = writer
        .write(&[UserDeletedV1 { id: UserId(0) }.into()])
        .await
        .expect("write failed");
    seen(&live, deleted).await;
    let after = live.snapshot().expect("snapshot failed");
    assert!(
        after
            .get(Ref::<User>::from(UserId(0)))
            .expect("get failed")
            .is_none()
    );
    assert!(
        after
            .select::<Todo>()
            .collect()
            .expect("collect failed")
            .is_empty()
    );

    rig.shutdown(handle).await;
}

#[allow(dead_code, reason = "easy switch")]
enum Db {
    InMemory,
    Zslg(PathBuf),
}

/// Whatever has to outlive the dispatcher: the in-process log, or the token
/// that stops the zslg node.
enum Rig {
    InMemory(TestLog),
    Zslg(CancellationToken),
}

impl Rig {
    async fn shutdown(self, handle: tokio::task::JoinHandle<Result<(), nvzn::Error>>) {
        match self {
            // Dropping the log drops the senders the fold subscribes to, which
            // ends the fold as an aborted subscription — not a clean stop, so
            // its result is not awaited here.
            Self::InMemory(log) => drop(log),
            // Cancelling the node ends the fold's stable-log subscription with
            // `ChannelEnd::Cancelled`, even on a quiet log, so the dispatcher
            // stops cleanly and can be awaited.
            Self::Zslg(token) => {
                token.cancel();
                handle
                    .await
                    .expect("dispatcher task panicked")
                    .expect("a cancelled dispatcher stops cleanly");
            }
        }
    }
}

#[allow(
    clippy::large_stack_frames,
    reason = "one-shot example startup; every await in it is already boxed, and what is left is \
              the two arms' locals side by side"
)]
async fn setup_db(db: Db, config: &StoreConfig) -> (Dispatcher, Rig) {
    match db {
        Db::InMemory => {
            let log = TestLog::new();
            let dispatcher = Dispatcher::in_memory(log.writer(), log.reader())
                .boxed()
                .await
                .expect("dispatcher");
            (dispatcher, Rig::InMemory(log))
        }
        Db::Zslg(zslg_path) => {
            let listen: std::net::SocketAddr = "127.0.0.1:0".parse().expect("valid addr");
            let peers = vec![PeerAddr {
                addr: listen.into(),
                server_name: "localhost".into(),
            }];
            let mut cfg = zslg::ReplicaNodeConfig::new(
                "localhost".into(),
                peers,
                "localhost".into(),
                listen,
                zslg_path,
                ObjectStoreConfig::InMemory,
            )
            .expect("cfg failed");
            cfg.membership_replica_node_count = 1;

            let node = zslg::ReplicaNode::start(cfg)
                .boxed()
                .await
                .expect("db failure")
                .wait_until_ready()
                .boxed()
                .await
                .expect("wait until ready");

            let token = node.shutdown_token();
            let writer = node.writer();
            let reader = node.reader();

            tokio::spawn(async move {
                node.run().boxed().await.expect("db failure");
            });

            let dispatcher = Dispatcher::new(config, writer, reader, &token)
                .boxed()
                .await
                .expect("dispatcher");
            (dispatcher, Rig::Zslg(token))
        }
    }
}
