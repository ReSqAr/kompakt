//! `btrb::Live` through the public API, on nvzn's in-memory log: a live write
//! returns once its own view serves it.

#![allow(unused_crate_dependencies, reason = "integration test")]
#![allow(clippy::unwrap_used, clippy::expect_used, reason = "test code")]

use btrb::{LiveSettings, Nvzn};
use nvzn::{
    Dispatcher, Error, Projection, ProjectionId, ProjectionTypeId, Ref, Tenant, View, Write,
    testing::TestLog,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
struct NoteId(u128);

impl From<NoteId> for ProjectionId {
    fn from(val: NoteId) -> Self {
        ProjectionId::from_u128(val.0)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct NoteSetV1 {
    id: NoteId,
    text: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct FileSeenV1 {
    id: u128,
}

nvzn::define_events! {
    event::Event = {
        event 1 := NoteSet {
            version 1 => NoteSetV1
        }
        event 2 := FileSeen {
            version 1 => FileSeenV1
        }
    } subsets {
        NoteEvent := NoteSet,
    } owned {
        LiveEvent := NoteSet,
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Note {
    id: NoteId,
    text: String,
}

impl Projection for Note {
    type Event<'a> = event::NoteEvent<'a>;
    const TYPE_ID: ProjectionTypeId = ProjectionTypeId::from_string_hash("Note", 0);

    fn id(&self) -> Ref<Self> {
        Ref::new(self.id.into())
    }

    fn next(event: &event::NoteEvent, view: &View<'_>) -> Result<Vec<Write<Self>>, Error> {
        let event::NoteEvent::NoteSet(n) = event;
        Write::insert(view, Ref::new(n.id.into()), || Note {
            id: n.id,
            text: n.text.clone(),
        })
    }
}

#[tokio::test]
async fn a_live_write_returns_once_its_own_view_serves_it() {
    let log = TestLog::new();
    let mut dispatcher = Dispatcher::in_memory(log.writer(), log.reader())
        .await
        .unwrap();
    let mut nvzn = Nvzn::<event::Event, event::LiveEvent>::new(
        dispatcher.new_handler::<event::Event>(Tenant::new("notes", 0)),
        LiveSettings::for_test(),
    );
    nvzn.register::<Note>().unwrap();
    let (_writer, _reader, live) = nvzn.finish().unwrap();
    let _fold = dispatcher.finalise().await.unwrap();

    // A live write before the view starts (after the replay) would return at
    // once, its view not started.
    tokio::time::timeout(Duration::from_secs(5), live.started())
        .await
        .expect("the view started");

    let written = live
        .write([NoteSetV1 {
            id: NoteId(1),
            text: "remember the Cant".into(),
        }
        .into()])
        .await
        .unwrap();
    let _id = written.data_id();
    let seen = tokio::task::spawn_blocking(move || {
        live.snapshot()
            .unwrap()
            .get(Ref::<Note>::new(NoteId(1).into()))
            .unwrap()
    })
    .await
    .unwrap();
    assert!(
        seen.is_some(),
        "a returned live write is served by its own view"
    );
}
