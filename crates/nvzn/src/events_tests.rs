//! Event-set and wire-format tests; they use the test projections, so they live
//! with the facade.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]

use nvzn_types::*;

use crate::testsupport::{
    NewTodoV1, NewUserV1, NewUserV2, NewUserV3, PingedV1, TodoDeletedV1, TodoId, UserId, event,
};
use proptest::prelude::*;

fn arb_user() -> impl Strategy<Value = NewUserV3> {
    (
        any::<u128>(),
        "[a-z]{1,8}",
        any::<Option<String>>(),
        any::<Option<String>>(),
    )
        .prop_map(|(id, name, address, email)| NewUserV3 {
            id: UserId(id),
            name,
            address,
            email,
        })
}

fn arb_event() -> impl Strategy<Value = event::Event> {
    prop_oneof![
        arb_user().prop_map(event::Event::NewUser),
        (any::<u128>(), any::<u128>())
            .prop_map(|(id, ts)| event::Event::Pinged(PingedV1 { id: UserId(id), ts })),
        (any::<u128>(), any::<u128>(), "[a-z]{1,8}").prop_map(|(id, user_id, title)| {
            event::Event::NewTodo(NewTodoV1 {
                id: TodoId(id),
                user_id: UserId(user_id),
                title,
            })
        }),
        any::<u128>().prop_map(|id| event::Event::TodoDeleted(TodoDeletedV1 { id: TodoId(id) })),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn events_wire_roundtrip(events in proptest::collection::vec(arb_event(), 0..8)) {
        let encoded = event::Event::encode(&events, Vec::new()).unwrap();
        let decoded = event::Event::decode(&encoded).unwrap();
        prop_assert_eq!(format!("{decoded:?}"), format!("{events:?}"));
    }
}

#[test]
fn encode_borrows_and_matches_the_owned_encoding() {
    let events = vec![
        event::Event::NewUser(NewUserV3 {
            id: UserId(7),
            name: "holden".to_string(),
            address: Some("Rocinante".to_string()),
            email: None,
        }),
        event::Event::Pinged(PingedV1 {
            id: UserId(7),
            ts: 42,
        }),
        event::Event::NewTodo(NewTodoV1 {
            id: TodoId(1),
            user_id: UserId(7),
            title: "Remember the Cant".to_string(),
        }),
    ];

    let owned: Vec<event::WireFormat> = events.iter().map(|e| e.clone().into()).collect();
    let via_owned = postcard::to_stdvec(&owned).unwrap();
    let via_borrowed = event::Event::encode(&events, Vec::new()).unwrap();

    assert_eq!(
        via_borrowed, via_owned,
        "the borrowed encoding must be byte-identical to the owned one"
    );

    let round_tripped = event::Event::decode(&via_borrowed).unwrap();
    assert_eq!(format!("{round_tripped:?}"), format!("{events:?}"));
}

/// An owned subset is a set like a view, and converts into the event
/// by move: the same event a direct conversion builds.
#[test]
fn an_owned_subset_moves_into_the_event_it_restricts() {
    use crate::EventSet;
    assert_eq!(
        <event::UserWrite as EventSet>::EVENTS,
        <event::UserEvent<'static> as EventSet>::EVENTS
    );
    let ping = PingedV1 {
        id: UserId(7),
        ts: 42,
    };
    let via_subset: event::Event = event::UserWrite::from(ping.clone()).into();
    let direct: event::Event = ping.into();
    assert_eq!(format!("{via_subset:?}"), format!("{direct:?}"));

    // And back: a member converts, a non-member is returned unchanged.
    let back = event::UserWrite::try_from(direct).unwrap();
    assert_eq!(
        format!("{:?}", event::Event::from(back)),
        format!("{via_subset:?}")
    );
    let todo: event::Event = NewTodoV1 {
        id: TodoId(1),
        user_id: UserId(7),
        title: "milk".into(),
    }
    .into();
    let refused = event::UserWrite::try_from(todo.clone()).unwrap_err();
    assert_eq!(format!("{refused:?}"), format!("{todo:?}"));
}

/// An empty slice still writes the zero-length sequence a reader expects.
#[test]
fn encode_of_no_events_matches_the_empty_vec() {
    let empty: Vec<event::Event> = Vec::new();
    let owned: Vec<event::WireFormat> = Vec::new();
    assert_eq!(
        event::Event::encode(&empty, Vec::new()).unwrap(),
        postcard::to_stdvec(&owned).unwrap()
    );
    assert!(
        event::Event::decode(&postcard::to_stdvec(&owned).unwrap())
            .unwrap()
            .is_empty()
    );
}

#[test]
fn events_decode_upgrades_v1() {
    let v1 = NewUserV1 {
        id: UserId(7),
        name: "holden".to_string(),
    };
    let bytes = postcard::to_stdvec(&vec![event::WireFormat::NewUserV1(v1)]).unwrap();
    let decoded = event::Event::decode(&bytes).unwrap();
    match decoded.first() {
        Some(event::Event::NewUser(u)) => {
            assert_eq!(u.id, UserId(7));
            assert_eq!(u.name, "holden");
            assert_eq!(u.address, None);
            assert_eq!(u.email, None);
        }
        other => panic!("unexpected variant {other:?}"),
    }
}

#[test]
fn events_decode_upgrades_v2() {
    let v2 = NewUserV2 {
        id: UserId(8),
        name: "naomi".to_string(),
        address: Some("Rocinante".to_string()),
    };
    let bytes = postcard::to_stdvec(&vec![event::WireFormat::NewUserV2(v2)]).unwrap();
    let decoded = event::Event::decode(&bytes).unwrap();
    match decoded.first() {
        Some(event::Event::NewUser(u)) => {
            assert_eq!(u.id, UserId(8));
            assert_eq!(u.address.as_deref(), Some("Rocinante"));
            assert_eq!(u.email, None);
        }
        other => panic!("unexpected variant {other:?}"),
    }
}

#[test]
fn events_decode_rejects_unknown_tag() {
    let mut payload = vec![0x01u8];
    payload.extend_from_slice(&[0xFF; 8]);
    payload.push(0x00);
    assert!(event::Event::decode(&payload).is_err());
}

#[test]
fn events_decode_rejects_empty_payload() {
    assert!(event::Event::decode(&[]).is_err());
}

#[test]
fn event_tags_are_unique() {
    fn tag_of(wire: &event::WireFormat) -> [u8; 8] {
        let bytes = postcard::to_stdvec(wire).unwrap();
        bytes[..8].try_into().expect("8 tag bytes")
    }

    let tags = [
        tag_of(&event::WireFormat::NewUserV1(NewUserV1 {
            id: UserId(0),
            name: String::new(),
        })),
        tag_of(&event::WireFormat::NewUserV2(NewUserV2 {
            id: UserId(0),
            name: String::new(),
            address: None,
        })),
        tag_of(&event::WireFormat::NewUserV3(NewUserV3 {
            id: UserId(0),
            name: String::new(),
            address: None,
            email: None,
        })),
        tag_of(&event::WireFormat::PingedV1(PingedV1 {
            id: UserId(0),
            ts: 0,
        })),
        tag_of(&event::WireFormat::NewTodoV1(NewTodoV1 {
            id: TodoId(0),
            user_id: UserId(0),
            title: String::new(),
        })),
        tag_of(&event::WireFormat::TodoDeletedV1(TodoDeletedV1 {
            id: TodoId(0),
        })),
    ];
    let mut seen = std::collections::HashSet::new();
    for tag in tags {
        assert!(seen.insert(tag), "duplicate tag {tag:?}");
    }
}

#[test]
fn subset_project_matches_variants() {
    let user = event::Event::NewUser(NewUserV3 {
        id: UserId(1),
        name: "ames".to_string(),
        address: None,
        email: None,
    });
    let ping = event::Event::Pinged(PingedV1 {
        id: UserId(1),
        ts: 0,
    });
    let todo = event::Event::TodoDeleted(TodoDeletedV1 { id: TodoId(1) });
    assert!(event::UserEvent::project(&user).is_some());
    assert!(event::UserEvent::project(&ping).is_some());
    assert!(event::UserEvent::project(&todo).is_none());
}

#[test]
fn chained_from_two_hop_conversion() {
    let v1 = NewUserV1 {
        id: UserId(1),
        name: "naomi".to_string(),
    };
    let v3: NewUserV3 = v1.into();
    assert_eq!(v3.name, "naomi");
    assert_eq!(v3.address, None);
    assert_eq!(v3.email, None);
}

#[test]
fn event_tags_are_stable() {
    fn tag_of(wire: &event::WireFormat) -> [u8; 8] {
        let bytes = postcard::to_stdvec(wire).unwrap();
        bytes[..8].try_into().expect("8 tag bytes")
    }
    fn tag(event: u32, version: u32) -> [u8; 8] {
        let mut t = [0u8; 8];
        t[..4].copy_from_slice(&event.to_be_bytes());
        t[4..].copy_from_slice(&version.to_be_bytes());
        t
    }

    assert_eq!(
        tag_of(&event::WireFormat::NewUserV1(NewUserV1 {
            id: UserId(0),
            name: String::new(),
        })),
        tag(1, 1)
    );
    assert_eq!(
        tag_of(&event::WireFormat::NewUserV2(NewUserV2 {
            id: UserId(0),
            name: String::new(),
            address: None,
        })),
        tag(1, 2)
    );
    assert_eq!(
        tag_of(&event::WireFormat::NewUserV3(NewUserV3 {
            id: UserId(0),
            name: String::new(),
            address: None,
            email: None,
        })),
        tag(1, 3)
    );
    assert_eq!(
        tag_of(&event::WireFormat::PingedV1(PingedV1 {
            id: UserId(0),
            ts: 0,
        })),
        tag(2, 1)
    );
    assert_eq!(
        tag_of(&event::WireFormat::NewTodoV1(NewTodoV1 {
            id: TodoId(0),
            user_id: UserId(0),
            title: String::new(),
        })),
        tag(3, 1)
    );
    assert_eq!(
        tag_of(&event::WireFormat::TodoDeletedV1(TodoDeletedV1 {
            id: TodoId(0)
        })),
        tag(4, 1)
    );
}
