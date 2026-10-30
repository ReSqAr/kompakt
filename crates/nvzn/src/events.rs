use serde::Serialize;
use serde::de::DeserializeOwned;
use serde::ser::SerializeSeq;

pub trait WithWireFormat: Clone + Send + Sync + Sized + From<Self::WireFormat> + 'static {
    type WireFormat: Serialize + DeserializeOwned + Send + Sync + From<Self>;

    /// This event's number: its `event N :=` in [`define_events!`], the key
    /// [`EventSet::EVENTS`] lists.
    fn number(&self) -> u32;

    type WireFormatRef<'a>: Serialize + From<&'a Self>
    where
        Self: 'a;

    fn encode<B>(events: &[Self], buf: B) -> Result<B, postcard::Error>
    where
        B: Extend<u8>,
    {
        postcard::to_extend(&AsWire(events), buf)
    }

    fn decode(payload: &[u8]) -> Result<Vec<Self>, postcard::Error> {
        let wire: Vec<Self::WireFormat> = postcard::from_bytes(payload)?;
        Ok(wire.into_iter().map(Self::from).collect())
    }
}

struct AsWire<'a, E>(&'a [E]);

impl<'a, E> Serialize for AsWire<'a, E>
where
    E: WithWireFormat,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for event in self.0 {
            seq.serialize_element(&E::WireFormatRef::from(event))?;
        }
        seq.end()
    }
}

/// A set of events by number: a subset enum of [`define_events!`], or a
/// whole event enum (which lists all of its events). What
/// [`Projection::EVENTS`](crate::Projection::EVENTS) and a live view's `S` are
/// compared by ([`is_live`](crate::is_live)).
pub trait EventSet {
    const EVENTS: &'static [u32];
}

/// Whether every number in `a` is in `b`. `const`, so [`is_live`] works in a
/// `const` assertion.
///
/// [`is_live`]: crate::is_live
pub(crate) const fn subset(a: &[u32], b: &[u32]) -> bool {
    let mut rest = a;
    while let [first, tail @ ..] = rest {
        if !contains(b, *first) {
            return false;
        }
        rest = tail;
    }
    true
}

pub(crate) const fn contains(set: &[u32], number: u32) -> bool {
    let mut rest = set;
    while let [first, tail @ ..] = rest {
        if *first == number {
            return true;
        }
        rest = tail;
    }
    false
}

/// A subset of `E`, borrowed from the event: what [`Projection::next`]
/// receives, so folding an event copies none of it.
///
/// [`Projection::next`]: crate::Projection::next
pub trait SubEventOf<'a, E>: Sized {
    fn project(event: &'a E) -> Option<Self>;
}

#[macro_export]
macro_rules! chained_from {
    (from: $src:ty, to: $dst:ty, via: $first:ty $(=> $rest:ty)* $(,)?) => {
        impl ::core::convert::From<$src> for $dst {
            fn from(v: $src) -> Self {
                let v: $first = ::core::convert::From::from(v);
                $( let v: $rest = ::core::convert::From::from(v); )*
                ::core::convert::From::from(v)
            }
        }
    };

    (from: $src:ty, to: $dst:ty $(,)?) => {
        impl ::core::convert::From<$src> for $dst {
            fn from(v: $src) -> Self {
                ::core::convert::From::from(v)
            }
        }
    };
}

/// Defines an event enum, its versioned wire format, and any subset enums.
///
/// Subsets come in two kinds, for two jobs:
///
/// ```text
///   subsets { UserEvent := NewUser | Pinged }   a view: UserEvent<'a> borrows from &Event
///                                               (SubEventOf::project); what folds read,
///                                               decoded once, never copied
///   owned   { Writes := NewUser }               owned: Writes { NewUser(NewUserV1) }, with
///                                               From<NewUserV1>, From<Writes> for Event and
///                                               TryFrom<Event> (a non-member comes back as
///                                               the error), all moves; what a write is built from
/// ```
///
/// An owned subset has no conversion from `&Event` (it would have to clone),
/// and an event outside it does not convert into it:
///
/// ```compile_fail
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Debug, Serialize, Deserialize, Clone)] pub struct NoteSetV1 { id: u128 }
/// # #[derive(Debug, Serialize, Deserialize, Clone)] pub struct FileSeenV1 { id: u128 }
/// nvzn::define_events! {
///     event::Event = {
///         event 1 := NoteSet { version 1 => NoteSetV1 }
///         event 2 := FileSeen { version 1 => FileSeenV1 }
///     } owned {
///         Notes := NoteSet,
///     }
/// }
/// fn main() {
///     let _: event::Notes = FileSeenV1 { id: 1 }.into();
/// }
/// ```
///
/// whereas a member does, and the subset moves into the event:
///
/// ```
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Debug, Serialize, Deserialize, Clone)] pub struct NoteSetV1 { id: u128 }
/// # #[derive(Debug, Serialize, Deserialize, Clone)] pub struct FileSeenV1 { id: u128 }
/// nvzn::define_events! {
///     event::Event = {
///         event 1 := NoteSet { version 1 => NoteSetV1 }
///         event 2 := FileSeen { version 1 => FileSeenV1 }
///     } owned {
///         Notes := NoteSet,
///     }
/// }
/// fn main() {
///     let note: event::Notes = NoteSetV1 { id: 1 }.into();
///     let _: event::Event = note.into();
/// }
/// ```
///
/// Each variant is identified on the wire by an `(event number, version
/// number)` tag, so both halves have to be unique: a repeated event number, or
/// a version repeated within one event, is a compile error (E0081, pointing at
/// both offending literals). See `__nvzn_unique_tags` in the expansion.
#[macro_export]
macro_rules! define_events {
    (
        $modname:ident :: $eventname:ident = {
            $(
                event $event_num:literal := $event:ident {
                    version $cur_num:literal => $current:ident $(,)?
                }
                $( upgrades from {
                    $( version $ver_num:literal => $variant:ident ),* $(,)?
                } )?
            )+
        }
        $( subsets {
            $(
                $sub:ident := $sub_first:ident $( | $sub_rest:ident )*
            ),* $(,)?
        } )?
        $( owned {
            $(
                $own:ident := $own_first:ident $( | $own_rest:ident )*
            ),* $(,)?
        } )?
    ) => {
        pub mod $modname {
            #![allow(
                clippy::large_stack_frames,
                reason = "the decode method keeps a slot per event in an unoptimised build, so its frame grows with every event"
            )]
            #![allow(
                clippy::enum_variant_names,
                reason = "the subset enums are named after the events they hold"
            )]
            #![allow(
                clippy::wildcard_imports,
                reason = "the module only wraps the macro: it sees what its parent defines"
            )]

            use super::*;

            mod __nvzn_tags {
                #![allow(
                    non_upper_case_globals,
                    reason = "each tag is named after the version type it identifies"
                )]

                const fn tag(event: u32, ver: u32) -> [u8; 8] {
                    let e = event.to_be_bytes();
                    let v = ver.to_be_bytes();
                    [e[0], e[1], e[2], e[3], v[0], v[1], v[2], v[3]]
                }

                $(
                    pub const $current: [u8; 8] = tag($event_num, $cur_num);
                    $( $( pub const $variant: [u8; 8] = tag($event_num, $ver_num); )* )?
                )+
            }


            // Compile-time uniqueness of the tag space.
            //
            // A tag is `(event number, version number)`, so a repeated event
            // number — or a version repeated within one event — mints two names
            // for one tag, and nothing downstream notices: `Serialize` writes
            // the same eight bytes for both, and `Deserialize` takes whichever
            // arm the expansion emitted first, so the second event silently
            // decodes as the first.
            //
            // Duplicate enum discriminants are rustc's own error (E0081), which
            // reports both offending literals at their site in the invocation.
            // Unique event numbers, plus versions unique within each event,
            // give unique tags.
            mod __nvzn_unique_tags {
                #![allow(
                    dead_code,
                    reason = "never constructed; these exist for their discriminants"
                )]

                #[repr(u32)]
                enum EventNumbersMustBeUnique {
                    $( $event = $event_num, )+
                }

                $(
                    // Named after the event, so E0081 names it too. The
                    // variants are the version types, which `WireFormat`
                    // already forces to be distinct.
                    #[repr(u32)]
                    enum $event {
                        $current = $cur_num,
                        $( $( $variant = $ver_num, )* )?
                    }
                )+
            }

            mod __nvzn_types {
                use super::*;

                $( pub type $event = $current; )+
            }

            mod __nvzn_numbers {
                #![allow(
                    non_upper_case_globals,
                    reason = "each number is named after its event"
                )]

                $( pub const $event: u32 = $event_num; )+
            }

            #[derive(Clone, Debug)]
            pub enum $eventname {
                $( $event($current), )+
            }

            pub enum WireFormat {
                $(
                    $current($current),
                    $( $( $variant($variant), )* )?
                )+
            }

            pub enum WireFormatRef<'a> {
                $( $current(&'a $current), )+
            }

            impl<'a> ::core::convert::From<&'a $eventname> for WireFormatRef<'a> {
                fn from(v: &'a $eventname) -> Self {
                    match v {
                        $( $eventname::$event(x) => WireFormatRef::$current(x), )+
                    }
                }
            }

            $(
                impl ::core::convert::From<$current> for $eventname {
                    fn from(v: $current) -> Self { $eventname::$event(v) }
                }
            )+

            impl ::core::convert::From<$eventname> for WireFormat {
                fn from(v: $eventname) -> Self {
                    match v {
                        $( $eventname::$event(x) => WireFormat::$current(x), )+
                    }
                }
            }

            impl ::core::convert::From<WireFormat> for $eventname {
                fn from(v: WireFormat) -> Self {
                    match v {
                        $(
                            WireFormat::$current(x) => $eventname::$event(x),
                            $( $(
                                WireFormat::$variant(x) => $eventname::$event(x.into()),
                            )* )?
                        )+
                    }
                }
            }

            impl $crate::WithWireFormat for $eventname {
                type WireFormat = WireFormat;
                type WireFormatRef<'a> = WireFormatRef<'a>;

                fn number(&self) -> u32 {
                    match self {
                        $( $eventname::$event(_) => __nvzn_numbers::$event, )+
                    }
                }
            }

            impl $crate::EventSet for $eventname {
                const EVENTS: &'static [u32] = &[$( __nvzn_numbers::$event, )+];
            }

            impl<'a> $crate::EventSet for &'a $eventname {
                const EVENTS: &'static [u32] = &[$( __nvzn_numbers::$event, )+];
            }

            impl<'a> $crate::SubEventOf<'a, $eventname> for &'a $eventname {
                fn project(event: &'a $eventname) -> ::core::option::Option<Self> {
                    ::core::option::Option::Some(event)
                }
            }

            // A projection folds a subset by reference: the event is decoded
            // once and every projection reads that one copy.
            $( $(
                #[derive(Clone, Copy, Debug)]
                pub enum $sub<'a> {
                    $sub_first(&'a __nvzn_types::$sub_first),
                    $( $sub_rest(&'a __nvzn_types::$sub_rest), )*
                }

                impl<'a> $crate::EventSet for $sub<'a> {
                    const EVENTS: &'static [u32] = &[
                        __nvzn_numbers::$sub_first,
                        $( __nvzn_numbers::$sub_rest, )*
                    ];
                }

                impl<'a> $crate::SubEventOf<'a, $eventname> for $sub<'a> {
                    fn project(event: &'a $eventname) -> ::core::option::Option<Self> {
                        #[allow(unreachable_patterns, reason="in case the user lists all variants the _ arm is unreachable")]
                        match event {
                            $eventname::$sub_first(x) => ::core::option::Option::Some(
                                $sub::$sub_first(x),
                            ),
                            $( $eventname::$sub_rest(x) => ::core::option::Option::Some(
                                $sub::$sub_rest(x),
                            ), )*
                            _ => ::core::option::Option::None,
                        }
                    }
                }
            )* )?

            // A write builds an owned subset: shaped like the event enum,
            // restricted to its members, and converted into it by move.
            $( $(
                #[derive(Clone, Debug)]
                pub enum $own {
                    $own_first(__nvzn_types::$own_first),
                    $( $own_rest(__nvzn_types::$own_rest), )*
                }

                impl $crate::EventSet for $own {
                    const EVENTS: &'static [u32] = &[
                        __nvzn_numbers::$own_first,
                        $( __nvzn_numbers::$own_rest, )*
                    ];
                }

                impl ::core::convert::From<$own> for $eventname {
                    fn from(v: $own) -> Self {
                        match v {
                            $own::$own_first(x) => $eventname::$own_first(x),
                            $( $own::$own_rest(x) => $eventname::$own_rest(x), )*
                        }
                    }
                }

                // The way back, by move: an event outside the subset is
                // returned as it came.
                impl ::core::convert::TryFrom<$eventname> for $own {
                    type Error = $eventname;
                    fn try_from(v: $eventname) -> ::core::result::Result<Self, $eventname> {
                        #[allow(unreachable_patterns, reason = "a subset may list every event")]
                        match v {
                            $eventname::$own_first(x) => ::core::result::Result::Ok($own::$own_first(x)),
                            $( $eventname::$own_rest(x) => ::core::result::Result::Ok($own::$own_rest(x)), )*
                            other => ::core::result::Result::Err(other),
                        }
                    }
                }

                impl ::core::convert::From<__nvzn_types::$own_first> for $own {
                    fn from(v: __nvzn_types::$own_first) -> Self { $own::$own_first(v) }
                }
                $(
                    impl ::core::convert::From<__nvzn_types::$own_rest> for $own {
                        fn from(v: __nvzn_types::$own_rest) -> Self { $own::$own_rest(v) }
                    }
                )*
            )* )?

            impl ::serde::Serialize for WireFormat {
                fn serialize<S>(&self, serializer: S) -> ::core::result::Result<S::Ok, S::Error>
                where
                    S: ::serde::Serializer,
                {
                    use ::serde::ser::SerializeTuple;

                    let mut tuple = serializer.serialize_tuple(2)?;
                    match self {
                        $(
                            Self::$current(value) => {
                                tuple.serialize_element(&__nvzn_tags::$current)?;
                                tuple.serialize_element(value)?;
                            }
                            $( $(
                                Self::$variant(value) => {
                                    tuple.serialize_element(&__nvzn_tags::$variant)?;
                                    tuple.serialize_element(value)?;
                                }
                            )* )?
                        )+
                    }
                    tuple.end()
                }
            }

            impl<'a> ::serde::Serialize for WireFormatRef<'a> {
                fn serialize<S>(&self, serializer: S) -> ::core::result::Result<S::Ok, S::Error>
                where
                    S: ::serde::Serializer,
                {
                    use ::serde::ser::SerializeTuple;

                    let mut tuple = serializer.serialize_tuple(2)?;
                    match self {
                        $(
                            Self::$current(value) => {
                                tuple.serialize_element(&__nvzn_tags::$current)?;
                                tuple.serialize_element(*value)?;
                            }
                        )+
                    }
                    tuple.end()
                }
            }

            impl<'de> ::serde::Deserialize<'de> for WireFormat {
                fn deserialize<D>(deserializer: D) -> ::core::result::Result<Self, D::Error>
                where
                    D: ::serde::Deserializer<'de>,
                {
                    struct EventVisitor;

                    impl<'de> ::serde::de::Visitor<'de> for EventVisitor {
                        type Value = WireFormat;

                        fn expecting(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                            f.write_str("an event tag followed by its payload")
                        }

                        #[deny(unreachable_patterns)]
                        fn visit_seq<A>(self, mut seq: A) -> ::core::result::Result<Self::Value, A::Error>
                        where
                            A: ::serde::de::SeqAccess<'de>,
                        {
                            let tag_bytes: [u8; 8] = seq
                                .next_element()?
                                .ok_or_else(|| ::serde::de::Error::invalid_length(0, &self))?;

                            match tag_bytes {
                                $(
                                    __nvzn_tags::$current => {
                                        let value = seq
                                            .next_element()?
                                            .ok_or_else(|| ::serde::de::Error::invalid_length(1, &self))?;
                                        ::core::result::Result::Ok(WireFormat::$current(value))
                                    }
                                    $( $(
                                        __nvzn_tags::$variant => {
                                            let value = seq
                                                .next_element()?
                                                .ok_or_else(|| ::serde::de::Error::invalid_length(1, &self))?;
                                            ::core::result::Result::Ok(WireFormat::$variant(value))
                                        }
                                    )* )?
                                )+
                                _ => ::core::result::Result::Err(::serde::de::Error::invalid_value(
                                    ::serde::de::Unexpected::Bytes(&tag_bytes),
                                    &self,
                                )),
                            }
                        }
                    }

                    deserializer.deserialize_tuple(2, EventVisitor)
                }
            }
        }
    };
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
mod tests {
    use super::*;
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
            any::<u128>()
                .prop_map(|id| event::Event::TodoDeleted(TodoDeletedV1 { id: TodoId(id) })),
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
}
