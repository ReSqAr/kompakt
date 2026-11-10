//! Flat unions of leaf errors.
//!
//! A *leaf* is an ordinary error enum owned by one domain. A *union* is the
//! error a crate returns: one variant per leaf it can carry, named after the
//! leaf's group, so the depth is always `Union::Leaf(Leaf::Variant)` however
//! long the call chain is.
//!
//! ```text
//!   error_union! {
//!       pub ReadError { Log(LogError), Snapshot(SnapshotError) }
//!       from ArchiveError { Log, Snapshot }      // ArchiveError ⊆ ReadError
//!       bridge SiblingError                      // the same variants both ways: from + into
//!       into PortError                           // ReadError ⊆ PortError (same variant names)
//!   }
//! ```
//!
//! The macro generates the enum, `From<Leaf>` per variant, `Display`,
//! `Error::source`, `From<Source>` for each `from` clause and `From<Union>` for
//! each `into` destination (the destination must have every variant of the union;
//! use `into` where the destination, such as a port crate's union, cannot name
//! this crate). `bridge Peer` is both at once for a peer that has exactly this
//! union's variants: `From<Peer>` and `From<Union>`, no variant list to repeat. The compiler
//! checks the subset rule in both directions:
//!
//! ```text
//!   target lacks a variant the clause names        E0599  no variant ...
//!   clause omits one of the source's variants      E0004  non-exhaustive match
//! ```
//!
//! Conventions that make conversions line up: a union holds leaves only, never
//! another union, and a leaf has the same variant name in every union.
//!
//! The two ways a `from` clause can be wrong are compile errors:
//!
//! ```compile_fail,E0599
//! # use error_union::error_union;
//! # #[derive(Debug)] pub enum A {} #[derive(Debug)] pub enum B {}
//! # impl core::fmt::Display for A { fn fmt(&self, _: &mut core::fmt::Formatter<'_>) -> core::fmt::Result { Ok(()) } }
//! # impl core::fmt::Display for B { fn fmt(&self, _: &mut core::fmt::Formatter<'_>) -> core::fmt::Result { Ok(()) } }
//! # impl core::error::Error for A {} impl core::error::Error for B {}
//! error_union! { pub Source { A(A), B(B) } }
//! // The target has no `B`: the clause cannot be satisfied.
//! error_union! { pub Target { A(A) } from Source { A, B } }
//! ```
//!
//! ```compile_fail,E0004
//! # use error_union::error_union;
//! # #[derive(Debug)] pub enum A {} #[derive(Debug)] pub enum B {}
//! # impl core::fmt::Display for A { fn fmt(&self, _: &mut core::fmt::Formatter<'_>) -> core::fmt::Result { Ok(()) } }
//! # impl core::fmt::Display for B { fn fmt(&self, _: &mut core::fmt::Formatter<'_>) -> core::fmt::Result { Ok(()) } }
//! # impl core::error::Error for A {} impl core::error::Error for B {}
//! error_union! { pub Source { A(A), B(B) } }
//! // The clause leaves out `B`: a `Source::B` would be dropped.
//! error_union! { pub Target { A(A), B(B) } from Source { A } }
//! ```

/// Declare a union of leaf errors. See the crate documentation.
#[macro_export]
macro_rules! error_union {
    (@into $name:ident, [$($variant:ident),+];) => {};
    (@into $name:ident, [$($variant:ident),+]; $dest:path $(, $rest:path)*) => {
        impl ::core::convert::From<$name> for $dest {
            fn from(error: $name) -> Self {
                match error {
                    $($name::$variant(leaf) => Self::$variant(leaf)),+
                }
            }
        }
        $crate::error_union!(@into $name, [$($variant),+]; $($rest),*);
    };
    (@bridges $name:ident, [$($variant:ident),+];) => {};
    (@bridges $name:ident, [$($variant:ident),+]; [$($peer:tt)+] $($rest:tt)*) => {
        $crate::error_union!(@bridge $name, [$($variant),+]; $($peer)+);
        $crate::error_union!(@bridges $name, [$($variant),+]; $($rest)*);
    };
    (@bridge $name:ident, [$($variant:ident),+]; $peer:path) => {
        impl ::core::convert::From<$peer> for $name {
            fn from(source: $peer) -> Self {
                use $peer as Source;
                match source {
                    $(Source::$variant(leaf) => Self::$variant(leaf)),+
                }
            }
        }
        $crate::error_union!(@into $name, [$($variant),+]; $peer);
    };
    (
        $(#[$meta:meta])*
        $vis:vis $name:ident { $($variant:ident($leaf:ty)),+ $(,)? }
        $(from $source:path { $($from_variant:ident),+ $(,)? })*
        // The path is matched segment by segment: a `path` fragment may not be
        // followed by another clause's keyword, so `bridge` goes before `into`.
        $(bridge $peer_first:ident $(:: $peer_rest:ident)*)*
        $(into $dest:path)*
    ) => {
        $(#[$meta])*
        $vis enum $name {
            $($variant($leaf)),+
        }

        $(
            impl ::core::convert::From<$leaf> for $name {
                fn from(leaf: $leaf) -> Self {
                    Self::$variant(leaf)
                }
            }
        )+

        impl ::core::fmt::Display for $name {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                match self {
                    $(Self::$variant(leaf) => ::core::fmt::Display::fmt(leaf, f)),+
                }
            }
        }

        impl ::core::error::Error for $name {
            fn source(&self) -> ::core::option::Option<&(dyn ::core::error::Error + 'static)> {
                match self {
                    $(Self::$variant(leaf) => ::core::option::Option::Some(leaf)),+
                }
            }
        }

        $(
            impl ::core::convert::From<$source> for $name {
                fn from(source: $source) -> Self {
                    use $source as Source;
                    match source {
                        $(Source::$from_variant(leaf) => Self::$from_variant(leaf)),+
                    }
                }
            }
        )*

        $crate::error_union!(@into $name, [$($variant),+]; $($dest),*);
        $crate::error_union!(@bridges $name, [$($variant),+]; $([$peer_first $(:: $peer_rest)*])*);
    };
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, reason = "test code")]
mod tests {
    #[derive(Debug, PartialEq, thiserror::Error)]
    enum LogError {
        #[error("gap")]
        Gap,
    }

    #[derive(Debug, PartialEq, thiserror::Error)]
    enum SnapshotError {
        #[error("object store")]
        ObjectStore,
    }

    #[derive(Debug, PartialEq, thiserror::Error)]
    enum LifecycleError {
        #[error("not ready")]
        NotReady,
    }

    error_union! {
        #[derive(Debug, PartialEq)]
        ArchiveError { Log(LogError), Snapshot(SnapshotError) }
    }

    error_union! {
        #[derive(Debug, PartialEq)]
        ReadError { Log(LogError), Snapshot(SnapshotError), Lifecycle(LifecycleError) }
        from ArchiveError { Log, Snapshot }
    }

    fn archive() -> Result<(), ArchiveError> {
        Err(SnapshotError::ObjectStore)?
    }

    fn read() -> Result<(), ReadError> {
        archive()?;
        Err(LifecycleError::NotReady)?
    }

    #[test]
    fn a_leaf_converts_into_the_union_that_holds_it() {
        assert_eq!(
            ReadError::from(LogError::Gap),
            ReadError::Log(LogError::Gap)
        );
    }

    #[test]
    fn a_narrower_union_converts_with_question_mark() {
        let e = (|| -> Result<(), ReadError> {
            archive()?;
            Ok(())
        })()
        .unwrap_err();
        assert_eq!(e, ReadError::Snapshot(SnapshotError::ObjectStore));
        assert_eq!(
            read().unwrap_err(),
            ReadError::Snapshot(SnapshotError::ObjectStore)
        );
    }

    #[test]
    fn display_and_source_delegate_to_the_leaf() {
        use std::error::Error as _;
        let e = ReadError::Lifecycle(LifecycleError::NotReady);
        assert_eq!(e.to_string(), "not ready");
        assert_eq!(e.source().unwrap().to_string(), "not ready");
    }

    error_union! {
        #[derive(Debug, PartialEq)]
        NarrowError { Log(LogError), Snapshot(SnapshotError) }
        into ArchiveError
        into WideError
    }

    error_union! {
        #[derive(Debug, PartialEq)]
        WideError { Log(LogError), Snapshot(SnapshotError) }
    }

    error_union! {
        #[derive(Debug, PartialEq)]
        LeftError { Log(LogError), Snapshot(SnapshotError) }
        bridge RightError
        into WideError
    }

    error_union! {
        #[derive(Debug, PartialEq)]
        RightError { Log(LogError), Snapshot(SnapshotError) }
    }

    #[test]
    fn bridge_converts_both_ways() {
        assert_eq!(
            LeftError::from(RightError::Log(LogError::Gap)),
            LeftError::Log(LogError::Gap)
        );
        assert_eq!(
            RightError::from(LeftError::Snapshot(SnapshotError::ObjectStore)),
            RightError::Snapshot(SnapshotError::ObjectStore)
        );
    }

    #[test]
    fn into_converts_to_each_named_destination() {
        assert_eq!(
            ArchiveError::from(NarrowError::Snapshot(SnapshotError::ObjectStore)),
            ArchiveError::Snapshot(SnapshotError::ObjectStore)
        );
        assert_eq!(
            WideError::from(NarrowError::Log(LogError::Gap)),
            WideError::Log(LogError::Gap)
        );
    }
}
