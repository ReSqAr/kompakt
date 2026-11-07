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
//!   }
//! ```
//!
//! The macro generates the enum, `From<Leaf>` per variant, `Display`,
//! `Error::source`, and `From<Source>` for each `from` clause. The compiler
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
    (
        $(#[$meta:meta])*
        $vis:vis $name:ident { $($variant:ident($leaf:ty)),+ $(,)? }
        $(from $source:path { $($from_variant:ident),+ $(,)? })*
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
    };
}

/// Narrow a union into a wider-or-equal one declared elsewhere: generates
/// `From<$src> for $dst` for each destination, mapping each named variant to
/// the variant of the same name. The compiler's exhaustiveness check enforces
/// that `$variant` lists every variant of `$src`. For use where the
/// destination cannot name the source (a port crate's union), so the source
/// side declares the conversion.
#[macro_export]
macro_rules! error_bridge {
    (@one $src:ident, $dst:path, [$($variant:ident),+]) => {
        impl From<$src> for $dst {
            fn from(e: $src) -> Self {
                match e { $( $src::$variant(x) => Self::$variant(x), )+ }
            }
        }
    };
    ($src:ident => ; $($variant:ident),+ $(,)?) => {};
    ($src:ident => $dst:path $(, $rest:path)* $(,)? ; $($variant:ident),+ $(,)?) => {
        $crate::error_bridge!(@one $src, $dst, [$($variant),+]);
        $crate::error_bridge!($src => $($rest),* ; $($variant),+);
    };
}

/// Declare `Home` for the union `$error`: the boundary verb that brings a value
/// carrying a port crate's error (a `Result`, or a plain or ordered channel of
/// results) back to `$error`, wherever `$error: From<port error>`. The calling
/// crate needs `crossbeam-combinators`.
#[macro_export]
macro_rules! error_home {
    ($error:ident) => {
        pub(crate) trait Home {
            type Out;
            fn home(self) -> Self::Out;
        }

        impl<T, E> Home for Result<T, E>
        where
            $error: From<E>,
        {
            type Out = Result<T, $error>;
            fn home(self) -> Self::Out {
                self.map_err(Into::into)
            }
        }

        impl<A, E, K> Home for crossbeam_combinators::Channel<Result<A, E>, K>
        where
            A: Send + 'static,
            $error: From<E>,
            E: Send + 'static,
            K: crossbeam_combinators::Kind,
        {
            type Out = crossbeam_combinators::Channel<Result<A, $error>, K>;
            fn home(self) -> Self::Out {
                self.err_into()
            }
        }

        impl<A, E, P, K> Home for crossbeam_combinators::OrderedChannel<Result<A, E>, P, K>
        where
            A: Send + 'static,
            $error: From<E>,
            E: Send + 'static,
            P: crossbeam_combinators::OrderPolicy,
            P::Key: crossbeam_combinators::Key<Result<A, $error>>,
            K: crossbeam_combinators::Kind,
        {
            type Out = crossbeam_combinators::OrderedChannel<Result<A, $error>, P, K>;
            fn home(self) -> Self::Out {
                self.err_into()
            }
        }
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
}
