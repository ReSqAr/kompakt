//! The store ports and the projection vocabulary.
//!
//! A [`Projection`] reads through a [`View`], which reads a
//! [`ReadTransaction`]; and the transactions' typed helpers are generic over
//! `Projection`. The two halves are one interface, so they live together here.
//! The staging overlay both stores share rides along (the `Store` trait names
//! its `CommitBatch` and `WriteTransactionImpl`).

mod config;
mod overlay;
mod projection;
mod store;
mod view;

pub use config::{DEFAULT_FOLD_COMMIT, StoreConfig};
pub use overlay::{CommitBatch, IndexAction, WriteTransactionImpl};
pub use projection::{AnyIndex, Index, Indices, Projection, Ref, Write};
pub use store::{
    ArcReadTransaction, ArcStore, ConfigHash, ReadTransaction, ReadTransactionExt, Row, Rows,
    Store, Watermark, WriteTransaction, WriteTransactionExt, reference_not_found,
};
pub use view::View;
