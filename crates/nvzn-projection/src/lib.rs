//! The projection model: what a projection is, and what it reads through.
//!
//! A [`Projection`] is a row type plus `next`, the fold from *(event, the data
//! so far)* to *the rows that change*. It reads through a [`View`], which reads
//! a [`ReadTransaction`] — the read port a store backend implements. That read
//! port lives here, with the model that is written against it, so the model
//! does not depend on the store; `nvzn-store` depends on this crate for the
//! write side.

mod projection;
mod read;
mod view;

pub use projection::{AnyIndex, Index, Indices, Projection, Ref, Write};
pub use read::{
    ArcReadTransaction, ReadTransaction, ReadTransactionExt, Row, Rows, Watermark,
    reference_not_found,
};
pub use view::View;

// The vocabulary the projection and store APIs are written in, re-exported so
// the layers above need not name the types crate.
pub use nvzn_types::{
    APP_ID_LEN, Encoder, Error, EventSet, EventWriter, INDEX_ID_LEN, IndexId, PROJECTION_ID_LEN,
    PROJECTION_TYPE_ID_LEN, ProjectionId, ProjectionTypeId, StopReason, SubEventOf, Syncer, Tenant,
    TenantId, WithWireFormat, chained_from, define_events,
};
