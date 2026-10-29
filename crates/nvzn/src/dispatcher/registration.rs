//! Registration: each tenant's projections become one handler on the
//! dispatcher, keyed by tenant id, and its registered projection types and
//! indices join the dispatcher's config.
//!
//! ```text
//!   config_hash = sha256( len ‖ (tenant ‖ projection type)*
//!                       ‖ len ‖ (tenant ‖ projection type ‖ index)* )
//! ```
//!
//! The hash is what `finalise` compares against the store's to choose between
//! resuming and refolding, so it covers exactly what changes the folded
//! state's shape. Length prefixes keep the two sets from aliasing.

use super::{Dispatcher, Encoder, EventWriter};
use crate::projection::fold_event;
use crate::{
    ConfigHash, Error, IndexId, Projection, ProjectionTypeId, ProjectorManager, Reader, SubEventOf,
    TenantId, WithWireFormat, WriteTransactionImpl,
};
use sha2::Digest;
use std::collections::BTreeSet;
use std::sync::Arc;

pub(super) fn compute_config_hash(
    registered_projection_types: &BTreeSet<(TenantId, ProjectionTypeId)>,
    registered_indices: &BTreeSet<(TenantId, ProjectionTypeId, IndexId)>,
) -> Result<ConfigHash, Error> {
    let mut h = sha2::Sha256::new();

    h.update(len_prefix(registered_projection_types.len())?);
    for (tenant, proj) in registered_projection_types {
        h.update(tenant.as_slice());
        h.update(proj.as_slice());
    }

    h.update(len_prefix(registered_indices.len())?);
    for (tenant, proj, index) in registered_indices {
        h.update(tenant.as_slice());
        h.update(proj.as_slice());
        h.update(index.as_slice());
    }

    Ok(h.finalize().into())
}

fn len_prefix(n: usize) -> Result<[u8; 8], Error> {
    u64::try_from(n)
        .map_err(|_| Error::Decode(format!("Invalid length of {}", n)))
        .map(u64::to_be_bytes)
}

pub struct AssociatedProjectorManager<'a, Ev> {
    pub(super) dispatcher: &'a mut Dispatcher,
    pub(super) manager: ProjectorManager<Ev>,
}

impl<'a, Ev> AssociatedProjectorManager<'a, Ev>
where
    Ev: WithWireFormat,
{
    /// Registers `P` and every index in its [`Projection::INDICES`].
    pub fn register<P>(&mut self) -> Result<(), Error>
    where
        P: Projection,
        for<'e> P::Event<'e>: SubEventOf<'e, Ev>,
    {
        self.manager.register::<P>()
    }

    pub fn finalise(self) -> Result<(EventWriter<Ev>, Reader<Ev>), Error> {
        let AssociatedProjectorManager {
            dispatcher,
            manager,
        } = self;

        let ProjectorManager {
            tenant,
            projectors,
            registered,
            registered_projection_types,
            registered_indices,
        } = manager;

        let tenant_id = tenant.id();
        if dispatcher.handlers.contains_key(&tenant_id) {
            return Err(Error::TenantAlreadyRegistered(tenant_id.as_u128()));
        }

        let tenant_event_handler = Arc::new(
            move |_tenant_id: TenantId, txn: &mut WriteTransactionImpl<'_>, payload: &[u8]| {
                let events = Ev::decode(payload)?;
                for event in &events {
                    fold_event(&projectors, tenant, event, txn)?;
                }
                Ok(())
            },
        );
        dispatcher
            .handlers
            .insert(tenant_id, tenant_event_handler.clone());

        for ptid in &registered_projection_types {
            dispatcher
                .registered_projection_types
                .insert((tenant_id, *ptid));
        }
        for (ptid, iid) in registered_indices {
            dispatcher.registered_indices.insert((tenant_id, ptid, iid));
        }

        let encoder = Encoder::new(tenant);
        Ok((
            EventWriter::new(encoder, dispatcher.writer.clone()),
            Reader::new(
                tenant,
                dispatcher.store.clone(),
                dispatcher.reader.clone(),
                dispatcher.syncer(),
                dispatcher.fold_state.clone(),
                dispatcher.subscriptions.clone(),
                Arc::new(registered_projection_types),
                Arc::new(registered),
            ),
        ))
    }
}
