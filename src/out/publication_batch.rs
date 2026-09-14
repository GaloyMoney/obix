//! Source-operation publication boundaries over the persistent event stream.
//!
//! The transactional head is locked from the first persist chunk until commit,
//! keeping every contribution to this namespace contiguous. Finalization seals
//! that range after all ordinary/reentrant publishers finish. Payloads are
//! stored once, in the event table; event and publication consumers share one
//! sequence namespace.

use es_entity::{
    AtomicOperation,
    hooks::{CommitHook, HookOperation, PreCommitRet},
};
use serde::{Serialize, de::DeserializeOwned};
use std::marker::PhantomData;

use super::{Outbox, PersistentOutboxEvent};
use crate::{EventSequence, tables::MailboxTables};

/// A complete committed source-operation publication.
pub struct PublicationBatch<P>
where
    P: Serialize + DeserializeOwned + Send,
{
    first_sequence: EventSequence,
    last_sequence: EventSequence,
    events: Vec<PersistentOutboxEvent<P>>,
}

impl<P> PublicationBatch<P>
where
    P: Serialize + DeserializeOwned + Send,
{
    pub fn first_sequence(&self) -> EventSequence {
        self.first_sequence
    }

    pub fn last_sequence(&self) -> EventSequence {
        self.last_sequence
    }

    pub fn events(&self) -> impl Iterator<Item = &PersistentOutboxEvent<P>> {
        self.events.iter()
    }
}

#[derive(sqlx::FromRow)]
struct StoredBatch {
    first_sequence: i64,
    last_sequence: i64,
}

fn batches_table<T: MailboxTables>() -> String {
    format!("{}_batches", T::persistent_outbox_events_table())
}

impl<P, T> Outbox<P, T>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    T: MailboxTables,
{
    /// Read up to `limit` complete publications after BEGIN or a previously
    /// delivered publication's last sequence. Never splits a publication.
    pub async fn load_publication_batches(
        &self,
        after: EventSequence,
        limit: u32,
    ) -> Result<Vec<PublicationBatch<P>>, sqlx::Error> {
        let after_i64 = i64::try_from(u64::from(after))
            .map_err(|_| sqlx::Error::Protocol("publication cursor exceeds BIGINT".into()))?;
        let rows = sqlx::query_as::<_, StoredBatch>(&format!(
            "SELECT first_sequence, last_sequence FROM {} WHERE last_sequence > $1 ORDER BY last_sequence LIMIT $2",
            batches_table::<T>()
        ))
        .bind(after_i64)
        .bind(i64::from(limit))
        .fetch_all(&self.pool).await?;
        let mut expected_first = u64::from(after) + 1;
        let mut batches = Vec::with_capacity(rows.len());
        for row in rows {
            if row.first_sequence <= 0
                || row.first_sequence as u64 != expected_first
                || row.last_sequence < row.first_sequence
            {
                return Err(sqlx::Error::Protocol("invalid publication boundary".into()));
            }
            let events = T::load_events_in_range::<P>(
                &self.pool,
                EventSequence::from(row.first_sequence as u64 - 1),
                EventSequence::from(row.last_sequence as u64),
            )
            .await?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
            if events.len() as i64 != row.last_sequence - row.first_sequence + 1
                || events.iter().enumerate().any(|(index, event)| {
                    event.payload.is_none()
                        || u64::from(event.sequence) != row.first_sequence as u64 + index as u64
                })
            {
                return Err(sqlx::Error::Protocol("incomplete publication".into()));
            }
            expected_first = row.last_sequence as u64 + 1;
            batches.push(PublicationBatch {
                first_sequence: EventSequence::from(row.first_sequence as u64),
                last_sequence: EventSequence::from(row.last_sequence as u64),
                events,
            });
        }
        Ok(batches)
    }
}

pub(super) struct SealPublicationBatch<T: MailboxTables> {
    first: EventSequence,
    last: EventSequence,
    count: u64,
    _tables: PhantomData<T>,
}

impl<T: MailboxTables> SealPublicationBatch<T> {
    pub(super) fn new<P>(events: &[PersistentOutboxEvent<P>]) -> Self
    where
        P: Serialize + DeserializeOwned + Send,
    {
        Self {
            first: events.first().expect("nonempty publication").sequence,
            last: events.last().expect("nonempty publication").sequence,
            count: events.len() as u64,
            _tables: PhantomData,
        }
    }
}

impl<T: MailboxTables> CommitHook for SealPublicationBatch<T> {
    fn is_finalizer(&self) -> bool {
        true
    }

    fn merge(&mut self, other: &mut Self) -> bool {
        self.first = self.first.min(other.first);
        self.last = self.last.max(other.last);
        self.count += other.count;
        true
    }

    async fn pre_commit(
        self,
        mut op: HookOperation<'_>,
    ) -> Result<PreCommitRet<'_, Self>, sqlx::Error> {
        if u64::from(self.last) - u64::from(self.first) + 1 != self.count {
            return Err(sqlx::Error::Protocol("noncontiguous publication".into()));
        }
        sqlx::query(&format!(
            "INSERT INTO {} (first_sequence, last_sequence) VALUES ($1, $2)",
            batches_table::<T>()
        ))
        .bind(u64::from(self.first) as i64)
        .bind(u64::from(self.last) as i64)
        .execute(op.as_executor())
        .await?;
        PreCommitRet::ok(self, op)
    }
}
