use std::marker::PhantomData;

/// Passive position token into an outbox's op-local publish buffer.
///
/// Within one [`es_entity::AtomicOperation`], every
/// [`publish_all_persisted`](super::Outbox::publish_all_persisted) call buffers
/// its events on a single (merged) commit hook — nothing is written to the
/// database until `op.commit()`. An `OpCursor` marks a position in that buffer
/// so the events published *after* it can be read back before commit via
/// [`Outbox::new_events`](super::Outbox::new_events) /
/// [`Outbox::map_new`](super::Outbox::map_new) — e.g. to atomically republish a
/// mapped projection of one outbox's events onto another outbox in the same
/// transaction.
///
/// The cursor is pure state — all reads go through the [`Outbox`](super::Outbox)
/// (mirroring how [`EventSequence`](crate::EventSequence) is a passive token
/// passed to [`listen_persisted`](super::Outbox::listen_persisted)). Advancing
/// reads take `&mut OpCursor`; the non-advancing
/// [`peek_new`](super::Outbox::peek_new) takes `&OpCursor`. The type parameters
/// pin the cursor to its outbox's payload/table types, so a cursor from one
/// outbox cannot be used with another.
///
/// Events past the cursor position have been *published into the op* but are
/// **not** persisted until `op.commit()` — a rollback discards them.
///
/// # Caveats
///
/// - A cursor is only meaningful with the *same op* it was created from;
///   reading it against a different op yields arbitrary (though memory-safe)
///   results.
/// - Obtaining a cursor requires an op that supports commit hooks. On an op
///   without hook support (e.g. a bare `sqlx::Transaction`, where publishes are
///   written immediately instead of buffered) there is no buffer to position
///   into, so [`Outbox::cursor`](super::Outbox::cursor) fails with
///   [`CursorError::HooksUnsupported`] rather than handing back a cursor that
///   would silently yield nothing.
#[derive(Debug)]
pub struct OpCursor<P, Tables> {
    pub(crate) pos: usize,
    _phantom: PhantomData<(P, Tables)>,
}

impl<P, Tables> OpCursor<P, Tables> {
    pub(crate) fn new(pos: usize) -> Self {
        Self {
            pos,
            _phantom: PhantomData,
        }
    }
}

/// Error returned by [`Outbox::cursor`](super::Outbox::cursor).
#[derive(Debug, thiserror::Error)]
pub enum CursorError {
    /// The operation does not support commit hooks, so there is no op-local
    /// publish buffer to position into. Use an op that supports hooks — e.g.
    /// one from [`Outbox::begin_op`](super::Outbox::begin_op) — rather than a
    /// bare `sqlx::Transaction`.
    #[error("OpCursor requires an operation that supports commit hooks; this operation does not")]
    HooksUnsupported,
}
