use std::any::TypeId;
use std::marker::PhantomData;
use std::sync::Arc;

use es_entity::hooks::{CommitHook, HookOperation, PreCommitRet};
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::{broadcast, mpsc};

use crate::out::event::{PersistentDelivery, PersistentOutboxEvent};
use crate::out::gap_fill::GapFillRequest;
use crate::out::post_persist_hook::PostPersistHooks;
use crate::sequence::EventSequence;
use crate::tables::MailboxTables;

pub struct PersistEvents<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    sender: broadcast::Sender<PersistentDelivery<P>>,
    /// Reports the committed batch's `(min, max)` to the debounced notifier.
    notifier_tx: mpsc::UnboundedSender<(EventSequence, EventSequence)>,
    /// Reports sequences this operation allocated but failed to commit to
    /// the [`GapFiller`](crate::out::gap_fill::GapFiller): see
    /// [`CommitHook::on_rollback`] below and the own-failure branches in
    /// `pre_commit`.
    abandoned_tx: mpsc::UnboundedSender<GapFillRequest>,
    pre_commit_events: Vec<P>,
    /// Persisted events, stashed chunk-by-chunk *as* `pre_commit` runs (not
    /// at its end): if a later chunk, a later hook, or the COMMIT itself
    /// fails, this holds exactly the sequences the rolled-back transaction
    /// allocated — which is what `on_rollback` (or the own-failure
    /// branches) reports for compensation. Consumed (emptied) by
    /// `post_commit` on the success path.
    post_commit_events: Vec<PersistentOutboxEvent<P>>,
    batch_size: usize,
    /// Snapshot of the outbox's registered post-persist hooks, taken when
    /// this commit hook is constructed (i.e. at the operation's first
    /// publish). Merged publishes keep the first snapshot.
    post_persist_hooks: PostPersistHooks<P>,
    /// Set on the force-execute path: `post_commit` never runs, so the
    /// persist statement must carry the in-tx NOTIFY. Also suppresses
    /// own-failure compensation — on that path the caller's bare transaction
    /// is still open when `pre_commit` returns and its fate is unknowable
    /// (it may savepoint-recover and commit), so those sequences are the
    /// proof-gated backstop's responsibility.
    ///
    /// Set only for an operation with no commit pass to join at all (a bare
    /// `sqlx::Transaction`). A repost from inside another outbox's
    /// [`PostPersistHook`] is not one: `add_commit_hook` succeeds there, so
    /// the repost joins the enclosing pass and gets the full
    /// `post_commit`/`on_rollback` lifecycle.
    ///
    /// [`PostPersistHook`]: crate::out::PostPersistHook
    notify_in_tx: bool,
    /// Snapshot of the outbox's declared upstream hook types
    /// (`Outbox::persist_after`) at construction. Merged publishes keep the
    /// first snapshot — identical by construction, since all instances of
    /// this concrete type come from the same outbox.
    runs_after: Arc<[TypeId]>,
    _phantom: PhantomData<Tables>,
}

impl<P, Tables> PersistEvents<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    pub fn new(
        sender: broadcast::Sender<PersistentDelivery<P>>,
        notifier_tx: mpsc::UnboundedSender<(EventSequence, EventSequence)>,
        abandoned_tx: mpsc::UnboundedSender<GapFillRequest>,
        events: impl IntoIterator<Item = impl Into<P>>,
        batch_size: usize,
        post_persist_hooks: PostPersistHooks<P>,
        runs_after: Arc<[TypeId]>,
    ) -> Self {
        Self {
            sender,
            notifier_tx,
            abandoned_tx,
            pre_commit_events: events.into_iter().map(Into::into).collect(),
            post_commit_events: Vec::new(),
            batch_size,
            post_persist_hooks,
            notify_in_tx: false,
            runs_after,
            _phantom: PhantomData,
        }
    }

    /// Use the in-transaction NOTIFY persist variant — force-execute path
    /// only; see the `notify_in_tx` field doc for its scope.
    pub(crate) fn with_in_tx_notify(mut self) -> Self {
        self.notify_in_tx = true;
        self
    }

    /// Events buffered on this hook, awaiting persistence at commit.
    /// Backs the [`Outbox::cursor`](crate::out::Outbox::cursor) read API.
    pub(crate) fn pending(&self) -> &[P] {
        &self.pre_commit_events
    }

    /// Own-failure compensation: report the sequences persisted so far to
    /// the [`GapFiller`](crate::out::gap_fill::GapFiller) before
    /// `pre_commit` returns its error. The transaction is still open here
    /// (its rollback follows once the error propagates), so the report
    /// must stay a channel send — the GapFiller's insert parks briefly
    /// on the dying transaction's speculative-insertion locks and resolves
    /// when the rollback lands; awaiting that insert inline would deadlock
    /// on our own transaction. Skipped on the force-execute path
    /// (`notify_in_tx`), where the caller's transaction may yet recover
    /// and commit.
    fn report_own_failure(&mut self) {
        if self.notify_in_tx {
            return;
        }
        let abandoned: Vec<EventSequence> = self
            .post_commit_events
            .drain(..)
            .map(|event| event.sequence)
            .collect();
        if !abandoned.is_empty() {
            let _ = self.abandoned_tx.send(GapFillRequest::Abandoned(abandoned));
        }
    }
}

impl<P, Tables> CommitHook for PersistEvents<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    async fn pre_commit(
        mut self,
        mut op: HookOperation<'_>,
    ) -> Result<PreCommitRet<'_, Self>, sqlx::Error> {
        let batch_size = self.batch_size.max(1);
        let events = std::mem::take(&mut self.pre_commit_events);
        self.post_commit_events.reserve(events.len());
        let mut events = events.into_iter();
        loop {
            let chunk: Vec<P> = events.by_ref().take(batch_size).collect();
            if chunk.is_empty() {
                break;
            }
            let persist = if self.notify_in_tx {
                Tables::persist_events_notifying(&mut op, chunk.into_iter()).await
            } else {
                Tables::persist_events(&mut op, chunk.into_iter()).await
            };
            let persisted_chunk = match persist {
                Ok(chunk) => chunk,
                Err(error) => {
                    // Earlier chunks were persisted and are now doomed
                    // with the transaction (the failing statement's own
                    // sequences are unknowable — burned inside the
                    // aborted statement; the backstop proves and fills
                    // them).
                    self.report_own_failure();
                    return Err(error);
                }
            };
            // Stash before running the post-persist hooks: a hook error
            // rolls the transaction back with these sequences already
            // allocated, and compensation must know about them.
            let chunk_start = self.post_commit_events.len();
            self.post_commit_events.extend(persisted_chunk);
            for hook in self.post_persist_hooks.iter() {
                if let Err(error) = hook
                    .on_persisted(&mut op, &self.post_commit_events[chunk_start..])
                    .await
                {
                    self.report_own_failure();
                    return Err(error);
                }
            }
        }
        PreCommitRet::ok(self, op)
    }

    fn post_commit(mut self) {
        let post_commit_events = std::mem::take(&mut self.post_commit_events);
        let batch_range = match (post_commit_events.first(), post_commit_events.last()) {
            (Some(first), Some(last)) => Some((first.sequence, last.sequence)),
            _ => None,
        };
        for event in post_commit_events {
            let _ = self.sender.send(PersistentDelivery::from(Ok(event)));
        }
        if let Some(range) = batch_range {
            let _ = self.notifier_tx.send(range);
        }
    }

    /// Rollback compensation (the reactive tier of gap filling).
    ///
    /// es-entity fires this when the commit failed after our `pre_commit`
    /// had completed — a later hook's `pre_commit` errored (the
    /// transaction is already rolled back when this runs, so the
    /// GapFiller's insert contends with nothing) or the COMMIT itself
    /// failed (the transaction is over server-side either way; the
    /// GapFiller's `ON CONFLICT DO NOTHING` insert is idempotent against
    /// a commit that actually landed). The stashed sequences are this
    /// process's own abandoned allocations; reporting them to the
    /// [`GapFiller`](crate::out::gap_fill::GapFiller) fills their
    /// placeholders in milliseconds instead of leaving downstream
    /// listeners stalled until the grace-gated backstop proves them lost.
    ///
    /// Signal-only per the trait contract: the DB work happens on the
    /// GapFiller task. Failures *inside* our own `pre_commit` never
    /// reach here (the failing hook is consumed by its own call) — those
    /// report from the error branches in `pre_commit` itself.
    fn on_rollback(mut self) {
        let abandoned: Vec<EventSequence> = self
            .post_commit_events
            .drain(..)
            .map(|event| event.sequence)
            .collect();
        if !abandoned.is_empty() {
            let _ = self.abandoned_tx.send(GapFillRequest::Abandoned(abandoned));
        }
    }

    fn merge(&mut self, other: &mut Self) -> bool {
        self.pre_commit_events.append(&mut other.pre_commit_events);
        true
    }

    fn runs_after(&self) -> &[TypeId] {
        &self.runs_after
    }
}
