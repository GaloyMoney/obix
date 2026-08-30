//! The per-key runner: one `job` execution per live subscription.
//!
//! Structurally the singleton runner's loop
//! ([`singleton`](crate::out::subscription::singleton)), minus the ephemeral
//! stream and plus the two things only a keyed member has: the hold verbs
//! (which park the cursor without advancing it) and linger-based dormancy
//! (an idle, caught-up member completes its job rather than holding a slot).
//!
//! The `subscriptions` row is read fresh on every run — the subscriber is
//! rebuilt from it via [`SubscriptionDef::instantiate`] on every wake, hold
//! expiry and retry, so nothing may be cached in the instance between runs.

use futures::{FutureExt, StreamExt};
use serde::{Serialize, de::DeserializeOwned};
use std::{marker::PhantomData, sync::Arc, time::Duration};

use job::{CurrentJob, Job, JobType, RetrySettings};

use super::{KeyMsg, KeyedSubscriber, KeyedSubscriberConfig, SubscriptionDef};
use crate::out::Outbox;
use crate::out::ctx::*;
use crate::tables::MailboxTables;

// === Object-safe flush bridge ===

struct KeyedSubscriberFlusher<S, P> {
    subscriber: Arc<S>,
    _payload: PhantomData<fn() -> P>,
}

impl<S, P> ItemFlush<S::Batch> for KeyedSubscriberFlusher<S, P>
where
    S: KeyedSubscriber<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    fn flush_items<'a>(
        &'a self,
        op: &'a mut es_entity::DbOp<'static>,
        items: S::Batch,
    ) -> BoxFuture<'a, Result<(), HandlerError>> {
        Box::pin(async move {
            let mut op = FlushOp::new(op);
            self.subscriber.flush(&mut op, items).await
        })
    }
}

// === Per-key runner ===

pub(in crate::out) struct KeyedSubscriberJobInitializer<D, P, Tables>
where
    D: SubscriptionDef<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    outbox: Outbox<P, Tables>,
    def: Arc<D>,
    job_type: JobType,
    linger: Duration,
    checkpoint_interval: Duration,
    max_batch_size: usize,
    max_concurrent_per_process: Option<usize>,
}

impl<D, P, Tables> KeyedSubscriberJobInitializer<D, P, Tables>
where
    D: SubscriptionDef<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    pub(in crate::out) fn new(
        outbox: Outbox<P, Tables>,
        def: Arc<D>,
        config: &KeyedSubscriberConfig,
    ) -> Self {
        Self {
            outbox,
            def,
            job_type: config.job_type.clone(),
            linger: config.linger,
            checkpoint_interval: config.checkpoint_interval,
            max_batch_size: config.max_batch_size,
            max_concurrent_per_process: config.max_concurrent_per_process,
        }
    }
}

impl<D, P, Tables> job::KeyedJobInitializer for KeyedSubscriberJobInitializer<D, P, Tables>
where
    D: SubscriptionDef<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    type Config = KeyMsg;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn retry_on_error_settings(&self) -> RetrySettings {
        RetrySettings::repeat_indefinitely()
    }

    fn max_concurrent_per_process(&self) -> Option<usize> {
        self.max_concurrent_per_process
    }

    /// A respawn resumes where the last generation left off — the watermark
    /// carries across generations, which is what makes the existing
    /// `Subscription` (checkpoint read-back) surface work per-key for free.
    fn inherits_state(&self) -> bool {
        true
    }

    fn init(
        &self,
        job: &Job,
        _spawner: job::KeyedJobSpawner<Self::Config>,
    ) -> Result<Box<dyn job::JobRunner>, Box<dyn std::error::Error>> {
        let KeyMsg { key } = job.config()?;
        let key: D::Key = key.parse().map_err(|_| -> Box<dyn std::error::Error> {
            "keyed subscriber: could not parse the persisted key".into()
        })?;
        Ok(Box::new(KeyedSubscriberJobRunner {
            outbox: self.outbox.clone(),
            def: self.def.clone(),
            key,
            job_type: self.job_type.clone(),
            linger: self.linger,
            checkpoint_interval: self.checkpoint_interval,
            max_batch_size: self.max_batch_size,
        }))
    }
}

/// Mirrors this member's checkpoint into its `subscriptions` row on whatever
/// op is persisting the checkpoint, so obix owns a copy of how far each
/// member has got without reading job-crate tables. Feeds the waker's
/// catch-up scan; see [`MailboxTables::subscriptions_behind`].
struct KeyedCheckpointMirror<Tables> {
    subscriber_type: JobType,
    key: String,
    _tables: PhantomData<fn() -> Tables>,
}

impl<Tables: MailboxTables> CheckpointMirror for KeyedCheckpointMirror<Tables> {
    fn mirror<'a>(
        &'a self,
        op: &'a mut es_entity::DbOp<'static>,
        checkpoint: crate::sequence::EventSequence,
    ) -> futures::future::BoxFuture<'a, Result<(), sqlx::Error>> {
        Box::pin(async move {
            Tables::update_subscription_checkpoint_in_op(
                op,
                self.subscriber_type.as_str(),
                &self.key,
                checkpoint,
            )
            .await
        })
    }
}

struct KeyedSubscriberJobRunner<D, P, Tables>
where
    D: SubscriptionDef<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    outbox: Outbox<P, Tables>,
    def: Arc<D>,
    key: D::Key,
    job_type: JobType,
    linger: Duration,
    checkpoint_interval: Duration,
    max_batch_size: usize,
}

#[async_trait::async_trait]
impl<D, P, Tables> job::JobRunner for KeyedSubscriberJobRunner<D, P, Tables>
where
    D: SubscriptionDef<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    async fn run(
        &self,
        mut current_job: CurrentJob,
    ) -> Result<job::JobCompletion, Box<dyn std::error::Error>> {
        let key_str = self.key.to_string();

        // Run start: the subscriptions row is the truth. Missing → cancelled;
        // a stray wake respawning a just-cancelled key hits this and dies
        // harmlessly (the waker can't reach it anyway — its lookups go
        // through this same table).
        let Some(row) =
            Tables::find_subscription(current_job.pool(), self.job_type.as_str(), &key_str).await?
        else {
            return Ok(job::JobCompletion::Complete);
        };

        // The factory runs fresh every run: every wake, hold expiry, retry,
        // on any node. The subscriber must be cheap to build and stateless
        // between runs — durable state is the cursor plus its own entities.
        let instance_config: D::InstanceConfig = serde_json::from_value(row.instance_config)?;
        let subscriber = Arc::new(self.def.instantiate(self.key.clone(), instance_config));
        let flusher = KeyedSubscriberFlusher::<D::Subscriber, P> {
            subscriber: subscriber.clone(),
            _payload: PhantomData,
        };

        let mut state = current_job
            .execution_state::<OutboxEventJobState>()?
            .unwrap_or(OutboxEventJobState {
                sequence: row.start_after,
                staged: None,
            });

        let mirror = KeyedCheckpointMirror::<Tables> {
            subscriber_type: self.job_type.clone(),
            key: key_str.clone(),
            _tables: PhantomData,
        };

        let mut persistent = self.outbox.listen_persisted(Some(state.sequence));

        let mut op_slot: Option<es_entity::DbOp<'static>> = None;
        let mut tracker = BatchTracker {
            collected: 0,
            persisted_seq: state.sequence,
            last_persist: tokio::time::Instant::now(),
        };
        let mut batch = <D::Subscriber as KeyedSubscriber<P>>::Batch::default();

        // Armed while nothing is pending; disarmed when THIS MEMBER does
        // work (see the outcome dispatch), not when any event arrives.
        // Firing means "this member has had nothing to do for `linger`" —
        // passivate to Dormant. `None` while `linger` is un-addable is how
        // the documented always-on setting is expressed.
        let mut linger_deadline: Option<tokio::time::Instant> = None;

        loop {
            let item = if tracker.collected > 0 {
                match persistent.next().now_or_never() {
                    Some(Some(item)) => item,
                    Some(None) => {
                        let mut parts = CtxParts {
                            op_slot: &mut op_slot,
                            current_job: &mut current_job,
                            state: &mut state,
                            tracker: &mut tracker,
                            mirror: Some(&mirror),
                        };
                        flush_batch(&mut parts, &mut batch, &flusher, "stream_closed")
                            .await
                            .map_err(|e| e as Box<dyn std::error::Error>)?;
                        return Ok(job::JobCompletion::RescheduleNow);
                    }
                    None => {
                        let mut parts = CtxParts {
                            op_slot: &mut op_slot,
                            current_job: &mut current_job,
                            state: &mut state,
                            tracker: &mut tracker,
                            mirror: Some(&mirror),
                        };
                        flush_batch(&mut parts, &mut batch, &flusher, "backlog_drained")
                            .await
                            .map_err(|e| e as Box<dyn std::error::Error>)?;
                        continue;
                    }
                }
            } else {
                if linger_deadline.is_none() {
                    // `checked_add`, not `+`: `linger` is documented as
                    // `Duration::MAX` == always-on, and adding that to an
                    // `Instant` panics. An un-addable linger leaves the
                    // deadline unarmed, which disables the passivation arm
                    // below — always-on falls out of the same arithmetic
                    // instead of being a special case that has to be
                    // remembered.
                    linger_deadline = tokio::time::Instant::now().checked_add(self.linger);
                }
                // Only read when the arm is enabled; the fallback keeps the
                // expression total, since `select!` evaluates a disabled
                // branch's expression (it just never polls the future).
                let deadline = linger_deadline.unwrap_or_else(tokio::time::Instant::now);

                tokio::select! {
                    biased;
                    _ = current_job.shutdown_requested() => {
                        if tracker.persisted_seq < state.sequence {
                            persist_checkpoint(&mut current_job, &state, Some(&mirror))
                                .await
                                .map_err(|e| e as Box<dyn std::error::Error>)?;
                        }
                        return Ok(job::JobCompletion::RescheduleNow);
                    }
                    _ = tokio::time::sleep_until(deadline), if linger_deadline.is_some() => {
                        // The deadline being due is not on its own a licence
                        // to passivate. `biased` polls this arm before the
                        // stream arm, so a member with events ALREADY READY
                        // would otherwise stop here and leave them at its
                        // checkpoint — and, because the waker races the same
                        // stream, it may already have classified those events
                        // while this member was still Active, found it live,
                        // no-op'd the spawn and checkpointed past them.
                        // Nothing would then wake it for those events: not
                        // the wake-key path (already consumed) and not the
                        // catch-up scan until the stream advances another
                        // three quarters of the cache. On an outbox that then
                        // goes quiet, never.
                        //
                        // So idle means "deadline due AND nothing ready",
                        // which is one non-blocking poll away.
                        match persistent.next().now_or_never() {
                            // Not idle after all — drop through and handle it.
                            Some(Some(item)) => item,
                            Some(None) => {
                                if tracker.persisted_seq < state.sequence {
                                    persist_checkpoint(&mut current_job, &state, Some(&mirror))
                                        .await
                                        .map_err(|e| e as Box<dyn std::error::Error>)?;
                                }
                                return Ok(job::JobCompletion::RescheduleNow);
                            }
                            None => {
                                // Genuinely idle. Passivating is exactly when
                                // the mirrored cursor starts to matter: from
                                // here nothing is reading the stream for this
                                // key, so the waker's catch-up scan is the
                                // only thing that will notice it drifting
                                // toward the edge of the cache. Written
                                // unconditionally, not only when the
                                // checkpoint itself needs persisting — the
                                // checkpoint may already be durable from an
                                // earlier tick while the mirror still trails
                                // it.
                                let mut op = es_entity::DbOp::init_with_clock(
                                    current_job.pool(),
                                    current_job.clock(),
                                )
                                .await?;
                                if tracker.persisted_seq < state.sequence {
                                    current_job
                                        .update_execution_state_in_op(&mut op, &state)
                                        .await?;
                                }
                                mirror.mirror(&mut op, state.sequence).await?;
                                return Ok(job::JobCompletion::CompleteWithOp(op));
                            }
                        }
                    }
                    _ = tokio::time::sleep_until(tracker.last_persist + self.checkpoint_interval),
                        if tracker.persisted_seq < state.sequence => {
                        persist_checkpoint(&mut current_job, &state, Some(&mirror))
                            .await
                            .map_err(|e| e as Box<dyn std::error::Error>)?;
                        tracker.persisted_seq = state.sequence;
                        tracker.last_persist = tokio::time::Instant::now();
                        continue;
                    }
                    event = persistent.next() => {
                        // Deliberately NOT disarmed here. A keyed member sees
                        // the whole shared stream and skips most of it, so
                        // resetting on arrival would tie its dormancy to
                        // stream traffic rather than to its own idleness: on
                        // any outbox busier than `linger` no member would
                        // ever passivate. The deadline is disarmed only when
                        // this member actually does work — see the outcome
                        // dispatch below.
                        match event {
                            Some(item) => item,
                            None => {
                                if tracker.persisted_seq < state.sequence {
                                    persist_checkpoint(&mut current_job, &state, Some(&mirror))
                                        .await
                                        .map_err(|e| e as Box<dyn std::error::Error>)?;
                                }
                                return Ok(job::JobCompletion::RescheduleNow);
                            }
                        }
                    }
                }
            };

            let event = match item {
                Ok(event) => event,
                Err(undecodable) => {
                    let mut parts = CtxParts {
                        op_slot: &mut op_slot,
                        current_job: &mut current_job,
                        state: &mut state,
                        tracker: &mut tracker,
                        mirror: Some(&mirror),
                    };
                    flush_batch(&mut parts, &mut batch, &flusher, "undecodable_event")
                        .await
                        .map_err(|e| e as Box<dyn std::error::Error>)?;
                    match subscriber.handle_undecodable(&undecodable).await {
                        Ok(()) => {
                            state.sequence = undecodable.sequence;
                            continue;
                        }
                        Err(error) => {
                            if tracker.persisted_seq < state.sequence {
                                persist_checkpoint(&mut current_job, &state, Some(&mirror))
                                    .await
                                    .map_err(|e| e as Box<dyn std::error::Error>)?;
                            }
                            return Err(error as Box<dyn std::error::Error>);
                        }
                    }
                }
            };

            let ctx = KeyedEventCtx {
                parts: CtxParts {
                    op_slot: &mut op_slot,
                    current_job: &mut current_job,
                    state: &mut state,
                    tracker: &mut tracker,
                    mirror: Some(&mirror),
                },
                batch: &mut batch,
                flusher: &flusher,
                event_seq: event.sequence,
            };
            let outcome = subscriber
                .handle(ctx, &event)
                .await
                .map_err(|e| e as Box<dyn std::error::Error>)?
                .outcome;

            match outcome {
                Outcome::Hold(at) => {
                    // Does NOT advance state.sequence: the cursor holds
                    // strictly before this event, so the next run re-reads
                    // and re-evaluates it.
                    let mut parts = CtxParts {
                        op_slot: &mut op_slot,
                        current_job: &mut current_job,
                        state: &mut state,
                        tracker: &mut tracker,
                        mirror: Some(&mirror),
                    };
                    flush_batch(&mut parts, &mut batch, &flusher, "hold_entry")
                        .await
                        .map_err(|e| e as Box<dyn std::error::Error>)?;
                    if tracker.persisted_seq < state.sequence {
                        persist_checkpoint(&mut current_job, &state, Some(&mirror))
                            .await
                            .map_err(|e| e as Box<dyn std::error::Error>)?;
                    }
                    return Ok(job::JobCompletion::RescheduleAt(at));
                }
                Outcome::CommitAndHold(at) => {
                    // Same non-advance as Hold: the staged op's work lands,
                    // but the checkpoint it carries is still pre-this-event.
                    let mut parts = CtxParts {
                        op_slot: &mut op_slot,
                        current_job: &mut current_job,
                        state: &mut state,
                        tracker: &mut tracker,
                        mirror: Some(&mirror),
                    };
                    flush_batch(&mut parts, &mut batch, &flusher, "staged_hold")
                        .await
                        .map_err(|e| e as Box<dyn std::error::Error>)?;
                    return Ok(job::JobCompletion::RescheduleAt(at));
                }
                Outcome::Skip => {
                    // Not this member's event, so it is still idle: the
                    // linger deadline stands. Scanning past other keys'
                    // traffic must not keep a member resident.
                    state.sequence = event.sequence;
                }
                Outcome::Commit => {
                    // Real work — this member is not idle. Restart linger.
                    linger_deadline = None;
                    state.sequence = event.sequence;
                    let mut parts = CtxParts {
                        op_slot: &mut op_slot,
                        current_job: &mut current_job,
                        state: &mut state,
                        tracker: &mut tracker,
                        mirror: Some(&mirror),
                    };
                    flush_batch(&mut parts, &mut batch, &flusher, "commit")
                        .await
                        .map_err(|e| e as Box<dyn std::error::Error>)?;
                }
                Outcome::Collect => {
                    // Real work — this member is not idle. Restart linger.
                    linger_deadline = None;
                    state.sequence = event.sequence;
                    if tracker.collected >= self.max_batch_size {
                        let mut parts = CtxParts {
                            op_slot: &mut op_slot,
                            current_job: &mut current_job,
                            state: &mut state,
                            tracker: &mut tracker,
                            mirror: Some(&mirror),
                        };
                        flush_batch(&mut parts, &mut batch, &flusher, "batch_full")
                            .await
                            .map_err(|e| e as Box<dyn std::error::Error>)?;
                    }
                }
            }
        }
    }
}
