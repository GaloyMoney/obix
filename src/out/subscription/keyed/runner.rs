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
        // harmlessly (the router/sweep can't route to it anyway — lookups go
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
            });

        let mut persistent = self.outbox.listen_persisted(Some(state.sequence));

        let mut op_slot: Option<es_entity::DbOp<'static>> = None;
        let mut tracker = BatchTracker {
            events_in_op: 0,
            collected: 0,
            persisted_seq: state.sequence,
            last_persist: tokio::time::Instant::now(),
        };
        let mut batch = <D::Subscriber as KeyedSubscriber<P>>::Batch::default();

        // Armed once the persistent backlog is drained and nothing is
        // pending; disarmed the moment a new event arrives. Firing means
        // "caught up and quiet long enough" — passivate to Dormant.
        let mut linger_deadline: Option<tokio::time::Instant> = None;

        loop {
            let item = if op_slot.is_some() || tracker.collected > 0 {
                match persistent.next().now_or_never() {
                    Some(Some(item)) => item,
                    Some(None) => {
                        let mut parts = CtxParts {
                            op_slot: &mut op_slot,
                            current_job: &mut current_job,
                            state: &state,
                            tracker: &mut tracker,
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
                            state: &state,
                            tracker: &mut tracker,
                        };
                        flush_batch(&mut parts, &mut batch, &flusher, "backlog_drained")
                            .await
                            .map_err(|e| e as Box<dyn std::error::Error>)?;
                        continue;
                    }
                }
            } else {
                if linger_deadline.is_none() {
                    linger_deadline = Some(tokio::time::Instant::now() + self.linger);
                }
                let deadline = linger_deadline.expect("armed above");

                tokio::select! {
                    biased;
                    _ = current_job.shutdown_requested() => {
                        if tracker.persisted_seq < state.sequence {
                            persist_checkpoint(&mut current_job, &state)
                                .await
                                .map_err(|e| e as Box<dyn std::error::Error>)?;
                        }
                        return Ok(job::JobCompletion::RescheduleNow);
                    }
                    _ = tokio::time::sleep_until(deadline) => {
                        if tracker.persisted_seq < state.sequence {
                            let mut op = es_entity::DbOp::init_with_clock(
                                current_job.pool(),
                                current_job.clock(),
                            )
                            .await?;
                            current_job
                                .update_execution_state_in_op(&mut op, &state)
                                .await?;
                            return Ok(job::JobCompletion::CompleteWithOp(op));
                        }
                        return Ok(job::JobCompletion::Complete);
                    }
                    _ = tokio::time::sleep_until(tracker.last_persist + self.checkpoint_interval),
                        if tracker.persisted_seq < state.sequence => {
                        persist_checkpoint(&mut current_job, &state)
                            .await
                            .map_err(|e| e as Box<dyn std::error::Error>)?;
                        tracker.persisted_seq = state.sequence;
                        tracker.last_persist = tokio::time::Instant::now();
                        continue;
                    }
                    event = persistent.next() => {
                        linger_deadline = None;
                        match event {
                            Some(item) => item,
                            None => {
                                if tracker.persisted_seq < state.sequence {
                                    persist_checkpoint(&mut current_job, &state)
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
                        state: &state,
                        tracker: &mut tracker,
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
                                persist_checkpoint(&mut current_job, &state)
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
                    state: &state,
                    tracker: &mut tracker,
                },
                batch: &mut batch,
                flusher: &flusher,
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
                        state: &state,
                        tracker: &mut tracker,
                    };
                    flush_batch(&mut parts, &mut batch, &flusher, "hold_entry")
                        .await
                        .map_err(|e| e as Box<dyn std::error::Error>)?;
                    if tracker.persisted_seq < state.sequence {
                        persist_checkpoint(&mut current_job, &state)
                            .await
                            .map_err(|e| e as Box<dyn std::error::Error>)?;
                    }
                    return Ok(job::JobCompletion::RescheduleAt(at));
                }
                Outcome::CommitAndHold(at) => {
                    // Same non-advance as Hold: the isolated op's work lands,
                    // but the checkpoint it carries is still pre-this-event.
                    let mut parts = CtxParts {
                        op_slot: &mut op_slot,
                        current_job: &mut current_job,
                        state: &state,
                        tracker: &mut tracker,
                    };
                    flush_batch(&mut parts, &mut batch, &flusher, "commit_and_hold")
                        .await
                        .map_err(|e| e as Box<dyn std::error::Error>)?;
                    return Ok(job::JobCompletion::RescheduleAt(at));
                }
                Outcome::Skip => {
                    state.sequence = event.sequence;
                }
                Outcome::Commit => {
                    state.sequence = event.sequence;
                    let mut parts = CtxParts {
                        op_slot: &mut op_slot,
                        current_job: &mut current_job,
                        state: &state,
                        tracker: &mut tracker,
                    };
                    flush_batch(&mut parts, &mut batch, &flusher, "commit")
                        .await
                        .map_err(|e| e as Box<dyn std::error::Error>)?;
                }
                Outcome::Defer | Outcome::Collect => {
                    state.sequence = event.sequence;
                    if tracker.events_in_op >= self.max_batch_size {
                        let mut parts = CtxParts {
                            op_slot: &mut op_slot,
                            current_job: &mut current_job,
                            state: &state,
                            tracker: &mut tracker,
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
