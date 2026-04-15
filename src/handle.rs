use std::future::Future;
use std::panic::AssertUnwindSafe;

use futures::FutureExt;
use tokio::task::JoinHandle;

#[derive(Debug)]
pub(crate) struct OwnedTaskHandle(Option<JoinHandle<()>>);

impl OwnedTaskHandle {
    pub fn new(inner: tokio::task::JoinHandle<()>) -> Self {
        Self(Some(inner))
    }
}

impl Drop for OwnedTaskHandle {
    fn drop(&mut self) {
        if let Some(handle) = self.0.take() {
            handle.abort();
        }
    }
}

/// Spawn a background task whose exit and panics surface as OTEL spans.
///
/// All long-lived obix tasks (cache loop, pg-listener forwarder) must go
/// through here so that any silent death — normal exit or panic — emits a
/// short-lived error span (queryable in Honeycomb) instead of leaving the
/// outbox half-alive with no signal.
pub(crate) fn spawn_supervised<F>(task_name: &'static str, fut: F) -> JoinHandle<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        match AssertUnwindSafe(fut).catch_unwind().await {
            Ok(()) => {
                tracing::error_span!(
                    "obix.supervisor.task_exited",
                    otel.status_code = "ERROR",
                    task = task_name,
                )
                .in_scope(|| ());
            }
            Err(panic) => {
                let msg = if let Some(s) = panic.downcast_ref::<&str>() {
                    (*s).to_string()
                } else if let Some(s) = panic.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "<unknown panic payload>".to_string()
                };
                tracing::error_span!(
                    "obix.supervisor.task_panicked",
                    otel.status_code = "ERROR",
                    task = task_name,
                    panic = %msg,
                )
                .in_scope(|| ());
            }
        }
    })
}
