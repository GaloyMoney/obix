//! Test utilities for verifying event publication in tests.
//!
//! This module is available when the `test-utils` feature is enabled.

use std::time::Duration;

use serde::{Serialize, de::DeserializeOwned};
use tokio_stream::StreamExt;

use crate::{Outbox, out::OutboxEventMarker, tables::MailboxTables};

/// Error type for [`expect_event`].
#[derive(Debug, thiserror::Error)]
pub enum ExpectEventError<E> {
    /// The event was not received within the timeout period.
    #[error("Timeout waiting for event")]
    Timeout,
    /// The trigger operation failed.
    #[error("Trigger failed: {0:?}")]
    TriggerFailed(E),
}

/// Executes a trigger (typically a use case) and waits for a matching event.
///
/// This function starts listening on the outbox, executes the provided trigger,
/// then waits for an event that matches the predicate. This is useful for testing
/// that domain events are correctly published when use cases execute.
///
/// # Type Parameters
///
/// * `OE` - The outbox event (payload) type
/// * `IE` - The inner event type to extract from the payload
/// * `Tables` - The mailbox tables implementation
/// * `R` - The result type returned by the trigger
/// * `T` - The extracted data type returned by the matcher
/// * `F` - The trigger closure type
/// * `Fut` - The future returned by the trigger
/// * `E` - The error type returned by the trigger
/// * `M` - The matcher predicate type
///
/// # Arguments
///
/// * `outbox` - The outbox to listen on
/// * `trigger` - Async closure that executes an operation which should trigger the event
/// * `matches` - Predicate receiving `(trigger_result, event)` that returns `Some(T)` if
///   the event matches, or `None` to continue waiting
///
/// # Returns
///
/// Returns `Ok((trigger_result, extracted_data))` on success, or an error if the
/// trigger fails or if no matching event is received within 5 seconds.
///
/// # Example
///
/// ```no_run
/// use obix::test_utils::{expect_event, ExpectEventError};
/// use serde::{Deserialize, Serialize};
///
/// // Define your domain event type
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// enum UserEvent {
///     Created { id: u64, email: String },
///     Updated { id: u64 },
/// }
///
/// // A simple user type returned by the use case
/// struct User {
///     id: u64,
///     email: String,
/// }
///
/// async fn example(
///     outbox: &obix::Outbox<UserEvent>,
/// ) -> Result<(), ExpectEventError<std::io::Error>> {
///     // Simulate a use case that creates a user and publishes an event
///     let create_user = || async {
///         Ok::<_, std::io::Error>(User { id: 1, email: "test@example.com".into() })
///     };
///
///     let (user, event) = expect_event(
///         &outbox,
///         create_user,
///         |result, event| match event {
///             UserEvent::Created { id, email } if *id == result.id => {
///                 Some(email.clone())
///             }
///             _ => None,
///         },
///     ).await?;
///
///     assert_eq!(user.id, 1);
///     assert_eq!(event, "test@example.com");
///     Ok(())
/// }
/// ```
pub async fn expect_event<OE, IE, Tables, R, T, F, Fut, E, M>(
    outbox: &Outbox<OE, Tables>,
    trigger: F,
    matches: M,
) -> Result<(R, T), ExpectEventError<E>>
where
    OE: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin + OutboxEventMarker<IE>,
    IE: Send + Sync + 'static,
    Tables: MailboxTables,
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<R, E>>,
    M: Fn(&R, &IE) -> Option<T>,
{
    let mut listener = outbox.listen_all(None);

    let result = trigger().await.map_err(ExpectEventError::TriggerFailed)?;

    let event = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let event = listener.next().await.expect("stream should not end");
            if let Some(extracted) = event.as_event::<IE>().and_then(|e| matches(&result, e)) {
                return extracted;
            }
        }
    })
    .await
    .map_err(|_| ExpectEventError::Timeout)?;

    Ok((result, event))
}
