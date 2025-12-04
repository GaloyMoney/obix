mod event;

use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::broadcast;

use crate::tables::DefaultMailboxTables;
pub use event::*;

pub struct Outbox<P, Tables = DefaultMailboxTables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    tables: Tables,
    event_sender: broadcast::Sender<OutboxEvent<P>>,
    // highest_known_sequence: Arc<AtomicU64>,
    // buffer_size: usize,
}
