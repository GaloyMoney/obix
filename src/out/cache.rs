use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::broadcast;

use std::collections::BTreeMap;

use super::event::*;
use crate::sequence::EventSequence;

pub struct OutboxEventCache<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    event_sender: broadcast::Sender<OutboxEvent<P>>,
    _phantom: std::marker::PhantomData<Tables>,
}

pub struct CacheInner<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    buffer_size: usize,
    sequenced_cache: BTreeMap<EventSequence, OutboxEvent<P>>,
}
