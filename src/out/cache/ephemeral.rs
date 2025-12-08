use serde::{Serialize, de::DeserializeOwned};

#[allow(dead_code)]
pub struct EphemeralOutboxEventCache<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    _phantom: std::marker::PhantomData<(P, Tables)>,
}
