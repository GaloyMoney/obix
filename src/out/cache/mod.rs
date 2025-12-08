mod ephemeral;
mod persistent;

#[allow(unused_imports)]
pub use ephemeral::EphemeralOutboxEventCache;
pub use persistent::{CacheHandle, PersistentOutboxEventCache};
