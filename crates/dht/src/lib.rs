mod bprotocol;
mod dht;
mod error;
mod peer_store;
mod persistence;
mod routing_table;
mod utils;

use std::sync::Arc;
use std::time::Duration;

pub use error::{Error, Result};

pub use crate::dht::DhtStats;
pub use crate::dht::{DhtConfig, DhtState, RequestPeersStream};
pub use librqbit_core::hash_id::Id20;
pub use persistence::{DhtPersistenceConfig, PersistentDht, dht_listen_addr};

pub type Dht = Arc<DhtState>;

// How long do we wait for a response from a DHT node.
pub(crate) const RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);
// After how long we consider a routing table node questionable.
pub(crate) const INACTIVITY_TIMEOUT: Duration = Duration::from_secs(15 * 60);
// How often each torrent's DHT lookup pass is re-run.
pub(crate) const LOOKUP_INTERVAL: Duration = Duration::from_secs(5 * 60);
// Maximum number of in-flight DHT requests per torrent lookup pass. This is
// the backpressure point that bounds memory: each queued request is a future
// waiting on a response.
pub(crate) const LOOKUP_MAX_INFLIGHT: usize = 16;
// Hard cap on requests sent per torrent lookup pass. Passes normally converge
// before hitting this; it bounds adversarial swarms.
pub(crate) const LOOKUP_MAX_REQUESTS: usize = 256;

pub struct DhtBuilder {}

impl DhtBuilder {
    #[allow(clippy::new_ret_no_self)]
    pub async fn new() -> crate::Result<Dht> {
        DhtState::new().await
    }

    pub async fn with_config(config: DhtConfig<'_>) -> crate::Result<Dht> {
        DhtState::with_config(config).await
    }
}

pub static DHT_BOOTSTRAP: &[&str] = &["dht.transmissionbt.com:6881", "dht.libtorrent.org:25401"];
