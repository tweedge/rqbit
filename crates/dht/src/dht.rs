use std::{
    cmp::Reverse,
    net::{Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU16, Ordering},
    },
    task::Poll,
    time::{Duration, Instant},
};

use crate::{
    Error, INACTIVITY_TIMEOUT, LOOKUP_INTERVAL, LOOKUP_MAX_INFLIGHT, LOOKUP_MAX_REQUESTS,
    RESPONSE_TIMEOUT,
    bprotocol::{
        self, AnnouncePeer, CompactNodeInfo, CompactNodeInfoOwned, ErrorDescription,
        FindNodeRequest, GetPeersRequest, Message, MessageKind, Node, PingRequest, Response, Want,
    },
    peer_store::PeerStore,
    routing_table::{InsertResult, NodeStatus, RoutingTable},
};
use backon::{ExponentialBuilder, Retryable};
use bencode::ByteBufOwned;
use dashmap::DashMap;
use futures::{
    FutureExt, Stream, StreamExt, TryFutureExt, future::BoxFuture, stream::FuturesUnordered,
};

use leaky_bucket::RateLimiter;
use librqbit_core::{
    compact_ip::{CompactSerialize, CompactSerializeFixedLen},
    crate_version,
    hash_id::Id20,
    peer_id::generate_azereus_style,
    spawn_utils::{spawn, spawn_with_cancel},
};
use librqbit_dualstack_sockets::{BindDevice, UdpSocket};
use parking_lot::RwLock;

use serde::Serialize;
use tokio::sync::mpsc::{Sender, UnboundedReceiver, UnboundedSender, channel, unbounded_channel};

use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, debug_span, error, info, trace, warn};

fn now() -> Instant {
    Instant::now()
}

#[derive(Debug, Serialize)]
pub struct DhtStats {
    #[serde(serialize_with = "crate::utils::serialize_id20")]
    pub id: Id20,
    pub outstanding_requests: usize,
    pub routing_table_size: usize,
    pub routing_table_size_v6: usize,
}

struct OutstandingRequest {
    done: tokio::sync::oneshot::Sender<crate::Result<ResponseOrError>>,
}

pub struct WorkerSendRequest {
    // If this is set, we are tracking the response in inflight_by_transaction_id
    our_tid: Option<u16>,
    message: Message<ByteBufOwned>,
    addr: SocketAddr,
}

#[derive(Debug)]
struct MaybeUsefulNode {
    id: Id20,
    addr: SocketAddr,
    last_request: Instant,
    last_response: Option<Instant>,
    errors_in_a_row: usize,
    returned_peers: bool,
}

fn make_rate_limiter() -> RateLimiter {
    // TODO: move to configuration, i'm lazy.
    let dht_queries_per_second = std::env::var("DHT_QUERIES_PER_SECOND")
        .map(|v| v.parse().expect("couldn't parse DHT_QUERIES_PER_SECOND"))
        .unwrap_or(250usize);

    let per_100_ms = dht_queries_per_second / 10;

    RateLimiter::builder()
        .initial(per_100_ms)
        .max(dht_queries_per_second)
        .interval(Duration::from_millis(100))
        .fair(false)
        .refill(per_100_ms)
        .build()
}

trait RecursiveRequestCallbacks: Sized + Send + Sync + 'static {
    fn on_request_start(&self, req: &RecursiveRequest<Self>, target_node: Id20, addr: SocketAddr);
    fn on_request_end(
        &self,
        req: &RecursiveRequest<Self>,
        target_node: Id20,
        addr: SocketAddr,
        resp: &crate::Result<ResponseOrError>,
    );
}

struct RecursiveRequestCallbacksGetPeers {
    // Id20::from_str("00000fffffffffffffffffffffffffffffffffff").unwrap()
    min_distance_to_announce: Id20,
    announce_port: Option<u16>,
}

impl RecursiveRequestCallbacks for RecursiveRequestCallbacksGetPeers {
    fn on_request_start(&self, _: &RecursiveRequest<Self>, _: Id20, _: SocketAddr) {}

    fn on_request_end(
        &self,
        req: &RecursiveRequest<Self>,
        target_node: Id20,
        addr: SocketAddr,
        resp: &crate::Result<ResponseOrError>,
    ) {
        let announce_port = match self.announce_port {
            Some(a) => a,
            None => return,
        };
        let resp = match resp {
            Ok(ResponseOrError::Response(resp)) => resp,
            _ => return,
        };
        let token = match &resp.token {
            Some(token) => token,
            None => return,
        };
        if req.info_hash.distance(&target_node) > self.min_distance_to_announce {
            trace!(
                "not announcing, {:?} is too far from {:?}",
                target_node, req.info_hash
            );
            return;
        }
        let (tid, message) = req.dht.create_request(
            Request::Announce {
                info_hash: req.info_hash,
                token: token.clone(),
                port: announce_port,
            },
            addr,
        );

        let _ = req.dht.worker_sender.send(WorkerSendRequest {
            our_tid: Some(tid),
            message,
            addr,
        });
    }
}

struct RecursiveRequestCallbacksFindNodes {}
impl RecursiveRequestCallbacks for RecursiveRequestCallbacksFindNodes {
    fn on_request_start(&self, req: &RecursiveRequest<Self>, target_node: Id20, addr: SocketAddr) {
        let mut rt = req.dht.get_table_for_addr(addr).write();
        match rt.add_node(target_node, addr) {
            InsertResult::WasExisting | InsertResult::ReplacedBad(_) | InsertResult::Added => {
                rt.mark_outgoing_request(&target_node, now());
            }
            InsertResult::Ignored => {}
        }
    }

    fn on_request_end(
        &self,
        req: &RecursiveRequest<Self>,
        target_node: Id20,
        addr: SocketAddr,
        resp: &crate::Result<ResponseOrError>,
    ) {
        let mut table = req.dht.get_table_for_addr(addr).write();
        if resp.is_ok() {
            table.mark_response(&target_node, now());
        } else {
            table.mark_error(&target_node);
        }
    }
}

struct RecursiveRequest<C: RecursiveRequestCallbacks> {
    max_depth: usize,
    useful_nodes_limit: usize,
    info_hash: Id20,
    request: Request,
    dht: Arc<DhtState>,
    useful_nodes: RwLock<Vec<MaybeUsefulNode>>,
    peer_tx: tokio::sync::mpsc::UnboundedSender<SocketAddr>,
    node_tx: tokio::sync::mpsc::UnboundedSender<(Option<Id20>, SocketAddr, usize)>,
    callbacks: C,
}

pub struct RequestPeersStream {
    rx: tokio::sync::mpsc::UnboundedReceiver<SocketAddr>,
    cancel_join_handle_v4: tokio::task::JoinHandle<()>,
    cancel_join_handle_v6: tokio::task::JoinHandle<()>,
}

impl RequestPeersStream {
    fn new(dht: Arc<DhtState>, info_hash: Id20, announce_port: Option<u16>) -> Self {
        let (peer_tx, peer_rx) = unbounded_channel();
        let make = |is_v4: bool, dht: Arc<DhtState>, peer_tx: UnboundedSender<SocketAddr>| {
            let (node_tx, node_rx) = unbounded_channel();
            let rp = Arc::new(RecursiveRequest {
                max_depth: 4,
                info_hash,
                useful_nodes_limit: 256,
                request: Request::GetPeers(info_hash),
                dht,
                useful_nodes: RwLock::new(Vec::new()),
                peer_tx,
                node_tx,
                callbacks: RecursiveRequestCallbacksGetPeers {
                    min_distance_to_announce: Id20::from_str(
                        "0000ffffffffffffffffffffffffffffffffffff",
                    )
                    .unwrap(),
                    announce_port,
                },
            });
            rp.spawn_lookup_task(node_rx, is_v4)
        };

        let v4 = make(true, dht.clone(), peer_tx.clone());
        let v6 = make(false, dht, peer_tx);

        Self {
            rx: peer_rx,
            cancel_join_handle_v4: v4,
            cancel_join_handle_v6: v6,
        }
    }
}

impl Drop for RequestPeersStream {
    fn drop(&mut self) {
        self.cancel_join_handle_v4.abort();
        self.cancel_join_handle_v6.abort();
    }
}

impl Stream for RequestPeersStream {
    type Item = SocketAddr;

    #[inline(never)]
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

impl RecursiveRequest<RecursiveRequestCallbacksFindNodes> {
    async fn find_node_for_routing_table(
        dht: Arc<DhtState>,
        target: Id20,
        addrs: impl Iterator<Item = SocketAddr>,
    ) -> crate::Result<()> {
        let (node_tx, mut node_rx) = unbounded_channel();
        let req = RecursiveRequest {
            max_depth: 4,
            info_hash: target,
            request: Request::FindNode(target),
            dht,
            useful_nodes_limit: 32,
            useful_nodes: RwLock::new(Vec::new()),
            peer_tx: unbounded_channel().0,
            node_tx,
            callbacks: RecursiveRequestCallbacksFindNodes {},
        };

        let request_one = |id, addr, depth| {
            req.request_one(id, addr, depth)
                .map_err(|e| {
                    debug!("error: {e:#}");
                    e
                })
                .instrument(debug_span!(
                    "find_node",
                    target = format!("{target:?}"),
                    addr = addr.to_string()
                ))
        };

        let mut futs = FuturesUnordered::new();

        let mut initial_addrs = 0;
        for addr in addrs {
            futs.push(request_one(None, addr, 0));
            initial_addrs += 1;
        }

        let mut successes = 0;
        let mut errors = 0;

        loop {
            tokio::select! {
                biased;

                r = node_rx.recv() => {
                    let (id, addr, depth) = r.unwrap();
                    futs.push(request_one(id, addr, depth))
                },
                f = futs.next() => {
                    let f = match f {
                        Some(f) => f,
                        None => {
                            // find_node recursion finished.
                            break;
                        }
                    };
                    if f.is_ok() {
                        successes += 1;
                    } else {
                        errors += 1;
                    }
                }
            }
        }
        if successes == 0 {
            return Err(Error::NoSuccessfulLookups { errors });
        }
        debug!(
            "finished, successes = {successes}, errors = {errors}, initial_addrs = {initial_addrs}"
        );
        Ok(())
    }
}

impl RecursiveRequest<RecursiveRequestCallbacksGetPeers> {
    /// Runs one bounded, terminating traversal pass: at most
    /// `lookup_max_inflight` requests in flight, at most `lookup_max_requests`
    /// sent, ending on Kademlia convergence (nothing in flight, no candidates
    /// queued) or budget exhaustion (queued candidates persist for the next
    /// pass, which processes them first).
    async fn run_lookup_pass(
        &self,
        node_rx: &mut UnboundedReceiver<(Option<Id20>, SocketAddr, usize)>,
    ) {
        let mut futs = FuturesUnordered::new();
        let max_inflight = self.dht.lookup_max_inflight.max(1);
        let mut budget = self.dht.lookup_max_requests.max(1);
        loop {
            while futs.len() >= max_inflight {
                if futs.next().await.is_none() {
                    break;
                }
            }
            if budget == 0 {
                while futs.next().await.is_some() {}
                break;
            }
            if futs.is_empty() && node_rx.is_empty() {
                break;
            }
            tokio::select! {
                biased;

                item = node_rx.recv() => match item {
                    Some((id, addr, depth)) => {
                        budget -= 1;
                        futs.push(
                            self.request_one(id, addr, depth)
                                .map_err(|e| debug!("error: {e:#}"))
                                .instrument(debug_span!("addr", addr=addr.to_string()))
                        );
                    }
                    None => {
                        while futs.next().await.is_some() {}
                        break;
                    }
                },
                Some(_) = futs.next(), if !futs.is_empty() => {}
            }
        }
    }

    /// Per-torrent lookup task: run a bounded terminating traversal pass, then
    /// sleep out the rest of the announce interval. Teardown is by task abort
    /// when the consumer drops the stream.
    fn spawn_lookup_task(
        self: &Arc<Self>,
        mut node_rx: UnboundedReceiver<(Option<Id20>, SocketAddr, usize)>,
        is_v4: bool,
    ) -> tokio::task::JoinHandle<()> {
        let this = self.clone();
        spawn::<crate::Error>(
            debug_span!(parent: None, "get_peers", is_v4, info_hash = format!("{:?}", self.info_hash)),
            "get_peers",
            async move {
                let this = &this;
                loop {
                    let pass_started = Instant::now();
                    let roots = match this.get_peers_root(is_v4) {
                        Ok(0) => {
                            // Routing table empty (e.g. still bootstrapping),
                            // no queries sent - retry soon.
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                        Ok(n) => n,
                        Err(e) => {
                            error!("dht: error in get_peers_root(): {e:#}");
                            tokio::time::sleep(this.dht.lookup_interval).await;
                            continue;
                        }
                    };
                    this.run_lookup_pass(&mut node_rx).await;
                    // Warm-up: re-query proportionally sooner while the routing table has
                    // fewer than 8 usable nodes, as the old loop did.
                    let cadence = if roots < 8 {
                        (this.dht.lookup_interval / 8)
                            .saturating_mul(roots as u32)
                            .max(Duration::from_secs(1))
                    } else {
                        this.dht.lookup_interval
                    };
                    tokio::time::sleep(cadence.saturating_sub(pass_started.elapsed())).await;
                }
            },
        )
    }

    fn get_peers_root(&self, is_v4: bool) -> crate::Result<usize> {
        let mut count = 0;
        let table = if is_v4 {
            &self.dht.routing_table_v4
        } else {
            &self.dht.routing_table_v6
        };
        for (id, addr) in table
            .read()
            .sorted_by_distance_from(self.info_hash, now())
            .iter()
            .map(|n| (n.id(), n.addr()))
            .take(8)
        {
            count += 1;
            self.node_tx
                .send((Some(id), addr, 0))
                .ok()
                .ok_or(Error::DhtDead)?;
        }

        Ok(count)
    }
}

impl<C: RecursiveRequestCallbacks> RecursiveRequest<C> {
    async fn request_one(
        &self,
        id: Option<Id20>,
        addr: SocketAddr,
        depth: usize,
    ) -> crate::Result<()> {
        if let Some(id) = id {
            self.callbacks.on_request_start(self, id, addr);
        }

        let response = self
            .dht
            .request(self.request.clone(), addr)
            .await
            .inspect(|r| {
                self.mark_node_responded(addr, r);
            });
        if let Some(id) = id {
            self.callbacks.on_request_end(self, id, addr, &response);
        }

        let response = match response {
            Ok(ResponseOrError::Response(r)) => r,
            Ok(ResponseOrError::Error(e)) => {
                debug!("error response: {e:?}");
                return Err(Error::ErrorResponse);
            }
            Err(e) => {
                self.mark_node_error(addr);
                return Err(e);
            }
        };

        if let Some(peers) = response.values {
            for peer in peers {
                self.peer_tx.send(peer.0).ok().ok_or(Error::ReceiverDead)?;
            }
        }

        let node_it = response
            .nodes
            .iter()
            .flat_map(|n| n.iter().map(|n| n.as_socketaddr()))
            .chain(
                response
                    .nodes6
                    .iter()
                    .flat_map(|n| n.iter().map(|n| n.as_socketaddr())),
            )
            .filter(|node| addr.is_ipv4() == node.addr.is_ipv4());

        let now = now();

        for node in node_it {
            let should_request = self.should_request_node(node.id, node.addr, depth, now);
            trace!(
                "should_request={}, id={:?}, addr={}, depth={}/{}",
                should_request, node.id, node.addr, depth, self.max_depth
            );
            if should_request {
                self.node_tx
                    .send((Some(node.id), node.addr, depth + 1))
                    .ok()
                    .ok_or(Error::ReceiverDead)?;
            }
        }
        Ok(())
    }

    fn mark_node_error(&self, addr: SocketAddr) -> bool {
        self.useful_nodes
            .write()
            .iter_mut()
            .find(|n| n.addr == addr)
            .map(|n| {
                n.errors_in_a_row += 1;
            })
            .is_some()
    }

    fn mark_node_responded(&self, addr: SocketAddr, response: &ResponseOrError) -> bool {
        self.useful_nodes
            .write()
            .iter_mut()
            .find(|n| n.addr == addr)
            .map(|node| {
                node.last_response = Some(now());
                node.errors_in_a_row = 0;
                match response {
                    ResponseOrError::Response(r) => {
                        node.returned_peers =
                            r.values.as_ref().map(|c| !c.is_empty()).unwrap_or(false)
                    }
                    ResponseOrError::Error(_) => {
                        node.returned_peers = false;
                    }
                }
            })
            .is_some()
    }

    fn should_request_node(
        &self,
        node_id: Id20,
        addr: SocketAddr,
        depth: usize,
        now: Instant,
    ) -> bool {
        if depth >= self.max_depth {
            return false;
        }

        let mut closest_nodes = self.useful_nodes.write();

        // If recently requested, ignore
        if let Some(existing) = closest_nodes.iter_mut().find(|n| n.id == node_id) {
            if now - existing.last_request > Duration::from_secs(60) {
                existing.last_request = now;
                return true;
            }
            return false;
        }

        closest_nodes.push(MaybeUsefulNode {
            id: node_id,
            addr,
            last_request: now,
            last_response: None,
            returned_peers: false,
            errors_in_a_row: 0,
        });

        closest_nodes.sort_by_key(|n| {
            let has_returned_peers_desc = Reverse(n.returned_peers);
            let has_responded_desc = Reverse(n.last_response.is_some() as u8);
            let distance = n.id.distance(&self.info_hash);
            let freshest_response = n.last_response.map(|r| now - r).unwrap_or(Duration::MAX);
            (
                has_returned_peers_desc,
                has_responded_desc,
                distance,
                freshest_response,
            )
        });
        if closest_nodes.len() > self.useful_nodes_limit {
            let popped = closest_nodes.pop().unwrap();
            if popped.id == node_id {
                return false;
            }
        }
        true
    }
}

pub struct DhtState {
    id: Id20,
    next_transaction_id: AtomicU16,

    // Created requests: (transaction_id, addr) => Requests.
    // If we get a response, it gets removed from here.
    inflight_by_transaction_id: DashMap<(u16, SocketAddr), OutstandingRequest>,

    routing_table_v4: RwLock<RoutingTable>,
    routing_table_v6: RwLock<RoutingTable>,
    listen_addr: SocketAddr,

    // Sending requests to the worker.
    rate_limiter: RateLimiter,
    // This is to send raw messages
    worker_sender: UnboundedSender<WorkerSendRequest>,

    cancellation_token: CancellationToken,

    pub(crate) peer_store: PeerStore,

    lookup_interval: Duration,
    lookup_max_inflight: usize,
    lookup_max_requests: usize,
}

impl DhtState {
    #[allow(clippy::too_many_arguments)]
    fn new_internal(
        id: Id20,
        sender: UnboundedSender<WorkerSendRequest>,
        routing_table_v4: Option<RoutingTable>,
        routing_table_v6: Option<RoutingTable>,
        listen_addr: SocketAddr,
        peer_store: PeerStore,
        cancellation_token: CancellationToken,
        lookup: LookupTuning,
    ) -> Self {
        let routing_table_v4 = routing_table_v4.unwrap_or_else(|| RoutingTable::new(id, None));
        let routing_table_v6 = routing_table_v6.unwrap_or_else(|| RoutingTable::new(id, None));
        Self {
            id,
            next_transaction_id: AtomicU16::new(0),
            inflight_by_transaction_id: Default::default(),
            routing_table_v4: RwLock::new(routing_table_v4),
            routing_table_v6: RwLock::new(routing_table_v6),
            worker_sender: sender,
            listen_addr,
            rate_limiter: make_rate_limiter(),
            peer_store,
            cancellation_token,
            lookup_interval: lookup.interval,
            lookup_max_inflight: lookup.max_inflight.max(1),
            lookup_max_requests: lookup.max_requests.max(1),
        }
    }

    async fn request(&self, request: Request, addr: SocketAddr) -> crate::Result<ResponseOrError> {
        self.rate_limiter.acquire_one().await;
        let (tid, message) = self.create_request(request, addr);
        let key = (tid, addr);
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.inflight_by_transaction_id
            .insert(key, OutstandingRequest { done: tx });
        trace!("sending {message:?}");
        match self.worker_sender.send(WorkerSendRequest {
            our_tid: Some(tid),
            message,
            addr,
        }) {
            Ok(_) => {}
            Err(_) => {
                self.inflight_by_transaction_id.remove(&key);
                return Err(Error::DhtDead);
            }
        };
        match tokio::time::timeout(RESPONSE_TIMEOUT, rx).await {
            Ok(Ok(r)) => r.map(|r| {
                trace!("received {r:?}");
                r
            }),
            Ok(Err(_)) => {
                self.inflight_by_transaction_id.remove(&key);
                Err(Error::DhtDead)
            }
            Err(_) => {
                self.inflight_by_transaction_id.remove(&key);
                Err(Error::ResponseTimeout(RESPONSE_TIMEOUT))
            }
        }
    }

    fn create_request(&self, request: Request, addr: SocketAddr) -> (u16, Message<ByteBufOwned>) {
        let transaction_id = self.next_transaction_id.fetch_add(1, Ordering::Relaxed);
        let transaction_id_buf = [(transaction_id >> 8) as u8, (transaction_id & 0xff) as u8];

        let want = if addr.is_ipv6() {
            Some(Want::V6)
        } else {
            Some(Want::V4)
        };

        let message = match request {
            Request::GetPeers(info_hash) => Message {
                transaction_id: ByteBufOwned::from(transaction_id_buf.as_ref()),
                version: None,
                ip: None,
                kind: MessageKind::GetPeersRequest(GetPeersRequest {
                    id: self.id,
                    info_hash,
                    want,
                }),
            },
            Request::FindNode(target) => Message {
                transaction_id: ByteBufOwned::from(transaction_id_buf.as_ref()),
                version: None,
                ip: None,
                kind: MessageKind::FindNodeRequest(FindNodeRequest {
                    id: self.id,
                    target,
                    want,
                }),
            },
            Request::Ping => Message {
                transaction_id: ByteBufOwned::from(transaction_id_buf.as_ref()),
                version: None,
                ip: None,
                kind: MessageKind::PingRequest(PingRequest { id: self.id }),
            },
            Request::Announce {
                info_hash,
                token,
                port,
            } => Message {
                kind: MessageKind::AnnouncePeer(AnnouncePeer {
                    id: self.id,
                    implied_port: 0,
                    info_hash,
                    port,
                    token,
                }),
                transaction_id: ByteBufOwned::from(transaction_id_buf.as_ref()),
                version: None,
                ip: None,
            },
        };
        (transaction_id, message)
    }

    fn generate_compact_nodes_both(
        &self,
        target: Id20,
        want: Want,
    ) -> (
        Option<CompactNodeInfoOwned<SocketAddrV4>>,
        Option<CompactNodeInfoOwned<SocketAddrV6>>,
    ) {
        let now = now();
        match want {
            Want::V4 => (
                Some(self.generate_compact_nodes(target, &self.routing_table_v4.read(), now)),
                None,
            ),
            Want::V6 => (
                None,
                Some(self.generate_compact_nodes(target, &self.routing_table_v6.read(), now)),
            ),
            Want::Both => (
                Some(self.generate_compact_nodes(target, &self.routing_table_v4.read(), now)),
                Some(self.generate_compact_nodes(target, &self.routing_table_v6.read(), now)),
            ),
            Want::None => (None, None),
        }
    }

    fn get_table_for_addr(&self, addr: SocketAddr) -> &RwLock<RoutingTable> {
        if addr.is_ipv4() {
            &self.routing_table_v4
        } else {
            &self.routing_table_v6
        }
    }

    fn generate_compact_nodes<A>(
        &self,
        target: Id20,
        table: &RoutingTable,
        now: Instant,
    ) -> CompactNodeInfo<ByteBufOwned, A>
    where
        A: CompactSerialize + CompactSerializeFixedLen + FromSocketAddr,
        Node<A>: CompactSerialize + CompactSerializeFixedLen,
    {
        let it = table
            .sorted_by_distance_from(target, now)
            .into_iter()
            .filter_map(|r| {
                Some(Node {
                    id: r.id(),
                    addr: A::from_socket_addr(r.addr())?,
                })
            })
            .take(8);
        CompactNodeInfo::new_from_iter(it)
    }

    fn on_received_message(
        self: &Arc<Self>,
        msg: Message<ByteBufOwned>,
        addr: SocketAddr,
    ) -> crate::Result<()> {
        match &msg.kind {
            // If it's a response to a request we made, find the request task, notify it with the response,
            // and let it handle it.
            MessageKind::Error(_) | MessageKind::Response(_) => {
                let tid = msg
                    .get_our_transaction_id()
                    .ok_or(Error::BadTransactionId)?;
                let request = match self
                    .inflight_by_transaction_id
                    .remove(&(tid, addr))
                    .map(|(_, v)| v)
                {
                    Some(req) => req,
                    None => {
                        trace!(?msg, "outstanding request not found");
                        return Err(Error::RequestNotFound);
                    }
                };

                let response_or_error = match msg.kind {
                    MessageKind::Error(e) => ResponseOrError::Error(e),
                    MessageKind::Response(r) => ResponseOrError::Response(r),
                    _ => unreachable!(),
                };
                match request.done.send(Ok(response_or_error)) {
                    Ok(_) => {}
                    Err(e) => {
                        debug!(
                            "received response, but the receiver task is closed: {:?}",
                            e
                        );
                    }
                }
                return Ok(());
            }
            _ => {}
        };

        trace!("received query from {addr}: {msg:?}");

        match &msg.kind {
            // Otherwise, respond to a query.
            MessageKind::PingRequest(req) => {
                let message = Message {
                    transaction_id: msg.transaction_id,
                    version: None,
                    ip: Some(addr),
                    kind: MessageKind::Response(bprotocol::Response {
                        id: self.id,
                        ..Default::default()
                    }),
                };
                self.get_table_for_addr(addr)
                    .write()
                    .mark_last_query(&req.id, now());
                self.worker_sender
                    .send(WorkerSendRequest {
                        our_tid: None,
                        message,
                        addr,
                    })
                    .ok()
                    .ok_or(Error::DhtDead)?;
                Ok(())
            }
            MessageKind::AnnouncePeer(ann) => {
                self.get_table_for_addr(addr)
                    .write()
                    .mark_last_query(&ann.id, now());
                let added = self.peer_store.store_peer(ann, addr);
                trace!("{addr}: added_peer={added}, announce={ann:?}");
                let message = Message {
                    transaction_id: msg.transaction_id,
                    version: None,
                    ip: Some(addr),
                    kind: MessageKind::Response(bprotocol::Response {
                        id: self.id,
                        ..Default::default()
                    }),
                };
                self.worker_sender
                    .send(WorkerSendRequest {
                        our_tid: None,
                        message,
                        addr,
                    })
                    .ok()
                    .ok_or(Error::DhtDead)?;
                Ok(())
            }
            MessageKind::GetPeersRequest(req) => {
                let want = req
                    .want
                    .unwrap_or(if addr.is_ipv6() { Want::V6 } else { Want::V4 });
                let (nodes, nodes6) = self.generate_compact_nodes_both(req.info_hash, want);
                let compact_peer_info = self.peer_store.get_for_info_hash(req.info_hash, want);
                self.get_table_for_addr(addr)
                    .write()
                    .mark_last_query(&req.id, now());
                let message = Message {
                    transaction_id: msg.transaction_id,
                    version: None,
                    ip: Some(addr),
                    kind: MessageKind::Response(bprotocol::Response {
                        id: self.id,
                        nodes,
                        nodes6,
                        values: Some(compact_peer_info),
                        token: Some(ByteBufOwned::from(
                            &self.peer_store.gen_token_for(req.id, addr)[..],
                        )),
                    }),
                };
                self.worker_sender
                    .send(WorkerSendRequest {
                        our_tid: None,
                        message,
                        addr,
                    })
                    .ok()
                    .ok_or(Error::DhtDead)?;
                Ok(())
            }
            MessageKind::FindNodeRequest(req) => {
                let want = req
                    .want
                    .unwrap_or(if addr.is_ipv6() { Want::V6 } else { Want::V4 });
                let (nodes, nodes6) = self.generate_compact_nodes_both(req.target, want);
                self.get_table_for_addr(addr)
                    .write()
                    .mark_last_query(&req.id, now());
                let message = Message {
                    transaction_id: msg.transaction_id,
                    version: None,
                    ip: Some(addr),
                    kind: MessageKind::Response(bprotocol::Response {
                        id: self.id,
                        nodes,
                        nodes6,
                        ..Default::default()
                    }),
                };
                self.worker_sender
                    .send(WorkerSendRequest {
                        our_tid: None,
                        message,
                        addr,
                    })
                    .ok()
                    .ok_or(Error::DhtDead)?;
                Ok(())
            }
            _ => unreachable!(),
        }
    }

    pub fn get_stats(&self) -> DhtStats {
        DhtStats {
            id: self.id,
            outstanding_requests: self.inflight_by_transaction_id.len(),
            routing_table_size: self.routing_table_v4.read().len(),
            routing_table_size_v6: self.routing_table_v6.read().len(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Request {
    GetPeers(Id20),
    FindNode(Id20),
    Announce {
        info_hash: Id20,
        token: ByteBufOwned,
        port: u16,
    },
    Ping,
}

enum ResponseOrError {
    Response(Response<ByteBufOwned>),
    Error(ErrorDescription<ByteBufOwned>),
}

impl core::fmt::Debug for ResponseOrError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Response(r) => write!(f, "{r:?}"),
            Self::Error(e) => write!(f, "{e:?}"),
        }
    }
}

struct DhtWorker {
    socket: UdpSocket,
    dht: Arc<DhtState>,
}

impl DhtWorker {
    fn on_send_error(&self, tid: u16, addr: SocketAddr, err: crate::Error) {
        if let Some((_, OutstandingRequest { done })) =
            self.dht.inflight_by_transaction_id.remove(&(tid, addr))
        {
            let _ = done.send(Err(err)).is_err();
        };
    }

    async fn bootstrap_hostname(&self, hostname: &str) -> crate::Result<()> {
        let addrs = tokio::net::lookup_host(hostname)
            .await
            .map_err(|err| Error::lookup(hostname, err))?
            .collect::<Vec<_>>();
        let v4 = RecursiveRequest::find_node_for_routing_table(
            self.dht.clone(),
            self.dht.id,
            addrs.iter().copied().filter(|a| a.is_ipv4()),
        )
        .instrument(debug_span!("v4"));

        let v6 = RecursiveRequest::find_node_for_routing_table(
            self.dht.clone(),
            self.dht.id,
            addrs.iter().copied().filter(|a| a.is_ipv6()),
        )
        .instrument(debug_span!("v6"));

        let (v4, v6) = tokio::join!(v4, v6);
        v4.or(v6)
    }

    async fn bootstrap_hostname_with_backoff(&self, addr: &str) -> crate::Result<()> {
        let backoff = ExponentialBuilder::new()
            .with_max_delay(Duration::from_secs(60))
            .with_jitter()
            .with_total_delay(Some(Duration::from_secs(86400)))
            .without_max_times();

        (|| self.bootstrap_hostname(addr))
            .retry(backoff)
            .notify(|error, retry_in| {
                warn!(?retry_in, ?addr, "error in bootstrap: {error:#}");
            })
            .instrument(debug_span!("bootstrap", hostname = addr))
            .await
    }

    async fn bootstrap(&self, bootstrap_addrs: &[String]) -> crate::Result<()> {
        let mut futs = FuturesUnordered::new();

        for addr in bootstrap_addrs.iter() {
            futs.push(self.bootstrap_hostname_with_backoff(addr));
        }
        let mut successes = 0;
        while let Some(resp) = futs.next().await {
            if resp.is_ok() {
                successes += 1
            }
        }
        if successes == 0 {
            return Err(Error::BootstrapFailed);
        }
        Ok(())
    }

    async fn bucket_refresher(&self, is_v4: bool) -> crate::Result<()> {
        let (tx, mut rx) = unbounded_channel();

        let table = if is_v4 {
            &self.dht.routing_table_v4
        } else {
            &self.dht.routing_table_v6
        };

        let mut futs = FuturesUnordered::new();
        let filler = async {
            let mut interval = tokio::time::interval(INACTIVITY_TIMEOUT);
            interval.tick().await;
            let mut iteration = 0;
            loop {
                interval.tick().await;
                let now = now();
                let mut found = 0;

                for bucket in table.read().iter_buckets() {
                    if now - bucket.leaf.last_refreshed < INACTIVITY_TIMEOUT {
                        continue;
                    }
                    found += 1;
                    let random_id = bucket.random_within();
                    tx.send(random_id).unwrap();
                }
                trace!("iteration {}, refreshing {} buckets", iteration, found);
                iteration += 1;
            }
        };

        tokio::pin!(filler);

        loop {
            tokio::select! {
                _ = &mut filler => {},
                random_id = rx.recv() => {
                    let random_id = random_id.unwrap();
                    let addrs = table
                        .read()
                        .sorted_by_distance_from(random_id, now())
                        .iter()
                        .map(|n| n.addr())
                        .take(8).collect::<Vec<_>>();
                    futs.push(
                        RecursiveRequest::find_node_for_routing_table(
                            self.dht.clone(), random_id, addrs.into_iter()
                        ).instrument(debug_span!("refresh_bucket"))
                    );
                },
                _ = futs.next(), if !futs.is_empty() => {},
            }
        }
    }

    async fn pinger(&self, is_v4: bool) -> crate::Result<()> {
        let table = if is_v4 {
            &self.dht.routing_table_v4
        } else {
            &self.dht.routing_table_v6
        };
        let mut futs = FuturesUnordered::new();
        let mut interval = tokio::time::interval(INACTIVITY_TIMEOUT / 4);
        let (tx, mut rx) = unbounded_channel();
        let looper = async {
            let mut iteration = 0;
            loop {
                interval.tick().await;
                let mut found = 0;
                let now = now();
                for node in table.read().iter() {
                    if matches!(
                        node.status(now),
                        NodeStatus::Questionable | NodeStatus::Unknown
                    ) {
                        found += 1;
                        tx.send((node.id(), node.addr())).unwrap();
                    }
                }
                trace!("iteration {}, pinging {} nodes", iteration, found);
                iteration += 1;
            }
        };

        tokio::pin!(looper);

        loop {
            tokio::select! {
                _ = &mut looper => {},
                r = rx.recv() => {
                    let (id, addr) = r.unwrap();
                    futs.push(async move {
                        table.write().mark_outgoing_request(&id, now());
                        match self.dht.request(Request::Ping, addr).await {
                            Ok(_) => {
                                table.write().mark_response(&id, now());
                            },
                            Err(e) => {
                                table.write().mark_error(&id);
                                debug!("error: {e:#}");
                            }
                        }
                    }.instrument(debug_span!("ping", addr=addr.to_string())))
                },
                _ = futs.next(), if !futs.is_empty() => {},
            }
        }
    }

    async fn framer(
        &self,
        socket: &UdpSocket,
        mut input_rx: UnboundedReceiver<WorkerSendRequest>,
        output_tx: Sender<(Message<ByteBufOwned>, SocketAddr)>,
    ) -> crate::Result<()> {
        let writer = async {
            let mut buf = Vec::new();
            while let Some(WorkerSendRequest {
                our_tid,
                message,
                addr,
            }) = input_rx.recv().await
            {
                if our_tid.is_none() {
                    trace!("{}: sending {:?}", addr, &message);
                }
                buf.clear();
                bprotocol::serialize_message(
                    &mut buf,
                    message.transaction_id,
                    message.version,
                    message.ip,
                    message.kind,
                )
                .unwrap();
                if let Err(e) = socket.send_to(&buf, addr).await {
                    debug!("error sending to {addr}: {e:#}");
                    if let Some(tid) = our_tid {
                        self.on_send_error(tid, addr, Error::Send(e));
                    }
                }
            }
            Err(Error::DhtDead)
        };
        let reader = async {
            let mut buf = vec![0u8; 16384];
            loop {
                let (size, addr) = socket.recv_from(&mut buf).await.map_err(Error::Recv)?;
                match bprotocol::deserialize_message::<ByteBufOwned>(&buf[..size]) {
                    Ok(msg) => match output_tx.send((msg, addr)).await {
                        Ok(_) => {}
                        Err(_) => return Err(Error::DhtDead),
                    },
                    Err(e) => debug!("{}: error deserializing incoming message: {}", addr, e),
                }
            }
        };
        let result = tokio::select! {
            err = writer => err,
            err = reader => err,
        };
        result
    }

    async fn start(
        self,
        in_rx: UnboundedReceiver<WorkerSendRequest>,
        bootstrap_addrs: &[String],
    ) -> crate::Result<()> {
        let (out_tx, mut out_rx) = channel(1);
        let framer = self
            .framer(&self.socket, in_rx, out_tx)
            .instrument(debug_span!("dht_framer"));

        let bootstrap = self.bootstrap(bootstrap_addrs);
        let mut bootstrap_done = false;

        let response_reader = {
            let this = &self;
            async move {
                while let Some((response, addr)) = out_rx.recv().await {
                    if let Err(e) = this.dht.on_received_message(response, addr) {
                        debug!("error in on_response, addr={:?}: {}", addr, e)
                    }
                }
                Err(Error::DhtDead)
            }
        }
        .instrument(debug_span!("dht_response_reader"));

        let pinger_v4 = self.pinger(true).instrument(debug_span!("pinger_v4"));
        let bucket_refresher_v4 = self
            .bucket_refresher(true)
            .instrument(debug_span!("bucket_refresher_v4"));

        let pinger_v6 = self.pinger(false).instrument(debug_span!("pinger_v6"));
        let bucket_refresher_v6 = self
            .bucket_refresher(false)
            .instrument(debug_span!("bucket_refresher_v6"));

        tokio::pin!(framer);
        tokio::pin!(bootstrap);
        tokio::pin!(response_reader);
        tokio::pin!(pinger_v4);
        tokio::pin!(bucket_refresher_v4);
        tokio::pin!(pinger_v6);
        tokio::pin!(bucket_refresher_v6);

        loop {
            tokio::select! {
                err = &mut framer => {
                    return Error::task_finished(&"framer", err);
                },
                result = &mut bootstrap, if !bootstrap_done => {
                    bootstrap_done = true;
                    result?;
                },
                err = &mut pinger_v4 => {
                    return Error::task_finished(&"pinger_v4", err);
                },
                err = &mut bucket_refresher_v4 => {
                    return Error::task_finished(&"bucket_refresher_v4", err);
                },
                err = &mut pinger_v6 => {
                    return Error::task_finished(&"pinger_v6", err);
                },
                err = &mut bucket_refresher_v6 => {
                    return Error::task_finished(&"bucket_refresher_v6", err);
                },
                err = &mut response_reader => {
                    return Error::task_finished(&"response_reader", err);
                }
            }
        }
    }
}

#[derive(Default)]
pub struct DhtConfig<'a> {
    pub peer_id: Option<Id20>,
    pub bootstrap_addrs: Option<Vec<String>>,
    pub routing_table: Option<RoutingTable>,
    pub routing_table_v6: Option<RoutingTable>,
    pub listen_addr: Option<SocketAddr>,
    pub peer_store: Option<PeerStore>,
    pub cancellation_token: Option<CancellationToken>,
    pub bind_device: Option<&'a BindDevice>,
    /// How often each torrent's DHT lookup pass re-runs
    /// (default [`crate::LOOKUP_INTERVAL`]).
    pub lookup_interval: Option<Duration>,
    /// Maximum in-flight DHT requests per torrent lookup pass
    /// (default [`crate::LOOKUP_MAX_INFLIGHT`]).
    pub lookup_max_inflight: Option<usize>,
    /// Hard cap on requests sent per torrent lookup pass
    /// (default [`crate::LOOKUP_MAX_REQUESTS`]).
    pub lookup_max_requests: Option<usize>,
}

#[derive(Clone, Copy)]
struct LookupTuning {
    interval: Duration,
    max_inflight: usize,
    max_requests: usize,
}

impl LookupTuning {
    fn from_config(config: &DhtConfig) -> Self {
        Self {
            interval: config
                .lookup_interval
                .unwrap_or(LOOKUP_INTERVAL)
                // Floor at 1s: sub-second intervals monopolize the rate limiter.
                .max(Duration::from_secs(1)),
            max_inflight: config.lookup_max_inflight.unwrap_or(LOOKUP_MAX_INFLIGHT),
            max_requests: config.lookup_max_requests.unwrap_or(LOOKUP_MAX_REQUESTS),
        }
    }
}

impl DhtState {
    pub async fn new() -> crate::Result<Arc<Self>> {
        Self::with_config(DhtConfig::default()).await
    }
    pub fn cancellation_token(&self) -> &CancellationToken {
        &self.cancellation_token
    }

    #[inline(never)]
    pub fn with_config<'a>(mut config: DhtConfig<'a>) -> BoxFuture<'a, crate::Result<Arc<Self>>> {
        async move {
            let addr = config
                .listen_addr
                .unwrap_or((Ipv6Addr::UNSPECIFIED, 0).into());
            let socket = UdpSocket::bind_udp(
                addr,
                librqbit_dualstack_sockets::BindOpts {
                    request_dualstack: true,
                    reuseport: false,
                    device: config.bind_device,
                },
            )
            .map_err(|e| Error::Bind(Box::new(e)))?;

            let listen_addr = socket.bind_addr();
            info!("DHT listening on {:?}", listen_addr);

            let peer_id = config
                .peer_id
                .unwrap_or_else(|| generate_azereus_style(*b"rQ", crate_version!()));
            info!("starting up DHT with peer id {:?}", peer_id);
            let lookup = LookupTuning::from_config(&config);
            let bootstrap_addrs = config
                .bootstrap_addrs
                .unwrap_or_else(|| crate::DHT_BOOTSTRAP.iter().map(|v| v.to_string()).collect());

            let token = config.cancellation_token.take().unwrap_or_default();

            let (in_tx, in_rx) = unbounded_channel();
            let state = Arc::new(Self::new_internal(
                peer_id,
                in_tx,
                config.routing_table,
                config.routing_table_v6,
                listen_addr,
                config.peer_store.unwrap_or_else(|| PeerStore::new(peer_id)),
                token,
                lookup,
            ));

            spawn_with_cancel(
                debug_span!("dht"),
                "dht",
                state.cancellation_token.clone(),
                {
                    let state = state.clone();
                    async move {
                        let worker = DhtWorker { socket, dht: state };
                        worker.start(in_rx, &bootstrap_addrs).await
                    }
                },
            );
            Ok(state)
        }
        .boxed()
    }

    pub fn get_peers(
        self: &Arc<Self>,
        info_hash: Id20,
        announce_port: Option<u16>,
    ) -> RequestPeersStream {
        RequestPeersStream::new(self.clone(), info_hash, announce_port)
    }

    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }

    pub fn stats(&self) -> DhtStats {
        self.get_stats()
    }

    pub fn with_routing_tables<R, F: FnOnce(&RoutingTable, &RoutingTable) -> R>(&self, f: F) -> R {
        f(&self.routing_table_v4.read(), &self.routing_table_v6.read())
    }

    // pub fn clone_routing_table(&self) -> RoutingTable {
    //     self.routing_table.read().clone()
    // }
}

trait FromSocketAddr: Sized {
    fn from_socket_addr(addr: SocketAddr) -> Option<Self>;
}

impl FromSocketAddr for SocketAddrV4 {
    fn from_socket_addr(addr: SocketAddr) -> Option<Self> {
        match addr {
            SocketAddr::V4(a) => Some(a),
            _ => None,
        }
    }
}

impl FromSocketAddr for SocketAddrV6 {
    fn from_socket_addr(addr: SocketAddr) -> Option<Self> {
        match addr {
            SocketAddr::V6(a) => Some(a),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DhtBuilder;
    use crate::bprotocol::{self, CompactNodeInfo, MessageKind, Node, Response};
    use bencode::ByteBufOwned;
    use futures::StreamExt;
    use librqbit_core::compact_ip::CompactSocketAddr;
    use std::net::{IpAddr, Ipv4Addr, SocketAddrV4};
    use std::sync::atomic::{AtomicU64, AtomicUsize};

    const INFO_HASH: [u8; 20] = [0xABu8; 20];
    // Node id prefix equal to the info hash's first two bytes, so that swarm
    // nodes are within min_distance_to_announce and announce_peer gets used.
    const ID_PREFIX: [u8; 2] = [0xAB, 0xAB];

    #[derive(Default)]
    struct SwarmCounters {
        get_peers_queries: AtomicU64,
        announce_queries: AtomicU64,
        inflight: AtomicUsize,
        max_inflight: AtomicU64,
    }

    struct Swarm {
        addrs: Vec<SocketAddr>,
    }

    // Spawns `n` responsive swarm nodes sharing `counters`. If
    // `fresh_unresponsive` is set, each get_peers response also includes that
    // many unique unreachable addresses (requests to them time out).
    async fn spawn_swarm(
        counters: Arc<SwarmCounters>,
        n: usize,
        respond_delay: Duration,
        fresh_unresponsive: usize,
    ) -> Swarm {
        let mut sockets = Vec::with_capacity(n);
        let mut addrs = Vec::with_capacity(n);
        for _ in 0..n {
            let sock = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            addrs.push(sock.local_addr().unwrap());
            sockets.push(sock);
        }
        for sock in sockets.into_iter() {
            let counters = counters.clone();
            let known_addrs = addrs.clone();
            tokio::spawn(swarm_node_task(
                sock,
                known_addrs,
                counters,
                respond_delay,
                fresh_unresponsive,
            ));
        }
        Swarm { addrs }
    }

    #[allow(clippy::too_many_arguments)]
    async fn swarm_node_task(
        sock: tokio::net::UdpSocket,
        known_addrs: Vec<SocketAddr>,
        counters: Arc<SwarmCounters>,
        respond_delay: Duration,
        fresh_unresponsive: usize,
    ) {
        let my_index = known_addrs
            .iter()
            .position(|a| *a == sock.local_addr().unwrap())
            .unwrap_or(0) as u16;
        let my_id = node_id(my_index);
        let mut buf = vec![0u8; 4096];
        loop {
            let (n, from) = match sock.recv_from(&mut buf).await {
                Ok(x) => x,
                Err(_) => continue,
            };
            let msg = match bprotocol::deserialize_message::<ByteBufOwned>(&buf[..n]) {
                Ok(m) => m,
                Err(_) => continue,
            };
            let inflight = counters.inflight.fetch_add(1, Ordering::SeqCst) + 1;
            counters
                .max_inflight
                .fetch_max(inflight as u64, Ordering::SeqCst);
            tokio::time::sleep(respond_delay).await;

            let response = match &msg.kind {
                MessageKind::GetPeersRequest(_) => {
                    counters.get_peers_queries.fetch_add(1, Ordering::SeqCst);
                    let mut nodes = Vec::new();
                    for i in 0..fresh_unresponsive {
                        let mut id = [0u8; 20];
                        id[..2].copy_from_slice(&INFO_HASH[..2]);
                        id[2..6].copy_from_slice(&(i as u32).to_be_bytes());
                        id[6..10].copy_from_slice(
                            &(counters.get_peers_queries.load(Ordering::SeqCst) as u32)
                                .to_be_bytes(),
                        );
                        let octet = (i * 7 % 250 + 1) as u8;
                        nodes.push(Node {
                            id: Id20::new(id),
                            addr: SocketAddrV4::new(Ipv4Addr::new(10, 200, 0, octet), 9),
                        });
                    }
                    for (j, a) in known_addrs.iter().take(3).enumerate() {
                        if let SocketAddr::V4(a4) = a {
                            nodes.push(Node {
                                id: node_id(j as u16),
                                addr: *a4,
                            });
                        }
                    }
                    MessageKind::Response(Response {
                        id: my_id,
                        token: Some(ByteBufOwned::from(b"aaaa".as_ref())),
                        nodes: Some(CompactNodeInfo::new_from_iter(nodes.into_iter())),
                        nodes6: None,
                        values: Some(vec![CompactSocketAddr::from(SocketAddr::new(
                            IpAddr::V4(Ipv4Addr::LOCALHOST),
                            30000,
                        ))]),
                    })
                }
                MessageKind::FindNodeRequest(_) => {
                    let mut nodes = Vec::new();
                    for (j, a) in known_addrs.iter().take(3).enumerate() {
                        if let SocketAddr::V4(a4) = a {
                            nodes.push(Node {
                                id: node_id(j as u16),
                                addr: *a4,
                            });
                        }
                    }
                    MessageKind::Response(Response {
                        id: my_id,
                        token: Some(ByteBufOwned::from(b"aaaa".as_ref())),
                        nodes: Some(CompactNodeInfo::new_from_iter(nodes.into_iter())),
                        nodes6: None,
                        values: None,
                    })
                }
                MessageKind::PingRequest(_) => MessageKind::Response(Response {
                    id: my_id,
                    ..Default::default()
                }),
                MessageKind::AnnouncePeer(_) => {
                    counters.announce_queries.fetch_add(1, Ordering::SeqCst);
                    MessageKind::Response(Response {
                        id: my_id,
                        ..Default::default()
                    })
                }
                _ => continue,
            };
            counters.inflight.fetch_sub(1, Ordering::SeqCst);
            let mut out = Vec::new();
            bprotocol::serialize_message(
                &mut out,
                msg.transaction_id.clone(),
                msg.version.clone(),
                msg.ip,
                response,
            )
            .unwrap();
            let _ = sock.send_to(&out, from).await;
        }
    }

    fn node_id(j: u16) -> Id20 {
        let mut id = [0u8; 20];
        id[..2].copy_from_slice(&ID_PREFIX);
        id[2..4].copy_from_slice(&j.to_be_bytes());
        Id20::new(id)
    }

    async fn wait_for_routing_table(dht: &DhtState, min_nodes: usize) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if dht.stats().routing_table_size >= min_nodes {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("routing table never filled");
    }

    #[tokio::test]
    async fn get_peers_lookups_are_periodic_bounded_and_announce() {
        let counters = Arc::new(SwarmCounters::default());
        let swarm = spawn_swarm(counters.clone(), 4, Duration::from_millis(2), 0).await;
        let dht = DhtBuilder::with_config(DhtConfig {
            bootstrap_addrs: Some(vec![swarm.addrs[0].to_string()]),
            listen_addr: Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)),
            lookup_interval: Some(Duration::from_secs(1)),
            lookup_max_inflight: Some(4),
            lookup_max_requests: Some(32),
            ..Default::default()
        })
        .await
        .unwrap();
        wait_for_routing_table(&dht, 2).await;

        let info_hash = Id20::new(INFO_HASH);
        let mut peer_rx = dht.get_peers(info_hash, Some(12345));

        // Collect peers for ~2s, covering ~2 passes at the 1s interval.
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut peers = 0;
        while Instant::now() < deadline {
            if tokio::time::timeout(Duration::from_millis(50), peer_rx.next())
                .await
                .is_ok()
            {
                peers += 1;
            }
        }

        let queries = counters.get_peers_queries.load(Ordering::SeqCst);
        let announces = counters.announce_queries.load(Ordering::SeqCst);
        let max_inflight = counters.max_inflight.load(Ordering::SeqCst);

        assert!(peers >= 10, "expected peers across passes, got {peers}");
        assert!(
            announces > 0,
            "expected announce_peer to be sent for close nodes"
        );
        assert!(
            max_inflight <= 4,
            "swarm observed {max_inflight} concurrent requests, cap is 4"
        );
        // ~2 passes expected in 2s at the 1s interval; each pass queries at most 8
        // roots (responses only return already-known nodes), so allow generous
        // slack, but far below any runaway growth.
        assert!(
            queries <= 6 * 32,
            "expected bounded queries per pass, got {queries} in ~2s"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn get_peers_pass_terminates_despite_unresponsive_nodes() {
        let counters = Arc::new(SwarmCounters::default());
        let swarm = spawn_swarm(counters.clone(), 1, Duration::from_millis(2), 2).await;
        let dht = DhtBuilder::with_config(DhtConfig {
            bootstrap_addrs: Some(vec![swarm.addrs[0].to_string()]),
            listen_addr: Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)),
            lookup_interval: Some(Duration::from_secs(1)),
            lookup_max_inflight: Some(2),
            lookup_max_requests: Some(8),
            ..Default::default()
        })
        .await
        .unwrap();
        wait_for_routing_table(&dht, 1).await;

        let info_hash = Id20::new(INFO_HASH);
        let peer_rx = dht.get_peers(info_hash, None);
        // Virtual time auto-advances when idle, so request timeouts resolve
        // instantly; run 60 virtual seconds and drop the stream.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        while tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        drop(peer_rx);

        let queries = counters.get_peers_queries.load(Ordering::SeqCst);
        // Each pass dequeues at most `lookup_max_requests` (8) candidates, so
        // queries reaching the live node stay bounded; these assertions prove
        // the task keeps cycling passes without a runaway query rate.
        assert!(
            queries > 10,
            "expected the lookup task to keep running periodic passes"
        );
        assert!(
            queries <= 60 * 8,
            "expected bounded queries across ~60 passes, got {queries}"
        );
    }
}
