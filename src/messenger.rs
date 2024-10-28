use std::{
    collections::HashMap,
    future::Future,
    io::Cursor,
    ops::DerefMut,
    sync::{
        atomic::{AtomicI32, Ordering},
        Arc,
    },
    task::Poll,
};

use futures::{future::LocalBoxFuture, FutureExt};
use parking_lot::Mutex;
use rsasl::{
    config::SASLConfig,
    mechname::MechanismNameError,
    prelude::{Mechname, SessionError},
};
use thiserror::Error;
use tracing::{debug, info, warn};

use crate::{
    backoff::ErrorOrThrottle,
    protocol::{
        api_key::ApiKey,
        api_version::ApiVersion,
        error::Error as ApiError,
        frame::{AsyncMessageRead, AsyncMessageWrite},
        messages::{
            ReadVersionedError, ReadVersionedType, RequestBody, RequestHeader, ResponseHeader,
            SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest,
            SaslHandshakeResponse, WriteVersionedError, WriteVersionedType,
        },
        primitives::{Int16, Int32, NullableString, TaggedFields},
    },
    throttle::maybe_throttle,
};
use crate::{
    client::SaslConfig,
    protocol::{api_version::ApiVersionRange, primitives::CompactString},
};
use crate::{
    connection::Credentials,
    protocol::{messages::ApiVersionsRequest, traits::ReadType},
};
use monoio::io::AsyncReadRent;
use monoio::io::AsyncWriteRent;
use monoio::io::OwnedWriteHalf;
use monoio::io::Split;
use monoio::io::Splitable;
use tokio::sync::oneshot::channel as tokio_oneshot;
use tokio::sync::oneshot::Sender as TokioOneshotSender;
use tokio::sync::Mutex as TokioMutex;

#[derive(Debug)]
struct Response {
    #[allow(dead_code)]
    header: ResponseHeader,
    data: Cursor<Vec<u8>>,
}

#[derive(Debug)]
struct ActiveRequest {
    channel: TokioOneshotSender<Result<Response, RequestError>>,
    use_tagged_fields_in_response: bool,
}

#[derive(Debug)]
enum MessengerState {
    /// Currently active requests by correlation ID.
    ///
    /// An active request is one that got prepared or send but the response wasn't received yet.
    RequestMap(HashMap<i32, ActiveRequest>),

    /// One or our streams died and we are unable to process any more requests.
    Poison(Arc<RequestError>),
}

impl MessengerState {
    /// Returns true if the state is poisoned.
    fn is_poisoned(&self) -> bool {
        matches!(self, Self::Poison(_))
    }

    /// Poisons the state with the given `err`.
    fn poison(&mut self, err: RequestError) -> Arc<RequestError> {
        match self {
            Self::RequestMap(map) => {
                let err = Arc::new(err);

                // inform all active requests
                for (_correlation_id, active_request) in map.drain() {
                    // it's OK if the other side is gone
                    active_request
                        .channel
                        .send(Err(RequestError::Poisoned(Arc::clone(&err))))
                        .ok();
                }

                *self = Self::Poison(Arc::clone(&err));
                err
            }
            Self::Poison(e) => {
                // already poisoned, used existing error
                Arc::clone(e)
            }
        }
    }
}

/// A connection to a single broker
///
/// Note: Requests to the same [`Messenger`] will be pipelined by Kafka
///
pub struct Messenger<RW: AsyncWriteRent> {
    /// The half of the stream that we use to send data TO the broker.
    ///
    /// This will be used by [`request`](Self::request) to queue up messages.
    stream_write: Arc<TokioMutex<OwnedWriteHalf<RW>>>,

    /// Client ID.
    client_id: Arc<str>,

    /// Every request will be identified by a correlation ID to enable that we
    /// can send multiple requests with one socket connection (Demultiplexing).
    ///
    /// This field is our correlation ID generator, it records the next correlation
    /// ID.
    correlation_id: AtomicI32,

    /// Version ranges that we think are supported by the broker.
    ///
    /// This needs to be bootstrapped by [`sync_versions`](Self::sync_versions).
    version_ranges: HashMap<ApiKey, ApiVersionRange>,

    /// Current stream state.
    ///
    /// Note that this and `stream_write` are separate struct to allow sending and receiving data concurrently.
    state: Arc<Mutex<MessengerState>>,

    /// Send the stop signal to the bg worker here when being dropped.
    ///
    /// Optional because we need to move it out and call `send(Self)` on it.
    bg_worker_stop_signal_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl<RW: AsyncWriteRent + std::fmt::Debug> std::fmt::Debug for Messenger<RW> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Messager")
            .field("stream_write", &self.stream_write)
            .field("client_id", &self.client_id)
            .field("correlation_id", &self.correlation_id)
            .field("version_ranges", &self.version_ranges)
            .field("state", &self.state)
            .field(
                "bg worker stopped",
                &self.bg_worker_stop_signal_tx.is_none(),
            )
            .finish()
    }
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum RequestError {
    #[error("Cannot find matching version for: {api_key:?}")]
    NoVersionMatch { api_key: ApiKey },

    #[error("Cannot write data: {0}")]
    WriteError(#[from] WriteVersionedError),

    #[error("Cannot write versioned data: {0}")]
    WriteMessageError(#[from] crate::protocol::frame::WriteError),

    #[error("Cannot read data: {0}")]
    ReadError(#[from] crate::protocol::traits::ReadError),

    #[error("Cannot read versioned data: {0}")]
    ReadVersionedError(#[from] ReadVersionedError),

    #[error("Cannot read/write data: {0}")]
    IO(#[from] std::io::Error),

    #[error(
        "Data left at the end of the message. Got {message_size} bytes but only read {read} bytes. api_key={api_key:?} api_version={api_version}"
    )]
    TooMuchData {
        message_size: u64,
        read: u64,
        api_key: ApiKey,
        api_version: ApiVersion,
    },

    #[error("Cannot read framed message: {0}")]
    ReadFramedMessageError(#[from] crate::protocol::frame::ReadError),

    #[error("Connection is poisoned: {0}")]
    Poisoned(Arc<RequestError>),
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum SyncVersionsError {
    #[error("Did not found a version for ApiVersion that works with that broker")]
    NoWorkingVersion,

    #[error("Request error: {0}")]
    RequestError(#[from] RequestError),

    #[error("Got flipped version from server for API key {api_key:?}: min={min:?} max={max:?}")]
    FlippedVersionRange {
        api_key: ApiKey,
        min: ApiVersion,
        max: ApiVersion,
    },
}

#[derive(Error, Debug)]
pub enum SaslError {
    #[error("Request error: {0}")]
    RequestError(#[from] RequestError),

    #[error("API error: {0}")]
    ApiError(#[from] ApiError),

    #[error("Invalid sasl mechanism: {0}")]
    InvalidSaslMechanism(#[from] MechanismNameError),

    #[error("Sasl session error: {0}")]
    SaslSessionError(#[from] SessionError),

    #[error("unsupported sasl mechanism")]
    UnsupportedSaslMechanism,
}

impl<RW> Messenger<RW>
where
    RW: AsyncReadRent + AsyncWriteRent + Split + 'static,
{
    pub fn new(stream: RW, max_message_size: usize, client_id: Arc<str>) -> Self {
        let (stream_read, stream_write) = Splitable::into_split(stream);
        let state = Arc::new(Mutex::new(MessengerState::RequestMap(HashMap::default())));
        let state_captured = Arc::clone(&state);
        let (bg_worker_stop_signal_tx, bg_worker_stop_signal_rx) =
            tokio::sync::oneshot::channel::<()>();

        monoio::spawn(async move {
            let mut stream_read = stream_read;
            let bg_worker_stop_signal_rx = bg_worker_stop_signal_rx.shared();

            loop {
                tokio::select! {
                    res_msg = stream_read.read_message(max_message_size) => {
                        match res_msg {
                            Ok(msg) => {
                                // message was read, so all subsequent errors should not poison the whole stream
                                let mut cursor = Cursor::new(msg);

                                // read header as version 0 (w/o tagged fields) first since this is a strict prefix or the more advanced
                                // header version
                                let mut header =
                                    match ResponseHeader::read_versioned(&mut cursor, ApiVersion(Int16(0)))
                                    {
                                        Ok(header) => header,
                                        Err(e) => {
                                            warn!(%e, "Cannot read message header, ignoring message");
                                            continue;
                                        }
                                    };

                                let active_request = match state_captured.lock().deref_mut() {
                                    MessengerState::RequestMap(map) => {
                                        if let Some(active_request) = map.remove(&header.correlation_id.0) {
                                            active_request
                                        } else {
                                            warn!(
                                                correlation_id = header.correlation_id.0,
                                                "Got response for unknown request",
                                            );
                                            continue;
                                        }
                                    }
                                    MessengerState::Poison(_) => {
                                        // stream is poisoned, no need to anything
                                        return;
                                    }
                                };

                                // optionally read tagged fields from the header as well
                                if active_request.use_tagged_fields_in_response {
                                    header.tagged_fields = match TaggedFields::read(&mut cursor) {
                                        Ok(fields) => Some(fields),
                                        Err(e) => {
                                            // we don't care if the other side is gone
                                            active_request
                                                .channel
                                                .send(Err(RequestError::ReadError(e)))
                                                .ok();
                                            continue;
                                        }
                                    };
                                }

                                // we don't care if the other side is gone
                                active_request
                                    .channel
                                    .send(Ok(Response {
                                        header,
                                        data: cursor,
                                    }))
                                    .ok();
                            }
                            Err(e) => {
                                state_captured
                                    .lock()
                                    .poison(RequestError::ReadFramedMessageError(e));
                                return;
                            }
                        }

                    }
                    _ = bg_worker_stop_signal_rx.clone() => {
                        return;
                    }
                }
            }
        });

        Self {
            stream_write: Arc::new(TokioMutex::new(stream_write)),
            client_id,
            correlation_id: AtomicI32::new(0),
            version_ranges: HashMap::new(),
            state,
            bg_worker_stop_signal_tx: Some(bg_worker_stop_signal_tx),
        }
    }

    #[cfg(feature = "unstable-fuzzing")]
    pub fn override_version_ranges(&mut self, ranges: HashMap<ApiKey, ApiVersionRange>) {
        self.set_version_ranges(ranges);
    }

    /// Set supported version range.
    fn set_version_ranges(&mut self, ranges: HashMap<ApiKey, ApiVersionRange>) {
        self.version_ranges = ranges;
    }

    pub async fn request<R>(&self, msg: R) -> Result<R::ResponseBody, RequestError>
    where
        R: RequestBody + Send + WriteVersionedType<Vec<u8>>,
        R::ResponseBody: ReadVersionedType<Cursor<Vec<u8>>>,
    {
        self.request_with_version_ranges(msg, &self.version_ranges)
            .await
    }

    async fn request_with_version_ranges<R>(
        &self,
        msg: R,
        version_ranges: &HashMap<ApiKey, ApiVersionRange>,
    ) -> Result<R::ResponseBody, RequestError>
    where
        R: RequestBody + Send + WriteVersionedType<Vec<u8>>,
        R::ResponseBody: ReadVersionedType<Cursor<Vec<u8>>>,
    {
        let body_api_version = version_ranges
            .get(&R::API_KEY)
            .and_then(|range_server| match_versions(*range_server, R::API_VERSION_RANGE))
            .ok_or(RequestError::NoVersionMatch {
                api_key: R::API_KEY,
            })?;

        // determine if our request and response headers shall contain tagged fields. This system is borrowed from
        // rdkafka ("flexver"), see:
        // - https://github.com/edenhill/librdkafka/blob/2b76b65212e5efda213961d5f84e565038036270/src/rdkafka_request.c#L973
        // - https://github.com/edenhill/librdkafka/blob/2b76b65212e5efda213961d5f84e565038036270/src/rdkafka_buf.c#L167-L174
        let use_tagged_fields_in_request =
            body_api_version >= R::FIRST_TAGGED_FIELD_IN_REQUEST_VERSION;
        let use_tagged_fields_in_response =
            body_api_version >= R::FIRST_TAGGED_FIELD_IN_RESPONSE_VERSION;

        // Correlation ID so that we can de-multiplex the responses.
        let correlation_id = self.correlation_id.fetch_add(1, Ordering::SeqCst);

        let header = RequestHeader {
            request_api_key: R::API_KEY,
            request_api_version: body_api_version,
            correlation_id: Int32(correlation_id),
            // Technically we don't need to send a client_id, but newer redpanda version fail to parse the message
            // without it. See https://github.com/influxdata/rskafka/issues/169 .
            client_id: Some(NullableString(Some(String::from(self.client_id.as_ref())))),
            tagged_fields: Some(TaggedFields::default()),
        };
        let header_version = if use_tagged_fields_in_request {
            ApiVersion(Int16(2))
        } else {
            ApiVersion(Int16(1))
        };

        let mut buf = Vec::new();
        header
            .write_versioned(&mut buf, header_version)
            .expect("Writing header to buffer should always work");
        msg.write_versioned(&mut buf, body_api_version)?;

        let (tx, rx) = tokio_oneshot();

        // to prevent stale data in inner state, ensure that we would remove the request again if we are cancelled while
        // sending the request
        let cleanup_on_cancel =
            CleanupRequestStateOnCancel::new(Arc::clone(&self.state), correlation_id);

        match self.state.lock().deref_mut() {
            MessengerState::RequestMap(map) => {
                map.insert(
                    correlation_id,
                    ActiveRequest {
                        channel: tx,
                        use_tagged_fields_in_response,
                    },
                );
            }
            MessengerState::Poison(e) => {
                return Err(RequestError::Poisoned(Arc::clone(e)));
            }
        }

        self.send_message(buf).await?;
        cleanup_on_cancel.message_sent();

        let mut response = rx.await.expect("Who closed this channel?!")?;
        let body = R::ResponseBody::read_versioned(&mut response.data, body_api_version)?;

        // check if we fully consumed the message, otherwise there might be a bug in our protocol code
        let read_bytes = response.data.position();
        let message_bytes = response.data.into_inner().len() as u64;
        if read_bytes != message_bytes {
            return Err(RequestError::TooMuchData {
                message_size: message_bytes,
                read: read_bytes,
                api_key: R::API_KEY,
                api_version: body_api_version,
            });
        }

        Ok(body)
    }

    async fn send_message(&self, msg: Vec<u8>) -> Result<(), RequestError> {
        match self.send_message_inner(msg).await {
            Ok(()) => Ok(()),
            Err(e) => {
                // need to poison the stream because message framing might be out-of-sync
                let mut state = self.state.lock();
                Err(RequestError::Poisoned(state.poison(e)))
            }
        }
    }

    async fn send_message_inner(&self, msg: Vec<u8>) -> Result<(), RequestError> {
        let mut stream_write = Arc::clone(&self.stream_write).lock_owned().await;

        // use a wrapper so that cancelation doesn't cancel the send operation and leaves half-send messages on the wire
        let fut = CancellationSafeFuture::new(async move {
            stream_write.write_message(msg).await?;
            stream_write.flush().await?;
            Ok(())
        });

        fut.await
    }

    /// Sync supported version range.
    ///
    /// Takes `&self mut` to ensure exclusive access.
    pub async fn sync_versions(&mut self) -> Result<(), SyncVersionsError> {
        'iter_upper_bound: for upper_bound in (ApiVersionsRequest::API_VERSION_RANGE.min().0 .0
            ..=ApiVersionsRequest::API_VERSION_RANGE.max().0 .0)
            .rev()
        {
            let version_ranges = HashMap::from([(
                ApiKey::ApiVersions,
                ApiVersionRange::new(
                    ApiVersionsRequest::API_VERSION_RANGE.min(),
                    ApiVersion(Int16(upper_bound)),
                ),
            )]);

            let body = ApiVersionsRequest {
                client_software_name: Some(CompactString(String::from(env!("CARGO_PKG_NAME")))),
                client_software_version: Some(CompactString(String::from(env!(
                    "CARGO_PKG_VERSION"
                )))),
                tagged_fields: Some(TaggedFields::default()),
            };

            'throttle: loop {
                match self
                    .request_with_version_ranges(&body, &version_ranges)
                    .await
                {
                    Ok(response) => {
                        if let Err(ErrorOrThrottle::Throttle(throttle)) =
                            maybe_throttle::<SyncVersionsError>(response.throttle_time_ms)
                        {
                            info!(
                                ?throttle,
                                request_name = "version sync",
                                "broker asked us to throttle"
                            );
                            monoio::time::sleep(throttle).await;
                            continue 'throttle;
                        }

                        if let Some(e) = response.error_code {
                            debug!(
                                %e,
                                version=upper_bound,
                                "Got error during version sync, cannot use version for ApiVersionRequest",
                            );
                            continue 'iter_upper_bound;
                        }

                        // check range sanity
                        for api_key in &response.api_keys {
                            if api_key.min_version.0 > api_key.max_version.0 {
                                return Err(SyncVersionsError::FlippedVersionRange {
                                    api_key: api_key.api_key,
                                    min: api_key.min_version,
                                    max: api_key.max_version,
                                });
                            }
                        }

                        let ranges = response
                            .api_keys
                            .into_iter()
                            .map(|x| {
                                (
                                    x.api_key,
                                    ApiVersionRange::new(x.min_version, x.max_version),
                                )
                            })
                            .collect();
                        debug!(
                            versions=%sorted_ranges_repr(&ranges),
                            "Detected supported broker versions",
                        );
                        self.set_version_ranges(ranges);
                        return Ok(());
                    }
                    Err(RequestError::NoVersionMatch { .. }) => {
                        unreachable!("Just set to version range to a non-empty range")
                    }
                    Err(RequestError::ReadVersionedError(e)) => {
                        debug!(
                            %e,
                            version=upper_bound,
                            "Cannot read ApiVersionResponse for version",
                        );
                        continue 'iter_upper_bound;
                    }
                    Err(RequestError::ReadError(e)) => {
                        debug!(
                            %e,
                            version=upper_bound,
                            "Cannot read ApiVersionResponse for version",
                        );
                        continue 'iter_upper_bound;
                    }
                    Err(e @ RequestError::TooMuchData { .. }) => {
                        debug!(
                            %e,
                            version=upper_bound,
                            "Cannot read ApiVersionResponse for version",
                        );
                        continue 'iter_upper_bound;
                    }
                    Err(e) => {
                        return Err(SyncVersionsError::RequestError(e));
                    }
                }
            }
        }

        Err(SyncVersionsError::NoWorkingVersion)
    }

    async fn sasl_authentication(
        &self,
        auth_bytes: Vec<u8>,
    ) -> Result<SaslAuthenticateResponse, SaslError> {
        let req = SaslAuthenticateRequest::new(auth_bytes);
        let resp = self.request(req).await?;
        if let Some(err) = resp.error_code {
            if let Some(s) = resp.error_message.0 {
                debug!("Sasl auth error message: {s}");
            }
            return Err(SaslError::ApiError(err));
        }

        Ok(resp)
    }

    async fn sasl_handshake(&self, mechanism: &str) -> Result<SaslHandshakeResponse, SaslError> {
        let req = SaslHandshakeRequest::new(mechanism);
        let resp = self.request(req).await?;
        if let Some(err) = resp.error_code {
            return Err(SaslError::ApiError(err));
        }
        Ok(resp)
    }

    pub async fn do_sasl(&self, config: SaslConfig) -> Result<(), SaslError> {
        let mechanism = config.mechanism();
        let resp = self.sasl_handshake(mechanism).await?;

        let Credentials { username, password } = config.credentials();
        let config = SASLConfig::with_credentials(None, username, password).unwrap();
        let sasl = rsasl::prelude::SASLClient::new(config);
        let raw_mechanisms = resp.mechanisms.0.unwrap_or_default();
        let mechanisms = raw_mechanisms
            .iter()
            .map(|mech| Mechname::parse(mech.0.as_bytes()).map_err(SaslError::InvalidSaslMechanism))
            .collect::<Result<Vec<_>, SaslError>>()?;
        debug!(?mechanisms, "Supported SASL mechanisms");
        let prefer_mechanism =
            Mechname::parse(mechanism.as_bytes()).map_err(SaslError::InvalidSaslMechanism)?;
        if !mechanisms.contains(&prefer_mechanism) {
            return Err(SaslError::UnsupportedSaslMechanism);
        }
        let mut session = sasl
            .start_suggested(&[prefer_mechanism])
            .map_err(|_| SaslError::UnsupportedSaslMechanism)?;
        debug!(?mechanism, "Using SASL Mechanism");
        // we step through the auth process, starting on our side with NO data received so far
        let mut data_received: Option<Vec<u8>> = None;
        loop {
            let mut to_sent = Cursor::new(Vec::new());
            let state = session.step(data_received.as_deref(), &mut to_sent)?;
            if !state.is_running() {
                break;
            }

            let authentication_response = self.sasl_authentication(to_sent.into_inner()).await?;
            data_received = Some(authentication_response.auth_bytes.0);
        }

        Ok(())
    }
}

impl<RW: AsyncWriteRent> Drop for Messenger<RW> {
    fn drop(&mut self) {
        // If the `state` is poisoned, then the bg worker is stopped, we don't
        // need to send the stop signal
        if self.state.lock().is_poisoned() {
            return;
        }

        self.bg_worker_stop_signal_tx
            .take()
            .expect("stop signal tx should be Some")
            .send(())
            .expect("failed to send");
    }
}

/// Helper function to construct a string that contains sorted `ranges` in the
/// following format:
///
/// "{ApiKey}: {ApiVersionRange}, {ApiKey}: {ApiVersionRange}, ..."
fn sorted_ranges_repr(ranges: &HashMap<ApiKey, ApiVersionRange>) -> String {
    let mut ranges = ranges
        .iter()
        .map(|(key, range)| (*key, *range))
        .collect::<Vec<_>>();
    ranges.sort_by_key(|(key, _range)| *key);
    let ranges: Vec<_> = ranges
        .into_iter()
        .map(|(key, range)| format!("{:?}: {}", key, range))
        .collect();
    ranges.join(", ")
}

/// If `range_a` and `range_b` overlaps, return the max version in the overlapping
/// range. Otherwise, return `None`.
fn match_versions(range_a: ApiVersionRange, range_b: ApiVersionRange) -> Option<ApiVersion> {
    if range_a.min() <= range_b.max() && range_b.min() <= range_a.max() {
        Some(range_a.max().min(range_b.max()))
    } else {
        None
    }
}

/// Helper that ensures that a request's correlation ID will be removed from
/// `Messenger`'s state if it the sending process does not complete after
/// we storing its `(correlation_ID, response_rx)` in the state.
struct CleanupRequestStateOnCancel {
    state: Arc<Mutex<MessengerState>>,
    correlation_id: i32,
    message_sent: bool,
}

impl CleanupRequestStateOnCancel {
    /// Create new helper.
    ///
    /// You must call [`message_sent`](Self::message_sent) when the request was sent.
    fn new(state: Arc<Mutex<MessengerState>>, correlation_id: i32) -> Self {
        Self {
            state,
            correlation_id,
            message_sent: false,
        }
    }

    /// Marks the request sent. Do NOT clean the state any longer.
    fn message_sent(mut self) {
        self.message_sent = true;
    }
}

impl Drop for CleanupRequestStateOnCancel {
    fn drop(&mut self) {
        // The request gets cancelled, remove the stored `(correlation_ID, response_rx)` from state.
        if !self.message_sent {
            if let MessengerState::RequestMap(map) = self.state.lock().deref_mut() {
                map.remove(&self.correlation_id);
            }
        }
    }
}

/// Wrapper around a future that cannot be cancelled.
///
/// When the future is dropped/cancelled, we'll spawn a tokio monoio to _rescue_ it.
struct CancellationSafeFuture<F>
where
    F: Future + 'static,
{
    /// Mark if the inner future finished. If not, we must spawn a helper task on drop.
    done: bool,

    /// Inner future.
    ///
    /// Wrapped in an `Option` so we can extract it during drop. Inside that option however we also need a pinned
    /// box because once this wrapper is polled, it will be pinned in memory -- even during drop. Now the inner
    /// future does not necessarily implement `Unpin`, so we need a heap allocation to pin it in memory even when we
    /// move it out of this option.
    inner: Option<LocalBoxFuture<'static, F::Output>>,
}

impl<F> Drop for CancellationSafeFuture<F>
where
    F: Future + 'static,
{
    fn drop(&mut self) {
        if !self.done {
            let inner = self.inner.take().expect("Double-drop?");
            monoio::spawn(async move {
                inner.await;
            });
        }
    }
}

impl<F> CancellationSafeFuture<F>
where
    F: Future,
{
    fn new(fut: F) -> Self {
        Self {
            done: false,
            inner: Some(Box::pin(fut)),
        }
    }
}

impl<F> Future for CancellationSafeFuture<F>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        match self.inner.as_mut().expect("no dropped").as_mut().poll(cx) {
            Poll::Ready(res) => {
                self.done = true;
                Poll::Ready(res)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_match_versions() {
        assert_eq!(
            match_versions(
                ApiVersionRange::new(ApiVersion(Int16(10)), ApiVersion(Int16(20))),
                ApiVersionRange::new(ApiVersion(Int16(10)), ApiVersion(Int16(20))),
            ),
            Some(ApiVersion(Int16(20))),
        );

        assert_eq!(
            match_versions(
                ApiVersionRange::new(ApiVersion(Int16(10)), ApiVersion(Int16(15))),
                ApiVersionRange::new(ApiVersion(Int16(13)), ApiVersion(Int16(20))),
            ),
            Some(ApiVersion(Int16(15))),
        );

        assert_eq!(
            match_versions(
                ApiVersionRange::new(ApiVersion(Int16(10)), ApiVersion(Int16(15))),
                ApiVersionRange::new(ApiVersion(Int16(15)), ApiVersion(Int16(20))),
            ),
            Some(ApiVersion(Int16(15))),
        );

        assert_eq!(
            match_versions(
                ApiVersionRange::new(ApiVersion(Int16(10)), ApiVersion(Int16(14))),
                ApiVersionRange::new(ApiVersion(Int16(15)), ApiVersion(Int16(20))),
            ),
            None,
        );
    }
}
