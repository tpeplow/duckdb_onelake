mod ffi;
mod ipc;
mod metrics;
mod parallel;

use std::collections::{HashSet, VecDeque};
use std::io::{self, Read};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Condvar,
};
use std::time::{Duration, Instant};

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_ipc::reader::StreamReader;
use arrow_schema::{ArrowError, SchemaRef};
use reqwest::blocking::Client;
use reqwest::header::{HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use reqwest::Url;
use serde::Deserialize;

use ipc::FramedReader;
use metrics::{elapsed, Metrics};

const MAX_ALLOCATION_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Clone, thiserror::Error)]
pub enum ReadError {
    #[error("Invalid table read request: {0}")]
    InvalidRequest(&'static str),
    #[error("OneLake {operation} returned HTTP {status}; no fallback or retry was attempted")]
    Http {
        operation: &'static str,
        status: u16,
    },
    #[error("OneLake transport failed")]
    Transport,
    #[error("Invalid OneLake read allocation")]
    Allocation,
    #[error("Invalid, unsupported, or incomplete Arrow stream")]
    Arrow,
    #[error("OneLake streams returned different schemas")]
    SchemaChanged,
    #[error("OneLake table read cancelled")]
    Cancelled,
    #[error("OneLake table read session expired")]
    Expired,
    #[error("Unexpected table reader failure")]
    Internal,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReadRequest {
    pub base_url: String,
    pub workspace_id: String,
    pub item_id: String,
    pub schema_name: String,
    pub table_name: String,
    pub columns: Option<Vec<String>>,
    #[serde(default = "serial_streams")]
    pub max_parallel_streams: usize,
}

fn serial_streams() -> usize {
    1
}

impl ReadRequest {
    fn table_url(&self) -> Result<Url, ReadError> {
        if self.max_parallel_streams == 0 {
            return Err(ReadError::InvalidRequest(
                "maxParallelStreams must be positive",
            ));
        }
        let mut url = Url::parse(&self.base_url)
            .map_err(|_| ReadError::InvalidRequest("invalid service origin"))?;
        let host = url.host_str().unwrap_or_default();
        if url.scheme() != "https"
            || !(host == "onelake.table.fabric.microsoft.com"
                || host.ends_with("-onelake.table.fabric.microsoft.com"))
            || !url.username().is_empty()
            || url.password().is_some()
            || url.port_or_known_default() != Some(443)
            || url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(ReadError::InvalidRequest(
                "expected a discovered HTTPS OneLake table service origin",
            ));
        }
        for id in [&self.workspace_id, &self.item_id] {
            uuid::Uuid::parse_str(id)
                .map_err(|_| ReadError::InvalidRequest("workspace and item must be GUIDs"))?;
        }
        for name in [&self.schema_name, &self.table_name] {
            if name.is_empty() || name == "." || name == ".." || name.chars().any(char::is_control)
            {
                return Err(ReadError::InvalidRequest("invalid schema or table name"));
            }
        }
        if self.columns.as_ref().is_some_and(|columns| {
            columns.is_empty() || columns.iter().any(|name| name.is_empty() || name == "*")
        }) {
            return Err(ReadError::InvalidRequest(
                "omit columns for the authorized schema, or supply nonempty explicit column names",
            ));
        }
        url.path_segments_mut()
            .map_err(|_| ReadError::InvalidRequest("invalid service origin"))?
            .clear()
            .extend([
                "v1.0",
                "workspaces",
                &self.workspace_id,
                "items",
                &self.item_id,
                "schemas",
                &self.schema_name,
                "tables",
                &self.table_name,
            ]);
        Ok(url)
    }
}

#[derive(Clone)]
pub struct Cancellation(Arc<AtomicBool>, Arc<Condvar>);

impl Cancellation {
    fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)), Arc::new(Condvar::new()))
    }

    pub fn cancel(&self) {
        self.0.store(true, Ordering::Relaxed);
        self.1.notify_all();
    }
}

#[derive(Clone)]
struct ReadState {
    cancellation: Cancellation,
    deadline: Instant,
}

impl ReadState {
    fn check(&self) -> Result<(), ReadError> {
        if self.cancellation.0.load(Ordering::Relaxed) {
            Err(ReadError::Cancelled)
        } else if Instant::now() >= self.deadline {
            Err(ReadError::Expired)
        } else {
            Ok(())
        }
    }
}

trait Transport: Send + Sync {
    fn allocate(
        &self,
        table_url: &Url,
        columns: Option<&[String]>,
    ) -> Result<VecDeque<String>, ReadError>;
    fn stream(&self, url: Url) -> Result<Box<dyn Read + Send>, ReadError>;
}

struct HttpTransport {
    client: Client,
    authorization: HeaderValue,
    metrics: Metrics,
}

fn child_url(base: &Url, components: &[&str]) -> Url {
    let mut url = base.clone();
    // All callers pass a validated hierarchical HTTPS URL.
    url.path_segments_mut().unwrap().extend(components);
    url
}

fn allocation(bytes: &[u8]) -> Result<VecDeque<String>, ReadError> {
    #[derive(Deserialize)]
    struct Allocation {
        streams: Vec<Stream>,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Stream {
        stream_id: String,
    }
    let response: Allocation = serde_json::from_slice(bytes).map_err(|_| ReadError::Allocation)?;
    let mut seen = HashSet::new();
    let mut streams = VecDeque::new();
    for stream in response.streams {
        if stream.stream_id.is_empty()
            || stream.stream_id == "."
            || stream.stream_id == ".."
            || stream.stream_id.chars().any(char::is_control)
            || !seen.insert(stream.stream_id.clone())
        {
            return Err(ReadError::Allocation);
        }
        streams.push_back(stream.stream_id);
    }
    // Without a stream there is no authoritative Arrow schema, even for an empty table.
    if streams.is_empty() {
        return Err(ReadError::Allocation);
    }
    Ok(streams)
}

impl HttpTransport {
    fn new(token: &str) -> Result<Self, ReadError> {
        if token.is_empty() {
            return Err(ReadError::InvalidRequest("access token is empty"));
        }
        let mut authorization = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_| ReadError::InvalidRequest("invalid access token header"))?;
        authorization.set_sensitive(true);
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .connect_timeout(Duration::from_secs(15))
            .timeout(Duration::from_secs(60))
            .build()
            .map_err(|_| ReadError::Transport)?;
        Ok(Self {
            client,
            authorization,
            metrics: Metrics::default(),
        })
    }
}

impl Transport for HttpTransport {
    fn allocate(
        &self,
        table_url: &Url,
        columns: Option<&[String]>,
    ) -> Result<VecDeque<String>, ReadError> {
        let body = match columns {
            Some(columns) => serde_json::json!({ "columns": columns }),
            None => serde_json::json!({}),
        };
        let response = self
            .client
            .post(child_url(table_url, &["read"]))
            .header(AUTHORIZATION, self.authorization.clone())
            .json(&body)
            .send()
            .map_err(|_| ReadError::Transport)?;
        if response.status().as_u16() != 200 {
            return Err(ReadError::Http {
                operation: "read allocation",
                status: response.status().as_u16(),
            });
        }
        let mut bytes = Vec::new();
        let result = response
            .take(MAX_ALLOCATION_BYTES + 1)
            .read_to_end(&mut bytes);
        self.metrics
            .update(|m| m.allocation_bytes += bytes.len() as u64);
        result.map_err(|_| ReadError::Transport)?;
        if bytes.len() as u64 > MAX_ALLOCATION_BYTES {
            return Err(ReadError::Allocation);
        }
        allocation(&bytes)
    }

    fn stream(&self, url: Url) -> Result<Box<dyn Read + Send>, ReadError> {
        let response = self
            .client
            .get(url)
            .header(AUTHORIZATION, self.authorization.clone())
            .header("Accept", "application/vnd.apache.arrow.stream")
            .send()
            .map_err(|_| ReadError::Transport)?;
        if response.status().as_u16() != 200 {
            return Err(ReadError::Http {
                operation: "read stream",
                status: response.status().as_u16(),
            });
        }
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        if !content_type
            .split(';')
            .next()
            .unwrap_or_default()
            .trim()
            .eq_ignore_ascii_case("application/vnd.apache.arrow.stream")
        {
            return Err(ReadError::Arrow);
        }
        Ok(Box::new(response))
    }
}

pub struct TableReader {
    transport: Arc<dyn Transport>,
    table_url: Url,
    streams: VecDeque<String>,
    current: Option<DecodedStream>,
    max_parallel_streams: usize,
    parallel: Option<parallel::ParallelReader>,
    #[cfg(test)]
    fail_spawn_at: Option<usize>,
    source_schema: SchemaRef,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    state: ReadState,
    finished: bool,
    metrics: Metrics,
    started: Option<Instant>,
}

struct MeasuredTransport {
    inner: Box<dyn Transport>,
    metrics: Metrics,
}

impl Transport for MeasuredTransport {
    fn allocate(
        &self,
        url: &Url,
        columns: Option<&[String]>,
    ) -> Result<VecDeque<String>, ReadError> {
        let start = self.metrics.start();
        self.metrics.update(|m| m.allocation_requests += 1);
        let result = self.inner.allocate(url, columns);
        let ns = elapsed(start);
        self.metrics.update(|m| {
            m.allocation_ns += ns;
            if let Ok(streams) = &result {
                m.allocated_streams += streams.len() as u64;
            }
        });
        result
    }

    fn stream(&self, url: Url) -> Result<Box<dyn Read + Send>, ReadError> {
        let start = self.metrics.start();
        let active = self.metrics.active_stream();
        self.metrics.update(|m| m.stream_requests += 1);
        let result = self.inner.stream(url);
        let ns = elapsed(start);
        self.metrics.update(|m| m.stream_open_ns += ns);
        result.map(|body| self.metrics.body(body, active))
    }
}

struct TimedInput {
    body: Box<dyn Read + Send>,
    read_ns: Arc<AtomicU64>,
    metrics: Metrics,
}

impl Read for TimedInput {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let start = self.metrics.start();
        let result = self.body.read(buffer);
        self.read_ns.fetch_add(elapsed(start), Ordering::Relaxed);
        result
    }
}

struct DecodedStream {
    reader: StreamReader<FramedReader>,
    read_ns: Arc<AtomicU64>,
}

impl DecodedStream {
    fn schema(&self) -> SchemaRef {
        self.reader.schema()
    }

    fn next_batch(
        &mut self,
        state: &ReadState,
        metrics: &Metrics,
        projection: &Option<Vec<usize>>,
    ) -> Result<Option<RecordBatch>, ReadError> {
        state.check()?;
        let before = self.read_ns.load(Ordering::Relaxed);
        let start = metrics.start();
        let result = (|| {
            let result = self.reader.next();
            state.check()?;
            result
                .map(|batch| {
                    let batch = batch.map_err(|_| ReadError::Arrow)?;
                    match projection {
                        Some(indices) => batch.project(indices).map_err(|_| ReadError::Arrow),
                        None => Ok(batch),
                    }
                })
                .transpose()
        })();
        let ns = elapsed(start)
            .saturating_sub(self.read_ns.load(Ordering::Relaxed).saturating_sub(before));
        metrics.update(|m| m.ipc_processing_ns += ns);
        result
    }
}

fn decode_stream(
    body: Box<dyn Read + Send>,
    state: ReadState,
    metrics: &Metrics,
) -> Result<DecodedStream, ReadError> {
    let read_ns = Arc::new(AtomicU64::new(0));
    let body = Box::new(TimedInput {
        body,
        read_ns: read_ns.clone(),
        metrics: metrics.clone(),
    });
    let start = metrics.start();
    let result = StreamReader::try_new_unbuffered(FramedReader::new(body, state.clone()), None)
        .map_err(|_| ReadError::Arrow);
    let ns = elapsed(start).saturating_sub(read_ns.load(Ordering::Relaxed));
    metrics.update(|m| m.ipc_processing_ns += ns);
    state.check()?;
    result.map(|reader| DecodedStream { reader, read_ns })
}

impl TableReader {
    /// `token` must represent the caller and use audience `https://storage.azure.com`.
    /// Token acquisition/refresh remains the responsibility of the extension's secret provider.
    pub fn open(request: ReadRequest, token: &str) -> Result<Self, ReadError> {
        Self::open_profiled(request, token, Metrics::default())
    }

    fn open_profiled(
        request: ReadRequest,
        token: &str,
        metrics: Metrics,
    ) -> Result<Self, ReadError> {
        let table_url = request.table_url()?;
        let mut transport = HttpTransport::new(token)?;
        transport.metrics = metrics.clone();
        let mut reader = Self::open_transport_profiled(
            table_url,
            request.columns,
            Box::new(transport),
            metrics,
        )?;
        reader.max_parallel_streams = request.max_parallel_streams.min(reader.streams.len() + 1);
        Ok(reader)
    }

    #[cfg(test)]
    fn open_transport(
        table_url: Url,
        columns: Option<Vec<String>>,
        transport: Box<dyn Transport>,
    ) -> Result<Self, ReadError> {
        Self::open_transport_profiled(table_url, columns, transport, Metrics::default())
    }

    fn open_transport_profiled(
        table_url: Url,
        columns: Option<Vec<String>>,
        transport: Box<dyn Transport>,
        metrics: Metrics,
    ) -> Result<Self, ReadError> {
        let started = metrics.start();
        let transport: Box<dyn Transport> = if started.is_some() {
            Box::new(MeasuredTransport {
                inner: transport,
                metrics: metrics.clone(),
            })
        } else {
            transport
        };
        let state = ReadState {
            cancellation: Cancellation::new(),
            deadline: Instant::now() + Duration::from_secs(60 * 60),
        };
        let mut streams = transport.allocate(&table_url, columns.as_deref())?;
        let first = streams.pop_front().ok_or(ReadError::Allocation)?;
        state.check()?;
        let body = transport.stream(child_url(&table_url, &["readStream", &first]))?;
        let current = decode_stream(body, state.clone(), &metrics)?;
        let source_schema = current.schema();
        let projection = match columns {
            Some(columns) => {
                if columns.len() != source_schema.fields().len() {
                    return Err(ReadError::Arrow);
                }
                let indices = columns
                    .iter()
                    .map(|column| source_schema.index_of(column).map_err(|_| ReadError::Arrow))
                    .collect::<Result<Vec<_>, _>>()?;
                if indices.iter().collect::<HashSet<_>>().len() != indices.len() {
                    return Err(ReadError::Arrow);
                }
                Some(indices)
            }
            None => None,
        };
        let schema = match &projection {
            Some(indices) => Arc::new(
                source_schema
                    .project(indices)
                    .map_err(|_| ReadError::Arrow)?,
            ),
            None => source_schema.clone(),
        };
        Ok(Self {
            transport: Arc::from(transport),
            table_url,
            streams,
            current: Some(current),
            max_parallel_streams: 1,
            parallel: None,
            #[cfg(test)]
            fail_spawn_at: None,
            source_schema,
            schema,
            projection,
            state,
            finished: false,
            metrics,
            started,
        })
    }

    pub fn cancellation(&self) -> Cancellation {
        self.state.cancellation.clone()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ReadError> {
        if self.max_parallel_streams > 1 {
            if self.parallel.is_none() {
                self.state.check()?;
                self.parallel = Some(parallel::ParallelReader::start(
                    self,
                    self.max_parallel_streams.min(self.streams.len() + 1),
                )?);
            }
            return self.parallel.as_ref().unwrap().next();
        }
        loop {
            self.state.check()?;
            if let Some(current) = &mut self.current {
                if let Some(batch) =
                    current.next_batch(&self.state, &self.metrics, &self.projection)?
                {
                    return Ok(Some(batch));
                }
            }
            self.state.check()?;
            self.current = None;
            let Some(stream) = self.streams.pop_front() else {
                return Ok(None);
            };
            let body = self
                .transport
                .stream(child_url(&self.table_url, &["readStream", &stream]))?;
            let reader = decode_stream(body, self.state.clone(), &self.metrics)?;
            if reader.schema() != self.source_schema {
                return Err(ReadError::SchemaChanged);
            }
            self.current = Some(reader);
        }
    }
}

impl Iterator for TableReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        // Arrow's C stream callbacks call this iterator; Rust panics must not cross that ABI.
        let result = catch_unwind(AssertUnwindSafe(|| self.next_batch()))
            .unwrap_or(Err(ReadError::Internal));
        match result {
            Ok(Some(batch)) => {
                self.metrics.update(|m| {
                    if m.batches == 0 {
                        m.first_batches = 1;
                        m.first_batch_ns = elapsed(self.started);
                    }
                    m.batches += 1;
                    m.rows += batch.num_rows() as u64;
                });
                Some(Ok(batch))
            }
            result => {
                self.finished = true;
                self.parallel = None;
                self.current = None;
                self.streams.clear();
                match result {
                    Err(error) => {
                        self.metrics.update(|m| m.errors += 1);
                        Some(Err(ArrowError::ExternalError(Box::new(error))))
                    }
                    _ => {
                        self.metrics.update(|m| m.completions += 1);
                        None
                    }
                }
            }
        }
    }
}

impl Drop for TableReader {
    fn drop(&mut self) {
        self.state.cancellation.cancel();
        self.parallel = None;
    }
}

impl RecordBatchReader for TableReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests;
