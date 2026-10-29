mod ffi;
mod ipc;

use std::collections::{HashSet, VecDeque};
use std::io::Read;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
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

const MAX_ALLOCATION_BYTES: u64 = 1024 * 1024;

#[derive(Debug, thiserror::Error)]
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
}

impl ReadRequest {
    fn table_url(&self) -> Result<Url, ReadError> {
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
pub struct Cancellation(Arc<AtomicBool>);

impl Cancellation {
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Relaxed);
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

trait Transport: Send {
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
        response
            .take(MAX_ALLOCATION_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| ReadError::Transport)?;
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
    transport: Box<dyn Transport>,
    table_url: Url,
    streams: VecDeque<String>,
    current: Option<StreamReader<FramedReader>>,
    source_schema: SchemaRef,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    state: ReadState,
    finished: bool,
}

impl TableReader {
    /// `token` must represent the caller and use audience `https://storage.azure.com`.
    /// Token acquisition/refresh remains the responsibility of the extension's secret provider.
    pub fn open(request: ReadRequest, token: &str) -> Result<Self, ReadError> {
        let table_url = request.table_url()?;
        Self::open_transport(
            table_url,
            request.columns,
            Box::new(HttpTransport::new(token)?),
        )
    }

    fn open_transport(
        table_url: Url,
        columns: Option<Vec<String>>,
        transport: Box<dyn Transport>,
    ) -> Result<Self, ReadError> {
        let state = ReadState {
            cancellation: Cancellation(Arc::new(AtomicBool::new(false))),
            deadline: Instant::now() + Duration::from_secs(60 * 60),
        };
        let mut streams = transport.allocate(&table_url, columns.as_deref())?;
        let first = streams.pop_front().ok_or(ReadError::Allocation)?;
        state.check()?;
        let body = transport.stream(child_url(&table_url, &["readStream", &first]))?;
        let current =
            StreamReader::try_new_unbuffered(FramedReader::new(body, state.clone()), None)
                .map_err(|_| ReadError::Arrow)?;
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
            transport,
            table_url,
            streams,
            current: Some(current),
            source_schema,
            schema,
            projection,
            state,
            finished: false,
        })
    }

    pub fn cancellation(&self) -> Cancellation {
        self.state.cancellation.clone()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ReadError> {
        loop {
            self.state.check()?;
            if let Some(current) = &mut self.current {
                if let Some(batch) = current.next() {
                    // Preserve cancellation/expiry instead of disguising them as decode failures.
                    self.state.check()?;
                    let batch = batch.map_err(|_| ReadError::Arrow)?;
                    // The service can return columns in table order, not request order.
                    return match &self.projection {
                        Some(indices) => batch
                            .project(indices)
                            .map(Some)
                            .map_err(|_| ReadError::Arrow),
                        None => Ok(Some(batch)),
                    };
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
            let reader =
                StreamReader::try_new_unbuffered(FramedReader::new(body, self.state.clone()), None)
                    .map_err(|_| ReadError::Arrow)?;
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
            Ok(Some(batch)) => Some(Ok(batch)),
            result => {
                self.finished = true;
                self.current = None;
                self.streams.clear();
                match result {
                    Err(error) => Some(Err(ArrowError::ExternalError(Box::new(error)))),
                    _ => None,
                }
            }
        }
    }
}

impl RecordBatchReader for TableReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests;
