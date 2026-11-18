use std::ffi::{c_char, CString};
use std::io::{self, Cursor, Write};
use std::mem::MaybeUninit;
use std::net::TcpListener;
use std::sync::Mutex;
use std::thread;

use arrow_array::ffi_stream::ArrowArrayStreamReader;
use arrow_array::types::Int32Type;
use arrow_array::{ArrayRef, DictionaryArray, Float64Array};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};

use super::*;

#[path = "parallel_tests.rs"]
mod parallel_tests;

fn request() -> ReadRequest {
    ReadRequest {
        base_url: "https://onelake.table.fabric.microsoft.com".into(),
        workspace_id: "00000000-0000-0000-0000-000000000001".into(),
        item_id: "00000000-0000-0000-0000-000000000002".into(),
        schema_name: "dbo".into(),
        table_name: "PricePaid".into(),
        columns: Some(vec!["Price".into()]),
        max_parallel_streams: 1,
    }
}

#[test]
#[ignore = "generates local parallel benchmark fixtures"]
fn benchmark_fixture() {
    use arrow_array::StringArray;
    let directory = std::path::PathBuf::from(std::env::var("ONELAKE_BENCH_FIXTURE_DIR").unwrap());
    std::fs::create_dir_all(&directory).unwrap();
    let mut streams = Vec::new();
    for stream in 0..8 {
        let mut writer: Option<StreamWriter<Vec<u8>>> = None;
        let mut ends = Vec::new();
        for batch in 0..32 {
            let first = (stream * 32 + batch) * 4096;
            let price = Float64Array::from(
                (first..first + 4096)
                    .map(|id| (id % 7 != 0).then_some(id as f64))
                    .collect::<Vec<_>>(),
            );
            let labels = StringArray::from(vec![Some("x".repeat(64)); 4096]);
            let dictionary: DictionaryArray<Int32Type> = (0..4096)
                .map(|i| {
                    if i % 9 == 0 {
                        None
                    } else if i % 2 == 0 {
                        Some("even")
                    } else {
                        Some("odd")
                    }
                })
                .collect();
            let batch = RecordBatch::try_from_iter(vec![
                ("Price", Arc::new(price) as ArrayRef),
                ("Label", Arc::new(labels) as ArrayRef),
                ("Category", Arc::new(dictionary) as ArrayRef),
            ])
            .unwrap();
            let writer = writer
                .get_or_insert_with(|| StreamWriter::try_new(Vec::new(), &batch.schema()).unwrap());
            writer.write(&batch).unwrap();
            ends.push(writer.get_ref().len());
        }
        let mut writer = writer.unwrap();
        writer.finish().unwrap();
        ends.push(writer.get_ref().len());
        let file = format!("stream{stream}.arrow");
        std::fs::write(directory.join(&file), writer.into_inner().unwrap()).unwrap();
        streams
            .push(serde_json::json!({"id": stream.to_string(), "file": file, "batch_ends": ends}));
    }
    std::fs::write(
        directory.join("manifest.json"),
        serde_json::to_vec(&serde_json::json!({"streams": streams})).unwrap(),
    )
    .unwrap();
}

#[test]
#[ignore = "reader-only client for the opt-in loopback benchmark"]
fn benchmark_client() {
    let url = Url::parse(&std::env::var("ONELAKE_BENCH_URL").unwrap()).unwrap();
    assert_eq!(url.host_str(), Some("127.0.0.1"));
    let cap: usize = std::env::var("ONELAKE_BENCH_CAP").unwrap().parse().unwrap();
    assert!(cap > 0);
    let scenario = std::env::var("ONELAKE_BENCH_SCENARIO").unwrap();
    let metrics = Metrics::enabled();
    let mut transport = HttpTransport::new("loopback-test-only").unwrap();
    transport.metrics = metrics.clone();
    let start = Instant::now();
    let mut reader =
        TableReader::open_transport_profiled(url, None, Box::new(transport), metrics.clone())
            .unwrap();
    reader.max_parallel_streams = cap.min(reader.streams.len() + 1);
    let mut rows = 0u64;
    let mut checksum = 0u64;
    for batch in reader.by_ref() {
        let batch = batch.unwrap();
        checksum += batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .flatten()
            .map(|value| value as u64)
            .sum::<u64>();
        rows += batch.num_rows() as u64;
        if scenario == "early" {
            break;
        }
        if scenario == "stalled" && rows == batch.num_rows() as u64 {
            thread::sleep(Duration::from_millis(100));
        }
    }
    let shutdown = Instant::now();
    drop(reader);
    let shutdown_seconds = shutdown.elapsed().as_secs_f64();
    let seconds = start.elapsed().as_secs_f64();
    let counters = metrics.snapshot();
    if scenario != "early" {
        let expected = counters.allocated_streams * 32 * 4096;
        assert_eq!(rows, expected);
        let multiples = (expected - 1) / 7;
        assert_eq!(
            checksum,
            expected * (expected - 1) / 2 - 7 * multiples * (multiples + 1) / 2
        );
        assert_eq!(counters.completions, 1);
    } else {
        assert_eq!(rows, 4096);
        assert_eq!(counters.completions, 0);
    }
    assert_eq!(counters.allocation_requests, 1);
    assert_eq!(counters.errors, 0);
    assert!(counters.peak_active_streams <= cap as u64);
    println!(
        "BENCH_RESULT {}",
        serde_json::json!({
            "seconds": seconds, "rows": rows, "checksum": checksum,
            "metrics": counters, "shutdown_seconds": shutdown_seconds,
        })
    );
    if std::env::var("ONELAKE_BENCH_WAIT_FOR_EXIT").as_deref() == Ok("1") {
        io::stdout().flush().unwrap();
        io::stdin().read_line(&mut String::new()).unwrap();
    }
}

fn encoded(name: &str, values: &[Option<f64>]) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Float64, true)]));
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut bytes, &schema).unwrap();
        if !values.is_empty() {
            let array = Arc::new(Float64Array::from(values.to_vec())) as ArrayRef;
            writer
                .write(&RecordBatch::try_new(schema, vec![array]).unwrap())
                .unwrap();
        }
        writer.finish().unwrap();
    }
    bytes
}

struct MockTransport {
    bodies: Mutex<Vec<Option<Box<dyn Read + Send>>>>,
    calls: Arc<Mutex<Vec<String>>>,
}

impl Transport for MockTransport {
    fn allocate(&self, _: &Url, columns: Option<&[String]>) -> Result<VecDeque<String>, ReadError> {
        assert!(columns.is_some());
        self.calls.lock().unwrap().push("allocate".into());
        Ok((0..self.bodies.lock().unwrap().len())
            .map(|n| format!("stream{n}"))
            .collect())
    }

    fn stream(&self, url: Url) -> Result<Box<dyn Read + Send>, ReadError> {
        self.calls.lock().unwrap().push(url.path().into());
        let index = url
            .path_segments()
            .unwrap()
            .next_back()
            .unwrap()
            .strip_prefix("stream")
            .unwrap()
            .parse::<usize>()
            .unwrap();
        self.bodies.lock().unwrap()[index]
            .take()
            .ok_or(ReadError::Transport)
    }
}

fn reader_with_bodies(bodies: Vec<Box<dyn Read + Send>>) -> (TableReader, Arc<Mutex<Vec<String>>>) {
    reader_with_columns(bodies, vec!["Price".into()])
}

fn reader_with_columns(
    bodies: Vec<Box<dyn Read + Send>>,
    columns: Vec<String>,
) -> (TableReader, Arc<Mutex<Vec<String>>>) {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let transport = MockTransport {
        bodies: Mutex::new(bodies.into_iter().map(Some).collect()),
        calls: calls.clone(),
    };
    let request = request();
    let reader = TableReader::open_transport(
        request.table_url().unwrap(),
        Some(columns),
        Box::new(transport),
    )
    .unwrap();
    (reader, calls)
}

#[test]
fn reorders_service_columns_to_match_requested_projection() {
    let first = Arc::new(Float64Array::from(vec![10.0, 20.0])) as ArrayRef;
    let second = Arc::new(Float64Array::from(vec![100.0, 200.0])) as ArrayRef;
    let batch = RecordBatch::try_from_iter(vec![("Price", first), ("Other", second)]).unwrap();
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut bytes, &batch.schema()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    let (mut reader, _) = reader_with_columns(
        vec![Box::new(Cursor::new(bytes))],
        vec!["Other".into(), "Price".into()],
    );
    assert_eq!(reader.schema().field(0).name(), "Other");
    assert_eq!(
        reader.next().unwrap().unwrap(),
        batch.project(&[1, 0]).unwrap()
    );
    assert!(reader.next().is_none());
}

fn reader(streams: Vec<Vec<u8>>) -> (TableReader, Arc<Mutex<Vec<String>>>) {
    reader_with_bodies(
        streams
            .into_iter()
            .map(|bytes| Box::new(Cursor::new(bytes)) as Box<dyn Read + Send>)
            .collect(),
    )
}

fn measured_reader(streams: Vec<Vec<u8>>, metrics: Metrics) -> TableReader {
    let transport = MockTransport {
        bodies: Mutex::new(
            streams
                .into_iter()
                .map(|bytes| Some(Box::new(Cursor::new(bytes)) as Box<dyn Read + Send>))
                .collect(),
        ),
        calls: Arc::new(Mutex::new(Vec::new())),
    };
    TableReader::open_transport_profiled(
        request().table_url().unwrap(),
        Some(vec!["Price".into()]),
        Box::new(transport),
        metrics,
    )
    .unwrap()
}

#[test]
fn metrics_count_consumed_bytes_and_survive_ffi_export() {
    let bodies = vec![
        encoded("Price", &[Some(10.0), None]),
        encoded("Price", &[]),
        encoded("Price", &[Some(20.0)]),
    ];
    let bytes = bodies.iter().map(|b| b.len() as u64).sum::<u64>();
    let metrics = Metrics::enabled();
    let reader = measured_reader(bodies, metrics.clone());
    let initial = metrics.snapshot();
    assert_eq!(initial.allocation_requests, 1);
    assert_eq!(initial.stream_requests, 1);
    assert_eq!(initial.rows, 0);
    assert!(initial.stream_bytes > 0 && initial.stream_bytes < bytes);
    let handle = ffi::test_handle(reader);
    let mut stream = MaybeUninit::uninit();
    unsafe {
        assert_eq!(
            ffi::ol_table_reader_export(handle, stream.as_mut_ptr(), std::ptr::null_mut(), 0),
            0
        );
        let mut stream = stream.assume_init();
        let mut imported = ArrowArrayStreamReader::from_raw(&mut stream).unwrap();
        let rows: usize = imported.by_ref().map(|b| b.unwrap().num_rows()).sum();
        assert_eq!(rows, 3);
        let mut snapshot = metrics::OlReadMetrics::default();
        assert_eq!(ffi::ol_table_reader_metrics(handle, &mut snapshot), 0);
        assert_eq!(snapshot.stream_bytes, bytes);
        assert_eq!(snapshot.stream_requests, 3);
        assert_eq!(snapshot.batches, 2);
        assert_eq!(snapshot.rows, 3);
        assert_eq!(snapshot.first_batches, 1);
        assert_eq!(snapshot.completions, 1);
        assert_eq!(snapshot.errors, 0);
        drop(imported);
        ffi::ol_table_reader_close(handle);
    }
}

#[test]
fn metrics_preserve_partial_cancelled_and_disabled_reads() {
    let body = encoded("Price", &[Some(10.0)]);
    let metrics = Metrics::enabled();
    let mut reader = measured_reader(vec![body.clone(), body.clone()], metrics.clone());
    reader.next().unwrap().unwrap();
    assert_eq!(metrics.snapshot().stream_requests, 1);
    assert_eq!(metrics.snapshot().completions, 0);
    reader.cancellation().cancel();
    assert!(reader.next().unwrap().is_err());
    assert!(reader.next().is_none());
    assert_eq!(metrics.snapshot().errors, 1);
    assert_eq!(metrics.snapshot().stream_requests, 1);
    assert_eq!(metrics.snapshot().completions, 0);
    let off = Metrics::default();
    assert_eq!(
        measured_reader(vec![body], off.clone())
            .next()
            .unwrap()
            .unwrap()
            .num_rows(),
        1
    );
    assert_eq!(off.snapshot().stream_bytes, 0);
    assert_eq!(off.snapshot().allocation_requests, 0);
    assert!(off.start().is_none());
    let mut truncated = encoded("Price", &[Some(10.0)]);
    truncated.truncate(truncated.len() - 12);
    let broken = Metrics::enabled();
    let mut reader = measured_reader(
        vec![truncated, encoded("Price", &[Some(20.0)])],
        broken.clone(),
    );
    assert!(reader.next().unwrap().is_err());
    assert!(reader.next().is_none());
    assert_eq!(broken.snapshot().errors, 1);
    assert_eq!(broken.snapshot().completions, 0);
    assert_eq!(broken.snapshot().stream_requests, 1);
}

#[test]
fn profiled_ffi_failure_returns_partial_counters_without_a_handle() {
    let json = CString::new("{").unwrap();
    let token = CString::new("test-token").unwrap();
    let mut reader = std::ptr::null_mut();
    let mut counters = metrics::OlReadMetrics::default();
    let mut error = [0 as c_char; 128];
    unsafe {
        assert_eq!(
            ffi::ol_table_reader_open_profiled(
                json.as_ptr(),
                token.as_ptr(),
                &mut reader,
                &mut counters,
                error.as_mut_ptr(),
                error.len()
            ),
            1
        );
    }
    assert!(reader.is_null());
    assert_eq!(counters.errors, 1);
    assert_eq!(counters.allocation_requests, 0);
}

#[test]
fn metrics_measure_http_allocation_bytes_and_failed_requests() {
    let metrics = Metrics::enabled();
    let mut http = HttpTransport::new("test-token").unwrap();
    http.metrics = metrics.clone();
    let transport = MeasuredTransport {
        inner: Box::new(http),
        metrics: metrics.clone(),
    };
    let body = r#"{"streams":[{"streamId":"opaque"}]}"#;
    let (url, server) = serve_once("200 OK", body);
    transport.allocate(&url, Some(&["Price".into()])).unwrap();
    server.join().unwrap();
    assert_eq!(metrics.snapshot().allocation_bytes, body.len() as u64);
    let (url, server) = serve_once("403 Forbidden", "sensitive-service-error");
    assert!(matches!(
        transport.allocate(&url, None),
        Err(ReadError::Http { status: 403, .. })
    ));
    server.join().unwrap();
    assert_eq!(metrics.snapshot().allocation_requests, 2);
    assert_eq!(metrics.snapshot().allocation_bytes, body.len() as u64);
    let body = encoded("Price", &[Some(2.0)]);
    let (url, server) = serve_response("200 OK", "application/vnd.apache.arrow.stream", &body);
    let mut bytes = Vec::new();
    transport
        .stream(url)
        .unwrap()
        .read_to_end(&mut bytes)
        .unwrap();
    server.join().unwrap();
    assert_eq!(metrics.snapshot().stream_bytes, body.len() as u64);
    assert_eq!(bytes, body);
}

#[test]
#[ignore = "controlled local instrumentation-overhead benchmark"]
fn metrics_overhead() {
    let body = encoded("Price", &vec![Some(10.0); 8192]);
    for enabled in [false, true] {
        for _ in 0..1000 {
            let metrics = if enabled {
                Metrics::enabled()
            } else {
                Metrics::default()
            };
            measured_reader(vec![body.clone(); 2], metrics).for_each(|batch| {
                batch.unwrap();
            });
        }
    }
    for round in 0..7 {
        for enabled in if round % 2 == 0 {
            [false, true]
        } else {
            [true, false]
        } {
            let start = Instant::now();
            for _ in 0..1000 {
                let metrics = if enabled {
                    Metrics::enabled()
                } else {
                    Metrics::default()
                };
                let rows: usize = measured_reader(vec![body.clone(); 2], metrics)
                    .map(|batch| batch.unwrap().num_rows())
                    .sum();
                assert_eq!(rows, 16384);
            }
            println!(
                "METRICS_OVERHEAD round={round} enabled={enabled} elapsed_ns={}",
                start.elapsed().as_nanos()
            );
        }
    }
}

#[test]
fn consumes_independent_streams_once_including_empty_streams() {
    let (mut reader, calls) = reader(vec![
        encoded("Price", &[Some(10.0), None]),
        encoded("Price", &[]),
        encoded("Price", &[Some(20.0)]),
    ]);
    let batches = reader.by_ref().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    assert_eq!(batches[0].column(0).null_count(), 1);
    assert!(reader.next().is_none());
    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 4);
    assert!(calls[1].ends_with("/readStream/stream0"));
    assert!(calls[2].ends_with("/readStream/stream1"));
    assert!(calls[3].ends_with("/readStream/stream2"));
}

#[test]
fn empty_table_preserves_its_schema() {
    let (mut reader, _) = reader(vec![encoded("Price", &[])]);
    assert_eq!(reader.schema().field(0).name(), "Price");
    assert!(reader.next().is_none());
}

#[test]
fn schema_change_is_terminal_without_reallocation() {
    let (mut reader, calls) = reader(vec![
        encoded("Price", &[Some(10.0)]),
        encoded("Other", &[Some(20.0)]),
        encoded("Price", &[Some(30.0)]),
    ]);
    assert!(reader.next().unwrap().is_ok());
    assert!(reader
        .next()
        .unwrap()
        .unwrap_err()
        .to_string()
        .contains("different schemas"));
    assert!(reader.next().is_none());
    assert_eq!(calls.lock().unwrap().len(), 3);
}

#[test]
fn cancellation_and_expiry_do_not_start_another_stream() {
    for cancelled in [true, false] {
        let (mut reader, calls) = reader(vec![
            encoded("Price", &[Some(10.0)]),
            encoded("Price", &[Some(20.0)]),
        ]);
        if cancelled {
            reader.cancellation().cancel();
        } else {
            reader.state.deadline = Instant::now();
        }
        let error = reader.next().unwrap().unwrap_err().to_string();
        assert!(error.contains(if cancelled { "cancelled" } else { "expired" }));
        assert!(reader.next().is_none());
        assert_eq!(calls.lock().unwrap().len(), 2);
    }
}

struct ResetAfterBytes(Cursor<Vec<u8>>);

struct CancelAtEof {
    input: Cursor<Vec<u8>>,
    cancellation: Arc<Mutex<Option<Cancellation>>>,
}

impl Read for CancelAtEof {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let count = self.input.read(bytes)?;
        if count == 0 {
            self.cancellation.lock().unwrap().as_ref().unwrap().cancel();
        }
        Ok(count)
    }
}

#[test]
fn cancellation_during_end_of_stream_does_not_start_the_next_get() {
    let cancellation = Arc::new(Mutex::new(None));
    let body = CancelAtEof {
        input: Cursor::new(encoded("Price", &[Some(10.0)])),
        cancellation: cancellation.clone(),
    };
    let (mut reader, calls) = reader_with_bodies(vec![
        Box::new(body),
        Box::new(Cursor::new(encoded("Price", &[Some(20.0)]))),
    ]);
    *cancellation.lock().unwrap() = Some(reader.cancellation());
    assert!(reader.next().unwrap().is_ok());
    assert!(reader
        .next()
        .unwrap()
        .unwrap_err()
        .to_string()
        .contains("cancelled"));
    assert_eq!(calls.lock().unwrap().len(), 2);
}

impl Read for ResetAfterBytes {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if self.0.position() as usize == self.0.get_ref().len() {
            Err(io::Error::new(io::ErrorKind::ConnectionReset, "test reset"))
        } else {
            self.0.read(bytes)
        }
    }
}

#[test]
fn detects_http_failure_after_valid_arrow_end_marker() {
    let body = ResetAfterBytes(Cursor::new(encoded("Price", &[Some(10.0)])));
    let (mut reader, calls) = reader_with_bodies(vec![Box::new(body)]);
    assert!(reader.next().unwrap().is_ok());
    assert!(reader.next().unwrap().is_err());
    assert!(reader.next().is_none());
    assert_eq!(calls.lock().unwrap().len(), 2);
}

#[test]
fn rejects_partial_batch_without_replaying_prior_data() {
    let complete = encoded("Price", &[Some(10.0), Some(20.0)]);
    let schema_end = 8 + i32::from_le_bytes(complete[4..8].try_into().unwrap()) as usize;
    for end in [schema_end + 2, schema_end + 12, complete.len() - 12] {
        let (mut reader, calls) = reader(vec![complete[..end].to_vec()]);
        assert!(reader.next().unwrap().is_err(), "truncation at byte {end}");
        assert!(reader.next().is_none());
        assert_eq!(calls.lock().unwrap().len(), 2);
    }
}

#[test]
fn preserves_dictionaries_and_releases_exported_arrays() {
    let array: DictionaryArray<Int32Type> = [Some("one"), None, Some("two"), Some("one")]
        .into_iter()
        .collect();
    let batch = RecordBatch::try_from_iter(vec![("Price", Arc::new(array) as ArrayRef)]).unwrap();
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new(&mut bytes, &batch.schema()).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    drop(writer);
    let (reader, _) = reader(vec![bytes]);
    let stream = arrow_array::ffi_stream::FFI_ArrowArrayStream::new(Box::new(reader));
    let mut imported = ArrowArrayStreamReader::try_new(stream).unwrap();
    assert_eq!(imported.next().unwrap().unwrap(), batch);
    assert!(imported.next().is_none());
}

#[test]
fn stopping_early_never_opens_the_remaining_streams() {
    let (mut reader, calls) = reader(vec![
        encoded("Price", &[Some(10.0)]),
        encoded("Price", &[Some(20.0)]),
    ]);
    assert!(reader.next().unwrap().is_ok());
    drop(reader);
    assert_eq!(calls.lock().unwrap().len(), 2);
}

#[test]
fn rejects_malformed_or_duplicate_allocations() {
    for bytes in [
        r#"{"streams":[]}"#,
        r#"{"streams":[{"streamId":""}]}"#,
        r#"{"streams":[{"streamId":".."}]}"#,
        r#"{"streams":[{"streamId":"one"},{"streamId":"one"}]}"#,
        r#"{"other":[]}"#,
    ] {
        assert!(allocation(bytes.as_bytes()).is_err());
    }
    assert_eq!(
        allocation(br#"{"streams":[{"streamId":"opaque/a?b"}]}"#).unwrap()[0],
        "opaque/a?b"
    );
}

#[test]
fn restricts_token_destination_and_encodes_path_components() {
    for origin in [
        "http://onelake.table.fabric.microsoft.com",
        "https://onelake.table.fabric.microsoft.com.attacker.invalid",
        "https://user@onelake.table.fabric.microsoft.com",
        "https://onelake.table.fabric.microsoft.com:444",
        "https://onelake.table.fabric.microsoft.com/extra",
        "https://onelake.table.fabric.microsoft.com/?query=1",
    ] {
        let mut request = request();
        request.base_url = origin.into();
        assert!(request.table_url().is_err());
    }
    let mut request = request();
    request.base_url = "https://example-onelake.table.fabric.microsoft.com".into();
    request.table_name = "Price/paid?#".into();
    let table_url = request.table_url().unwrap();
    assert!(table_url.as_str().ends_with("/Price%2Fpaid%3F%23"));
    let stream_url = child_url(&table_url, &["readStream", "opaque/a?b"]);
    assert!(stream_url.as_str().ends_with("/readStream/opaque%2Fa%3Fb"));
}

#[test]
fn rejects_oversized_metadata_before_reading_or_allocating_it() {
    let state = ReadState {
        cancellation: Cancellation::new(),
        deadline: Instant::now() + Duration::from_secs(60),
    };
    let mut bytes = vec![255; 4];
    bytes.extend_from_slice(&i32::MAX.to_le_bytes());
    let mut framed = FramedReader::new(Box::new(Cursor::new(bytes)), state);
    assert!(framed.read(&mut [0u8; 1]).is_err());
}

#[test]
fn rejects_oversized_body_before_reading_or_allocating_it() {
    let mut bytes = encoded("Price", &[Some(10.0)]);
    let schema_end = 8 + i32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    let metadata_start = schema_end + 8;
    let message = arrow_ipc::root_as_message(&bytes[metadata_start..]).unwrap();
    let body_field = metadata_start
        + message._tab.loc()
        + message._tab.vtable().get(arrow_ipc::Message::VT_BODYLENGTH) as usize;
    bytes[body_field..body_field + 8].copy_from_slice(&i64::MAX.to_le_bytes());
    let (mut reader, _) = reader(vec![bytes]);
    assert!(reader.next().unwrap().is_err());
    assert!(reader.next().is_none());
}

fn serve_once(status: &str, body: &str) -> (Url, thread::JoinHandle<String>) {
    serve_response(status, "application/json", body.as_bytes())
}

fn serve_response(
    status: &str,
    content_type: &str,
    body: &[u8],
) -> (Url, thread::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = Url::parse(&format!("http://{}/table", listener.local_addr().unwrap())).unwrap();
    let mut response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\nLocation: https://example.invalid/\r\n\r\n",
        body.len()
    ).into_bytes();
    response.extend_from_slice(body);
    let server = thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut bytes = Vec::new();
        let mut byte = [0u8; 1];
        while !bytes.ends_with(b"\r\n\r\n") {
            socket.read_exact(&mut byte).unwrap();
            bytes.push(byte[0]);
        }
        let headers = String::from_utf8(bytes.clone()).unwrap();
        let length = headers
            .lines()
            .find_map(|line| {
                let (key, value) = line.split_once(':')?;
                key.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap_or(0);
        let mut body = vec![0u8; length];
        socket.read_exact(&mut body).unwrap();
        bytes.extend_from_slice(&body);
        socket.write_all(&response).unwrap();
        String::from_utf8(bytes).unwrap()
    });
    (url, server)
}

#[test]
fn checks_stream_status_and_content_type_and_reads_real_http_body() {
    let transport = HttpTransport::new("test-token").unwrap();
    for status in [
        "401 Unauthorized",
        "403 Forbidden",
        "429 Too Many Requests",
        "302 Found",
    ] {
        let (url, server) = serve_once(status, "sensitive-service-error");
        assert!(matches!(transport.stream(url), Err(ReadError::Http { .. })));
        server.join().unwrap();
    }
    let (url, server) = serve_once("200 OK", "not an Arrow stream");
    assert!(matches!(transport.stream(url), Err(ReadError::Arrow)));
    server.join().unwrap();
    let body = encoded("Price", &[Some(10.0)]);
    let (url, server) = serve_response("200 OK", "application/vnd.apache.arrow.stream", &body);
    let mut response = transport.stream(url).unwrap();
    let mut received = Vec::new();
    response.read_to_end(&mut received).unwrap();
    assert_eq!(received, body);
    let request = server.join().unwrap().to_ascii_lowercase();
    assert!(request.starts_with("get /table "));
    assert!(request.contains("accept: application/vnd.apache.arrow.stream"));
}

#[test]
fn sends_verified_projection_and_never_follows_redirects_or_exposes_error_body() {
    let transport = HttpTransport::new("test-token").unwrap();
    let (url, server) = serve_once("200 OK", r#"{"streams":[{"streamId":"first"}]}"#);
    let streams = transport.allocate(&url, Some(&["Price".into()])).unwrap();
    assert_eq!(streams[0], "first");
    let request = server.join().unwrap();
    assert!(request.starts_with("POST /table/read "));
    assert!(request.ends_with(r#"{"columns":["Price"]}"#));
    for status in ["302 Found", "500 Internal Server Error"] {
        let (url, server) = serve_once(status, "sensitive-service-error");
        let error = transport
            .allocate(&url, Some(&["Price".into()]))
            .unwrap_err();
        assert!(matches!(error, ReadError::Http { .. }));
        assert!(!error.to_string().contains("sensitive-service-error"));
        assert!(!error.to_string().contains("test-token"));
        server.join().unwrap();
    }
}

#[test]
fn omitted_projection_requests_authorized_schema_without_a_literal_wildcard() {
    let transport = HttpTransport::new("test-token").unwrap();
    let (url, server) = serve_once("200 OK", r#"{"streams":[{"streamId":"schema"}]}"#);
    transport.allocate(&url, None).unwrap();
    assert!(server.join().unwrap().ends_with("{}"));
    let mut request = request();
    request.columns = None;
    assert!(request.table_url().is_ok());
    for columns in [vec![], vec!["*".to_string()], vec!["".to_string()]] {
        request.columns = Some(columns);
        assert!(request.table_url().is_err());
    }
}

#[test]
fn arrow_ffi_transfers_ownership_once_and_cancellation_survives_export() {
    let (reader, _) = reader(vec![encoded("Price", &[Some(10.0)])]);
    let handle = ffi::test_handle(reader);
    let mut output = MaybeUninit::uninit();
    let mut error = [0 as c_char; 128];
    unsafe {
        assert_eq!(
            ffi::ol_table_reader_export(
                handle,
                output.as_mut_ptr(),
                error.as_mut_ptr(),
                error.len()
            ),
            0
        );
        let mut stream = output.assume_init();
        let mut duplicate = MaybeUninit::uninit();
        assert_eq!(
            ffi::ol_table_reader_export(
                handle,
                duplicate.as_mut_ptr(),
                error.as_mut_ptr(),
                error.len()
            ),
            1
        );
        let mut reader = ArrowArrayStreamReader::from_raw(&mut stream).unwrap();
        assert_eq!(reader.next().unwrap().unwrap().num_rows(), 1);
        ffi::ol_table_reader_cancel(handle);
        assert!(reader.next().unwrap().is_err());
        drop(reader);
        ffi::ol_table_reader_close(handle);
    }
}

#[test]
fn ffi_rejects_invalid_input_and_terminates_small_error_buffers() {
    let request = CString::new("{}").unwrap();
    let token = CString::new("test-token").unwrap();
    let mut output = std::ptr::null_mut();
    let mut error = [42 as c_char; 1];
    let status = unsafe {
        ffi::ol_table_reader_open(
            request.as_ptr(),
            token.as_ptr(),
            &mut output,
            error.as_mut_ptr(),
            error.len(),
        )
    };
    assert_eq!(status, 1);
    assert!(output.is_null());
    assert_eq!(error[0], 0);
}

#[test]
fn closing_handle_leaves_exported_stream_safe_to_release() {
    let (reader, _) = reader(vec![encoded("Price", &[Some(10.0)])]);
    let handle = ffi::test_handle(reader);
    let mut stream = MaybeUninit::uninit();
    unsafe {
        assert_eq!(
            ffi::ol_table_reader_export(handle, stream.as_mut_ptr(), std::ptr::null_mut(), 0),
            0
        );
        let mut reader = ArrowArrayStreamReader::try_new(stream.assume_init()).unwrap();
        ffi::ol_table_reader_close(handle);
        assert!(reader.next().unwrap().is_err());
        drop(reader);
    }
}

#[test]
#[ignore = "requires explicit approval for the target in ONELAKE_TABLE_READER_REQUEST"]
fn approved_live_first_batch() {
    let request: ReadRequest = serde_json::from_str(
        &std::env::var("ONELAKE_TABLE_READER_REQUEST").expect("set the approved request JSON"),
    )
    .unwrap();
    let token = std::env::var("AZURE_STORAGE_TOKEN").expect("set a caller Storage token");
    let mut reader = TableReader::open(request, &token).unwrap();
    let batch = reader.next().expect("expected a record batch").unwrap();
    assert!(batch.num_rows() > 0);
    println!(
        "Decoded {} rows in the first batch; the rest was not consumed.",
        batch.num_rows()
    );
}
