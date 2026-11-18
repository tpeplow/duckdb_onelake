use std::sync::atomic::AtomicUsize;
use std::sync::mpsc::{self, Receiver, Sender};

use super::*;

fn parallel(bodies: Vec<Box<dyn Read + Send>>, cap: usize, metrics: Metrics) -> TableReader {
    let transport = MockTransport {
        bodies: Mutex::new(bodies.into_iter().map(Some).collect()),
        calls: Arc::new(Mutex::new(Vec::new())),
    };
    let mut reader = TableReader::open_transport_profiled(
        request().table_url().unwrap(),
        Some(vec!["Price".into()]),
        Box::new(transport),
        metrics,
    )
    .unwrap();
    reader.max_parallel_streams = cap.min(reader.streams.len() + 1);
    reader
}

fn bodies(count: usize) -> Vec<Box<dyn Read + Send>> {
    (0..count)
        .map(|i| {
            Box::new(Cursor::new(encoded("Price", &[Some(i as f64), None]))) as Box<dyn Read + Send>
        })
        .collect()
}

#[test]
fn validates_client_local_budget_before_network() {
    let mut value = serde_json::json!({
        "baseUrl": request().base_url, "workspaceId": request().workspace_id,
        "itemId": request().item_id, "schemaName": "dbo", "tableName": "PricePaid",
    });
    let parsed: ReadRequest = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(parsed.max_parallel_streams, 1);
    for invalid in [
        serde_json::json!(-1),
        serde_json::json!("2"),
        serde_json::json!(null),
        serde_json::json!(1.5),
    ] {
        value["maxParallelStreams"] = invalid;
        assert!(serde_json::from_value::<ReadRequest>(value.clone()).is_err());
    }
    let mut parsed = request();
    parsed.max_parallel_streams = 0;
    assert!(matches!(
        TableReader::open(parsed, "unused"),
        Err(ReadError::InvalidRequest(_))
    ));
}

#[test]
fn preserves_multiset_and_caps_including_empty_and_single_streams() {
    for (count, cap) in [(1, 8), (9, 1), (9, 2), (9, 4), (9, 20)] {
        let metrics = Metrics::enabled();
        let mut input = bodies(count);
        input.push(Box::new(Cursor::new(encoded("Price", &[]))));
        let mut reader = parallel(input, cap, metrics.clone());
        let values = reader
            .by_ref()
            .map(|batch| {
                let batch = batch.unwrap();
                assert_eq!(batch.column(0).null_count(), 1);
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(0) as usize
            })
            .collect::<Vec<_>>();
        let mut sorted = values;
        sorted.sort_unstable();
        assert_eq!(sorted, (0..count).collect::<Vec<_>>());
        assert!(reader.next().is_none());
        assert_eq!(metrics.snapshot().rows, (2 * count) as u64);
        assert_eq!(metrics.snapshot().stream_requests, (count + 1) as u64);
        assert_eq!(metrics.snapshot().allocated_streams, (count + 1) as u64);
        assert!(metrics.snapshot().peak_active_streams <= cap.min(count + 1) as u64);
        assert_eq!(metrics.snapshot().completions, 1);
        assert_eq!(metrics.snapshot().first_batches, 1);
    }
    let mut reader = parallel(bodies(1), 8, Metrics::default());
    reader.next().unwrap().unwrap();
    assert!(reader.parallel.is_none());
}

struct Gate {
    open: Mutex<bool>,
    changed: Condvar,
}

impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            open: Mutex::new(false),
            changed: Condvar::new(),
        })
    }
    fn release(&self) {
        *self.open.lock().unwrap() = true;
        self.changed.notify_all();
    }
}

struct GatedBody {
    input: Cursor<Vec<u8>>,
    schema_end: u64,
    id: usize,
    arrived: Sender<usize>,
    gate: Arc<Gate>,
    live: Arc<AtomicUsize>,
    entered: bool,
}

impl Read for GatedBody {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if !self.entered && self.input.position() >= self.schema_end {
            self.entered = true;
            self.arrived.send(self.id).unwrap();
            let open = self.gate.open.lock().unwrap();
            let (open, timeout) = self
                .gate
                .changed
                .wait_timeout_while(open, Duration::from_secs(5), |v| !*v)
                .unwrap();
            if timeout.timed_out() && !*open {
                return Err(io::Error::other("test gate timeout"));
            }
        }
        self.input.read(output)
    }
}

impl Drop for GatedBody {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

type Gated = (
    Vec<Box<dyn Read + Send>>,
    Vec<Arc<Gate>>,
    Receiver<usize>,
    Arc<AtomicUsize>,
);

fn gated(count: usize) -> Gated {
    let (sender, receiver) = mpsc::channel();
    let live = Arc::new(AtomicUsize::new(count));
    let gates = (0..count).map(|_| Gate::new()).collect::<Vec<_>>();
    let input = gates
        .iter()
        .enumerate()
        .map(|(id, gate)| {
            let bytes = encoded("Price", &[Some(id as f64)]);
            let schema_end = 8 + i32::from_le_bytes(bytes[4..8].try_into().unwrap()) as u64;
            Box::new(GatedBody {
                input: Cursor::new(bytes),
                schema_end,
                id,
                arrived: sender.clone(),
                gate: gate.clone(),
                live: live.clone(),
                entered: false,
            }) as Box<dyn Read + Send>
        })
        .collect();
    (input, gates, receiver, live)
}

#[test]
fn overlaps_reads_and_delivers_ready_stream_before_stalled_first_stream() {
    let (input, gates, arrived, live) = gated(8);
    let metrics = Metrics::enabled();
    let mut reader = parallel(input, 3, metrics.clone());
    assert!(reader.parallel.is_none());
    assert_eq!(metrics.snapshot().stream_requests, 1);
    let (sent, received) = mpsc::channel();
    let worker = thread::spawn(move || {
        let first = reader.next().unwrap().unwrap();
        sent.send((reader, first)).unwrap();
    });
    let entered = (0..3)
        .map(|_| arrived.recv_timeout(Duration::from_secs(3)).unwrap())
        .collect::<Vec<_>>();
    assert!(entered.contains(&0));
    assert_eq!(metrics.snapshot().peak_active_streams, 3);
    let fast = *entered.iter().find(|id| **id != 0).unwrap();
    gates[fast].release();
    let (mut reader, first) = received.recv_timeout(Duration::from_secs(3)).unwrap();
    assert_eq!(
        first
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        fast as f64
    );
    for gate in gates {
        gate.release();
    }
    assert_eq!(
        reader
            .by_ref()
            .map(|b| b.unwrap().num_rows())
            .sum::<usize>(),
        7
    );
    drop(reader);
    worker.join().unwrap();
    assert_eq!(live.load(Ordering::SeqCst), 0);
    assert_eq!(metrics.snapshot().stream_requests, 8);
    assert_eq!(metrics.snapshot().peak_active_streams, 3);
}

fn many_batches() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "Price",
        DataType::Float64,
        true,
    )]));
    let mut writer = StreamWriter::try_new(Vec::new(), &schema).unwrap();
    for i in 0..20 {
        writer
            .write(
                &RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(Float64Array::from(vec![Some(i as f64)]))],
                )
                .unwrap(),
            )
            .unwrap();
    }
    writer.finish().unwrap();
    writer.into_inner().unwrap()
}

fn wait_full(reader: &TableReader, cap: usize) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while reader.parallel.as_ref().unwrap().snapshot().0 != cap {
        assert!(
            Instant::now() < deadline,
            "workers did not fill their slots"
        );
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(reader.parallel.as_ref().unwrap().snapshot().1, cap);
}

#[test]
fn credits_bound_prefetch_and_early_drop_joins_full_queues() {
    let metrics = Metrics::enabled();
    let bytes = many_batches();
    let mut reader = parallel(
        vec![
            Box::new(Cursor::new(bytes.clone())),
            Box::new(Cursor::new(bytes.clone())),
        ],
        2,
        metrics.clone(),
    );
    reader.next().unwrap().unwrap();
    wait_full(&reader, 2);
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rows, 1);
    assert_eq!(snapshot.batches, 1);
    let schema_end = 8 + i32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    let metadata_len =
        i32::from_le_bytes(bytes[schema_end + 4..schema_end + 8].try_into().unwrap()) as usize;
    let body_len = arrow_ipc::root_as_message(&bytes[schema_end + 8..])
        .unwrap()
        .bodyLength() as usize;
    assert_eq!(
        snapshot.stream_bytes as usize,
        2 * schema_end + 3 * (8 + metadata_len + body_len)
    );
    assert!(snapshot.stream_bytes < bytes.len() as u64);
    drop(reader);
    let final_snapshot = metrics.snapshot();
    assert_eq!(final_snapshot.stream_bytes, snapshot.stream_bytes);
    assert_eq!(final_snapshot.completions, 0);
    assert_eq!(final_snapshot.errors, 0);
}

#[test]
fn parallel_streams_preserve_independent_dictionaries_and_reordered_projection() {
    let mut inputs = Vec::new();
    let mut expected = Vec::new();
    for (id, values) in [
        (0, vec![Some("one"), None, Some("two")]),
        (1, vec![Some("two"), Some("one"), None]),
    ] {
        let dictionary: DictionaryArray<Int32Type> = values.into_iter().collect();
        let batch = RecordBatch::try_from_iter(vec![
            ("Price", Arc::new(dictionary) as ArrayRef),
            (
                "Other",
                Arc::new(Float64Array::from(vec![id as f64; 3])) as ArrayRef,
            ),
        ])
        .unwrap();
        let mut writer = StreamWriter::try_new(Vec::new(), &batch.schema()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        inputs.push(Box::new(Cursor::new(writer.into_inner().unwrap())) as Box<dyn Read + Send>);
        expected.push(batch.project(&[1, 0]).unwrap());
    }
    let (mut reader, _) = reader_with_columns(inputs, vec!["Other".into(), "Price".into()]);
    reader.max_parallel_streams = 2;
    let mut batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
    batches.sort_by_key(|batch| {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0) as usize
    });
    assert_eq!(batches, expected);
}

#[test]
fn cancellation_and_expiry_while_consumer_waits_for_io_are_not_decode_errors() {
    for expire in [false, true] {
        let (input, gates, arrived, live) = gated(2);
        let metrics = Metrics::enabled();
        let mut reader = parallel(input, 2, metrics.clone());
        if expire {
            reader.state.deadline = Instant::now() + Duration::from_millis(100);
        }
        let deadline = reader.state.deadline;
        let cancellation = reader.cancellation();
        let worker = thread::spawn(move || {
            let error = reader.next().unwrap().unwrap_err().to_string();
            assert!(reader.next().is_none());
            error
        });
        for _ in 0..2 {
            arrived.recv_timeout(Duration::from_secs(3)).unwrap();
        }
        if expire {
            thread::sleep(deadline.saturating_duration_since(Instant::now()));
        } else {
            cancellation.cancel();
        }
        for gate in gates {
            gate.release();
        }
        let error = worker.join().unwrap();
        assert!(error.contains(if expire { "expired" } else { "cancelled" }));
        assert_eq!(live.load(Ordering::SeqCst), 0);
        assert_eq!(metrics.snapshot().rows, 0);
        assert_eq!(metrics.snapshot().errors, 1);
        assert_eq!(metrics.snapshot().completions, 0);
    }
}

struct PanicBody(Cursor<Vec<u8>>);
impl Read for PanicBody {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let end = 8 + i32::from_le_bytes(self.0.get_ref()[4..8].try_into().unwrap()) as u64;
        assert!(self.0.position() < end, "injected worker panic");
        self.0.read(output)
    }
}

#[test]
fn worker_errors_are_terminal_without_reallocation_or_false_completion() {
    let mut truncated = encoded("Price", &[Some(10.0)]);
    truncated.truncate(truncated.len() - 12);
    for broken in [
        Box::new(Cursor::new(encoded("Other", &[Some(10.0)]))) as Box<dyn Read + Send>,
        Box::new(Cursor::new(truncated)),
        Box::new(PanicBody(Cursor::new(encoded("Price", &[Some(10.0)])))),
    ] {
        let metrics = Metrics::enabled();
        let mut reader = parallel(
            vec![Box::new(Cursor::new(many_batches())), broken],
            2,
            metrics.clone(),
        );
        let result = reader.by_ref().collect::<Result<Vec<_>, _>>();
        assert!(result.is_err());
        assert!(reader.next().is_none());
        assert_eq!(metrics.snapshot().errors, 1);
        assert_eq!(metrics.snapshot().completions, 0);
        assert_eq!(metrics.snapshot().allocation_requests, 1);
        assert_eq!(metrics.snapshot().stream_requests, 2);
    }
}

struct FailOpen(MockTransport);

impl Transport for FailOpen {
    fn allocate(
        &self,
        url: &Url,
        columns: Option<&[String]>,
    ) -> Result<VecDeque<String>, ReadError> {
        self.0.allocate(url, columns)
    }

    fn stream(&self, url: Url) -> Result<Box<dyn Read + Send>, ReadError> {
        if url.path().ends_with("/stream1") {
            return Err(ReadError::Http {
                operation: "read stream",
                status: 403,
            });
        }
        self.0.stream(url)
    }
}

#[test]
fn parallel_http_open_failure_is_not_an_empty_stream_or_retried() {
    let metrics = Metrics::enabled();
    let transport = FailOpen(MockTransport {
        bodies: Mutex::new(bodies(2).into_iter().map(Some).collect()),
        calls: Arc::new(Mutex::new(Vec::new())),
    });
    let mut reader = TableReader::open_transport_profiled(
        request().table_url().unwrap(),
        Some(vec!["Price".into()]),
        Box::new(transport),
        metrics.clone(),
    )
    .unwrap();
    reader.max_parallel_streams = 2;
    let error = reader
        .by_ref()
        .collect::<Result<Vec<_>, _>>()
        .unwrap_err()
        .to_string();
    assert!(error.contains("403"));
    assert!(reader.next().is_none());
    assert_eq!(metrics.snapshot().allocation_requests, 1);
    assert_eq!(metrics.snapshot().stream_requests, 2);
    assert_eq!(metrics.snapshot().errors, 1);
    assert_eq!(metrics.snapshot().completions, 0);
}

#[test]
fn cancellation_expiry_and_worker_start_failure_release_pool() {
    for failure in 0..3 {
        let metrics = Metrics::enabled();
        let mut reader = parallel(
            vec![
                Box::new(Cursor::new(many_batches())),
                Box::new(Cursor::new(many_batches())),
            ],
            2,
            metrics.clone(),
        );
        match failure {
            0 => {
                reader.next().unwrap().unwrap();
                wait_full(&reader, 2);
                reader.cancellation().cancel();
            }
            1 => reader.state.deadline = Instant::now(),
            _ => reader.fail_spawn_at = Some(1),
        }
        assert!(reader.next().unwrap().is_err());
        assert!(reader.next().is_none());
        assert!(reader.parallel.is_none());
        assert_eq!(metrics.snapshot().errors, 1);
        assert_eq!(metrics.snapshot().completions, 0);
    }
}

#[test]
fn exported_arrays_survive_parallel_stream_and_handle_release_in_either_order() {
    for handle_first in [true, false] {
        let reader = parallel(
            vec![
                Box::new(Cursor::new(many_batches())),
                Box::new(Cursor::new(many_batches())),
            ],
            2,
            Metrics::enabled(),
        );
        let handle = ffi::test_handle(reader);
        let mut stream = MaybeUninit::uninit();
        unsafe {
            assert_eq!(
                ffi::ol_table_reader_export(handle, stream.as_mut_ptr(), std::ptr::null_mut(), 0),
                0
            );
            let mut imported = ArrowArrayStreamReader::try_new(stream.assume_init()).unwrap();
            let batch = imported.next().unwrap().unwrap();
            if handle_first {
                ffi::ol_table_reader_close(handle);
                drop(imported);
            } else {
                drop(imported);
                ffi::ol_table_reader_close(handle);
            }
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(0),
                0.0
            );
        }
    }
}
