use std::io::{self, Read};
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Cumulative client-observed counters for one reader. Times are wall-clock nanoseconds.
#[repr(C)]
#[derive(Clone, Copy, Default, serde::Serialize)]
pub struct OlReadMetrics {
    pub allocation_requests: u64,
    pub allocation_bytes: u64,
    pub allocation_ns: u64,
    pub stream_requests: u64,
    pub stream_open_ns: u64,
    pub stream_bytes: u64,
    pub stream_read_ns: u64,
    pub ipc_processing_ns: u64,
    pub batches: u64,
    pub rows: u64,
    pub first_batches: u64,
    pub first_batch_ns: u64,
    pub completions: u64,
    pub errors: u64,
    pub allocated_streams: u64,
    pub peak_active_streams: u64,
}

#[derive(Default)]
struct Counters {
    values: OlReadMetrics,
    active_streams: u64,
}

#[derive(Clone, Default)]
pub(crate) struct Metrics(Option<Arc<Mutex<Counters>>>);

impl Metrics {
    pub(crate) fn enabled() -> Self {
        Self(Some(Arc::new(Mutex::new(Counters::default()))))
    }

    pub(crate) fn start(&self) -> Option<Instant> {
        self.0.as_ref().map(|_| Instant::now())
    }

    pub(crate) fn update(&self, update: impl FnOnce(&mut OlReadMetrics)) {
        if let Some(data) = &self.0 {
            update(&mut data.lock().unwrap_or_else(|e| e.into_inner()).values);
        }
    }

    pub(crate) fn snapshot(&self) -> OlReadMetrics {
        self.0
            .as_ref()
            .map(|data| data.lock().unwrap_or_else(|e| e.into_inner()).values)
            .unwrap_or_default()
    }

    pub(crate) fn active_stream(&self) -> ActiveStream {
        if let Some(data) = &self.0 {
            let mut data = data.lock().unwrap_or_else(|e| e.into_inner());
            data.active_streams += 1;
            data.values.peak_active_streams =
                data.values.peak_active_streams.max(data.active_streams);
        }
        ActiveStream(self.clone())
    }

    pub(crate) fn body(
        &self,
        input: Box<dyn Read + Send>,
        active: ActiveStream,
    ) -> Box<dyn Read + Send> {
        if self.0.is_none() {
            input
        } else {
            Box::new(MeasuredBody {
                input,
                metrics: self.clone(),
                _active: active,
            })
        }
    }
}

pub(crate) struct ActiveStream(Metrics);

impl Drop for ActiveStream {
    fn drop(&mut self) {
        if let Some(data) = &self.0 .0 {
            data.lock()
                .unwrap_or_else(|e| e.into_inner())
                .active_streams -= 1;
        }
    }
}

pub(crate) fn elapsed(start: Option<Instant>) -> u64 {
    start
        .map(|start| start.elapsed().as_nanos().min(u64::MAX as u128) as u64)
        .unwrap_or(0)
}

struct MeasuredBody {
    input: Box<dyn Read + Send>,
    metrics: Metrics,
    _active: ActiveStream,
}

impl Read for MeasuredBody {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let start = self.metrics.start();
        let result = self.input.read(buffer);
        let ns = elapsed(start);
        self.metrics.update(|m| {
            m.stream_read_ns += ns;
            m.stream_bytes += result.as_ref().copied().unwrap_or(0) as u64;
        });
        result
    }
}
