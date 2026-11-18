use std::sync::{Mutex, MutexGuard};
use std::thread::{self, JoinHandle};

use super::*;

struct Pending {
    streams: VecDeque<String>,
    ready: VecDeque<(usize, RecordBatch)>,
    busy: Vec<bool>,
    workers: usize,
    error: Option<ReadError>,
}

struct Coordinator {
    pending: Mutex<Pending>,
    transport: Arc<dyn Transport>,
    table_url: Url,
    source_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    state: ReadState,
    metrics: Metrics,
}

impl Coordinator {
    fn lock(&self) -> MutexGuard<'_, Pending> {
        self.pending.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn wait<'a>(&self, pending: MutexGuard<'a, Pending>) -> MutexGuard<'a, Pending> {
        // Also bound waits for expiry and cancellation racing the condition-variable wait.
        self.state
            .cancellation
            .1
            .wait_timeout(pending, Duration::from_millis(20))
            .unwrap_or_else(|e| e.into_inner())
            .0
    }

    fn fail(&self, error: ReadError) {
        let mut pending = self.lock();
        if pending.error.is_none() {
            pending.error = Some(error);
        }
        pending.streams.clear();
        pending.ready.clear();
        self.state.cancellation.cancel();
    }

    fn worker(&self, index: usize, mut current: Option<DecodedStream>) -> Result<(), ReadError> {
        loop {
            let stream = {
                let mut pending = self.lock();
                while pending.busy[index] {
                    self.state.check()?;
                    pending = self.wait(pending);
                }
                self.state.check()?;
                pending.busy[index] = true;
                if current.is_none() {
                    match pending.streams.pop_front() {
                        Some(id) => Some(id),
                        None => return Ok(()),
                    }
                } else {
                    None
                }
            };
            if let Some(stream) = stream {
                self.state.check()?;
                let body = self
                    .transport
                    .stream(child_url(&self.table_url, &["readStream", &stream]))?;
                let reader = decode_stream(body, self.state.clone(), &self.metrics)?;
                if reader.schema() != self.source_schema {
                    return Err(ReadError::SchemaChanged);
                }
                current = Some(reader);
            }
            let batch = current.as_mut().unwrap().next_batch(
                &self.state,
                &self.metrics,
                &self.projection,
            )?;
            if batch.is_none() {
                // Release the old response before opening another stream.
                current = None;
            }
            let mut pending = self.lock();
            self.state.check()?;
            if let Some(batch) = batch {
                pending.ready.push_back((index, batch));
            } else {
                pending.busy[index] = false;
            }
            self.state.cancellation.1.notify_all();
        }
    }
}

pub(super) struct ParallelReader {
    coordinator: Arc<Coordinator>,
    workers: Vec<JoinHandle<()>>,
}

impl ParallelReader {
    pub(super) fn start(reader: &mut TableReader, count: usize) -> Result<Self, ReadError> {
        let coordinator = Arc::new(Coordinator {
            pending: Mutex::new(Pending {
                streams: std::mem::take(&mut reader.streams),
                ready: VecDeque::new(),
                busy: vec![false; count],
                workers: count,
                error: None,
            }),
            transport: reader.transport.clone(),
            table_url: reader.table_url.clone(),
            source_schema: reader.source_schema.clone(),
            projection: reader.projection.clone(),
            state: reader.state.clone(),
            metrics: reader.metrics.clone(),
        });
        let mut pool = Self {
            coordinator,
            workers: Vec::with_capacity(count),
        };
        let mut first = reader.current.take();
        for index in 0..count {
            #[cfg(test)]
            if reader.fail_spawn_at == Some(index) {
                return Err(ReadError::Internal);
            }
            let coordinator = pool.coordinator.clone();
            let current = first.take();
            let worker = thread::Builder::new()
                .name("onelake-stream".into())
                .spawn(move || {
                    let result =
                        catch_unwind(AssertUnwindSafe(|| coordinator.worker(index, current)))
                            .unwrap_or(Err(ReadError::Internal));
                    if let Err(error) = result {
                        coordinator.fail(error);
                    }
                    coordinator.lock().workers -= 1;
                    coordinator.state.cancellation.1.notify_all();
                })
                .map_err(|_| ReadError::Internal)?;
            pool.workers.push(worker);
        }
        Ok(pool)
    }

    pub(super) fn next(&self) -> Result<Option<RecordBatch>, ReadError> {
        let mut pending = self.coordinator.lock();
        loop {
            if let Some(error) = &pending.error {
                return Err(error.clone());
            }
            self.coordinator.state.check()?;
            if let Some((worker, batch)) = pending.ready.pop_front() {
                pending.busy[worker] = false;
                self.coordinator.state.cancellation.1.notify_all();
                return Ok(Some(batch));
            }
            if pending.workers == 0 {
                return Ok(None);
            }
            pending = self.coordinator.wait(pending);
        }
    }

    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> (usize, usize, usize) {
        let pending = self.coordinator.lock();
        (
            pending.ready.len(),
            pending.busy.iter().filter(|v| **v).count(),
            pending.workers,
        )
    }
}

impl Drop for ParallelReader {
    fn drop(&mut self) {
        self.coordinator.state.cancellation.cancel();
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}
