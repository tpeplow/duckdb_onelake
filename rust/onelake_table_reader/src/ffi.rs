use std::ffi::{c_char, CStr};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;

use arrow_array::ffi_stream::FFI_ArrowArrayStream;

use crate::{Cancellation, ReadError, ReadRequest, TableReader};

pub struct OlTableReader {
    reader: Option<TableReader>,
    cancellation: Cancellation,
}

fn handle(reader: TableReader) -> *mut OlTableReader {
    Box::into_raw(Box::new(OlTableReader {
        cancellation: reader.cancellation(),
        reader: Some(reader),
    }))
}

unsafe fn report(
    error_buffer: *mut c_char,
    error_buffer_len: usize,
    operation: impl FnOnce() -> Result<(), ReadError>,
) -> i32 {
    let result = catch_unwind(AssertUnwindSafe(operation)).unwrap_or(Err(ReadError::Internal));
    let (status, message) = match result {
        Ok(()) => (0, String::new()),
        Err(error) => {
            let status = match error {
                ReadError::InvalidRequest(_) => 1,
                ReadError::Http { .. } | ReadError::Transport => 2,
                ReadError::Allocation | ReadError::Arrow | ReadError::SchemaChanged => 3,
                ReadError::Cancelled => 4,
                ReadError::Expired => 5,
                ReadError::Internal => 100,
            };
            (status, error.to_string())
        }
    };
    if !error_buffer.is_null() && error_buffer_len > 0 {
        let count = message.len().min(error_buffer_len - 1);
        ptr::copy_nonoverlapping(message.as_ptr(), error_buffer.cast(), count);
        *error_buffer.add(count) = 0;
    }
    status
}

/// Opens one read session; credentials and service error bodies are never copied to error_buffer.
/// Status: 0 success, 1 invalid input, 2 transport, 3 protocol, 4 cancelled, 5 expired, 100 internal.
///
/// # Safety
/// Input strings must be valid NUL-terminated UTF-8. `output` must be writable. An optional
/// error buffer must be writable for its stated length. The caller must close the returned handle.
#[no_mangle]
pub unsafe extern "C" fn ol_table_reader_open(
    request_json: *const c_char,
    token: *const c_char,
    output: *mut *mut OlTableReader,
    error_buffer: *mut c_char,
    error_buffer_len: usize,
) -> i32 {
    if !output.is_null() {
        *output = ptr::null_mut();
    }
    report(error_buffer, error_buffer_len, || {
        if request_json.is_null() || token.is_null() || output.is_null() {
            return Err(ReadError::InvalidRequest("null FFI argument"));
        }
        let request: ReadRequest = serde_json::from_slice(CStr::from_ptr(request_json).to_bytes())
            .map_err(|_| ReadError::InvalidRequest("invalid request JSON"))?;
        let token = CStr::from_ptr(token)
            .to_str()
            .map_err(|_| ReadError::InvalidRequest("token must be UTF-8"))?;
        *output = handle(TableReader::open(request, token)?);
        Ok(())
    })
}

/// Transfers the reader into an Arrow C stream exactly once.
///
/// # Safety
/// `reader` must be a live handle, exclusively accessed for this call. `output` must point
/// to uninitialized or already released Arrow stream storage. Release that stream exactly once.
/// The optional error buffer must be writable for its stated length.
#[no_mangle]
pub unsafe extern "C" fn ol_table_reader_export(
    reader: *mut OlTableReader,
    output: *mut FFI_ArrowArrayStream,
    error_buffer: *mut c_char,
    error_buffer_len: usize,
) -> i32 {
    report(error_buffer, error_buffer_len, || {
        if reader.is_null() || output.is_null() {
            return Err(ReadError::InvalidRequest("null FFI argument"));
        }
        let reader = (*reader)
            .reader
            .take()
            .ok_or(ReadError::InvalidRequest("reader already exported"))?;
        ptr::write(output, FFI_ArrowArrayStream::new(Box::new(reader)));
        Ok(())
    })
}

/// Requests cancellation. A blocked HTTP read remains bounded by the transport timeout.
///
/// # Safety
/// `reader` must be null or a live handle; do not race this call with closing the handle.
#[no_mangle]
pub unsafe extern "C" fn ol_table_reader_cancel(reader: *const OlTableReader) {
    if let Some(reader) = reader.as_ref() {
        reader.cancellation.cancel();
    }
}

/// Cancels the session and frees the handle. An exported Arrow stream must still be released.
///
/// # Safety
/// `reader` must be null or a live handle returned by open, closed once without concurrent access.
#[no_mangle]
pub unsafe extern "C" fn ol_table_reader_close(reader: *mut OlTableReader) {
    if !reader.is_null() {
        let _ = catch_unwind(AssertUnwindSafe(|| {
            let reader = Box::from_raw(reader);
            reader.cancellation.cancel();
            drop(reader);
        }));
    }
}

#[cfg(test)]
pub(crate) fn test_handle(reader: TableReader) -> *mut OlTableReader {
    handle(reader)
}
