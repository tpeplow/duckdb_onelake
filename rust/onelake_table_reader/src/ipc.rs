use std::io::{self, Cursor, Read};

use crate::ReadState;

const MAX_METADATA_BYTES: usize = 1024 * 1024;
const MAX_BODY_BYTES: usize = 256 * 1024 * 1024;

pub(crate) struct FramedReader {
    input: Box<dyn Read + Send>,
    state: ReadState,
    message: Cursor<Vec<u8>>,
    dictionary_bytes: usize,
    done: bool,
}

fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "invalid or unsupported Arrow IPC frame",
    )
}

impl FramedReader {
    pub(crate) fn new(input: Box<dyn Read + Send>, state: ReadState) -> Self {
        Self {
            input,
            state,
            message: Cursor::new(Vec::new()),
            dictionary_bytes: 0,
            done: false,
        }
    }

    fn check(&self) -> io::Result<()> {
        self.state.check().map_err(io::Error::other)
    }

    fn read_exact(&mut self, bytes: &mut [u8]) -> io::Result<()> {
        for chunk in bytes.chunks_mut(64 * 1024) {
            self.check()?;
            self.input.read_exact(chunk)?;
        }
        Ok(())
    }

    fn next_message(&mut self) -> io::Result<bool> {
        self.check()?;
        if self.done {
            return Ok(false);
        }
        // Bound the IPC metadata/body before Arrow allocates from untrusted length fields.
        self.message = Cursor::new(Vec::new());
        let mut prefix = [0u8; 4];
        if self.input.read(&mut prefix[..1])? == 0 {
            self.done = true;
            return Ok(false);
        }
        self.read_exact(&mut prefix[1..])?;
        let mut bytes = prefix.to_vec();
        if prefix == [255; 4] {
            self.read_exact(&mut prefix)?;
            bytes.extend_from_slice(&prefix);
        }
        let metadata_length = i32::from_le_bytes(prefix);
        if metadata_length == 0 {
            // A valid Arrow end marker does not prove the HTTP transfer completed.
            let mut extra = [0u8; 1];
            if self.input.read(&mut extra)? != 0 {
                return Err(invalid());
            }
            self.done = true;
            self.message = Cursor::new(bytes);
            return Ok(true);
        }
        let metadata_length = usize::try_from(metadata_length).map_err(|_| invalid())?;
        if metadata_length > MAX_METADATA_BYTES {
            return Err(invalid());
        }
        let mut metadata = vec![0u8; metadata_length];
        self.read_exact(&mut metadata)?;
        let message = arrow_ipc::root_as_message(&metadata).map_err(|_| invalid())?;
        let body_length = usize::try_from(message.bodyLength()).map_err(|_| invalid())?;
        if body_length > MAX_BODY_BYTES {
            return Err(invalid());
        }
        let batch = message.header_as_record_batch().or_else(|| {
            message
                .header_as_dictionary_batch()
                .and_then(|dictionary| dictionary.data())
        });
        // Compressed buffer expansion needs its own bounds before enabling compression.
        if batch.is_some_and(|batch| batch.compression().is_some()) {
            return Err(invalid());
        }
        if message.header_type() == arrow_ipc::MessageHeader::DictionaryBatch {
            self.dictionary_bytes = self
                .dictionary_bytes
                .checked_add(body_length)
                .filter(|size| *size <= MAX_BODY_BYTES)
                .ok_or_else(invalid)?;
        }
        bytes.extend_from_slice(&metadata);
        let offset = bytes.len();
        bytes.resize(offset + body_length, 0);
        self.read_exact(&mut bytes[offset..])?;
        self.message = Cursor::new(bytes);
        Ok(true)
    }
}

impl Read for FramedReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        self.check()?;
        // Arrow accepts UnexpectedEof while reading a message prefix as clean EOF. Our
        // framing layer also reads its body here, so truncation must stay an error.
        if self.message.position() as usize == self.message.get_ref().len()
            && !self.next_message().map_err(|error| {
                if error.kind() == io::ErrorKind::UnexpectedEof {
                    invalid()
                } else {
                    error
                }
            })?
        {
            return Ok(0);
        }
        self.message.read(output)
    }
}
