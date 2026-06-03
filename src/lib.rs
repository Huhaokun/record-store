use std::os::unix::fs::FileExt;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use log::error;
use thiserror::Error;
use tokio::{
    fs::{File, OpenOptions},
    io::AsyncWriteExt,
    sync::Mutex,
    task::JoinHandle,
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Record {
    checksum: u32,
    payload: BytesMut,
}

const MAGIC_HEADER: u64 = 5786;
const RECORD_OVERHEAD_LEN: u64 = (size_of::<u64>() + size_of::<u64>() + size_of::<u32>()) as u64;
#[cfg(test)]
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(1);

impl Record {
    pub fn new(payload: impl Into<BytesMut>) -> Self {
        let payload = payload.into();

        Self {
            checksum: crc32fast::hash(&payload),
            payload,
        }
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn len_on_disk(&self) -> u64 {
        RECORD_OVERHEAD_LEN + self.payload.len() as u64
    }

    async fn write_to(&self, f: &mut File) -> Result<(), RError> {
        f.write_u64(MAGIC_HEADER).await?;
        f.write_u64(self.payload.len() as u64).await?;
        f.write_u32(self.checksum).await?;
        f.write_all(&self.payload).await?;
        Ok(())
    }

    fn read_from(f: &std::fs::File, offset: u64) -> Result<Self, RError> {
        let mut magic_header = [0; size_of::<u64>()];
        f.read_exact_at(&mut magic_header, offset)?;

        let magic_header = u64::from_be_bytes(magic_header);
        if magic_header != MAGIC_HEADER {
            return Err(RError::InvalidMagicHeader {
                expected: MAGIC_HEADER,
                actual: magic_header,
            });
        }

        let mut length = [0; size_of::<u64>()];
        f.read_exact_at(&mut length, offset + size_of::<u64>() as u64)?;
        let length = u64::from_be_bytes(length);

        let payload_len = usize::try_from(length).map_err(|_| RError::RecordTooLarge(length))?;
        let mut checksum = [0; size_of::<u32>()];
        f.read_exact_at(
            &mut checksum,
            offset + size_of::<u64>() as u64 + size_of::<u64>() as u64,
        )?;
        let checksum = u32::from_be_bytes(checksum);

        let mut payload = BytesMut::zeroed(payload_len);
        f.read_exact_at(&mut payload, offset + RECORD_OVERHEAD_LEN)?;

        let actual_checksum = crc32fast::hash(&payload);
        if actual_checksum != checksum {
            return Err(RError::InvalidChecksum {
                expected: checksum,
                actual: actual_checksum,
            });
        }

        Ok(Self { checksum, payload })
    }
}

#[derive(Debug, Error)]
pub enum RError {
    #[error("Invalid record magic header: expected {expected}, got {actual}")]
    InvalidMagicHeader { expected: u64, actual: u64 },

    #[error("Invalid record checksum: expected {expected}, got {actual}")]
    InvalidChecksum { expected: u32, actual: u32 },

    #[error("Record payload is too large: {0} bytes")]
    RecordTooLarge(u64),

    #[error("RecordStore IO error: {0}")]
    IoError(#[from] std::io::Error),
}

#[allow(async_fn_in_trait)]
pub trait RecordStore {
    async fn append(&mut self, r: Record) -> Result<u64, RError>;

    async fn fetch(&self, offset: u64) -> Result<Record, RError>;
}

#[derive(Debug)]
pub struct SingleFileRecordStore {
    writer_f: Arc<Mutex<File>>,
    reader_f: Arc<std::fs::File>,
    offset: u64,
    flush_task: JoinHandle<()>,
}

impl SingleFileRecordStore {
    pub async fn new(path: String, flush_interval: Duration) -> Result<Self, RError> {
        let writer_f = Arc::new(Mutex::new(File::create_new(path.clone()).await?));
        let reader_f = std::fs::File::open(path)?;
        let flush_task = Self::spawn_periodic_flush(writer_f.clone(), flush_interval);

        Ok(Self {
            writer_f,
            reader_f: Arc::new(reader_f),
            offset: 0,
            flush_task,
        })
    }

    pub async fn open(path: String, flush_interval: Duration) -> Result<Self, RError> {
        let reader_f = Arc::new(std::fs::File::open(path.clone())?);
        let offset = reader_f.metadata()?.len();
        let writer_f = Arc::new(Mutex::new(
            OpenOptions::new().append(true).open(path).await?,
        ));
        let flush_task = Self::spawn_periodic_flush(writer_f.clone(), flush_interval);

        Ok(Self {
            writer_f,
            reader_f,
            offset,
            flush_task,
        })
    }

    fn spawn_periodic_flush(file: Arc<Mutex<File>>, flush_interval: Duration) -> JoinHandle<()> {
        tokio::spawn(async move {
            let flush_interval = flush_interval.max(Duration::from_millis(1));

            loop {
                tokio::time::sleep(flush_interval).await;

                let mut file = file.lock().await;
                if file
                    .flush()
                    .await
                    .inspect_err(|e| error!("failed to flush {}, flush task exist", e))
                    .is_err()
                {
                    break;
                }
            }
        })
    }
}

impl Drop for SingleFileRecordStore {
    fn drop(&mut self) {
        self.flush_task.abort();
    }
}

impl RecordStore for SingleFileRecordStore {
    async fn append(&mut self, r: Record) -> Result<u64, RError> {
        let start_offset = self.offset;
        let record_len = r.len_on_disk();

        let mut file = self.writer_f.lock().await;
        r.write_to(&mut file).await?;

        self.offset += record_len;

        Ok(start_offset)
    }

    async fn fetch(&self, offset: u64) -> Result<Record, RError> {
        Record::read_from(&self.reader_f, offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::io::AsyncWriteExt;
    use tokio::time::{Instant, sleep};

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

    struct TempFile {
        path: PathBuf,
    }

    impl TempFile {
        fn new(test_name: &str) -> Self {
            let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "record-store-{test_name}-{}-{id}.log",
                std::process::id()
            ));
            let _ = std::fs::remove_file(&path);

            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }

        fn path_string(&self) -> String {
            self.path.to_string_lossy().into_owned()
        }
    }

    impl Drop for TempFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    fn record(payload: &[u8]) -> Record {
        Record::new(BytesMut::from(payload))
    }

    async fn fetch_after_background_flush(
        store: &SingleFileRecordStore,
        offset: u64,
    ) -> Result<Record, RError> {
        let deadline = Instant::now() + Duration::from_secs(1);

        loop {
            match store.fetch(offset).await {
                Ok(record) => return Ok(record),
                Err(RError::IoError(io_err))
                    if io_err.kind() == std::io::ErrorKind::UnexpectedEof
                        && Instant::now() < deadline =>
                {
                    sleep(Duration::from_millis(5)).await;
                }
                Err(err) => return Err(err),
            }
        }
    }

    #[tokio::test]
    async fn appends_and_fetches_records_by_returned_offsets() {
        let file = TempFile::new("append-fetch");
        let mut store = SingleFileRecordStore::new(file.path_string(), Duration::from_millis(5))
            .await
            .unwrap();

        let first_offset = store.append(record(b"first")).await.unwrap();
        let second_offset = store.append(record(b"second")).await.unwrap();

        assert_eq!(first_offset, 0);
        assert_eq!(second_offset, RECORD_OVERHEAD_LEN + 5);
        assert_eq!(store.offset, second_offset + RECORD_OVERHEAD_LEN + 6);

        let first = fetch_after_background_flush(&store, first_offset)
            .await
            .unwrap();
        let second = fetch_after_background_flush(&store, second_offset)
            .await
            .unwrap();

        assert_eq!(first.payload(), b"first");
        assert_eq!(second.payload(), b"second");
    }

    #[tokio::test]
    async fn supports_empty_payloads() {
        let file = TempFile::new("empty-payload");
        let mut store = SingleFileRecordStore::new(file.path_string(), Duration::from_millis(5))
            .await
            .unwrap();

        let empty_offset = store.append(record(b"")).await.unwrap();
        let next_offset = store.append(record(b"next")).await.unwrap();

        assert_eq!(empty_offset, 0);
        assert_eq!(next_offset, RECORD_OVERHEAD_LEN);
        assert_eq!(
            fetch_after_background_flush(&store, empty_offset)
                .await
                .unwrap()
                .payload(),
            b""
        );
        assert_eq!(
            fetch_after_background_flush(&store, next_offset)
                .await
                .unwrap()
                .payload(),
            b"next"
        );
    }

    #[tokio::test]
    async fn fetch_past_written_data_returns_unexpected_eof() {
        let file = TempFile::new("fetch-eof");
        let mut store = SingleFileRecordStore::new(file.path_string(), DEFAULT_FLUSH_INTERVAL)
            .await
            .unwrap();
        store.append(record(b"payload")).await.unwrap();

        let err = store.fetch(RECORD_OVERHEAD_LEN + 7).await.unwrap_err();

        assert!(matches!(
            err,
            RError::IoError(ref io_err) if io_err.kind() == std::io::ErrorKind::UnexpectedEof
        ));
    }

    #[tokio::test]
    async fn rejects_records_with_invalid_magic_header() {
        let file = TempFile::new("invalid-magic");
        let mut writer = File::create(file.path()).await.unwrap();
        writer.write_u64(MAGIC_HEADER + 1).await.unwrap();
        writer.write_u64(0).await.unwrap();
        writer.flush().await.unwrap();

        let reader = std::fs::File::open(file.path()).unwrap();
        let err = Record::read_from(&reader, 0).unwrap_err();

        assert!(matches!(
            err,
            RError::InvalidMagicHeader {
                expected: MAGIC_HEADER,
                actual
            } if actual == MAGIC_HEADER + 1
        ));
    }

    #[tokio::test]
    async fn new_fails_when_target_file_already_exists() {
        let file = TempFile::new("already-exists");
        std::fs::File::create(file.path()).unwrap();

        let err = SingleFileRecordStore::new(file.path_string(), DEFAULT_FLUSH_INTERVAL)
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            RError::IoError(ref io_err) if io_err.kind() == std::io::ErrorKind::AlreadyExists
        ));
    }
}
