use std::time::Duration;

use bytes::BytesMut;
use record_store::{RError, Record, RecordStore, SingleFileRecordStore};

#[tokio::main]
async fn main() -> Result<(), RError> {
    let mut store =
        SingleFileRecordStore::new("./test.log".to_string(), Duration::from_secs(10)).await?;

    store.append(Record::new(BytesMut::from("aa"))).await?;

    Ok(())
}
