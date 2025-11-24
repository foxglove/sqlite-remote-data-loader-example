use std::convert::Infallible;
use std::io::{Seek, SeekFrom, Write};
use std::num::NonZeroU16;
use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::extract::Query;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Json, Router, routing::get};
use bytes::{BufMut, Bytes, BytesMut};
use chrono::{DateTime, Utc};
use futures::{SinkExt, StreamExt, TryStreamExt};

use serde::{Deserialize, Serialize};
use serde_with::base64::Base64;
use serde_with::serde_as;

use foxglove::{Context, Encode, McapWriteOptions, McapWriter};

#[serde_as]
#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct Schema {
    id: NonZeroU16,
    name: String,
    encoding: String,
    #[serde_as(as = "Base64")]
    data: Vec<u8>,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct Topic {
    name: String,
    message_encoding: String,
    schema_id: Option<NonZeroU16>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Source {
    topics: Vec<Topic>,
    schemas: Vec<Schema>,
    url: String,
    start_time: chrono::DateTime<Utc>,
    end_time: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Manifest {
    sources: Vec<Source>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ManifestQueryParams {
    recording: String,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct DataQueryParams {
    topic_name: String,
    recording_name: String,
    start_time: chrono::DateTime<Utc>,
    end_time: chrono::DateTime<Utc>,
}

fn internal_error(msg: impl Into<String>) -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, msg.into()).into_response()
}

/// This endpoint returns a manifest file for the requested recording.
///
/// This manifest returns separate source URLs for each topic to demonstrate how a single recording
/// could be split up into multiple source URLs. You might split up your recording by time range,
/// you may put topics that are commonly used in the same layouts together, or split out topics
/// that are quite large into their own source URL.
async fn get_manifest(
    Query(params): Query<ManifestQueryParams>,
) -> Result<Json<Manifest>, Response> {
    let ManifestQueryParams { recording } = params;

    let mut client = asqlite::Connection::builder()
        .create(false)
        .write(false)
        .open("signals.db")
        .await
        .expect("failed to open db");

    // Query all the topics for this recording and their start and end times.
    let topics = client.query::<(u64, u64, String)>(
        "select min(s.timestamp), max(s.timestamp), t.name FROM signals s
           INNER JOIN recordings r ON r.id = s.recording_id
           INNER JOIN topics t ON t.id = s.topic_id
           WHERE r.name = ?
           GROUP BY t.name
        ",
        asqlite::params!(&recording),
    );

    let topics = topics.try_collect::<Vec<_>>().await.map_err(|e| {
        eprintln!("failed to read manifest from db: {e}");
        internal_error("failed to read manifest from db")
    })?;

    let mut sources = vec![];

    // We use this Point schema created using the Foxglove SDK. It contains most of the fields required
    // by the manifest endpoint.
    let schema = Point::get_schema().expect("point should have schema");

    let message_encoding = Point::get_message_encoding();

    for (start_nanos, end_nanos, topic_name) in topics.into_iter() {
        // The schema_id on the topic needs to match the id on the schemas array in the manifest.
        // This repo only uses a single schema - so just hard code this to one.
        let schema_id = NonZeroU16::new(1).unwrap();

        // We're using nanoseconds in our sqlite database, but the manifest expects ISO timestamps.
        let start_time = DateTime::from_timestamp_nanos(start_nanos as _);
        let end_time = DateTime::from_timestamp_nanos(end_nanos as _);

        let params = serde_url_params::to_string(&DataQueryParams {
            topic_name: topic_name.clone(),
            recording_name: recording.clone(),
            start_time,
            end_time,
        })
        .map_err(|_| internal_error("failed to serialize url params"))?;

        sources.push(Source {
            topics: vec![Topic {
                name: format!("/{topic_name}"),
                message_encoding: message_encoding.clone(),
                schema_id: Some(schema_id),
            }],
            schemas: vec![Schema {
                id: schema_id,
                name: schema.name.clone(),
                data: schema.data.to_vec(),
                encoding: schema.encoding.clone(),
            }],
            start_time,
            end_time,
            // This project expects the server to be hosted at localhost:3000. This should be a
            // hostname that the external connector can reach from your cluster.
            url: format!("http://localhost:3000/data?{params}"),
        })
    }

    Ok(Json(Manifest { sources }))
}

// This is some glue that makes it easier for the Foxglove MCAP writer to return bytes to Axum.
#[derive(Clone, Default)]
struct SharedBuffer {
    buffer: Arc<Mutex<BytesMut>>,
    pos: u64,
}

impl SharedBuffer {
    fn take(&self) -> Bytes {
        let bytes = &mut *self.buffer.lock().expect("poisoned");
        bytes.split().freeze()
    }
}

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let bytes = &mut *self.buffer.lock().expect("poisoned");
        bytes.put_slice(buf);
        self.pos += buf.len() as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

// We set the `disable_seeking` option on the MCAP writer to ensure that it doesn't seek. This impl
// makes sure it can still get the current position of the stream.
impl Seek for SharedBuffer {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        match pos {
            SeekFrom::Start(n) if self.pos == n => Ok(n),
            SeekFrom::Current(0) => Ok(self.pos),
            _ => Err(std::io::Error::other("seek on unseekable file")),
        }
    }
}

#[derive(foxglove::Encode)]
struct Point {
    value: f64,
}

async fn get_data(Query(params): Query<DataQueryParams>) -> Result<Response, Response> {
    let DataQueryParams {
        topic_name,
        recording_name,
        start_time,
        end_time,
    } = params;

    let start_time_nanos = u64::try_from(
        start_time
            .timestamp_nanos_opt()
            .ok_or_else(|| internal_error("failed to convert start time to nanos"))?,
    )
    .map_err(|_| internal_error("start time was negative"))?;

    let end_time_nanos = u64::try_from(
        end_time
            .timestamp_nanos_opt()
            .ok_or_else(|| internal_error("failed to convert end time to nanos"))?,
    )
    .map_err(|_| internal_error("end time was negative"))?;

    let (mut sender, recv) = futures::channel::mpsc::channel::<Bytes>(2);

    tokio::spawn(async move {
        let buffer = SharedBuffer::default();

        let context = Context::new();

        let writer = McapWriter::with_options(McapWriteOptions::new().disable_seeking(true))
            .context(&context)
            .create(buffer.clone())
            .expect("failed to create writer");

        let channel = context
            .channel_builder(format!("/{topic_name}"))
            .build::<Point>();

        let mut client = asqlite::Connection::builder()
            .create(false)
            .write(false)
            .open("signals.db")
            .await
            .expect("failed to open db");

        let mut rows = client.query::<(u64, f64)>(
            "SELECT s.timestamp, s.value FROM signals s
                INNER JOIN topics t ON t.id = s.topic_id
                INNER JOIN recordings r ON r.id = s.recording_id
                WHERE s.timestamp >= ? AND s.timestamp <= ? AND t.name = ? AND r.name = ?",
            asqlite::params!(start_time_nanos, end_time_nanos, topic_name, recording_name),
        );

        while let Some((timestamp, value)) =
            rows.next().await.transpose().expect("failed to read row")
        {
            channel.log_with_time(&Point { value }, timestamp);

            let bytes = buffer.take();

            if !bytes.is_empty() && sender.send(bytes).await.is_err() {
                return;
            };
        }

        writer.close().expect("failed to close writer");

        // If the context is dropped before the writer is closed it'll stop logging messages
        drop(context);

        let bytes = buffer.take();

        if !bytes.is_empty() {
            let _ = sender.send(bytes).await;
        }

        let _ = sender.flush().await;
    });

    Ok((
        StatusCode::OK,
        Body::from_stream(recv.map(Ok::<_, Infallible>)),
    )
        .into_response())
}

#[tokio::main]
async fn main() {
    let app = Router::new()
        // The manifest endpoint required by the external connector
        .route("/manifest", get(get_manifest))
        // The data that backs the "source url" provided in the manifest
        .route("/data", get(get_data));

    println!("Listening on http://127.0.0.1:3000");

    axum::serve(
        tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap(),
        app,
    )
    .await
    .unwrap();
}
