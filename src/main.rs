use std::collections::BTreeMap;
use std::convert::Infallible;
use std::io::{BufWriter, SeekFrom, Write};
use std::num::NonZeroU16;
use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::extract::Query;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Json, Router, routing::get};
use bytes::{BufMut, Bytes, BytesMut};
use chrono::{DateTime, Utc};
use futures::{SinkExt, StreamExt};
use mcap::Summary;
use mcap::records::ChunkIndex;
use mcap::write::NoSeek;
use serde::{Deserialize, Serialize};
use serde_with::base64::Base64;
use serde_with::serde_as;

use mcap::sans_io::{SummaryReadEvent, SummaryReader, indexed_reader::*};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

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
    schema_id: Option<NonZeroU16>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Source {
    topics: Vec<Topic>,
    schemas: Vec<Schema>,
    data_url: String,
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
    name: String,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct DataQueryParams {
    name: String,
    start_time: chrono::DateTime<Utc>,
    end_time: chrono::DateTime<Utc>,
}

async fn read_summary(
    file: &mut tokio::io::BufReader<tokio::fs::File>,
) -> Result<Summary, Response> {
    let mut summary_reader = SummaryReader::new();

    while let Some(event) = summary_reader.next_event() {
        let event = event.map_err(|_| internal_error("summary reader returned error"))?;

        match event {
            SummaryReadEvent::ReadRequest(amt) => {
                let buf = summary_reader.insert(amt);
                let amt = file
                    .read_exact(buf)
                    .await
                    .map_err(|_| internal_error("failed to read summary bytes"))?;
                summary_reader.notify_read(amt);
            }
            SummaryReadEvent::SeekRequest(pos) => {
                let pos = file
                    .seek(pos)
                    .await
                    .map_err(|_| internal_error("failed to seek summary"))?;
                summary_reader.notify_seeked(pos);
            }
        }
    }

    let summary = summary_reader
        .finish()
        .ok_or_else(|| internal_error("missing summary"))?;

    Ok(summary)
}

async fn get_manifest(
    Query(params): Query<ManifestQueryParams>,
) -> Result<Json<Manifest>, Response> {
    let ManifestQueryParams { name } = params;

    let file = tokio::fs::File::open(format!("./files/{name}"))
        .await
        .map_err(|_| (StatusCode::NOT_FOUND).into_response())?;

    let mut file = tokio::io::BufReader::new(file);

    let mut summary = read_summary(&mut file).await?;

    // Sort chunks by the order they appear
    summary
        .chunk_indexes
        .sort_by(|a, b| a.chunk_start_offset.cmp(&b.chunk_start_offset));

    let mut topics = vec![];
    let mut schemas = vec![];

    for channel in summary.channels.values() {
        let schema_id = if let Some(schema) = &channel.schema {
            let id = NonZeroU16::new(schema.id).expect("schema id should never be zero");

            schemas.push(Schema {
                id,
                name: schema.name.clone(),
                data: schema.data.to_vec(),
                encoding: schema.encoding.clone(),
            });

            Some(id)
        } else {
            None
        };

        topics.push(Topic {
            name: channel.topic.clone(),
            schema_id,
        });
    }

    if summary.chunk_indexes.is_empty() {
        // No chunks no worries!
        return Ok(Json(Manifest { sources: vec![] }));
    };

    let mut current_chunks = vec![];
    let mut chunk_size = 0;

    let mut sources = vec![];

    fn chunks_to_source(
        name: String,
        schemas: Vec<Schema>,
        topics: Vec<Topic>,
        chunks: &Vec<&ChunkIndex>,
    ) -> Result<Source, Response> {
        let first = chunks.first().expect("there will be at least one chunk");
        let last = chunks.last().expect("there will be at least one chunk");

        let start_time = DateTime::from_timestamp_nanos(
            first
                .message_start_time
                .try_into()
                .map_err(|_| internal_error("failed to convert start time to i64"))?,
        );

        let end_time = DateTime::from_timestamp_nanos(
            last.message_end_time
                .try_into()
                .map_err(|_| internal_error("failed to convert end time to i64"))?,
        );

        let params = serde_url_params::to_string(&DataQueryParams {
            name: name.clone(),
            start_time,
            end_time,
        })
        .map_err(|_| internal_error("failed to serialize url params"))?;

        Ok(Source {
            start_time,
            end_time,
            topics,
            schemas,
            data_url: format!("/data?{params}"),
        })
    }

    for chunk in summary.chunk_indexes.iter() {
        current_chunks.push(chunk);
        chunk_size += chunk.chunk_length;

        // Break up into 20MB sources
        let max_size = 20 * 1024 * 1024;

        if chunk_size > max_size {
            let source = chunks_to_source(
                name.clone(),
                schemas.clone(),
                topics.clone(),
                &current_chunks,
            )?;
            sources.push(source);
        }
    }

    if !current_chunks.is_empty() {
        let source = chunks_to_source(
            name.clone(),
            schemas.clone(),
            topics.clone(),
            &current_chunks,
        )?;
        sources.push(source);
    }

    Ok(Json(Manifest { sources }))
}

fn internal_error(msg: impl Into<String>) -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, msg.into()).into_response()
}

#[derive(Clone, Default)]
struct SharedBuffer(Arc<Mutex<BytesMut>>);

impl SharedBuffer {
    fn take(&self) -> Bytes {
        let bytes = &mut *self.0.lock().expect("poisoned");
        bytes.split().freeze()
    }
}

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let bytes = &mut *self.0.lock().expect("poisoned");
        bytes.put_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

async fn get_data(Query(params): Query<DataQueryParams>) -> Result<Response, Response> {
    let DataQueryParams {
        name,
        start_time,
        end_time,
    } = params;

    let file = tokio::fs::File::open(format!("./files/{name}"))
        .await
        .map_err(|_| (StatusCode::NOT_FOUND).into_response())?;

    let mut file = tokio::io::BufReader::new(file);

    let summary = read_summary(&mut file).await?;

    let start = u64::try_from(
        start_time
            .timestamp_nanos_opt()
            .ok_or_else(|| internal_error("start time was invalid date"))?,
    )
    .map_err(|_| internal_error("start time was negative"))?;

    let end = u64::try_from(
        end_time
            .timestamp_nanos_opt()
            .ok_or_else(|| internal_error("end time was invalid date"))?,
    )
    .map_err(|_| internal_error("end time was negative"))?;

    let mut reader = IndexedReader::new_with_options(
        &summary,
        IndexedReaderOptions::new()
            .log_time_on_or_after(start)
            .log_time_before(end),
    )
    .map_err(|_| internal_error("failed to create reader"))?;

    let mut chunk_buffer = vec![];
    let shared_buffer = SharedBuffer::default();

    let mut writer = mcap::WriteOptions::new()
        .disable_seeking(true)
        .create(NoSeek::new(BufWriter::new(shared_buffer.clone())))
        .map_err(|_| internal_error("failed to create writer"))?;

    let mut old_channel_to_new_channel = BTreeMap::new();

    // Write all the required channels
    for channel in summary.channels.values() {
        let id = if let Some(schema) = &channel.schema {
            writer
                .add_schema(&schema.name, &schema.encoding, &schema.data)
                .map_err(|_| internal_error("failed to write schema"))?
        } else {
            0
        };

        let new_channel_id = writer
            .add_channel(
                id,
                &channel.topic,
                &channel.message_encoding,
                &channel.metadata,
            )
            .map_err(|_| internal_error("failed to write channel"))?;

        old_channel_to_new_channel.insert(channel.id, new_channel_id);
    }

    let (mut sender, recv) = futures::channel::mpsc::channel::<Bytes>(2);

    tokio::spawn(async move {
        let res: Result<(), Box<dyn std::error::Error>> = async {
            while let Some(event) = reader.next_event() {
                let bytes = shared_buffer.take();

                if !bytes.is_empty()
                    && let Err(_) = sender.send(bytes).await
                {
                    return Ok(());
                }

                let event = event?;

                match event {
                    IndexedReadEvent::Message { mut header, data } => {
                        let Some(new_channel_id) =
                            old_channel_to_new_channel.get(&header.channel_id)
                        else {
                            continue;
                        };

                        header.channel_id = *new_channel_id;
                        writer.write_to_known_channel(&header, data)?;
                    }
                    IndexedReadEvent::ReadChunkRequest { offset, length } => {
                        chunk_buffer.resize(length, 0);
                        file.seek(SeekFrom::Start(offset)).await?;
                        file.read_exact(&mut chunk_buffer).await?;
                        reader.insert_chunk_record_data(offset, &chunk_buffer[..])?;
                    }
                }
            }

            writer.finish()?;

            let bytes = shared_buffer.take();

            if !bytes.is_empty() {
                let _ = sender.send(bytes).await;
            }

            Ok(())
        }
        .await;

        if let Err(e) = res {
            eprintln!("err: {e}");
        }
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
        .route("/data", get(get_data))
        .route("/manifest", get(get_manifest));

    println!("Listening on http://127.0.0.1:3000");

    axum::serve(
        tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap(),
        app,
    )
    .await
    .unwrap();
}
