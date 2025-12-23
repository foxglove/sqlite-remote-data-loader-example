use std::convert::Infallible;
use std::num::NonZeroU16;

use axum::body::Body;
use axum::extract::Query;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Json, Router, routing::get};
use bytes::BufMut;
use chrono::{DateTime, TimeDelta, Utc};
use futures::{StreamExt, TryStreamExt};

use serde::{Deserialize, Serialize};
use serde_with::base64::Base64;
use serde_with::serde_as;

// The `Encode` trait is used to encode messages and produce schemas.
use foxglove::Encode;
// The `create_mcap_stream` helper is used to write MCAP messages to a streaming Axum response.
use foxglove::stream::create_mcap_stream;

mod manifest_types {
    //! This module contains all the types required to serialize the manifest. These use serde and
    //! serde_json and conform to the manifest JSON schema.

    use super::*;

    /// A schema describes the data that exists within a topic.
    ///
    /// It can be produced using the Foxglove SDK.
    #[serde_as]
    #[derive(Serialize, Clone)]
    #[serde(rename_all = "camelCase")]
    pub struct Schema {
        pub id: NonZeroU16,
        pub name: String,
        pub encoding: String,
        #[serde_as(as = "Base64")]
        pub data: Vec<u8>,
    }

    /// A topic has a name, message encoding and an optional schema.
    ///
    /// The message encoding and schema can be produced by the Foxglove SDK.
    #[derive(Serialize, Clone)]
    #[serde(rename_all = "camelCase")]
    pub struct Topic {
        pub name: String,
        pub message_encoding: String,
        pub schema_id: Option<NonZeroU16>,
    }

    /// A source that the remote data can load to service requests from the Foxglove app.
    ///
    /// It must contain a list of topics the source contains and their schemas, as well as the
    /// start and end times of the underlying data.
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Source {
        pub topics: Vec<Topic>,
        pub schemas: Vec<Schema>,
        pub url: String,
        pub start_time: chrono::DateTime<Utc>,
        pub end_time: chrono::DateTime<Utc>,
    }

    /// The manifest as returned by the manifest endpoint.
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Manifest {
        pub sources: Vec<Source>,
    }
}

/// These query params are passed by the Foxglove app, through the remote data loader and to the
/// manifest endpoint. They are used to return the appropriate sources to service whatever
/// data is requested through the params.
///
/// For this example we have a single param called "recording". It will be configured through the
/// app by adding `ds.recording=` to the URL.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ManifestQueryParams {
    recording: String,
}

// These query parameters are an implementation detail of this upstream API. They are used for
// constructing the source URL.
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct SourceUrlQueryParams {
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
/// This manifest returns separate source URLs for each topic in 10s chunks to demonstrate how a
/// single recording could be split up into multiple source URLs. It is recommended to split up
/// your recording into reasonable time ranges so that only the data that is needed is downloaded
/// by the connector. You may also group topics that are commonly used in the same layouts
/// together, or split topics out that are quite large into their own source URL.
async fn get_manifest(
    Query(params): Query<ManifestQueryParams>,
) -> Result<Json<manifest_types::Manifest>, Response> {
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
        let mut current_start_time = DateTime::from_timestamp_nanos(start_nanos as _);
        let end_time = DateTime::from_timestamp_nanos(end_nanos as _);

        loop {
            // We're splitting up each recording into chunks of 10s to that the Foxglove connector
            // can avoid downloading data that it doesn't need.
            let chunk_end_time = (current_start_time + TimeDelta::seconds(10)).min(end_time);

            let params = serde_url_params::to_string(&SourceUrlQueryParams {
                topic_name: topic_name.clone(),
                recording_name: recording.clone(),
                start_time: current_start_time,
                end_time: chunk_end_time,
            })
            .map_err(|_| internal_error("failed to serialize url params"))?;

            // Add the 10s chunk of data to the sources array to be returned in the manifest.
            sources.push(manifest_types::Source {
                topics: vec![manifest_types::Topic {
                    name: format!("/{topic_name}"),
                    message_encoding: message_encoding.clone(),
                    schema_id: Some(schema_id),
                }],
                // The schemas for the source can be produced using the schema from the SDK.
                schemas: vec![manifest_types::Schema {
                    id: schema_id,
                    name: schema.name.clone(),
                    data: schema.data.to_vec(),
                    encoding: schema.encoding.clone(),
                }],
                start_time: current_start_time,
                end_time: chunk_end_time,
                // This project expects the server to be hosted at localhost:3000. This should be a
                // hostname that the remote data loader can reach from your cluster.
                url: format!("http://localhost:3000/data?{params}"),
            });

            current_start_time = chunk_end_time + TimeDelta::nanoseconds(1);

            if current_start_time > end_time {
                break;
            }
        }
    }

    Ok(Json(manifest_types::Manifest { sources }))
}

/// The simple schema returned by this API.
///
/// In practice, you'll have multiple different message types / schemas that likely use the Foxglove
/// built in message schemas.
#[derive(foxglove::Encode)]
struct Point {
    value: f64,
}

/// This endpoint is the "source URL" provided by the manifest. It returns a streaming MCAP
/// response that will be ingested by the remote data loader.
async fn get_data(Query(params): Query<SourceUrlQueryParams>) -> Result<Response, Response> {
    let SourceUrlQueryParams {
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

    // Create an `McapStream` that can be returned from this handler.
    let (mut handle, stream) = create_mcap_stream();

    tokio::spawn(async move {
        let channel = handle
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

        while let Some(next) = rows.next().await {
            let (timestamp, value) = next.expect("failed to read row");

            channel.log_with_time(&Point { value }, timestamp);

            // Only flush the buffer if enough data has accumulated. There's no point in flushing
            // every individual message.
            if handle.buffer_size() > 4096 {
                let res = handle.flush().await;

                // If flushing the handle causes an error it is due to the stream being closed.
                // Stop writing as we won't be able to flush any more data.
                if res.is_err() {
                    return;
                }
            }
        }

        let _ = handle.close().await;
    });

    Ok((
        StatusCode::OK,
        Body::from_stream(stream.map(Ok::<_, Infallible>)),
    )
        .into_response())
}

#[tokio::main]
async fn main() {
    let app = Router::new()
        // The manifest endpoint required by the remote data loader
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
