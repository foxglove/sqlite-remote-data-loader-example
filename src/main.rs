use std::convert::Infallible;
use std::num::NonZeroU16;

use axum::body::Body;
use axum::extract::Query;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Json, Router, routing::get};
use chrono::{DateTime, TimeDelta, Utc};
use futures::StreamExt;

use serde::{Deserialize, Serialize};
use serde_with::base64::Base64;
use serde_with::serde_as;

// The `Encode` trait is used to encode messages and produce schemas.
use foxglove::Encode;
// The `create_mcap_stream` helper is used to write MCAP messages to a streaming Axum response.
use foxglove::stream::create_mcap_stream;

use influxdb2::Client;
use influxdb2::models::Query as FluxQuery;
use influxdb2_structmap::value::Value;

// ---------------------------------------------------------------------------
// InfluxDB configuration – matches docker-compose.yml defaults
// ---------------------------------------------------------------------------
const INFLUXDB_URL: &str = "http://localhost:8086";
const INFLUXDB_ORG: &str = "myorg";
const INFLUXDB_TOKEN: &str = "my-super-secret-token";
const INFLUXDB_BUCKET: &str = "mybucket";

fn influx_client() -> Client {
    Client::new(INFLUXDB_URL, INFLUXDB_ORG, INFLUXDB_TOKEN)
}

// ---------------------------------------------------------------------------
// Manifest types – these conform to the Foxglove remote data loader manifest
// JSON schema.
// ---------------------------------------------------------------------------
mod manifest_types {
    use super::*;

    /// A schema describes the data that exists within a topic.
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
    #[derive(Serialize, Clone)]
    #[serde(rename_all = "camelCase")]
    pub struct Topic {
        pub name: String,
        pub message_encoding: String,
        pub schema_id: Option<NonZeroU16>,
    }

    /// A source returned inside the manifest.
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

// ---------------------------------------------------------------------------
// Query parameter types
// ---------------------------------------------------------------------------

/// Manifest query params – passed by the Foxglove app via `ds.measurement=…` in the URL.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ManifestQueryParams {
    measurement: String,
}

/// Source URL query params – these are internal to this upstream API and encode what data a
/// particular source URL should return.
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct SourceUrlQueryParams {
    measurement: String,
    field: String,
    start_time: chrono::DateTime<Utc>,
    end_time: chrono::DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn internal_error(msg: impl Into<String>) -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, msg.into()).into_response()
}

/// Extract a `DateTime<Utc>` from a FluxRecord's `_time` value.
fn extract_time(record: &influxdb2::api::query::FluxRecord) -> Option<DateTime<Utc>> {
    match record.values.get("_time")? {
        Value::TimeRFC(dt) => Some(dt.with_timezone(&Utc)),
        _ => None,
    }
}

/// Extract a `f64` from a FluxRecord's `_value` value.
fn extract_f64_value(record: &influxdb2::api::query::FluxRecord) -> Option<f64> {
    record.values.get("_value")?.f64()
}

/// Extract a string from a FluxRecord's `_value` value.
#[allow(dead_code)]
fn extract_string_value(record: &influxdb2::api::query::FluxRecord) -> Option<String> {
    record.values.get("_value")?.string()
}

// ---------------------------------------------------------------------------
// GET /manifest
// ---------------------------------------------------------------------------

/// Returns a manifest for the requested InfluxDB measurement.
///
/// Each _field_ in the measurement becomes a separate Foxglove topic (e.g.
/// `/airSensors/temperature`). The time range is split into 60-second chunks so the Foxglove
/// connector can avoid downloading data it doesn't need.
async fn get_manifest(
    Query(params): Query<ManifestQueryParams>,
) -> Result<Json<manifest_types::Manifest>, Response> {
    let ManifestQueryParams { measurement } = params;
    let client = influx_client();

    // --- Discover the field names for this measurement ---------------------------
    // The influxdb2 crate has a built-in helper that uses the Flux `schema` package.
    let field_names = client
        .list_measurement_field_keys(INFLUXDB_BUCKET, &measurement, Some("0"), None)
        .await
        .map_err(|e| {
            eprintln!("Failed to query field keys: {e}");
            internal_error("failed to query field keys from InfluxDB")
        })?;

    if field_names.is_empty() {
        return Err(internal_error(format!(
            "no fields found for measurement \"{measurement}\""
        )));
    }

    // --- Determine the time bounds of the data -----------------------------------
    let first_query = FluxQuery::new(format!(
        r#"from(bucket: "{INFLUXDB_BUCKET}")
  |> range(start: 0)
  |> filter(fn: (r) => r._measurement == "{measurement}")
  |> group()
  |> sort(columns: ["_time"], desc: false)
  |> limit(n: 1)
  |> keep(columns: ["_time"])"#
    ));
    let last_query = FluxQuery::new(format!(
        r#"from(bucket: "{INFLUXDB_BUCKET}")
  |> range(start: 0)
  |> filter(fn: (r) => r._measurement == "{measurement}")
  |> group()
  |> sort(columns: ["_time"], desc: true)
  |> limit(n: 1)
  |> keep(columns: ["_time"])"#
    ));

    let (first_records, last_records) = tokio::try_join!(
        async {
            client.query_raw(Some(first_query)).await.map_err(|e| {
                eprintln!("Failed to query first time: {e}");
                internal_error("failed to query time bounds")
            })
        },
        async {
            client.query_raw(Some(last_query)).await.map_err(|e| {
                eprintln!("Failed to query last time: {e}");
                internal_error("failed to query time bounds")
            })
        },
    )?;

    let start_time = first_records
        .first()
        .and_then(extract_time)
        .ok_or_else(|| internal_error("no data found for measurement (start)"))?;

    let end_time = last_records
        .first()
        .and_then(extract_time)
        .ok_or_else(|| internal_error("no data found for measurement (end)"))?;

    // --- Build the manifest ------------------------------------------------------
    let schema = Point::get_schema().expect("Point should have a schema");
    let message_encoding = Point::get_message_encoding();
    let schema_id = NonZeroU16::new(1).unwrap();

    let mut sources = Vec::new();

    for field_name in &field_names {
        let mut current_start = start_time;

        loop {
            // Split data into 60-second chunks so only the needed data is downloaded.
            let chunk_end = (current_start + TimeDelta::seconds(60)).min(end_time);

            let url_params = serde_url_params::to_string(&SourceUrlQueryParams {
                measurement: measurement.clone(),
                field: field_name.clone(),
                start_time: current_start,
                end_time: chunk_end,
            })
            .map_err(|_| internal_error("failed to serialize url params"))?;

            sources.push(manifest_types::Source {
                topics: vec![manifest_types::Topic {
                    name: format!("/{measurement}/{field_name}"),
                    message_encoding: message_encoding.clone(),
                    schema_id: Some(schema_id),
                }],
                schemas: vec![manifest_types::Schema {
                    id: schema_id,
                    name: schema.name.clone(),
                    data: schema.data.to_vec(),
                    encoding: schema.encoding.clone(),
                }],
                start_time: current_start,
                end_time: chunk_end,
                // The remote data loader must be able to reach this URL.
                url: format!("http://localhost:3000/data?{url_params}"),
            });

            current_start = chunk_end + TimeDelta::nanoseconds(1);
            if current_start > end_time {
                break;
            }
        }
    }

    Ok(Json(manifest_types::Manifest { sources }))
}

// ---------------------------------------------------------------------------
// The simple schema for every data point we return.
// ---------------------------------------------------------------------------
#[derive(foxglove::Encode)]
struct Point {
    value: f64,
}

// ---------------------------------------------------------------------------
// GET /data
// ---------------------------------------------------------------------------

/// Returns a streaming MCAP response for a given measurement + field + time range.
///
/// This is the "source URL" provided in the manifest. The Foxglove remote data loader calls it
/// to fetch the actual data.
async fn get_data(Query(params): Query<SourceUrlQueryParams>) -> Result<Response, Response> {
    let SourceUrlQueryParams {
        measurement,
        field,
        start_time,
        end_time,
    } = params;

    // Create an `McapStream` that can be returned from this handler.
    let (mut handle, stream) = create_mcap_stream();

    tokio::spawn(async move {
        let channel = handle
            .channel_builder(format!("/{measurement}/{field}"))
            .build::<Point>();

        let client = influx_client();

        let data_query = FluxQuery::new(format!(
            r#"from(bucket: "{INFLUXDB_BUCKET}")
  |> range(start: {start}, stop: {stop})
  |> filter(fn: (r) => r._measurement == "{measurement}" and r._field == "{field}")
  |> sort(columns: ["_time"])"#,
            start = start_time.to_rfc3339(),
            stop = end_time.to_rfc3339(),
        ));

        let records = match client.query_raw(Some(data_query)).await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Failed to query data: {e}");
                return;
            }
        };

        for record in &records {
            let Some(time) = extract_time(record) else {
                continue;
            };
            let Some(value) = extract_f64_value(record) else {
                continue;
            };

            let timestamp_nanos = time.timestamp_nanos_opt().unwrap_or(0) as u64;
            channel.log_with_time(&Point { value }, timestamp_nanos);

            // Only flush when enough data has accumulated – no point flushing every message.
            if handle.buffer_size() > 4096 {
                if handle.flush().await.is_err() {
                    return; // Stream closed – stop writing.
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

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let app = Router::new()
        // The manifest endpoint required by the remote data loader.
        // Query with ?measurement=airSensors (or whatever measurement your data uses).
        .route("/manifest", get(get_manifest))
        // The data that backs the "source url" provided in the manifest.
        .route("/data", get(get_data));

    println!("Listening on http://127.0.0.1:3000");
    println!("Try: curl 'http://localhost:3000/manifest?measurement=airSensors'");

    axum::serve(
        tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap(),
        app,
    )
    .await
    .unwrap();
}
