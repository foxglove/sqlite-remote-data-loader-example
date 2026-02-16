#!/bin/bash
#
# Downloads air-sensor sample data from the InfluxDB sample data repository
# (https://github.com/influxdata/influxdb2-sample-data) and loads it into InfluxDB.
#
# If the download fails (e.g. no internet), it falls back to generating synthetic
# air-sensor data locally.
#
# Usage:
#   ./scripts/seed.sh
#
# Environment variables (with defaults matching docker-compose.yml):
#   INFLUXDB_URL    - InfluxDB URL        (default: http://localhost:8086)
#   INFLUXDB_TOKEN  - Admin API token      (default: my-super-secret-token)
#   INFLUXDB_ORG    - Organization name    (default: myorg)
#   INFLUXDB_BUCKET - Bucket name          (default: mybucket)
#
set -euo pipefail

INFLUXDB_URL="${INFLUXDB_URL:-http://localhost:8086}"
INFLUXDB_TOKEN="${INFLUXDB_TOKEN:-my-super-secret-token}"
INFLUXDB_ORG="${INFLUXDB_ORG:-myorg}"
INFLUXDB_BUCKET="${INFLUXDB_BUCKET:-mybucket}"

DATA_URL="https://raw.githubusercontent.com/influxdata/influxdb2-sample-data/master/air-sensor-data/air-sensor-data.lp"

TEMP_FILE=$(mktemp)
trap "rm -f $TEMP_FILE" EXIT

# Wait for InfluxDB to be ready
echo "Waiting for InfluxDB to be ready at ${INFLUXDB_URL}..."
for i in $(seq 1 30); do
    if curl -sf "${INFLUXDB_URL}/health" > /dev/null 2>&1; then
        echo "InfluxDB is ready."
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "ERROR: InfluxDB did not become ready in time."
        echo "Make sure InfluxDB is running: docker compose up -d"
        exit 1
    fi
    sleep 1
done

echo ""
echo "Downloading air-sensor sample data from GitHub..."
echo "  URL: ${DATA_URL}"

if curl -fSL "$DATA_URL" -o "$TEMP_FILE" 2>/dev/null; then
    LINE_COUNT=$(wc -l < "$TEMP_FILE" | tr -d ' ')
    echo "  Downloaded ${LINE_COUNT} lines of line-protocol data."
else
    echo "  Download failed. Generating synthetic air-sensor data instead..."

    python3 -c "
import time, math, random

random.seed(42)
# Start 24 hours ago, generate 10-second interval samples
start = int(time.time()) - 86400

for i in range(8640):  # 24 hours at 10s intervals
    ts = (start + i * 10) * 1000000000  # nanosecond precision

    # Sensor TLM0100
    temp  = 72 + 5 * math.sin(i / 100.0) + random.gauss(0, 0.5)
    humid = 35 + 3 * math.cos(i / 100.0) + random.gauss(0, 0.3)
    co    = 0.5 + 0.2 * math.sin(i / 200.0) + abs(random.gauss(0, 0.05))
    print(f'airSensors,sensor_id=TLM0100 temperature={temp:.2f},humidity={humid:.2f},co={co:.4f} {ts}')

    # Sensor TLM0101
    temp2  = 70 + 4 * math.sin(i / 120.0) + random.gauss(0, 0.5)
    humid2 = 36 + 2 * math.cos(i / 80.0) + random.gauss(0, 0.3)
    co2    = 0.4 + 0.15 * math.sin(i / 150.0) + abs(random.gauss(0, 0.05))
    print(f'airSensors,sensor_id=TLM0101 temperature={temp2:.2f},humidity={humid2:.2f},co={co2:.4f} {ts}')
" > "$TEMP_FILE"

    LINE_COUNT=$(wc -l < "$TEMP_FILE" | tr -d ' ')
    echo "  Generated ${LINE_COUNT} lines of synthetic data."
fi

echo ""
echo "Writing data to InfluxDB..."
echo "  Org:    ${INFLUXDB_ORG}"
echo "  Bucket: ${INFLUXDB_BUCKET}"

# Write in chunks to avoid overwhelming the API with a single massive request
CHUNK_SIZE=5000
TOTAL_LINES=$(wc -l < "$TEMP_FILE" | tr -d ' ')
OFFSET=0

while [ "$OFFSET" -lt "$TOTAL_LINES" ]; do
    REMAINING=$((TOTAL_LINES - OFFSET))
    CURRENT_CHUNK=$((REMAINING < CHUNK_SIZE ? REMAINING : CHUNK_SIZE))

    sed -n "$((OFFSET + 1)),$((OFFSET + CURRENT_CHUNK))p" "$TEMP_FILE" | \
        curl -s -X POST "${INFLUXDB_URL}/api/v2/write?org=${INFLUXDB_ORG}&bucket=${INFLUXDB_BUCKET}&precision=ns" \
            -H "Authorization: Token ${INFLUXDB_TOKEN}" \
            -H "Content-Type: text/plain; charset=utf-8" \
            --data-binary @-

    OFFSET=$((OFFSET + CURRENT_CHUNK))
    echo "  Written ${OFFSET}/${TOTAL_LINES} lines..."
done

echo ""
echo "Done! Data loaded into InfluxDB."
echo ""
echo "You can verify the data with:"
echo "  curl -X POST '${INFLUXDB_URL}/api/v2/query?org=${INFLUXDB_ORG}' \\"
echo "    -H 'Authorization: Token ${INFLUXDB_TOKEN}' \\"
echo "    -H 'Content-Type: application/vnd.flux' \\"
echo "    -d 'from(bucket: \"${INFLUXDB_BUCKET}\") |> range(start: 0) |> limit(n: 5)'"
echo ""
echo "Start the server with: cargo run --bin server"
echo "Then open a manifest with: curl 'http://localhost:3000/manifest?measurement=airSensors'"
