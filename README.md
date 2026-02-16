# sqlite-remote-data-loader-example

This is an example remote data loader upstream API that connects to an SQLite database.

## Running

Start by running the seed binary to create a database with test data:

```sh
cargo run --bin seed --release
```

This will create a database at `signals.db`.

With this database created, start the server with the following:

```sh
cargo run --bin server --release
```

This will launch the server on `localhost:3000`.

### Visualize Data

To view this data in Foxglove, start a `remote-data-loader` on `localhost:8080` and view recordings with:

```
https://app.foxglove.dev/~/view?ds=experimental-remote-data-loader&ds.dataLoaderUrl=http://localhost:8080/&ds.recording=first-recording)
```

How to run it

# 1. Start InfluxDBdocker compose up -d# 2. Seed with sample data./scripts/seed.sh# 3. Run the servercargo run --bin server# 4. Test the manifestcurl 'http://localhost:3000/manifest?measurement=airSensors'
