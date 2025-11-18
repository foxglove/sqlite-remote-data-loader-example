use std::time::Duration;

use asqlite::convert::ParamList;

const CHUNK_SIZE: usize = 1_000;
const CHUNK_COUNT: usize = 100;

#[tokio::main]
async fn main() {
    let _ = std::fs::remove_file("signals.db");

    let mut client = asqlite::Connection::builder()
        .create(true)
        .write(true)
        .open("signals.db")
        .await
        .expect("failed to open db");

    client
        .execute(
            "CREATE TABLE recordings (id INTEGER PRIMARY KEY, name TEXT)",
            (),
        )
        .await
        .expect("failed to create table");

    client
        .execute(
            "CREATE TABLE topics (id INTEGER PRIMARY KEY, name TEXT)",
            (),
        )
        .await
        .expect("failed to create table");

    client
        .execute(
            "CREATE TABLE signals (timestamp INTEGER, value REAL, topic_id INTEGER, recording_id INTEGER, FOREIGN KEY (topic_id) REFERENCES topics(id), FOREIGN KEY (recording_id) REFERENCES recordings(id))",
            (),
        )
        .await
        .expect("failed to create table");

    client
        .insert("INSERT INTO topics VALUES (1, \"sin\"), (2, \"cos\")", ())
        .await
        .expect("failed to insert topics");

    client
        .insert(
            "INSERT INTO recordings VALUES (1, \"first-recording\"), (2, \"second-recording\")",
            (),
        )
        .await
        .expect("failed to insert recordings");

    let mut stmt_text = String::from("INSERT INTO signals VALUES ");
    stmt_text.reserve("(?,?,?,?),".len() * CHUNK_SIZE);

    for i in 0..(CHUNK_SIZE * 2) {
        stmt_text += "(?,?,?,?)";

        if (i + 1) != (CHUNK_SIZE * 2) {
            stmt_text += ",";
        }
    }

    let stmt = client
        .prepare(&stmt_text, true)
        .await
        .expect("failed to prepare statement");

    let mut point = 0;

    for recording in 1..=2 {
        for _ in 0..CHUNK_COUNT {
            let mut builder = ParamList::builder(CHUNK_SIZE * 8);

            for _ in 0..CHUNK_SIZE {
                let timestamp = Duration::from_millis(point as u64).as_nanos() as u64;

                let sin_value = ((point as f64) / 10.).sin() * 10.;
                let cos_value = ((point as f64) / 10.).cos() * 10.;

                builder = builder
                    .param(timestamp)
                    .param(sin_value)
                    .param(1)
                    .param(recording)
                    .param(timestamp)
                    .param(cos_value)
                    .param(2)
                    .param(recording);

                point += 1;
            }

            client
                .insert(&stmt, builder.build())
                .await
                .expect("failed to insert points");
        }
    }
}
