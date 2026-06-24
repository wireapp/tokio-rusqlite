//! Basic functionality tests for the inline backend on thread-less targets
//! (`target_os = "unknown"`, most notably `wasm32-unknown-unknown`).
//!
//! These run under `wasm-bindgen-test` rather than `#[tokio::test]`: the inline
//! backend yields a `!Send` [`Connection`], so it must be driven by a
//! single-threaded executor, which `wasm-bindgen-test`'s async support
//! provides.
//!
//! # Running
//!
//! These compile for `wasm32-unknown-unknown`, but *running* them needs two
//! things this crate does not provide on its own:
//!
//! - a wasm test runner (`wasm-bindgen-test-runner`, typically via `wasm-pack
//!   test --node` or `--headless --firefox`); and
//! - a SQLite build that actually links for `wasm32-unknown-unknown`. The tests
//!   below use only in-memory databases, which avoid any external VFS; a
//!   persistent IndexedDB-backed VFS additionally requires a browser
//!   environment (add `wasm_bindgen_test_configure!(run_in_browser);`).

use crate::*;
use wasm_bindgen_test::wasm_bindgen_test;

/// Opening an in-memory database succeeds and spins up the inline backend.
#[wasm_bindgen_test]
async fn open_in_memory_works() {
    assert!(Connection::open_in_memory().await.is_ok());
}

/// A `call` runs against the connection and returns its result.
#[wasm_bindgen_test]
async fn call_runs_on_the_connection() {
    let conn = Connection::open_in_memory().await.expect("open");

    let affected = conn
        .call(|conn| {
            conn.execute(
                "CREATE TABLE person (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
                [],
            )
        })
        .await
        .expect("create table");

    assert_eq!(affected, 0);
}

/// A full insert-then-query round trip works end to end.
#[wasm_bindgen_test]
async fn insert_and_query_round_trip() {
    let conn = Connection::open_in_memory().await.expect("open");

    let names = conn
        .call(|conn| {
            conn.execute(
                "CREATE TABLE person (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
                [],
            )?;
            conn.execute("INSERT INTO person (name) VALUES (?1)", ["Steven"])?;

            let mut stmt = conn.prepare("SELECT name FROM person")?;
            let names = stmt
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<std::result::Result<Vec<String>, rusqlite::Error>>()?;
            Ok::<_, rusqlite::Error>(names)
        })
        .await
        .expect("round trip");

    assert_eq!(names, vec!["Steven".to_string()]);
}

/// `call_raw` reports the closure's value without wrapping the error type.
#[wasm_bindgen_test]
async fn call_raw_reports_values() {
    let conn = Connection::open_in_memory().await.expect("open");

    let ok = conn.call_raw(|_| Ok::<(), ()>(())).await.expect("call");
    assert!(ok.is_ok());

    let err = conn.call_raw(|_| Err::<(), ()>(())).await.expect("call");
    assert!(err.is_err());
}

/// Closing a connection succeeds.
#[wasm_bindgen_test]
async fn close_succeeds() {
    let conn = Connection::open_in_memory().await.expect("open");
    assert!(conn.close().await.is_ok());
}

/// Closing a second handle to an already-closed connection also succeeds.
#[wasm_bindgen_test]
async fn double_close_is_ok() {
    let conn = Connection::open_in_memory().await.expect("open");
    let conn2 = conn.clone();

    assert!(conn.close().await.is_ok());
    assert!(conn2.close().await.is_ok());
}

/// Calling after the connection has been closed reports `ConnectionClosed`.
///
/// This exercises the inline backend's `take()`/`None` path: once one handle
/// closes the connection, a clone observes the empty slot rather than a live
/// connection.
#[wasm_bindgen_test]
async fn call_after_close_reports_closed() {
    let conn = Connection::open_in_memory().await.expect("open");
    let conn2 = conn.clone();

    conn.close().await.expect("close");

    let result = conn2.call(|conn| conn.execute("SELECT 1", [])).await;
    assert!(matches!(result.unwrap_err(), Error::ConnectionClosed));
}
