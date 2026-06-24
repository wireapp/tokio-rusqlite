//! Unit tests, split by the capabilities of the target.
//!
//! The threaded backend (any target with a known OS) is driven with
//! `#[tokio::test]`. The inline backend — selected on `target_os = "unknown"`,
//! e.g. `wasm32-unknown-unknown` — yields a `!Send` [`crate::Connection`] and
//! cannot use tokio, so it is exercised with `wasm-bindgen-test` instead.

#[cfg(not(target_os = "unknown"))]
mod native;

#[cfg(target_os = "unknown")]
mod wasm;
