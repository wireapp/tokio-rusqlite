//! Connection owner that runs every call inline on the current thread.
//!
//! This backend is selected on targets whose operating system is unknown,
//! most notably `wasm32-unknown-unknown`, where threads cannot be spawned.
//! Instead of moving the connection onto a background thread, it keeps the
//! connection in an [`Rc<RefCell<Option<_>>>`] and runs each submitted
//! function synchronously.
//!
//! On these targets there is only a single thread, so running inline neither
//! blocks a *separate* executor thread (there is none) nor risks a deadlock:
//! every function runs to completion before control returns, so the [`RefCell`]
//! is never borrowed across an `.await`, and a second task cannot observe the
//! borrow. The connection is wrapped in an [`Option`] so that [`Handle::close`]
//! can consume it.
//!
//! The resulting [`Handle`] is `!Send + !Sync`, which is acceptable for a
//! single-threaded target. A [`crate::Connection`] built on it is therefore
//! also `!Send + !Sync`, and is meant to be driven by a single-threaded
//! (`spawn_local`-style) executor.
//!
//! Unlike the threaded backend, SQLite work runs on the calling thread;
//! there is nowhere else to run it.

use std::cell::RefCell;
use std::rc::Rc;

use crate::{CallFn, Closed};

/// Shared owner of an in-process connection.
///
/// Clones share the same connection through the [`Rc`]; the connection lives
/// until the last `Handle` is dropped or it is explicitly closed.
//
// Note that `Rc` is always `!Send + !Sync`; we can never move or share this
// type between threads. But of course, this type only ever exists where threads
// aren't an option in the first place.
#[derive(Clone)]
pub(crate) struct Handle {
    conn: Rc<RefCell<Option<rusqlite::Connection>>>,
}

impl Handle {
    /// Wrap an already-open connection in a handle.
    pub(crate) fn from_connection(conn: rusqlite::Connection) -> Self {
        Self {
            conn: Rc::new(RefCell::new(Some(conn))),
        }
    }

    /// Run `function` against the connection immediately.
    ///
    /// Unlike the threaded backend, the function has already finished by the
    /// time this returns. Fails with [`Closed`] if the connection has already
    /// been closed.
    pub(crate) fn submit(&self, function: CallFn) -> std::result::Result<(), Closed> {
        let mut conn = self.conn.borrow_mut();
        let conn = conn.as_mut().ok_or(Closed)?;
        function(conn);
        Ok(())
    }

    /// Close the connection, consuming it.
    ///
    /// Closing an already-closed connection reports success. On a SQLite-level
    /// failure the connection is put back so that a later retry can succeed,
    /// and the error is returned.
    pub(crate) async fn close(&self) -> rusqlite::Result<()> {
        let Some(conn) = self.conn.borrow_mut().take() else {
            return Ok(());
        };

        conn.close().map_err(|(conn, e)| {
            *self.conn.borrow_mut() = Some(conn);
            e
        })
    }
}

/// Open a connection with `open` and wrap it in a [`Handle`].
///
/// The connection is opened synchronously on the current thread, so the
/// returned future is always immediately ready. Returns the error from `open`
/// if it fails.
pub(crate) async fn start<F>(open: F) -> rusqlite::Result<Handle>
where
    F: FnOnce() -> rusqlite::Result<rusqlite::Connection>,
{
    Ok(Handle::from_connection(open()?))
}
