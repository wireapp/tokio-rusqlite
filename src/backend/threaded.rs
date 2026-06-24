//! Connection owner backed by a dedicated background thread.
//!
//! This is the default backend: it is selected on every target whose operating
//! system is known (`#[cfg(not(target_os = "unknown"))]`).
//!
//! [`rusqlite::Connection`] is [`Send`] but not [`Sync`], and its calls block
//! the calling thread until SQLite returns. To keep the async executor free,
//! this backend moves the connection onto a thread of its own and serializes
//! access by funneling every request through an unbounded channel. The
//! background thread owns the connection outright.
//!
//! The [`Handle`] returned to the rest of the crate is just the sending half of
//! that channel: it is cheap to clone and is `Send + Sync`, so a
//! [`crate::Connection`] built on it can be shared freely across a
//! multi-threaded runtime.

use crossbeam_channel::{Receiver, Sender};
use futures_channel::oneshot;
use std::thread;

use crate::{CallFn, Closed, BUG_TEXT};

/// A message sent from a [`Handle`] to the connection's background thread.
enum Message {
    /// Run the boxed function against the connection.
    Execute(CallFn),
    /// Close the connection, reporting the result back through the channel.
    Close(oneshot::Sender<std::result::Result<(), rusqlite::Error>>),
}

/// The sending half of the channel to a connection's background thread.
///
/// Cloning a `Handle` yields another sender to the same thread; the thread (and
/// the connection it owns) lives until every `Handle` is dropped or the
/// connection is explicitly closed.
#[derive(Clone)]
pub(crate) struct Handle {
    sender: Sender<Message>,
}

impl Handle {
    /// Build a handle that serves an already-open connection from a new thread.
    pub(crate) fn from_connection(conn: rusqlite::Connection) -> Self {
        let (sender, receiver) = crossbeam_channel::unbounded::<Message>();
        thread::spawn(move || event_loop(conn, receiver));

        Self { sender }
    }

    /// Queue `function` to run on the background thread.
    ///
    /// Returns immediately; the function runs once the thread reaches it in the
    /// queue. Fails with [`Closed`] if the connection has already been closed.
    pub(crate) fn submit(&self, function: CallFn) -> std::result::Result<(), Closed> {
        self.sender
            .send(Message::Execute(function))
            .map_err(|_| Closed)
    }

    /// Close the connection, consuming it on the background thread.
    ///
    /// If the channel is already closed then the connection was closed by a
    /// previous call (possibly through a clone of this handle), which we report
    /// as success. On a SQLite-level failure the connection is retained by the
    /// thread so a later retry can succeed, and the error is returned.
    pub(crate) async fn close(&self) -> rusqlite::Result<()> {
        let (sender, receiver) = oneshot::channel();

        if self.sender.send(Message::Close(sender)).is_err() {
            // The thread is gone, so the connection has already been closed.
            return Ok(());
        }

        // A receive error likewise means the thread ended and the connection
        // was closed in the meantime.
        receiver.await.unwrap_or(Ok(()))
    }
}

/// Spawn a background thread that opens a connection with `open` and serves
/// requests against it.
///
/// The connection is opened *on the spawned thread* so that a slow open never
/// blocks the calling task; the outcome of opening is reported back through a
/// oneshot before the thread enters its serving loop. Returns the error from
/// `open` if it fails.
pub(crate) async fn start<F>(open: F) -> rusqlite::Result<Handle>
where
    F: FnOnce() -> rusqlite::Result<rusqlite::Connection> + Send + 'static,
{
    let (sender, receiver) = crossbeam_channel::unbounded::<Message>();
    let (result_sender, result_receiver) = oneshot::channel();

    thread::spawn(move || {
        let conn = match open() {
            Ok(c) => c,
            Err(e) => {
                let _ = result_sender.send(Err(e));
                return;
            }
        };

        if let Err(_e) = result_sender.send(Ok(())) {
            return;
        }

        event_loop(conn, receiver);
    });

    result_receiver
        .await
        .expect(BUG_TEXT)
        .map(|_| Handle { sender })
}

/// Serve requests against `conn` until every [`Handle`] is dropped or the
/// connection is successfully closed.
fn event_loop(mut conn: rusqlite::Connection, receiver: Receiver<Message>) {
    while let Ok(message) = receiver.recv() {
        match message {
            Message::Execute(f) => f(&mut conn),
            Message::Close(s) => {
                let result = conn.close();

                match result {
                    Ok(v) => {
                        s.send(Ok(v)).expect(BUG_TEXT);
                        break;
                    }
                    Err((c, e)) => {
                        conn = c;
                        s.send(Err(e)).expect(BUG_TEXT);
                    }
                }
            }
        }
    }
}
