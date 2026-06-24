//! Asynchronous handle for rusqlite library.
//!
//! # Guide
//!
//! This library provides [`Connection`] struct. [`Connection`] struct is a handle
//! to call functions in background thread and can be cloned cheaply.
//! [`Connection::call`] method calls provided function in the background thread
//! and returns its result asynchronously.
//!
//! # Design
//!
//! A thread is spawned for each opened connection handle. When `call` method
//! is called: provided function is boxed, sent to the thread through mpsc
//! channel and executed. Return value is then sent by oneshot channel from
//! the thread and then returned from function.
//!
//! # Example
//!
//! ```rust,no_run
//! use tokio_rusqlite::{params, Connection, Result};
//!
//! #[derive(Debug)]
//! struct Person {
//!     id: i32,
//!     name: String,
//!     data: Option<Vec<u8>>,
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let conn = Connection::open_in_memory().await?;
//!
//!     let people = conn
//!         .call(|conn| {
//!             conn.execute(
//!                 "CREATE TABLE person (
//!                     id    INTEGER PRIMARY KEY,
//!                     name  TEXT NOT NULL,
//!                     data  BLOB
//!                 )",
//!                 [],
//!             )?;
//!
//!             let steven = Person {
//!                 id: 1,
//!                 name: "Steven".to_string(),
//!                 data: None,
//!             };
//!
//!             conn.execute(
//!                 "INSERT INTO person (name, data) VALUES (?1, ?2)",
//!                 params![steven.name, steven.data],
//!             )?;
//!
//!             let mut stmt = conn.prepare("SELECT id, name, data FROM person")?;
//!             let people = stmt
//!                 .query_map([], |row| {
//!                     Ok(Person {
//!                         id: row.get(0)?,
//!                         name: row.get(1)?,
//!                         data: row.get(2)?,
//!                     })
//!                 })?
//!                 .collect::<std::result::Result<Vec<Person>, rusqlite::Error>>()?;
//!
//!             Ok(people)
//!         })
//!         .await?;
//!
//!     for person in people {
//!         println!("Found person {:?}", person);
//!     }
//!
//!     Ok(())
//! }
//! ```

#![forbid(unsafe_code)]
#![warn(
    clippy::await_holding_lock,
    clippy::cargo_common_metadata,
    clippy::dbg_macro,
    clippy::empty_enums,
    clippy::enum_glob_use,
    clippy::inefficient_to_string,
    clippy::mem_forget,
    clippy::mutex_integer,
    clippy::needless_continue,
    clippy::todo,
    clippy::unimplemented,
    clippy::wildcard_imports,
    future_incompatible,
    missing_docs,
    missing_debug_implementations,
    unreachable_pub
)]

#[cfg(test)]
mod tests;

use futures_channel::oneshot;
use std::{
    fmt::{self, Debug, Display},
    path::Path,
};

pub use rusqlite::{self, *};

// The backend serializes access to the wrapped `rusqlite::Connection`.
#[cfg_attr(target_os = "unknown", path = "backend/inline.rs")]
#[cfg_attr(not(target_os = "unknown"), path = "backend/threaded.rs")]
mod backend;

pub(crate) const BUG_TEXT: &str = "bug in tokio-rusqlite, please report";

#[derive(Debug)]
/// Represents the errors specific for this library.
#[non_exhaustive]
pub enum Error<E = rusqlite::Error> {
    /// The connection to the SQLite has been closed and cannot be queried any more.
    ConnectionClosed,

    /// An error occured while closing the SQLite connection.
    /// This `Error` variant contains the [`Connection`], which can be used to retry the close operation
    /// and the underlying [`rusqlite::Error`] that made it impossile to close the database.
    Close((Connection, rusqlite::Error)),

    /// An application-specific error occured.
    Error(E),
}

impl<E: Display> Display for Error<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::ConnectionClosed => write!(f, "ConnectionClosed"),
            Error::Close((_, e)) => write!(f, "Close((Connection, \"{e}\"))"),
            Error::Error(e) => write!(f, "Error(\"{e}\")"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for Error<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::ConnectionClosed => None,
            Error::Close((_, e)) => Some(e),
            Error::Error(e) => Some(e),
        }
    }
}

impl From<rusqlite::Error> for Error {
    fn from(value: rusqlite::Error) -> Self {
        Error::Error(value)
    }
}

/// The result returned on method calls in this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// A boxed function to be run against the [`rusqlite::Connection`].
///
/// Each backend (see the private `backend` module) runs these against its
/// connection in submission order.
pub(crate) type CallFn = Box<dyn FnOnce(&mut rusqlite::Connection) + Send + 'static>;

/// Marker error signalling that a connection is no longer available.
///
/// Returned by an backend's `submit` when the underlying connection has already
/// been closed.
#[derive(Debug)]
pub(crate) struct Closed;

/// A handle to call functions in a backend.
///
/// On targets with an OS, functions are called in a separate thread and communication
/// is via channels.
///
/// On targets without an OS, functions are called inline, blocking the current thread.
#[derive(Clone)]
pub struct Connection {
    handle: backend::Handle,
}

impl Connection {
    /// Open a new connection to a SQLite database.
    ///
    /// `Connection::open(path)` is equivalent to
    /// `Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE |
    /// OpenFlags::SQLITE_OPEN_CREATE)`.
    ///
    /// # Failure
    ///
    /// Will return `Err` if `path` cannot be converted to a C-compatible
    /// string or if the underlying SQLite open call fails.
    pub async fn open<P: AsRef<Path>>(path: P) -> std::result::Result<Self, rusqlite::Error> {
        let path = path.as_ref().to_owned();
        backend::start(move || rusqlite::Connection::open(path))
            .await
            .map(|handle| Self { handle })
    }

    /// Open a new connection to an in-memory SQLite database.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite open call fails.
    pub async fn open_in_memory() -> std::result::Result<Self, rusqlite::Error> {
        backend::start(rusqlite::Connection::open_in_memory)
            .await
            .map(|handle| Self { handle })
    }

    /// Open a new connection to a SQLite database.
    ///
    /// [Database Connection](http://www.sqlite.org/c3ref/open.html) for a
    /// description of valid flag combinations.
    ///
    /// # Failure
    ///
    /// Will return `Err` if `path` cannot be converted to a C-compatible
    /// string or if the underlying SQLite open call fails.
    pub async fn open_with_flags<P: AsRef<Path>>(
        path: P,
        flags: OpenFlags,
    ) -> std::result::Result<Self, rusqlite::Error> {
        let path = path.as_ref().to_owned();
        backend::start(move || rusqlite::Connection::open_with_flags(path, flags))
            .await
            .map(|handle| Self { handle })
    }

    /// Open a new connection to a SQLite database using the specific flags
    /// and vfs name.
    ///
    /// [Database Connection](http://www.sqlite.org/c3ref/open.html) for a
    /// description of valid flag combinations.
    ///
    /// # Failure
    ///
    /// Will return `Err` if either `path` or `vfs` cannot be converted to a
    /// C-compatible string or if the underlying SQLite open call fails.
    pub async fn open_with_flags_and_vfs<P: AsRef<Path>>(
        path: P,
        flags: OpenFlags,
        vfs: &str,
    ) -> std::result::Result<Self, rusqlite::Error> {
        let path = path.as_ref().to_owned();
        let vfs = vfs.to_owned();
        backend::start(move || rusqlite::Connection::open_with_flags_and_vfs(path, flags, &*vfs))
            .await
            .map(|handle| Self { handle })
    }

    /// Open a new connection to an in-memory SQLite database.
    ///
    /// [Database Connection](http://www.sqlite.org/c3ref/open.html) for a
    /// description of valid flag combinations.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite open call fails.
    pub async fn open_in_memory_with_flags(
        flags: OpenFlags,
    ) -> std::result::Result<Self, rusqlite::Error> {
        backend::start(move || rusqlite::Connection::open_in_memory_with_flags(flags))
            .await
            .map(|handle| Self { handle })
    }

    /// Open a new connection to an in-memory SQLite database using the
    /// specific flags and vfs name.
    ///
    /// [Database Connection](http://www.sqlite.org/c3ref/open.html) for a
    /// description of valid flag combinations.
    ///
    /// # Failure
    ///
    /// Will return `Err` if `vfs` cannot be converted to a C-compatible
    /// string or if the underlying SQLite open call fails.
    pub async fn open_in_memory_with_flags_and_vfs(
        flags: OpenFlags,
        vfs: &str,
    ) -> std::result::Result<Self, rusqlite::Error> {
        let vfs = vfs.to_owned();
        backend::start(move || {
            rusqlite::Connection::open_in_memory_with_flags_and_vfs(flags, &*vfs)
        })
        .await
        .map(|handle| Self { handle })
    }

    /// Call a function in background thread and get the result
    /// asynchronously.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the database connection has been closed.
    /// Will return `Error::Error` wrapping the inner error if `function` failed.
    pub async fn call<F, R, E>(&self, function: F) -> std::result::Result<R, Error<E>>
    where
        F: FnOnce(&mut rusqlite::Connection) -> std::result::Result<R, E> + 'static + Send,
        R: Send + 'static,
        E: Send + 'static,
    {
        self.call_raw(function)
            .await
            .map_err(|_| Error::ConnectionClosed)
            .and_then(|result| result.map_err(Error::Error))
    }

    /// Call a function in background thread and get the result
    /// asynchronously.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the database connection has been closed.
    pub async fn call_raw<F, R>(&self, function: F) -> Result<R>
    where
        F: FnOnce(&mut rusqlite::Connection) -> R + 'static + Send,
        R: Send + 'static,
    {
        let (sender, receiver) = oneshot::channel::<R>();

        self.handle
            .submit(Box::new(move |conn| {
                let value = function(conn);
                let _ = sender.send(value);
            }))
            .map_err(|_| Error::ConnectionClosed)?;

        receiver.await.map_err(|_| Error::ConnectionClosed)
    }

    /// Call a function in background thread and get the result
    /// asynchronously.
    ///
    /// This method can cause a `panic` if the underlying database connection is closed.
    /// it is a more user-friendly alternative to the [`Connection::call`] method.
    /// It should be safe if the connection is never explicitly closed (using the [`Connection::close`] call).
    ///
    /// Calling this on a closed connection will cause a `panic`.
    pub async fn call_unwrap<F, R>(&self, function: F) -> R
    where
        F: FnOnce(&mut rusqlite::Connection) -> R + Send + 'static,
        R: Send + 'static,
    {
        let (sender, receiver) = oneshot::channel::<R>();

        self.handle
            .submit(Box::new(move |conn| {
                let value = function(conn);
                let _ = sender.send(value);
            }))
            .expect("database connection should be open");

        receiver.await.expect(BUG_TEXT)
    }

    /// Close the database connection.
    ///
    /// This is functionally equivalent to the `Drop` implementation for
    /// `Connection`. It consumes the `Connection`, but on error returns it
    /// to the caller for retry purposes.
    ///
    /// If successful, any following `close` operations performed
    /// on `Connection` copies will succeed immediately.
    ///
    /// On the other hand, any calls to [`Connection::call`] will return a [`Error::ConnectionClosed`],
    /// and any calls to [`Connection::call_unwrap`] will cause a `panic`.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite close call fails.
    pub async fn close(self) -> Result<()> {
        match self.handle.close().await {
            Ok(()) => Ok(()),
            Err(e) => Err(Error::Close((self, e))),
        }
    }
}

impl Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection").finish()
    }
}

impl From<rusqlite::Connection> for Connection {
    fn from(conn: rusqlite::Connection) -> Self {
        Self {
            handle: backend::Handle::from_connection(conn),
        }
    }
}
