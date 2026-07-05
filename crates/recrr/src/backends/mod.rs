//! Ready-made [`Db`](crate::Db) implementations for common SQL drivers.
//!
//! Each backend is behind its own feature and off by default — enable the one
//! matching your driver:
//!
//! ```toml
//! recrr = { version = "0.1", features = ["turso"] }    # pure-Rust SQLite
//! recrr = { version = "0.1", features = ["rusqlite"] }  # bundled C SQLite
//! ```
//!
//! Writing your own backend is a few dozen lines — implement [`Db`](crate::Db)
//! for your connection type. See these modules' source for reference.

#[cfg(feature = "turso")]
mod turso;
#[cfg(feature = "turso")]
#[cfg_attr(docsrs, doc(cfg(feature = "turso")))]
pub use turso::TursoDb;

#[cfg(feature = "rusqlite")]
mod rusqlite;
#[cfg(feature = "rusqlite")]
#[cfg_attr(docsrs, doc(cfg(feature = "rusqlite")))]
pub use rusqlite::SqliteDb;
