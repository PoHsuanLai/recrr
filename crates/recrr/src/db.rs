//! The database abstraction `recrr` runs on.
//!
//! `recrr` performs only parameterized `execute`/`query` and reads back rows of
//! tagged [`Value`]s — no transactions, prepared statements, or batching. Any
//! SQLite-family driver (turso, rusqlite, libSQL, sqlx) can implement [`Db`] in
//! a few dozen lines. The core SQL assumes SQLite syntax, including
//! `INSERT OR REPLACE`, `INSERT OR IGNORE`, and `randomblob(16)`.

use async_trait::async_trait;

/// A SQL value, mirroring SQLite's storage classes.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl Value {
    /// The integer, if this is [`Value::Integer`].
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            Value::Integer(i) => Some(*i),
            _ => None,
        }
    }

    /// The text, if this is [`Value::Text`].
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Value::Text(s) => Some(s),
            _ => None,
        }
    }

    /// The bytes, if this is [`Value::Blob`].
    pub fn as_blob(&self) -> Option<&[u8]> {
        match self {
            Value::Blob(b) => Some(b),
            _ => None,
        }
    }

    /// The float, if this is [`Value::Real`].
    pub fn as_real(&self) -> Option<f64> {
        match self {
            Value::Real(f) => Some(*f),
            _ => None,
        }
    }
}

/// One row of a query result: values positional by column index.
#[derive(Clone, Debug, Default)]
pub struct Row(pub Vec<Value>);

impl Row {
    /// The value at column `i`, or [`Value::Null`] if out of bounds.
    pub fn get(&self, i: usize) -> Value {
        self.0.get(i).cloned().unwrap_or(Value::Null)
    }
}

/// An error surfaced by a [`Db`] implementation.
///
/// `recrr` only ever propagates these; it never inspects the variant, so an
/// adapter may map every driver error to [`Error::Backend`].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A query expected at least one row but returned none.
    #[error("query returned no rows")]
    NoRows,
    /// Any error from the underlying database driver.
    #[error("database backend error: {0}")]
    Backend(String),
}

/// The database interface `recrr` operates over.
///
/// Implementations run parameterized statements against a live connection.
/// `?N` positional placeholders bind to `params` in order. `recrr` interpolates
/// table and column names directly into SQL (guarded by the [`Schema`] whitelist),
/// so identifiers never arrive as params.
///
/// [`Schema`]: crate::Schema
#[async_trait]
pub trait Db: Send + Sync {
    /// Run a statement, returning the number of rows affected.
    async fn execute(&self, sql: &str, params: Vec<Value>) -> Result<u64, Error>;

    /// Run a query, returning all rows eagerly.
    ///
    /// Result sets in `recrr` are small (per-row clock entries), so eager
    /// collection keeps the trait object-safe and trivial to implement.
    async fn query(&self, sql: &str, params: Vec<Value>) -> Result<Vec<Row>, Error>;
}
