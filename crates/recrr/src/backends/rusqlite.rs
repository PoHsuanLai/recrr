//! [`Db`] adapter for [rusqlite](https://docs.rs/rusqlite), bundled C SQLite.
//!
//! rusqlite is synchronous; this backend runs statements inline behind a
//! [`Mutex`] to satisfy the async [`Db`] trait. That is fine for embedded,
//! single-process use — the same model `recrr` targets — but it does not offload
//! blocking work to a thread pool, so avoid it for high-concurrency servers.

use std::sync::Mutex;

use async_trait::async_trait;

use crate::{Db, Error, Row, Value};

/// A [`Db`] backed by a rusqlite [`Connection`](rusqlite::Connection).
pub struct SqliteDb {
    conn: Mutex<rusqlite::Connection>,
}

impl SqliteDb {
    /// Wrap an existing rusqlite connection.
    pub fn new(conn: rusqlite::Connection) -> Self {
        Self {
            conn: Mutex::new(conn),
        }
    }

    /// Open a fresh in-memory database. Handy for tests and ephemeral replicas.
    pub fn in_memory() -> Result<Self, Error> {
        let conn = rusqlite::Connection::open_in_memory().map_err(map_err)?;
        Ok(Self::new(conn))
    }

    /// Open (or create) a database at `path`.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, Error> {
        let conn = rusqlite::Connection::open(path).map_err(map_err)?;
        Ok(Self::new(conn))
    }
}

fn to_sqlite(v: &Value) -> rusqlite::types::Value {
    use rusqlite::types::Value as S;
    match v {
        Value::Null => S::Null,
        Value::Integer(i) => S::Integer(*i),
        Value::Real(f) => S::Real(*f),
        Value::Text(s) => S::Text(s.clone()),
        Value::Blob(b) => S::Blob(b.clone()),
    }
}

fn from_sqlite(v: rusqlite::types::Value) -> Value {
    use rusqlite::types::Value as S;
    match v {
        S::Null => Value::Null,
        S::Integer(i) => Value::Integer(i),
        S::Real(f) => Value::Real(f),
        S::Text(s) => Value::Text(s),
        S::Blob(b) => Value::Blob(b),
    }
}

fn map_err(e: rusqlite::Error) -> Error {
    Error::Backend(e.to_string())
}

#[async_trait]
impl Db for SqliteDb {
    async fn execute(&self, sql: &str, params: Vec<Value>) -> Result<u64, Error> {
        let conn = self.conn.lock().unwrap();
        let bound: Vec<rusqlite::types::Value> = params.iter().map(to_sqlite).collect();
        let n = conn
            .execute(sql, rusqlite::params_from_iter(bound.iter()))
            .map_err(map_err)?;
        Ok(n as u64)
    }

    async fn query(&self, sql: &str, params: Vec<Value>) -> Result<Vec<Row>, Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(sql).map_err(map_err)?;
        let col_count = stmt.column_count();
        let bound: Vec<rusqlite::types::Value> = params.iter().map(to_sqlite).collect();
        let rows = stmt
            .query_map(rusqlite::params_from_iter(bound.iter()), |row| {
                let mut vals = Vec::with_capacity(col_count);
                for i in 0..col_count {
                    vals.push(from_sqlite(row.get(i)?));
                }
                Ok(Row(vals))
            })
            .map_err(map_err)?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r.map_err(map_err)?);
        }
        Ok(out)
    }
}
