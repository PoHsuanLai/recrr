//! [`Db`] adapter for [turso](https://docs.turso.tech), the pure-Rust SQLite.

use async_trait::async_trait;
use turso::params::Params;

use crate::{Db, Error, Row, Value};

/// A [`Db`] backed by a `turso::Connection`.
///
/// `turso::Connection` is `Clone`, so `TursoDb` is cheap to clone and share.
///
/// ```no_run
/// # async fn f() -> Result<(), Box<dyn std::error::Error>> {
/// let db = turso::Builder::new_local("app.db").build().await?;
/// let conn = db.connect()?;
/// let backend = recrr::backends::TursoDb::new(conn);
/// # Ok(()) }
/// ```
#[derive(Clone)]
pub struct TursoDb {
    conn: turso::Connection,
}

impl TursoDb {
    /// Wrap a turso connection.
    pub fn new(conn: turso::Connection) -> Self {
        Self { conn }
    }

    /// The underlying connection.
    pub fn connection(&self) -> &turso::Connection {
        &self.conn
    }
}

fn to_turso(v: Value) -> turso::Value {
    match v {
        Value::Null => turso::Value::Null,
        Value::Integer(i) => turso::Value::Integer(i),
        Value::Real(f) => turso::Value::Real(f),
        Value::Text(s) => turso::Value::Text(s),
        Value::Blob(b) => turso::Value::Blob(b),
    }
}

fn from_turso(v: turso::Value) -> Value {
    match v {
        turso::Value::Null => Value::Null,
        turso::Value::Integer(i) => Value::Integer(i),
        turso::Value::Real(f) => Value::Real(f),
        turso::Value::Text(s) => Value::Text(s),
        turso::Value::Blob(b) => Value::Blob(b),
    }
}

fn params(values: Vec<Value>) -> Params {
    if values.is_empty() {
        Params::None
    } else {
        Params::Positional(values.into_iter().map(to_turso).collect())
    }
}

fn map_err(e: turso::Error) -> Error {
    Error::Backend(e.to_string())
}

#[async_trait]
impl Db for TursoDb {
    async fn execute(&self, sql: &str, p: Vec<Value>) -> Result<u64, Error> {
        self.conn.execute(sql, params(p)).await.map_err(map_err)
    }

    async fn query(&self, sql: &str, p: Vec<Value>) -> Result<Vec<Row>, Error> {
        let mut rows = self.conn.query(sql, params(p)).await.map_err(map_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(map_err)? {
            // Column count isn't exposed pre-materialization; walk indices until
            // get_value errors (past the last column).
            let mut vals = Vec::new();
            let mut i = 0;
            while let Ok(v) = row.get_value(i) {
                vals.push(from_turso(v));
                i += 1;
            }
            out.push(Row(vals));
        }
        Ok(out)
    }
}
