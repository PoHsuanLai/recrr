//! Shared test support: a Rotero-like application schema over the shipped
//! rusqlite backend. Using the real [`SqliteDb`](recrr::backends::SqliteDb) here
//! also serves as an end-to-end test of that backend.

use recrr::backends::SqliteDb;
use recrr::{Db, PkSpec, Schema, SkeletonValue, TableSpec, Value};

/// Open a fresh in-memory database with the application tables created.
pub async fn new_db() -> SqliteDb {
    let db = SqliteDb::in_memory().unwrap();
    // Create the app tables through the Db interface, one statement at a time
    // (no multi-statement batch on the trait).
    for stmt in APP_SCHEMA_STMTS {
        db.execute(stmt, vec![]).await.unwrap();
    }
    db
}

const APP_SCHEMA_STMTS: &[&str] = &[
    "CREATE TABLE papers (
        id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        authors TEXT NOT NULL,
        is_favorite INTEGER NOT NULL,
        is_read INTEGER NOT NULL,
        date_added TEXT NOT NULL,
        date_modified TEXT NOT NULL,
        citation_count INTEGER,
        cover BLOB
    )",
    "CREATE TABLE collections (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        parent_id TEXT,
        position INTEGER NOT NULL
    )",
    "CREATE TABLE paper_collections (
        paper_id TEXT NOT NULL,
        collection_id TEXT NOT NULL,
        PRIMARY KEY (paper_id, collection_id)
    )",
];

/// The [`Schema`] matching the application tables, mirroring how an app configures recrr.
pub fn test_schema() -> Schema {
    Schema::new(vec![
        TableSpec::new(
            "papers",
            [
                "title",
                "authors",
                "is_favorite",
                "is_read",
                "date_added",
                "date_modified",
                "citation_count",
                "cover",
            ],
        )
        .with_skeleton([
            ("title", SkeletonValue::Literal(Value::Text(String::new()))),
            (
                "authors",
                SkeletonValue::Literal(Value::Text("[]".to_string())),
            ),
            ("is_favorite", SkeletonValue::Literal(Value::Integer(0))),
            ("is_read", SkeletonValue::Literal(Value::Integer(0))),
            ("date_added", SkeletonValue::NowRfc3339),
            ("date_modified", SkeletonValue::NowRfc3339),
        ]),
        TableSpec::new("collections", ["name", "parent_id", "position"]).with_skeleton([
            ("name", SkeletonValue::Literal(Value::Text(String::new()))),
            ("position", SkeletonValue::Literal(Value::Integer(0))),
        ]),
        TableSpec::new("paper_collections", []).with_pk(PkSpec::composite(
            "paper_id",
            "collection_id",
            ':',
        )),
    ])
}

/// The tracked columns of `papers`, in order — handy for `track_insert`.
pub const PAPER_COLS: &[&str] = &[
    "title",
    "authors",
    "is_favorite",
    "is_read",
    "date_added",
    "date_modified",
    "citation_count",
    "cover",
];
