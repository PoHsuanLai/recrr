//! Shared test support: a Rotero-like application schema over the shipped
//! rusqlite backend. Using the real [`SqliteDb`](recrr::backends::SqliteDb) here
//! also serves as an end-to-end test of that backend.
//!
//! Also hosts the [`Device`] driver (a database + recrr handle) and the one-way
//! [`sync`] helper, shared by both the example-based `convergence` tests and the
//! randomized `proptest_convergence` tests.
//!
//! Each test binary compiles this module separately and uses a different subset
//! of the helpers, so unused-helper warnings here are expected — silence them.
#![allow(dead_code)]

use recrr::backends::SqliteDb;
use recrr::{ChangeRow, Crr, Db, PkSpec, Schema, SkeletonValue, TableSpec, Value};

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

/// A test "device": its own in-memory database + recrr handle. One `Device`
/// models one replica in a peer-to-peer sync scenario.
pub struct Device {
    pub crr: Crr<SqliteDb>,
}

impl Device {
    pub async fn new() -> Self {
        let crr = Crr::new(new_db().await, test_schema());
        crr.init().await.unwrap();
        Self { crr }
    }

    /// Insert a paper (real row + tracking).
    pub async fn insert_paper(&self, id: &str, title: &str) {
        let now = "2026-01-01T00:00:00Z";
        self.crr
            .db()
            .execute(
                "INSERT INTO papers (id, title, authors, is_favorite, is_read, date_added, date_modified) \
                 VALUES (?1, ?2, '[]', 0, 0, ?3, ?3)",
                vec![
                    Value::Text(id.to_string()),
                    Value::Text(title.to_string()),
                    Value::Text(now.to_string()),
                ],
            )
            .await
            .unwrap();
        self.crr
            .track_insert("papers", id, PAPER_COLS)
            .await
            .unwrap();
    }

    pub async fn set_title(&self, id: &str, title: &str) {
        self.crr
            .db()
            .execute(
                "UPDATE papers SET title = ?1 WHERE id = ?2",
                vec![Value::Text(title.to_string()), Value::Text(id.to_string())],
            )
            .await
            .unwrap();
        self.crr
            .track_update("papers", id, &["title"])
            .await
            .unwrap();
    }

    pub async fn set_favorite(&self, id: &str, fav: bool) {
        self.crr
            .db()
            .execute(
                "UPDATE papers SET is_favorite = ?1 WHERE id = ?2",
                vec![Value::Integer(fav as i64), Value::Text(id.to_string())],
            )
            .await
            .unwrap();
        self.crr
            .track_update("papers", id, &["is_favorite"])
            .await
            .unwrap();
    }

    pub async fn set_cover(&self, id: &str, bytes: &[u8]) {
        self.crr
            .db()
            .execute(
                "UPDATE papers SET cover = ?1 WHERE id = ?2",
                vec![Value::Blob(bytes.to_vec()), Value::Text(id.to_string())],
            )
            .await
            .unwrap();
        self.crr
            .track_update("papers", id, &["cover"])
            .await
            .unwrap();
    }

    pub async fn delete_paper(&self, id: &str) {
        self.crr
            .db()
            .execute(
                "DELETE FROM papers WHERE id = ?1",
                vec![Value::Text(id.to_string())],
            )
            .await
            .unwrap();
        self.crr.track_delete("papers", id).await.unwrap();
    }

    /// Read a paper's title, or None if the row is gone.
    pub async fn title(&self, id: &str) -> Option<String> {
        let rows = self
            .crr
            .db()
            .query(
                "SELECT title FROM papers WHERE id = ?1",
                vec![Value::Text(id.to_string())],
            )
            .await
            .unwrap();
        rows.into_iter()
            .next()
            .map(|r| r.get(0).as_text().unwrap_or_default().to_string())
    }

    pub async fn cover(&self, id: &str) -> Option<Vec<u8>> {
        let rows = self
            .crr
            .db()
            .query(
                "SELECT cover FROM papers WHERE id = ?1",
                vec![Value::Text(id.to_string())],
            )
            .await
            .unwrap();
        rows.into_iter()
            .next()
            .and_then(|r| r.get(0).as_blob().map(|b| b.to_vec()))
    }

    pub async fn paper_exists(&self, id: &str) -> bool {
        self.title(id).await.is_some()
    }

    pub async fn changes(&self) -> Vec<ChangeRow> {
        self.crr.changes_since(0).await.unwrap()
    }

    pub async fn apply(&self, changes: &[ChangeRow]) -> recrr::MergeResult {
        self.crr.apply_changes(changes).await.unwrap()
    }
}

/// One-way sync: apply every change from `src` into `dst`.
pub async fn sync(src: &Device, dst: &Device) {
    let changes = src.changes().await;
    dst.apply(&changes).await;
}
