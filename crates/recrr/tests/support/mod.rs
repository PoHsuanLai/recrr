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
use recrr::{ChangeRow, Crdt, Crr, Db, Schema, Value};

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
    // `notes` exists in the real table from the start but is only *tracked* by
    // recrr once a device runs `migrate_add_notes` — this models a schema
    // rollout where the column is added and backfilled mid-life.
    "CREATE TABLE papers (
        id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        authors TEXT NOT NULL,
        is_favorite INTEGER NOT NULL,
        is_read INTEGER NOT NULL,
        date_added TEXT NOT NULL,
        date_modified TEXT NOT NULL,
        citation_count INTEGER,
        cover BLOB,
        notes TEXT NOT NULL DEFAULT ''
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

// The application tables, derived from structs — mirroring how an app would
// configure recrr with `#[derive(Crdt)]`. `notes` is tracked from the start but
// only backfilled once a device runs `migrate_add_notes`.
#[derive(Crdt)]
#[crdt(table = "papers")]
#[allow(dead_code)] // fields drive codegen, not all are read
pub struct Paper {
    #[crdt(pk)]
    id: String,
    #[crdt(skeleton = "\"\"")]
    title: String,
    #[crdt(skeleton = "\"[]\"")]
    authors: String,
    #[crdt(skeleton = "0")]
    is_favorite: i64,
    #[crdt(skeleton = "0")]
    is_read: i64,
    #[crdt(skeleton = "now_rfc3339")]
    date_added: String,
    #[crdt(skeleton = "now_rfc3339")]
    date_modified: String,
    citation_count: Option<i64>,
    cover: Option<Vec<u8>>,
    notes: String,
}

#[derive(Crdt)]
#[crdt(table = "collections")]
#[allow(dead_code)]
pub struct Collection {
    #[crdt(pk)]
    id: String,
    #[crdt(skeleton = "\"\"")]
    name: String,
    parent_id: Option<String>,
    #[crdt(skeleton = "0")]
    position: i64,
}

#[derive(Crdt)]
#[crdt(table = "paper_collections", pk = (paper_id, collection_id; sep = ':'))]
#[allow(dead_code)]
pub struct PaperCollection {
    paper_id: String,
    collection_id: String,
}

/// The [`Schema`] matching the application tables, mirroring how an app configures recrr.
pub fn test_schema() -> Schema {
    recrr::schema![Paper, Collection, PaperCollection]
}

/// The tracked columns of `papers` used by `insert_paper` (everything except the
/// mid-life `notes` column). `Paper::ALL` would include `notes`; this subset
/// matches what the initial insert actually writes.
pub const PAPER_COLS: &[&str] = &[
    Paper::TITLE,
    Paper::AUTHORS,
    Paper::IS_FAVORITE,
    Paper::IS_READ,
    Paper::DATE_ADDED,
    Paper::DATE_MODIFIED,
    Paper::CITATION_COUNT,
    Paper::COVER,
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
            .track_update("papers", id, &[Paper::TITLE])
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
            .track_update("papers", id, &[Paper::IS_FAVORITE])
            .await
            .unwrap();
    }

    pub async fn set_authors(&self, id: &str, authors: &str) {
        self.crr
            .db()
            .execute(
                "UPDATE papers SET authors = ?1 WHERE id = ?2",
                vec![
                    Value::Text(authors.to_string()),
                    Value::Text(id.to_string()),
                ],
            )
            .await
            .unwrap();
        self.crr
            .track_update("papers", id, &[Paper::AUTHORS])
            .await
            .unwrap();
    }

    /// Set the nullable `citation_count`. `None` writes SQL NULL, exercising the
    /// `Value::Null` merge path.
    pub async fn set_citation_count(&self, id: &str, count: Option<i64>) {
        let val = match count {
            Some(n) => Value::Integer(n),
            None => Value::Null,
        };
        self.crr
            .db()
            .execute(
                "UPDATE papers SET citation_count = ?1 WHERE id = ?2",
                vec![val, Value::Text(id.to_string())],
            )
            .await
            .unwrap();
        self.crr
            .track_update("papers", id, &[Paper::CITATION_COUNT])
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
            .track_update("papers", id, &[Paper::COVER])
            .await
            .unwrap();
    }

    /// Set the `notes` column (only meaningful after `migrate_add_notes`, but the
    /// generator may call it anytime; it tracks the update regardless).
    pub async fn set_notes(&self, id: &str, notes: &str) {
        self.crr
            .db()
            .execute(
                "UPDATE papers SET notes = ?1 WHERE id = ?2",
                vec![Value::Text(notes.to_string()), Value::Text(id.to_string())],
            )
            .await
            .unwrap();
        self.crr
            .track_update("papers", id, &[Paper::NOTES])
            .await
            .unwrap();
    }

    /// Run the `notes` column migration on this device: backfill clock entries for
    /// existing live rows so the (already-tracked) column syncs. Idempotent, so
    /// the generator may issue it more than once per device.
    pub async fn migrate_add_notes(&self) {
        self.crr
            .migrate_add_column("papers", "notes")
            .await
            .unwrap();
    }

    pub async fn notes(&self, id: &str) -> Option<String> {
        let rows = self
            .crr
            .db()
            .query(
                "SELECT notes FROM papers WHERE id = ?1",
                vec![Value::Text(id.to_string())],
            )
            .await
            .unwrap();
        rows.into_iter()
            .next()
            .map(|r| r.get(0).as_text().unwrap_or_default().to_string())
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

    // --- collections (single-PK table, exercises a second tracked table) ------

    pub async fn insert_collection(&self, id: &str, name: &str) {
        self.crr
            .db()
            .execute(
                "INSERT INTO collections (id, name, position) VALUES (?1, ?2, 0)",
                vec![Value::Text(id.to_string()), Value::Text(name.to_string())],
            )
            .await
            .unwrap();
        self.crr
            .track_insert("collections", id, Collection::ALL)
            .await
            .unwrap();
    }

    pub async fn set_collection_name(&self, id: &str, name: &str) {
        self.crr
            .db()
            .execute(
                "UPDATE collections SET name = ?1 WHERE id = ?2",
                vec![Value::Text(name.to_string()), Value::Text(id.to_string())],
            )
            .await
            .unwrap();
        self.crr
            .track_update("collections", id, &[Collection::NAME])
            .await
            .unwrap();
    }

    pub async fn delete_collection(&self, id: &str) {
        self.crr
            .db()
            .execute(
                "DELETE FROM collections WHERE id = ?1",
                vec![Value::Text(id.to_string())],
            )
            .await
            .unwrap();
        self.crr.track_delete("collections", id).await.unwrap();
    }

    pub async fn collection_name(&self, id: &str) -> Option<String> {
        let rows = self
            .crr
            .db()
            .query(
                "SELECT name FROM collections WHERE id = ?1",
                vec![Value::Text(id.to_string())],
            )
            .await
            .unwrap();
        rows.into_iter()
            .next()
            .map(|r| r.get(0).as_text().unwrap_or_default().to_string())
    }

    pub async fn collection_exists(&self, id: &str) -> bool {
        self.collection_name(id).await.is_some()
    }

    // --- paper_collections (composite-PK junction, no tracked columns) --------

    /// Link a paper to a collection. The composite pk is `"{paper}:{collection}"`.
    pub async fn link(&self, paper: &str, collection: &str) {
        self.crr
            .db()
            .execute(
                "INSERT INTO paper_collections (paper_id, collection_id) VALUES (?1, ?2)",
                vec![
                    Value::Text(paper.to_string()),
                    Value::Text(collection.to_string()),
                ],
            )
            .await
            .unwrap();
        let pk = format!("{paper}:{collection}");
        self.crr
            .track_insert("paper_collections", &pk, &[])
            .await
            .unwrap();
    }

    pub async fn unlink(&self, paper: &str, collection: &str) {
        self.crr
            .db()
            .execute(
                "DELETE FROM paper_collections WHERE paper_id = ?1 AND collection_id = ?2",
                vec![
                    Value::Text(paper.to_string()),
                    Value::Text(collection.to_string()),
                ],
            )
            .await
            .unwrap();
        let pk = format!("{paper}:{collection}");
        self.crr
            .track_delete("paper_collections", &pk)
            .await
            .unwrap();
    }

    pub async fn link_exists(&self, paper: &str, collection: &str) -> bool {
        let rows = self
            .crr
            .db()
            .query(
                "SELECT 1 FROM paper_collections WHERE paper_id = ?1 AND collection_id = ?2",
                vec![
                    Value::Text(paper.to_string()),
                    Value::Text(collection.to_string()),
                ],
            )
            .await
            .unwrap();
        !rows.is_empty()
    }

    /// Link a paper to a collection *without* tracking it, modelling a build
    /// that never called `init()`: the row commits and the clock never learns
    /// about it.
    pub async fn link_untracked(&self, paper: &str, collection: &str) {
        self.crr
            .db()
            .execute(
                "INSERT INTO paper_collections (paper_id, collection_id) VALUES (?1, ?2)",
                vec![
                    Value::Text(paper.to_string()),
                    Value::Text(collection.to_string()),
                ],
            )
            .await
            .unwrap();
    }

    /// Adopt an untracked link, as the repair pass does.
    pub async fn adopt_link(&self, paper: &str, collection: &str) {
        let pk = format!("{paper}:{collection}");
        self.crr
            .track_adopt("paper_collections", &pk, &[])
            .await
            .unwrap();
    }

    /// The sentinel clock for a link, or 0 when absent. Odd means alive, even
    /// means deleted — the distinction the repair pass turns on.
    pub async fn link_sentinel(&self, paper: &str, collection: &str) -> i64 {
        let rows = self
            .crr
            .db()
            .query(
                "SELECT col_ver FROM paper_collections__crr_clock \
                 WHERE pk = ?1 AND col_name = '__sentinel'",
                vec![Value::Text(format!("{paper}:{collection}"))],
            )
            .await
            .unwrap();
        rows.into_iter()
            .next()
            .map(|r| r.get(0).as_integer().unwrap_or(0))
            .unwrap_or(0)
    }

    /// The `col_ver` of a tracked column on a paper, or 0 when absent.
    pub async fn paper_col_ver(&self, id: &str, col: &str) -> i64 {
        let rows = self
            .crr
            .db()
            .query(
                "SELECT col_ver FROM papers__crr_clock WHERE pk = ?1 AND col_name = ?2",
                vec![Value::Text(id.to_string()), Value::Text(col.to_string())],
            )
            .await
            .unwrap();
        rows.into_iter()
            .next()
            .map(|r| r.get(0).as_integer().unwrap_or(0))
            .unwrap_or(0)
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

/// One-way *lossy* sync: deliver only a deterministic subset of `src`'s
/// changeset to `dst`, modelling a truncated `.crr` file or a dropped CloudKit
/// record. `keep` decides, per change (by index), whether it is delivered — a
/// pure function of the caller's seed so failures replay deterministically.
///
/// A CRDT must tolerate this: partial/out-of-order delivery may leave `dst`
/// transiently inconsistent, but a later *complete* delivery (e.g. the final
/// gossip-to-fixpoint, which always ships full changesets) must still converge.
pub async fn sync_lossy(src: &Device, dst: &Device, keep: impl Fn(usize) -> bool) {
    let changes = src.changes().await;
    let subset: Vec<ChangeRow> = changes
        .into_iter()
        .enumerate()
        .filter(|(i, _)| keep(*i))
        .map(|(_, c)| c)
        .collect();
    dst.apply(&subset).await;
}
