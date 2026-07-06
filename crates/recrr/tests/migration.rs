//! Schema-migration tests: the explicit `migrate_*` primitives and the versioned
//! changeset envelope, against the in-memory rusqlite backend.
//!
//! These build `Crr<SqliteDb>` handles directly (rather than the fixed-schema
//! `support::Device`) because migration is precisely about two replicas — or one
//! replica across time — holding *different* schemas.

mod support;

use recrr::backends::SqliteDb;
use recrr::{Crr, Db, PkSpec, Schema, SkeletonValue, TableSpec, Value};

// --- schema builders: a "papers" table, with and without a `notes` column ------

fn schema_v1() -> Schema {
    Schema::new(vec![TableSpec::new("papers", ["title"]).with_skeleton([(
        "title",
        SkeletonValue::Literal(Value::Text(String::new())),
    )])])
}

fn schema_v2_with_notes() -> Schema {
    Schema::new(vec![TableSpec::new("papers", ["title", "notes"])
        .with_skeleton([
            ("title", SkeletonValue::Literal(Value::Text(String::new()))),
            ("notes", SkeletonValue::Literal(Value::Text(String::new()))),
        ])])
}

/// Fresh in-memory DB with a `papers` table that already has the `notes` column
/// (so both v1 and v2 schemas run against the same physical table — v1 simply
/// doesn't track `notes`). Keeps the tests focused on recrr metadata, not app DDL.
async fn new_db_with_notes() -> SqliteDb {
    let db = SqliteDb::in_memory().unwrap();
    db.execute(
        "CREATE TABLE papers (id TEXT PRIMARY KEY, title TEXT NOT NULL, notes TEXT NOT NULL DEFAULT '')",
        vec![],
    )
    .await
    .unwrap();
    db
}

async fn insert_paper(crr: &Crr<SqliteDb>, id: &str, title: &str, cols: &[&str]) {
    crr.db()
        .execute(
            "INSERT INTO papers (id, title) VALUES (?1, ?2)",
            vec![Value::Text(id.to_string()), Value::Text(title.to_string())],
        )
        .await
        .unwrap();
    crr.track_insert("papers", id, cols).await.unwrap();
}

async fn clock_rows(crr: &Crr<SqliteDb>, col: &str) -> Vec<(String, i64, i64)> {
    // (pk, col_ver, db_ver) for a given col_name in the papers clock table.
    let rows = crr
        .db()
        .query(
            "SELECT pk, col_ver, db_ver FROM papers__crr_clock WHERE col_name = ?1 ORDER BY pk",
            vec![Value::Text(col.to_string())],
        )
        .await
        .unwrap();
    rows.into_iter()
        .map(|r| {
            (
                r.get(0).as_text().unwrap_or_default().to_string(),
                r.get(1).as_integer().unwrap_or(0),
                r.get(2).as_integer().unwrap_or(0),
            )
        })
        .collect()
}

async fn notes_of(crr: &Crr<SqliteDb>, id: &str) -> Option<String> {
    let rows = crr
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

// --- add column + backfill ---------------------------------------------------

#[tokio::test]
async fn add_column_backfills_live_rows_and_syncs() {
    // A device starts on v1 (no notes), inserts two live papers and one deleted.
    let crr = Crr::new(new_db_with_notes().await, schema_v1());
    crr.init().await.unwrap();
    insert_paper(&crr, "p1", "A", &["title"]).await;
    insert_paper(&crr, "p2", "B", &["title"]).await;
    insert_paper(&crr, "p3", "C", &["title"]).await;
    // Delete p3 so it is a dead row at backfill time.
    crr.db()
        .execute("DELETE FROM papers WHERE id = 'p3'", vec![])
        .await
        .unwrap();
    crr.track_delete("papers", "p3").await.unwrap();

    // Migrate to v2: real column already exists; rebind schema and backfill.
    let crr = crr.with_schema(schema_v2_with_notes());
    let ver_before = crr.schema_version().await;
    crr.migrate_add_column("papers", "notes").await.unwrap();

    // Live rows p1, p2 get a notes clock entry; dead p3 does NOT.
    let notes = clock_rows(&crr, "notes").await;
    let pks: Vec<&str> = notes.iter().map(|(pk, _, _)| pk.as_str()).collect();
    assert_eq!(pks, vec!["p1", "p2"], "only live rows backfilled");
    assert!(
        notes.iter().all(|(_, ver, _)| *ver == 1),
        "backfill at col_ver=1"
    );
    // One shared db_ver across the batch.
    assert_eq!(notes[0].2, notes[1].2, "backfill shares one db_ver");
    // Schema version bumped.
    assert_eq!(crr.schema_version().await, ver_before + 1);

    // A fresh v2 peer converges on the backfilled notes column.
    let peer = Crr::new(new_db_with_notes().await, schema_v2_with_notes());
    peer.init().await.unwrap();
    let cs = crr.changeset_since(0).await.unwrap();
    peer.apply_changeset(&cs).await.unwrap();
    // Set a real notes value on the source and re-sync so the peer sees content.
    crr.db()
        .execute("UPDATE papers SET notes = 'hi' WHERE id = 'p1'", vec![])
        .await
        .unwrap();
    crr.track_update("papers", "p1", &["notes"]).await.unwrap();
    let cs = crr.changeset_since(0).await.unwrap();
    peer.apply_changeset(&cs).await.unwrap();
    assert_eq!(notes_of(&peer, "p1").await.as_deref(), Some("hi"));
}

#[tokio::test]
async fn add_column_is_idempotent() {
    let crr = Crr::new(new_db_with_notes().await, schema_v2_with_notes());
    crr.init().await.unwrap();
    insert_paper(&crr, "p1", "A", &["title"]).await;

    crr.migrate_add_column("papers", "notes").await.unwrap();
    let after_first = clock_rows(&crr, "notes").await;
    let ver_first = crr.schema_version().await;

    crr.migrate_add_column("papers", "notes").await.unwrap();
    let after_second = clock_rows(&crr, "notes").await;

    assert_eq!(
        after_first, after_second,
        "re-running backfill changes nothing"
    );
    // Version still advances (each call is a recorded migration step).
    assert_eq!(crr.schema_version().await, ver_first + 1);
}

// --- drop column -------------------------------------------------------------

#[tokio::test]
async fn drop_column_removes_clocks_and_incoming_is_reported_not_silent() {
    // Source still on v2 (tracks notes); target migrates to drop notes.
    let src = Crr::new(new_db_with_notes().await, schema_v2_with_notes());
    src.init().await.unwrap();
    insert_paper(&src, "p1", "A", &["title", "notes"]).await;
    src.db()
        .execute("UPDATE papers SET notes = 'keep' WHERE id = 'p1'", vec![])
        .await
        .unwrap();
    src.track_update("papers", "p1", &["notes"]).await.unwrap();

    let tgt = Crr::new(new_db_with_notes().await, schema_v2_with_notes());
    tgt.init().await.unwrap();
    insert_paper(&tgt, "p1", "A", &["title", "notes"]).await;
    tgt.migrate_drop_column("papers", "notes").await.unwrap();
    assert!(
        clock_rows(&tgt, "notes").await.is_empty(),
        "notes clocks removed"
    );

    // Now rebind tgt to a schema that no longer tracks notes, and apply src's
    // changeset: the notes change must be *reported* as skipped_unknown.
    let tgt = tgt.with_schema(schema_v1());
    let cs = src.changeset_since(0).await.unwrap();
    let res = tgt.apply_changeset(&cs).await.unwrap();
    assert!(
        res.skipped_unknown > 0,
        "unknown notes column reported, not silent"
    );
    assert!(res.peer_schema.is_some(), "schema mismatch surfaced");
    // Title (a known column) still merged.
    let rows = tgt
        .db()
        .query("SELECT title FROM papers WHERE id = 'p1'", vec![])
        .await
        .unwrap();
    assert_eq!(rows[0].get(0).as_text(), Some("A"));
}

// --- rename table ------------------------------------------------------------

#[tokio::test]
async fn rename_table_preserves_history() {
    let db = SqliteDb::in_memory().unwrap();
    db.execute(
        "CREATE TABLE papers (id TEXT PRIMARY KEY, title TEXT NOT NULL)",
        vec![],
    )
    .await
    .unwrap();
    let crr = Crr::new(db, schema_v1());
    crr.init().await.unwrap();
    insert_paper(&crr, "p1", "Original", &["title"]).await;

    // Rename the real table, then the clock table.
    crr.db()
        .execute("ALTER TABLE papers RENAME TO articles", vec![])
        .await
        .unwrap();
    let renamed_schema = Schema::new(vec![TableSpec::new("articles", ["title"])
        .with_skeleton([("title", SkeletonValue::Literal(Value::Text(String::new())))])]);
    let crr = crr.with_schema(renamed_schema);
    crr.migrate_rename_table("papers", "articles")
        .await
        .unwrap();

    // History preserved: the pre-rename edit still emits under the new table name.
    let changes = crr.changes_since(0).await.unwrap();
    assert!(
        changes
            .iter()
            .any(|c| c.table_name == "articles" && c.col_name == "title"),
        "pre-rename title change survived the rename"
    );
    // The (pk, col_name) PK survived: a duplicate insert conflicts.
    let dup = crr
        .db()
        .execute(
            "INSERT INTO articles__crr_clock (pk, col_name, col_ver, db_ver, site_id, seq) \
             VALUES ('p1', 'title', 9, 9, X'00', 0)",
            vec![],
        )
        .await;
    assert!(
        dup.is_err(),
        "primary key (pk,col_name) preserved across rename"
    );
}

#[tokio::test]
async fn rename_table_rejects_preexisting_clock() {
    let db = SqliteDb::in_memory().unwrap();
    db.execute(
        "CREATE TABLE papers (id TEXT PRIMARY KEY, title TEXT NOT NULL)",
        vec![],
    )
    .await
    .unwrap();
    // Pre-create the destination clock table via init under the new name.
    let crr = Crr::new(
        db,
        Schema::new(vec![
            TableSpec::new("papers", ["title"]),
            TableSpec::new("articles", ["title"]),
        ]),
    );
    crr.init().await.unwrap();
    let err = crr.migrate_rename_table("papers", "articles").await;
    assert!(
        err.is_err(),
        "refuses to clobber an existing destination clock table"
    );
}

// --- change PK shape ---------------------------------------------------------

#[tokio::test]
async fn change_pk_reencodes_and_converges() {
    // A single-PK table whose keys we re-encode to composite "a:b".
    let db = SqliteDb::in_memory().unwrap();
    db.execute(
        "CREATE TABLE t (k TEXT PRIMARY KEY, v TEXT NOT NULL DEFAULT '')",
        vec![],
    )
    .await
    .unwrap();
    let crr = Crr::new(
        db,
        Schema::new(vec![TableSpec::new("t", ["v"]).with_pk(PkSpec::single("k"))]),
    );
    crr.init().await.unwrap();
    crr.db()
        .execute("INSERT INTO t (k, v) VALUES ('a', 'x')", vec![])
        .await
        .unwrap();
    crr.track_insert("t", "a", &["v"]).await.unwrap();

    // Re-encode "a" -> "a:1".
    let new_pk = PkSpec::composite("k1", "k2", ':');
    crr.migrate_change_pk("t", &new_pk, |old| Some(format!("{old}:1")))
        .await
        .unwrap();

    let rows = crr
        .db()
        .query("SELECT DISTINCT pk FROM t__crr_clock", vec![])
        .await
        .unwrap();
    let pks: Vec<String> = rows
        .into_iter()
        .map(|r| r.get(0).as_text().unwrap_or_default().to_string())
        .collect();
    assert_eq!(pks, vec!["a:1".to_string()], "clock pk re-encoded");
}

#[tokio::test]
async fn change_pk_rejects_collisions_and_ambiguity() {
    let db = SqliteDb::in_memory().unwrap();
    db.execute(
        "CREATE TABLE t (k TEXT PRIMARY KEY, v TEXT NOT NULL DEFAULT '')",
        vec![],
    )
    .await
    .unwrap();
    let crr = Crr::new(
        db,
        Schema::new(vec![TableSpec::new("t", ["v"]).with_pk(PkSpec::single("k"))]),
    );
    crr.init().await.unwrap();
    for k in ["a", "b"] {
        crr.db()
            .execute(
                "INSERT INTO t (k, v) VALUES (?1, '')",
                vec![Value::Text(k.into())],
            )
            .await
            .unwrap();
        crr.track_insert("t", k, &["v"]).await.unwrap();
    }
    let comp = PkSpec::composite("k1", "k2", ':');

    // Collision: both map to the same new pk.
    let collide = crr
        .migrate_change_pk("t", &comp, |_| Some("same:1".to_string()))
        .await;
    assert!(collide.is_err(), "collision rejected");

    // Malformed composite: a produced key with no separator cannot decode into
    // the two-component key the target PkSpec requires.
    let malformed = crr
        .migrate_change_pk("t", &comp, |old| Some(old.to_string()))
        .await;
    assert!(
        malformed.is_err(),
        "separator-less composite encoding rejected"
    );
}

// --- fingerprint + envelope --------------------------------------------------

#[tokio::test]
async fn fingerprint_is_stable_and_order_insensitive() {
    // Same tables/cols in different declaration order -> same fingerprint.
    let a = Schema::new(vec![
        TableSpec::new("papers", ["title", "notes"]),
        TableSpec::new("tags", ["label"]),
    ]);
    let b = Schema::new(vec![
        TableSpec::new("tags", ["label"]),
        TableSpec::new("papers", ["notes", "title"]),
    ]);
    assert_eq!(a.fingerprint(), b.fingerprint(), "order-insensitive");

    // A real structural change -> different fingerprint.
    let c = Schema::new(vec![
        TableSpec::new("papers", ["title"]),
        TableSpec::new("tags", ["label"]),
    ]);
    assert_ne!(a.fingerprint(), c.fingerprint(), "column change detected");
}

#[tokio::test]
async fn matching_schemas_report_no_peer_mismatch() {
    let src = Crr::new(new_db_with_notes().await, schema_v2_with_notes());
    src.init().await.unwrap();
    insert_paper(&src, "p1", "A", &["title", "notes"]).await;

    let tgt = Crr::new(new_db_with_notes().await, schema_v2_with_notes());
    tgt.init().await.unwrap();

    let cs = src.changeset_since(0).await.unwrap();
    let res = tgt.apply_changeset(&cs).await.unwrap();
    assert_eq!(res.peer_schema, None, "identical schemas -> no mismatch");
    assert_eq!(res.skipped_unknown, 0);
}
