//! Runtime tests for `#[derive(Crdt)]`: the generated `TableSpec` must match the
//! hand-written one, and a derive-built device must interoperate on the wire with
//! a hand-schema device (equal fingerprints).
//!
//! The derived structs are schema declarations, not data holders — their fields
//! exist to drive codegen, so silence the "never read" lint.
#![allow(dead_code)]

use recrr::backends::SqliteDb;
use recrr::{Crdt, CrdtTable, Crr, Db, PkSpec, Schema, SkeletonValue, TableSpec, Value};

// A derived mirror of the hand-written `papers` spec in `support::test_schema`.
#[derive(Crdt)]
#[crdt(table = "papers")]
struct Paper {
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
    // A field that exists on the struct but is not synced.
    #[crdt(skip)]
    local_only: Option<String>,
}

// A rename + a junction table, to cover those codegen paths.
#[derive(Crdt)]
#[crdt(table = "widgets")]
struct Widget {
    #[crdt(pk)]
    id: String,
    #[crdt(rename = "is_favorite")]
    favorite: bool,
}

#[derive(Crdt)]
#[crdt(table = "paper_collections", pk = (paper_id, collection_id; sep = ':'))]
struct PaperCollection {
    paper_id: String,
    collection_id: String,
}

#[test]
fn column_constants_honor_field_names_and_rename() {
    assert_eq!(Paper::TITLE, "title");
    assert_eq!(Paper::AUTHORS, "authors");
    assert_eq!(Paper::NOTES, "notes");
    assert_eq!(Paper::ID, "id");
    // The const is named after the Rust field; the value honors `rename`.
    assert_eq!(Widget::FAVORITE, "is_favorite");
}

#[test]
fn all_lists_tracked_columns_in_declaration_order_excluding_pk_and_skip() {
    assert_eq!(
        Paper::ALL,
        &[
            "title",
            "authors",
            "is_favorite",
            "is_read",
            "date_added",
            "date_modified",
            "citation_count",
            "cover",
            "notes",
        ]
    );
    // `id` (pk) and `local_only` (skip) are absent.
    assert!(!Paper::ALL.contains(&"id"));
    assert!(!Paper::ALL.contains(&"local_only"));
}

#[test]
fn table_spec_matches_hand_written() {
    let spec = Paper::table_spec();
    assert_eq!(spec.name, "papers");
    assert_eq!(spec.columns, Paper::ALL.to_vec());
    match spec.pk {
        PkSpec::Single { column } => assert_eq!(column, "id"),
        _ => panic!("expected single pk"),
    }
    // Skeleton entries were generated for the columns that declared one.
    let skeleton_cols: Vec<&str> = spec.skeleton.iter().map(|(c, _)| c.as_str()).collect();
    assert_eq!(
        skeleton_cols,
        vec![
            "title",
            "authors",
            "is_favorite",
            "is_read",
            "date_added",
            "date_modified",
        ]
    );
}

#[test]
fn composite_pk_junction_has_no_tracked_columns() {
    let spec = PaperCollection::table_spec();
    assert_eq!(spec.name, "paper_collections");
    assert!(
        spec.columns.is_empty(),
        "key columns are not tracked columns"
    );
    assert!(PaperCollection::ALL.is_empty());
    match spec.pk {
        PkSpec::Composite { columns, sep } => {
            assert_eq!(
                columns,
                ("paper_id".to_string(), "collection_id".to_string())
            );
            assert_eq!(sep, ':');
        }
        _ => panic!("expected composite pk"),
    }
}

#[test]
fn derived_and_hand_written_schemas_have_the_same_fingerprint() {
    // The derived schema for the two tables the hand-written support schema also
    // defines with the same shapes: `papers` and `paper_collections`. Fingerprint
    // ignores skeletons, so only tables/columns/pk shape must match.
    let hand = Schema::new(vec![
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
                "notes",
            ],
        ),
        TableSpec::new("paper_collections", []).with_pk(PkSpec::composite(
            "paper_id",
            "collection_id",
            ':',
        )),
    ]);
    let derived = recrr::schema![Paper, PaperCollection];
    assert_eq!(
        hand.fingerprint(),
        derived.fingerprint(),
        "derived schema must be wire-compatible with the hand-written one"
    );
}

#[test]
fn schema_of_builds_a_single_table_schema() {
    let s = Schema::of::<Widget>();
    assert_eq!(s.tables.len(), 1);
    assert_eq!(s.tables[0].name, "widgets");
}

/// A full round-trip: a derive-configured device syncs a change to a hand-schema
/// device and both converge — proving the generated spec drives real sync.
#[tokio::test]
async fn derive_built_device_round_trips_with_hand_schema_device() {
    async fn new_papers_db() -> SqliteDb {
        let db = SqliteDb::in_memory().unwrap();
        db.execute(
            "CREATE TABLE papers (
                id TEXT PRIMARY KEY, title TEXT NOT NULL DEFAULT '',
                authors TEXT NOT NULL DEFAULT '[]',
                is_favorite INTEGER NOT NULL DEFAULT 0,
                is_read INTEGER NOT NULL DEFAULT 0,
                date_added TEXT NOT NULL DEFAULT '',
                date_modified TEXT NOT NULL DEFAULT '',
                citation_count INTEGER, cover BLOB,
                notes TEXT NOT NULL DEFAULT ''
            )",
            vec![],
        )
        .await
        .unwrap();
        db
    }

    // Source uses the DERIVED schema; target uses a hand-written equivalent.
    let src = Crr::new(new_papers_db().await, Schema::of::<Paper>());
    src.init().await.unwrap();
    let tgt = Crr::new(
        new_papers_db().await,
        Schema::new(vec![TableSpec::new("papers", Paper::ALL.iter().copied())
            .with_skeleton([(
                "title",
                SkeletonValue::Literal(Value::Text(String::new())),
            )])]),
    );
    tgt.init().await.unwrap();

    src.db()
        .execute(
            "INSERT INTO papers (id, title) VALUES ('p1', 'Hello')",
            vec![],
        )
        .await
        .unwrap();
    src.track_insert("papers", "p1", Paper::ALL).await.unwrap();

    let cs = src.changeset_since(0).await.unwrap();
    let res = tgt.apply_changeset(&cs).await.unwrap();
    assert_eq!(res.peer_schema, None, "same fingerprint => no mismatch");

    let rows = tgt
        .db()
        .query("SELECT title FROM papers WHERE id = 'p1'", vec![])
        .await
        .unwrap();
    assert_eq!(rows[0].get(0).as_text(), Some("Hello"));
}
