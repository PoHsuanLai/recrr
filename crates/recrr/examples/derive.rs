//! The two-device sync demo, but with the schema derived from a struct.
//!
//! Run with: `cargo run --example derive --features rusqlite`
//!
//! Compare with `examples/two_devices.rs`: here the `TableSpec` and the column
//! names come from `#[derive(Crdt)]`, so the schema is single-sourced from the
//! `Paper` type and `track_*` calls use compile-checked `Paper::TITLE` constants
//! instead of bare string literals.

use recrr::backends::SqliteDb;
use recrr::{Crdt, Crr, Db, Schema, Value};

#[derive(Crdt)]
#[crdt(table = "papers")]
#[allow(dead_code)] // fields drive codegen; not all are read at runtime
struct Paper {
    #[crdt(pk)]
    id: String,
    #[crdt(skeleton = "\"\"")]
    title: String,
    #[crdt(rename = "is_favorite")]
    favorite: bool,
}

async fn open_device() -> Crr<SqliteDb> {
    let db = SqliteDb::in_memory().unwrap();
    db.execute(
        "CREATE TABLE papers (id TEXT PRIMARY KEY, title TEXT NOT NULL DEFAULT '', is_favorite INTEGER NOT NULL DEFAULT 0)",
        vec![],
    )
    .await
    .unwrap();
    // Schema::of::<Paper>() replaces a hand-written TableSpec.
    let crr = Crr::new(db, Schema::of::<Paper>());
    crr.init().await.unwrap();
    crr
}

async fn add_paper(crr: &Crr<SqliteDb>, id: &str, title: &str) {
    crr.db()
        .execute(
            "INSERT INTO papers (id, title) VALUES (?1, ?2)",
            vec![Value::Text(id.into()), Value::Text(title.into())],
        )
        .await
        .unwrap();
    // Compile-checked column names — a typo or a renamed field breaks the build.
    crr.track_insert("papers", id, Paper::ALL).await.unwrap();
}

async fn set_favorite(crr: &Crr<SqliteDb>, id: &str, fav: bool) {
    crr.db()
        .execute(
            "UPDATE papers SET is_favorite = ?1 WHERE id = ?2",
            vec![Value::Integer(fav as i64), Value::Text(id.into())],
        )
        .await
        .unwrap();
    crr.track_update("papers", id, &[Paper::FAVORITE])
        .await
        .unwrap();
}

async fn favorite(crr: &Crr<SqliteDb>, id: &str) -> bool {
    let rows = crr
        .db()
        .query(
            "SELECT is_favorite FROM papers WHERE id = ?1",
            vec![Value::Text(id.into())],
        )
        .await
        .unwrap();
    rows.into_iter()
        .next()
        .map(|r| r.get(0).as_integer().unwrap_or(0) == 1)
        .unwrap_or(false)
}

async fn sync(src: &Crr<SqliteDb>, dst: &Crr<SqliteDb>) {
    let cs = src.changeset_since(0).await.unwrap();
    dst.apply_changeset(&cs).await.unwrap();
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let a = open_device().await;
    let b = open_device().await;

    add_paper(&a, "p1", "A Relational CRDT").await;
    sync(&a, &b).await;

    // Each device toggles favorite; sync both directions and converge.
    set_favorite(&a, "p1", true).await;
    sync(&a, &b).await;
    sync(&b, &a).await;

    let fa = favorite(&a, "p1").await;
    let fb = favorite(&b, "p1").await;
    println!("A: p1.favorite = {fa}");
    println!("B: p1.favorite = {fb}");
    assert_eq!(fa, fb, "the derived-schema devices converged");
    // In your tests, this catches a forgotten track_* call:
    a.debug_assert_all_tracked("papers").await.unwrap();
    println!("\n✓ derive-configured devices converged (favorite = {fa})");
}
