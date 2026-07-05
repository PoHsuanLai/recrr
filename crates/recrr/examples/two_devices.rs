//! A minimal two-device sync demo, end to end, over the rusqlite backend.
//!
//! Run with: `cargo run --example two_devices --features rusqlite`
//!
//! Two independent in-memory databases ("devices") each edit the same row while
//! offline, then exchange changesets and converge to the same state — with no
//! server and no coordination.

use recrr::backends::SqliteDb;
use recrr::{Crr, Db, PkSpec, Schema, SkeletonValue, TableSpec, Value};

/// The one table we sync: a tiny notes table.
fn schema() -> Schema {
    Schema::new(vec![TableSpec::new("notes", ["body"])
        .with_pk(PkSpec::single("id"))
        .with_skeleton([(
            "body",
            SkeletonValue::Literal(Value::Text(String::new())),
        )])])
}

async fn open_device() -> Crr<SqliteDb> {
    let db = SqliteDb::in_memory().unwrap();
    db.execute(
        "CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT NOT NULL)",
        vec![],
    )
    .await
    .unwrap();
    let crr = Crr::new(db, schema());
    crr.init().await.unwrap();
    crr
}

async fn write_note(crr: &Crr<SqliteDb>, id: &str, body: &str, is_new: bool) {
    crr.db()
        .execute(
            "INSERT INTO notes (id, body) VALUES (?1, ?2)
             ON CONFLICT(id) DO UPDATE SET body = excluded.body",
            vec![Value::Text(id.into()), Value::Text(body.into())],
        )
        .await
        .unwrap();
    if is_new {
        crr.track_insert("notes", id, &["body"]).await.unwrap();
    } else {
        crr.track_update("notes", id, &["body"]).await.unwrap();
    }
}

async fn read_note(crr: &Crr<SqliteDb>, id: &str) -> Option<String> {
    let rows = crr
        .db()
        .query(
            "SELECT body FROM notes WHERE id = ?1",
            vec![Value::Text(id.into())],
        )
        .await
        .unwrap();
    rows.into_iter()
        .next()
        .map(|r| r.get(0).as_text().unwrap_or_default().to_string())
}

/// One-way sync: ship every change from `src` and apply it on `dst`.
async fn sync(src: &Crr<SqliteDb>, dst: &Crr<SqliteDb>) {
    let changes = src.changes_since(0).await.unwrap();
    // In a real app you'd serialize `changes` to JSON, move the bytes over your
    // transport of choice (a shared folder, cloud storage, HTTP), and apply on
    // the other side. Here we hand them over directly.
    let result = dst.apply_changes(&changes).await.unwrap();
    println!(
        "  synced: {} applied, {} skipped",
        result.applied, result.skipped
    );
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let a = open_device().await;
    let b = open_device().await;

    // A creates a note and syncs it to B.
    write_note(&a, "n1", "hello from A", true).await;
    println!("A creates note n1, sync A -> B:");
    sync(&a, &b).await;
    println!("  B sees: {:?}", read_note(&b, "n1").await);

    // Now both edit the SAME note while "offline" — a real conflict.
    write_note(&a, "n1", "apple", false).await;
    write_note(&b, "n1", "banana", false).await;
    println!("\nConcurrent edits — A: \"apple\", B: \"banana\"");

    // Exchange both directions. LWW tie-break (equal version -> higher value)
    // picks "banana" deterministically on BOTH devices.
    sync(&a, &b).await;
    sync(&b, &a).await;

    let final_a = read_note(&a, "n1").await;
    let final_b = read_note(&b, "n1").await;
    println!("\nAfter sync:");
    println!("  A: {final_a:?}");
    println!("  B: {final_b:?}");
    assert_eq!(final_a, final_b, "both devices converged");
    println!("\n✓ converged to {:?} with no server", final_a.unwrap());
}
