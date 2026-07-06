//! Adding a tracked column mid-life and converging it across two devices.
//!
//! Run with: `cargo run --example migration --features rusqlite`
//!
//! Two devices sync a `notes` table, accumulating rows. Then the app schema grows
//! a `color` column. Each device runs its own `ALTER TABLE`, rebinds to the
//! evolved schema, and calls `migrate_add_column` to backfill clock entries for
//! the rows that predate the column — so those existing rows sync the new column
//! instead of silently never emitting it. Finally the devices converge on `color`.

use recrr::backends::SqliteDb;
use recrr::{Crdt, Crr, Db, Schema, Value};

// The schema is derived from a struct, and it *evolves*: v2 adds a `color`
// column. Deriving both versions makes the change self-documenting.
#[derive(Crdt)]
#[crdt(table = "notes")]
#[allow(dead_code)]
struct NoteV1 {
    #[crdt(pk)]
    id: String,
    #[crdt(skeleton = "\"\"")]
    body: String,
}

#[derive(Crdt)]
#[crdt(table = "notes")]
#[allow(dead_code)]
struct NoteV2 {
    #[crdt(pk)]
    id: String,
    #[crdt(skeleton = "\"\"")]
    body: String,
    color: String,
}

async fn open_device() -> Crr<SqliteDb> {
    let db = SqliteDb::in_memory().unwrap();
    db.execute(
        "CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT NOT NULL)",
        vec![],
    )
    .await
    .unwrap();
    let crr = Crr::new(db, Schema::of::<NoteV1>());
    crr.init().await.unwrap();
    crr
}

async fn add_note(crr: &Crr<SqliteDb>, id: &str, body: &str) {
    crr.db()
        .execute(
            "INSERT INTO notes (id, body) VALUES (?1, ?2)",
            vec![Value::Text(id.into()), Value::Text(body.into())],
        )
        .await
        .unwrap();
    crr.track_insert("notes", id, NoteV1::ALL).await.unwrap();
}

async fn set_color(crr: &Crr<SqliteDb>, id: &str, color: &str) {
    crr.db()
        .execute(
            "UPDATE notes SET color = ?1 WHERE id = ?2",
            vec![Value::Text(color.into()), Value::Text(id.into())],
        )
        .await
        .unwrap();
    crr.track_update("notes", id, &[NoteV2::COLOR])
        .await
        .unwrap();
}

async fn read_color(crr: &Crr<SqliteDb>, id: &str) -> Option<String> {
    let rows = crr
        .db()
        .query(
            "SELECT color FROM notes WHERE id = ?1",
            vec![Value::Text(id.into())],
        )
        .await
        .unwrap();
    rows.into_iter()
        .next()
        .and_then(|r| r.get(0).as_text().map(str::to_string))
}

/// Migrate one device from v1 to v2: real DDL, rebind schema, backfill clocks.
async fn migrate_to_v2(crr: Crr<SqliteDb>) -> Crr<SqliteDb> {
    crr.db()
        .execute(
            "ALTER TABLE notes ADD COLUMN color TEXT NOT NULL DEFAULT ''",
            vec![],
        )
        .await
        .unwrap();
    let crr = crr.with_schema(Schema::of::<NoteV2>());
    crr.migrate_add_column("notes", NoteV2::COLOR)
        .await
        .unwrap();
    println!("  migrated to schema v{}", crr.schema_version().await);
    crr
}

async fn sync(src: &Crr<SqliteDb>, dst: &Crr<SqliteDb>) {
    let cs = src.changeset_since(0).await.unwrap();
    let r = dst.apply_changeset(&cs).await.unwrap();
    if let Some((ver, _)) = r.peer_schema {
        println!(
            "  (peer on schema v{ver}; {} change(s) held back as unknown)",
            r.skipped_unknown
        );
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let a = open_device().await;
    let b = open_device().await;

    // Accumulate some rows on v1 and sync them across.
    add_note(&a, "n1", "first").await;
    add_note(&a, "n2", "second").await;
    sync(&a, &b).await;
    println!("Two notes created on v1 and synced to B.\n");

    // The app schema grows a `color` column. Migrate BOTH devices.
    println!("Migrating device A:");
    let a = migrate_to_v2(a).await;
    println!("Migrating device B:");
    let b = migrate_to_v2(b).await;

    // A colors an existing (pre-migration) row. Backfill is what lets this sync.
    set_color(&a, "n1", "red").await;
    println!("\nA sets n1.color = \"red\"; sync A -> B:");
    sync(&a, &b).await;

    let color_a = read_color(&a, "n1").await;
    let color_b = read_color(&b, "n1").await;
    println!("  A: n1.color = {color_a:?}");
    println!("  B: n1.color = {color_b:?}");
    assert_eq!(color_a, color_b, "the migrated-in column converged");
    println!(
        "\n✓ existing rows converged on the new column after migration ({:?})",
        color_a.unwrap()
    );
}
