//! Convergence and merge tests, run entirely against an in-memory SQLite [`Db`]
//! — no turso, proving `recrr` is database-agnostic. Ports the scenarios from
//! Rotero's original `crr_sync_test` / `crr_robustness_test`.

mod support;

use recrr::{ChangeRow, Db, Value};
use support::{sync, Device};

#[tokio::test]
async fn insert_tracks_sentinel_and_columns() {
    let d = Device::new().await;
    d.insert_paper("p1", "Test").await;

    let changes = d.changes().await;
    let paper: Vec<_> = changes
        .iter()
        .filter(|c| c.table_name == "papers")
        .collect();
    assert!(paper.len() > 1, "sentinel + column entries expected");

    let sentinel = paper.iter().find(|c| c.col_name == "__sentinel").unwrap();
    assert_eq!(sentinel.cl, 1, "alive sentinel has CL=1");
}

#[tokio::test]
async fn update_increments_col_ver() {
    let d = Device::new().await;
    d.insert_paper("p1", "A").await;
    let v1 = d.crr.current_db_version().await.unwrap();

    d.set_favorite("p1", true).await;
    let v2 = d.crr.current_db_version().await.unwrap();
    assert!(v2 > v1, "db_version advances on update");

    let changes = d.crr.changes_since(v1).await.unwrap();
    let fav = changes
        .iter()
        .find(|c| c.col_name == "is_favorite")
        .unwrap();
    assert_eq!(fav.col_ver, 2, "col_ver is 2 after one update");
}

#[tokio::test]
async fn delete_sets_even_cl() {
    let d = Device::new().await;
    d.insert_paper("p1", "Doomed").await;
    d.delete_paper("p1").await;

    let changes = d.changes().await;
    let sentinel = changes
        .iter()
        .find(|c| c.table_name == "papers" && c.col_name == "__sentinel")
        .unwrap();
    assert_eq!(sentinel.cl, 2, "deleted sentinel has even CL");
}

#[tokio::test]
async fn two_device_sync_propagates_insert_and_update() {
    let a = Device::new().await;
    let b = Device::new().await;

    a.insert_paper("p1", "Shared Paper").await;
    a.set_favorite("p1", true).await;

    sync(&a, &b).await;

    assert_eq!(b.title("p1").await.as_deref(), Some("Shared Paper"));
    let fav = b
        .crr
        .db()
        .query(
            "SELECT is_favorite FROM papers WHERE id = ?1",
            vec![Value::Text("p1".into())],
        )
        .await
        .unwrap();
    assert_eq!(fav[0].get(0).as_integer(), Some(1));
}

#[tokio::test]
async fn lww_tie_break_higher_value_wins() {
    let a = Device::new().await;
    let b = Device::new().await;

    // Same row on both, same starting title.
    a.insert_paper("p1", "Original").await;
    b.insert_paper("p1", "Original").await;

    // Concurrent edits: both bump title to col_ver 2.
    a.set_title("p1", "Title from A").await;
    b.set_title("p1", "Title from B").await;

    // Apply A into B. Equal col_ver -> higher value wins. "B" > "A".
    sync(&a, &b).await;
    assert_eq!(b.title("p1").await.as_deref(), Some("Title from B"));

    // Symmetric: apply B into A -> A also converges to "Title from B".
    sync(&b, &a).await;
    assert_eq!(a.title("p1").await.as_deref(), Some("Title from B"));
}

#[tokio::test]
async fn delete_beats_concurrent_edit() {
    let a = Device::new().await;
    let b = Device::new().await;

    a.insert_paper("p1", "X").await;
    sync(&a, &b).await;

    // A deletes; B edits concurrently.
    a.delete_paper("p1").await;
    b.set_title("p1", "edited on B").await;

    // Cross-sync both ways -> both converge to deleted.
    sync(&a, &b).await;
    sync(&b, &a).await;

    assert!(!a.paper_exists("p1").await, "A: deleted");
    assert!(
        !b.paper_exists("p1").await,
        "B: delete wins over concurrent edit"
    );
}

#[tokio::test]
async fn apply_is_idempotent() {
    let a = Device::new().await;
    let b = Device::new().await;

    a.insert_paper("p1", "Once").await;
    a.set_title("p1", "Twice").await;
    let changes = a.changes().await;

    let r1 = b.apply(&changes).await;
    assert!(r1.applied > 0);
    let r2 = b.apply(&changes).await;
    assert_eq!(
        r2.applied, 0,
        "re-applying the same changes applies nothing"
    );
    assert_eq!(b.title("p1").await.as_deref(), Some("Twice"));
}

#[tokio::test]
async fn resurrect_after_delete() {
    let a = Device::new().await;
    let b = Device::new().await;

    a.insert_paper("p1", "Alive").await;
    sync(&a, &b).await;

    // A deletes (sentinel CL=2), B applies -> deleted on B.
    a.delete_paper("p1").await;
    sync(&a, &b).await;
    assert!(!b.paper_exists("p1").await, "B sees delete");

    // Resurrect: a peer re-creates the row after seeing the delete, emitting a
    // higher odd CL (3 = alive). This is the merge-side resurrect path.
    let site = vec![9u8; 16];
    let resurrect = vec![
        ChangeRow {
            table_name: "papers".into(),
            pk: "p1".into(),
            col_name: "__sentinel".into(),
            col_val: serde_json::Value::Null,
            col_ver: 3,
            db_ver: 999,
            site_id: site.clone(),
            seq: 0,
            cl: 3,
        },
        ChangeRow {
            table_name: "papers".into(),
            pk: "p1".into(),
            col_name: "title".into(),
            col_val: serde_json::Value::String("Reborn".into()),
            col_ver: 3,
            db_ver: 999,
            site_id: site,
            seq: 1,
            cl: 3,
        },
    ];
    let result = b.apply(&resurrect).await;
    assert!(result.applied > 0, "resurrect applied");
    assert_eq!(
        b.title("p1").await.as_deref(),
        Some("Reborn"),
        "resurrect brings the row back with new value"
    );
}

#[tokio::test]
async fn column_before_sentinel_out_of_order() {
    let a = Device::new().await;
    let b = Device::new().await;

    a.insert_paper("p1", "Ordered").await;
    a.set_title("p1", "New Title").await;

    // Feed B only the non-sentinel column changes first, then the rest.
    let all = a.changes().await;
    let (cols, sentinels): (Vec<_>, Vec<_>) =
        all.into_iter().partition(|c| c.col_name != "__sentinel");
    b.apply(&cols).await; // columns arrive with no local sentinel yet
    b.apply(&sentinels).await;

    assert_eq!(b.title("p1").await.as_deref(), Some("New Title"));
}

#[tokio::test]
async fn composite_key_junction_syncs() {
    let a = Device::new().await;
    let b = Device::new().await;

    // Link paper p1 to collection c1 in the junction table.
    a.crr
        .db()
        .execute(
            "INSERT INTO paper_collections (paper_id, collection_id) VALUES ('p1','c1')",
            vec![],
        )
        .await
        .unwrap();
    a.crr
        .track_insert("paper_collections", "p1:c1", &[])
        .await
        .unwrap();

    sync(&a, &b).await;

    let rows = b
        .crr
        .db()
        .query(
            "SELECT paper_id, collection_id FROM paper_collections",
            vec![],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0).as_text(), Some("p1"));
    assert_eq!(rows[0].get(1).as_text(), Some("c1"));
}

#[tokio::test]
async fn blob_column_round_trips() {
    // Regression: the original hex-out/text-in asymmetry corrupted blobs.
    let a = Device::new().await;
    let b = Device::new().await;

    a.insert_paper("p1", "With Cover").await;
    let cover = vec![0u8, 1, 2, 255, 128, 0, 42];
    a.set_cover("p1", &cover).await;

    sync(&a, &b).await;

    assert_eq!(
        b.cover("p1").await,
        Some(cover),
        "blob survives the JSON changeset round-trip byte-for-byte"
    );
}

#[tokio::test]
async fn three_device_convergence() {
    let a = Device::new().await;
    let b = Device::new().await;
    let c = Device::new().await;

    a.insert_paper("p1", "Start").await;
    a.set_title("p1", "aaa").await;
    b.insert_paper("p1", "Start").await;
    b.set_title("p1", "bbb").await;
    c.insert_paper("p1", "Start").await;
    c.set_title("p1", "ccc").await;

    // Gossip every pair both directions, twice, to settle.
    for _ in 0..2 {
        sync(&a, &b).await;
        sync(&b, &c).await;
        sync(&c, &a).await;
        sync(&a, &c).await;
        sync(&c, &b).await;
        sync(&b, &a).await;
    }

    let ta = a.title("p1").await;
    let tb = b.title("p1").await;
    let tc = c.title("p1").await;
    assert_eq!(ta, tb);
    assert_eq!(tb, tc);
    assert_eq!(
        ta.as_deref(),
        Some("ccc"),
        "highest value wins across all three"
    );
}

/// Regression (found by `proptest_convergence::prop_converges`): a row that is
/// inserted and then deleted **before its first sync** must not resurrect as a
/// blank skeleton on a peer.
///
/// Mechanism: `changes_since` stamps every ChangeRow's `cl` with the row's
/// *current* sentinel CL, so the pre-sync insert's column entries carry the
/// delete's even CL (2). They also sort *before* the sentinel by `(db_ver, seq)`
/// — the delete overwrites the sentinel to a higher db_ver. The fix: the
/// column-before-sentinel path in `merge.rs` seeds the tombstone sentinel but
/// refuses to create a row for an even (deleted) CL, so existence converges to
/// "absent" no matter the arrival order.
#[tokio::test]
async fn insert_then_delete_before_sync_does_not_resurrect() {
    let a = Device::new().await;
    let b = Device::new().await;

    // A creates a paper and deletes it, all before ever syncing.
    a.insert_paper("p1", "Ephemeral").await;
    a.delete_paper("p1").await;

    // A's whole history is one changeset. B applies it.
    sync(&a, &b).await;

    assert!(
        !b.paper_exists("p1").await,
        "peer must not resurrect a row that was deleted before its first sync"
    );
}
