//! Tests for the "forgot-to-track" audit (`untracked_rows` /
//! `debug_assert_all_tracked`).

mod support;

use recrr::Db;
use support::Device;

#[tokio::test]
async fn tracked_rows_are_not_reported() {
    let dev = Device::new().await;
    dev.insert_paper("p1", "A").await; // insert + track_insert
    let untracked = dev.crr.untracked_rows("papers").await.unwrap();
    assert!(untracked.is_empty(), "a properly tracked row is clean");
    dev.crr.debug_assert_all_tracked("papers").await.unwrap();
}

#[tokio::test]
async fn a_row_inserted_without_tracking_is_reported() {
    let dev = Device::new().await;
    // Raw INSERT with NO track_insert — the footgun.
    dev.crr
        .db()
        .execute(
            "INSERT INTO papers (id, title, authors, is_favorite, is_read, date_added, date_modified) \
             VALUES ('ghost', 'X', '[]', 0, 0, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
            vec![],
        )
        .await
        .unwrap();

    let untracked = dev.crr.untracked_rows("papers").await.unwrap();
    assert_eq!(untracked, vec!["ghost".to_string()]);
}

#[tokio::test]
#[should_panic(expected = "untracked row")]
async fn debug_assert_panics_on_untracked_row() {
    let dev = Device::new().await;
    dev.crr
        .db()
        .execute(
            "INSERT INTO papers (id, title, authors, is_favorite, is_read, date_added, date_modified) \
             VALUES ('ghost', 'X', '[]', 0, 0, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
            vec![],
        )
        .await
        .unwrap();
    // Panics in debug builds (tests run with debug_assertions on).
    dev.crr.debug_assert_all_tracked("papers").await.unwrap();
}

#[tokio::test]
async fn deleted_rows_do_not_count_as_untracked() {
    let dev = Device::new().await;
    dev.insert_paper("p1", "A").await;
    dev.delete_paper("p1").await; // real row gone, sentinel even (dead)
    let untracked = dev.crr.untracked_rows("papers").await.unwrap();
    assert!(
        untracked.is_empty(),
        "a deleted row is absent from the real table, so nothing is untracked"
    );
}

#[tokio::test]
async fn composite_pk_rows_are_audited() {
    let dev = Device::new().await;
    // Tracked link.
    dev.link("p1", "c1").await;
    assert!(dev
        .crr
        .untracked_rows("paper_collections")
        .await
        .unwrap()
        .is_empty());

    // Untracked raw insert into the junction table.
    dev.crr
        .db()
        .execute(
            "INSERT INTO paper_collections (paper_id, collection_id) VALUES ('p2', 'c2')",
            vec![],
        )
        .await
        .unwrap();
    let untracked = dev.crr.untracked_rows("paper_collections").await.unwrap();
    assert_eq!(untracked, vec!["p2:c2".to_string()]);
}
