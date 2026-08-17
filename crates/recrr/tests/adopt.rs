//! Adoption of rows that exist in the table but are not correctly tracked.
//!
//! A build that never called `init()` writes rows whose clock entries were never
//! created. `changes_since` reads only from the clock tables, so those rows never
//! reach a peer — they stay on the machine that made them, looking perfectly fine
//! locally. `track_adopt` is the repair.
//!
//! The assertions here go through a *second device* wherever the outcome is about
//! syncing. Asserting locally is what let this class of bug hide: the row is
//! visible on the machine that owns it whether or not tracking is correct.

mod support;

use support::{sync, Device};

/// The plain case: a row that was never tracked at all becomes syncable.
#[tokio::test]
async fn an_untracked_row_reaches_a_peer_after_adoption() {
    let a = Device::new().await;
    let b = Device::new().await;

    a.insert_paper("p1", "Paper").await;
    a.insert_collection("c1", "Collection").await;
    a.link_untracked("p1", "c1").await;

    // Before adoption the link is real locally but invisible to the clock.
    assert!(a.link_exists("p1", "c1").await);
    assert_eq!(a.link_sentinel("p1", "c1").await, 0);

    a.adopt_link("p1", "c1").await;
    sync(&a, &b).await;

    assert!(
        b.link_exists("p1", "c1").await,
        "an adopted row must materialize on the peer"
    );
}

/// The case the repair exists for, and the one a `WHERE pk IS NULL` predicate
/// misses.
///
/// `track_delete` bumps the sentinel to an even value and *leaves it in place*,
/// so a row that was ever deleted always has a clock entry. Junction tables key
/// on `"{a}:{b}"`, so removing a tag and adding it back reuses the same clock pk.
/// Adopting only rows with no clock entry therefore skips exactly the row that
/// the user re-created — and because the sentinel still reads even, the peer is
/// actively told the row is deleted.
#[tokio::test]
async fn a_re_added_row_is_adopted_rather_than_read_as_deleted() {
    let a = Device::new().await;
    let b = Device::new().await;

    a.insert_paper("p1", "Paper").await;
    a.insert_collection("c1", "Collection").await;

    // Attach, then detach — the sentinel is now even (deleted).
    a.link("p1", "c1").await;
    a.unlink("p1", "c1").await;
    let after_delete = a.link_sentinel("p1", "c1").await;
    assert_eq!(after_delete % 2, 0, "a deleted row's sentinel must be even");

    // The broken build re-attaches: the row commits, tracking does not run.
    a.link_untracked("p1", "c1").await;

    a.adopt_link("p1", "c1").await;

    let after_adopt = a.link_sentinel("p1", "c1").await;
    assert_eq!(
        after_adopt % 2,
        1,
        "adoption must leave the row alive, got sentinel {after_adopt}"
    );
    assert!(
        after_adopt > after_delete,
        "the sentinel must advance past the delete ({after_delete}), got {after_adopt}"
    );

    sync(&a, &b).await;
    assert!(
        b.link_exists("p1", "c1").await,
        "the re-added row must reach the peer alive, not as a delete"
    );
}

/// Adoption must not disturb a row that is already tracked and alive.
///
/// Re-seeding would reset `col_ver` to 1 and silently discard an edit a peer had
/// not yet seen. This is what makes it safe for the repair pass to hand every row
/// in a table to `track_adopt` without first deciding which ones need it.
#[tokio::test]
async fn adopting_a_live_row_preserves_its_versions() {
    let a = Device::new().await;

    a.insert_paper("p1", "First").await;
    a.set_title("p1", "Second").await;

    let before = a.paper_col_ver("p1", "title").await;
    assert!(before >= 2, "an edited column must be past 1, got {before}");

    a.crr.track_adopt("papers", "p1", &["title"]).await.unwrap();

    assert_eq!(
        a.paper_col_ver("p1", "title").await,
        before,
        "adopting a live row must not rewind its column versions"
    );
}

/// A newer edit on a peer must still win after the local row was adopted.
///
/// Adoption seeds columns at `col_ver = 1`, so a peer holding a genuine later
/// edit has to beat it. If adoption wrote a high version instead, the repair
/// would silently overwrite other devices' work.
#[tokio::test]
async fn a_peer_edit_beats_an_adopted_value() {
    let a = Device::new().await;
    let b = Device::new().await;

    a.insert_paper("p1", "Original").await;
    sync(&a, &b).await;

    // B makes a genuine edit.
    b.set_title("p1", "Edited on B").await;

    // A adopts the same row (as the repair pass would).
    a.crr.track_adopt("papers", "p1", &["title"]).await.unwrap();

    sync(&b, &a).await;

    assert_eq!(
        a.title("p1").await.as_deref(),
        Some("Edited on B"),
        "a real later edit must survive the repair"
    );
}

/// Adoption is idempotent, so a repair pass that runs twice cannot corrupt.
#[tokio::test]
async fn adopting_twice_is_the_same_as_adopting_once() {
    let a = Device::new().await;
    let b = Device::new().await;

    a.insert_paper("p1", "Paper").await;
    a.insert_collection("c1", "Collection").await;
    a.link_untracked("p1", "c1").await;

    a.adopt_link("p1", "c1").await;
    let once = a.link_sentinel("p1", "c1").await;

    a.adopt_link("p1", "c1").await;
    assert_eq!(
        a.link_sentinel("p1", "c1").await,
        once,
        "a second adoption must not move the sentinel"
    );

    sync(&a, &b).await;
    assert!(b.link_exists("p1", "c1").await);
}

/// A row deleted on a peer must stay deleted: adoption repairs untracked rows,
/// it must not resurrect ones that were legitimately removed elsewhere.
#[tokio::test]
async fn adoption_does_not_resurrect_a_peer_delete() {
    let a = Device::new().await;
    let b = Device::new().await;

    a.insert_paper("p1", "Paper").await;
    a.insert_collection("c1", "Collection").await;
    a.link("p1", "c1").await;
    sync(&a, &b).await;
    assert!(b.link_exists("p1", "c1").await);

    // B removes the link and A learns about it.
    b.unlink("p1", "c1").await;
    sync(&b, &a).await;
    assert!(!a.link_exists("p1", "c1").await);

    // A repair pass runs over A. The row is gone from the table, so a scan
    // would not offer it — but adopting it explicitly must not bring it back,
    // locally or for the peer.
    a.adopt_link("p1", "c1").await;

    let sentinel = a.link_sentinel("p1", "c1").await;
    assert_eq!(
        sentinel % 2,
        0,
        "an absent row's sentinel must stay even (deleted), got {sentinel}"
    );

    sync(&a, &b).await;

    assert!(
        !b.link_exists("p1", "c1").await,
        "a legitimately deleted row must not be resurrected by the repair"
    );
    assert!(
        !a.link_exists("p1", "c1").await,
        "the row must stay deleted locally too"
    );
}
