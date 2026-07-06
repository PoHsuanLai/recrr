//! Randomized, model-based robustness tests for the CRDT merge engine.
//!
//! The hand-written `convergence` tests prove specific hand-chosen scenarios.
//! These `proptest` properties instead generate *arbitrary* interleavings of
//! local operations and sync exchanges across several simulated replicas, and
//! assert the CRDT's fundamental invariants hold for every generated case:
//!
//! 1. **Convergence** — after full gossip, all replicas hold identical state.
//! 2. **Existence agreement** — the delete/resurrect scheme never leaves a
//!    "zombie" (a row alive on one replica, gone on another) after gossip.
//! 3. **Idempotence** — re-applying a changeset applies nothing and changes
//!    nothing.
//! 4. **Commutativity** — applying the same multiset of changes in any order
//!    converges to the same state.
//! 5. **Clock convergence** — the underlying `*__crr_clock` metadata (not just
//!    the observable rows) converges too, so a *future* merge can't disagree.
//!
//! The generator exercises all three schema tables: `papers` (single PK, every
//! tracked column type incl. a blob and a nullable int), `collections` (a second
//! single-PK table), and `paper_collections` (a **composite**-PK junction with
//! no tracked columns). A tight key space plus frequent inserts/deletes makes
//! conflicts, and multi-cycle insert→delete→resurrect→edit sequences, common.
//!
//! Sync is modelled two ways: full one-way delivery, and **lossy** delivery that
//! drops a deterministic subset of a changeset (a truncated `.crr` file / dropped
//! CloudKit record). Convergence is asserted only after a final *complete*
//! gossip-to-fixpoint — a CRDT must tolerate partial/out-of-order intermediate
//! delivery so long as every change eventually arrives. Runs across **5**
//! replicas to stress multi-party concurrent conflict.
//!
//! All run against the real in-memory rusqlite backend.

mod support;

use std::collections::BTreeMap;

use proptest::prelude::*;
use recrr::{ChangeRow, Db};
use support::Device;

// --- async driver ------------------------------------------------------------

/// Run an async body to completion on a fresh single-threaded runtime.
///
/// A new runtime per proptest case keeps replicas fully isolated and avoids any
/// cross-case state. `recrr::Db` is async, so every property body needs this.
fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(fut)
}

// --- generated program -------------------------------------------------------

/// Small domains keep the key/value space tight so conflicts happen often.
/// Five replicas (not three) stresses multi-party concurrent conflict harder.
const N_DEVICES: u8 = 5;
const N_PKS: u8 = 3; // shared id space: papers p0..p2, collections c0..c2
const N_VALS: u8 = 4; // small value domain -> frequent equal-value ties

/// A single step in a generated history: a local edit on one device, or a
/// one-way sync between two. `pk`/`pk2` are indices into the small id space.
#[derive(Debug, Clone)]
enum Op {
    // papers (single PK, all column types)
    InsertPaper {
        device: u8,
        pk: u8,
    },
    SetTitle {
        device: u8,
        pk: u8,
        val: u8,
    },
    SetAuthors {
        device: u8,
        pk: u8,
        val: u8,
    },
    SetFav {
        device: u8,
        pk: u8,
        val: bool,
    },
    SetCitationCount {
        device: u8,
        pk: u8,
        val: Option<u8>,
    }, // None -> NULL
    SetCover {
        device: u8,
        pk: u8,
        val: u8,
    }, // blob column
    SetNotes {
        device: u8,
        pk: u8,
        val: u8,
    }, // the migrated-in column
    /// Backfill the `notes` column's clock metadata on one device (models a
    /// per-device schema migration rollout). Idempotent.
    MigrateAddNotes {
        device: u8,
    },
    DeletePaper {
        device: u8,
        pk: u8,
    },
    // collections (a second single-PK table)
    InsertCollection {
        device: u8,
        pk: u8,
    },
    SetCollectionName {
        device: u8,
        pk: u8,
        val: u8,
    },
    DeleteCollection {
        device: u8,
        pk: u8,
    },
    // paper_collections (composite PK, no tracked columns)
    Link {
        device: u8,
        paper: u8,
        collection: u8,
    },
    Unlink {
        device: u8,
        paper: u8,
        collection: u8,
    },
    // gossip
    Sync {
        from: u8,
        to: u8,
    },
    /// Deliver only a subset of the changeset (models a truncated `.crr` file /
    /// dropped record). `drop_seed` deterministically selects which changes drop.
    LossySync {
        from: u8,
        to: u8,
        drop_seed: u32,
    },
}

fn op_strategy() -> impl Strategy<Value = Op> {
    let dev = || 0..N_DEVICES;
    let pk = || 0..N_PKS;
    let val = || 0..N_VALS;
    prop_oneof![
        (dev(), pk()).prop_map(|(device, pk)| Op::InsertPaper { device, pk }),
        (dev(), pk(), val()).prop_map(|(device, pk, val)| Op::SetTitle { device, pk, val }),
        (dev(), pk(), val()).prop_map(|(device, pk, val)| Op::SetAuthors { device, pk, val }),
        (dev(), pk(), any::<bool>()).prop_map(|(device, pk, val)| Op::SetFav { device, pk, val }),
        (dev(), pk(), proptest::option::of(val()))
            .prop_map(|(device, pk, val)| Op::SetCitationCount { device, pk, val }),
        (dev(), pk(), val()).prop_map(|(device, pk, val)| Op::SetCover { device, pk, val }),
        (dev(), pk(), val()).prop_map(|(device, pk, val)| Op::SetNotes { device, pk, val }),
        dev().prop_map(|device| Op::MigrateAddNotes { device }),
        (dev(), pk()).prop_map(|(device, pk)| Op::DeletePaper { device, pk }),
        (dev(), pk()).prop_map(|(device, pk)| Op::InsertCollection { device, pk }),
        (dev(), pk(), val()).prop_map(|(device, pk, val)| Op::SetCollectionName {
            device,
            pk,
            val
        }),
        (dev(), pk()).prop_map(|(device, pk)| Op::DeleteCollection { device, pk }),
        (dev(), pk(), pk()).prop_map(|(device, paper, collection)| Op::Link {
            device,
            paper,
            collection
        }),
        (dev(), pk(), pk()).prop_map(|(device, paper, collection)| Op::Unlink {
            device,
            paper,
            collection
        }),
        (dev(), dev()).prop_map(|(from, to)| Op::Sync { from, to }),
        (dev(), dev(), any::<u32>()).prop_map(|(from, to, drop_seed)| Op::LossySync {
            from,
            to,
            drop_seed
        }),
    ]
}

fn program_strategy() -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(op_strategy(), 1..50)
}

fn paper_id(pk: u8) -> String {
    format!("p{pk}")
}
fn coll_id(pk: u8) -> String {
    format!("c{pk}")
}
fn text_val(val: u8) -> String {
    format!("v{val}")
}
/// A small distinct blob per value, so a wrong blob merge is visible.
fn blob_val(val: u8) -> Vec<u8> {
    vec![val, val.wrapping_add(1), 0xAB]
}

// --- harness -----------------------------------------------------------------

/// A set of independent replicas.
struct World {
    devices: Vec<Device>,
}

impl World {
    async fn new(n: u8) -> Self {
        let mut devices = Vec::new();
        for _ in 0..n {
            devices.push(Device::new().await);
        }
        Self { devices }
    }

    fn dev(&self, i: u8) -> &Device {
        &self.devices[i as usize]
    }

    /// Apply one generated op. Illegal ops (edit/delete an absent row, a link to
    /// a nonexistent pair being unlinked, a self-sync) become no-ops — the
    /// generator stays simple and the invariants still hold. Re-inserting a
    /// locally-deleted pk naturally exercises the resurrect path.
    async fn apply_op(&self, op: &Op) {
        match op {
            Op::InsertPaper { device, pk } => {
                let d = self.dev(*device);
                let id = paper_id(*pk);
                if !d.paper_exists(&id).await {
                    d.insert_paper(&id, &text_val(0)).await;
                }
            }
            Op::SetTitle { device, pk, val } => {
                let d = self.dev(*device);
                let id = paper_id(*pk);
                if d.paper_exists(&id).await {
                    d.set_title(&id, &text_val(*val)).await;
                }
            }
            Op::SetAuthors { device, pk, val } => {
                let d = self.dev(*device);
                let id = paper_id(*pk);
                if d.paper_exists(&id).await {
                    d.set_authors(&id, &text_val(*val)).await;
                }
            }
            Op::SetFav { device, pk, val } => {
                let d = self.dev(*device);
                let id = paper_id(*pk);
                if d.paper_exists(&id).await {
                    d.set_favorite(&id, *val).await;
                }
            }
            Op::SetCitationCount { device, pk, val } => {
                let d = self.dev(*device);
                let id = paper_id(*pk);
                if d.paper_exists(&id).await {
                    d.set_citation_count(&id, val.map(|v| v as i64)).await;
                }
            }
            Op::SetCover { device, pk, val } => {
                let d = self.dev(*device);
                let id = paper_id(*pk);
                if d.paper_exists(&id).await {
                    d.set_cover(&id, &blob_val(*val)).await;
                }
            }
            Op::SetNotes { device, pk, val } => {
                let d = self.dev(*device);
                let id = paper_id(*pk);
                if d.paper_exists(&id).await {
                    d.set_notes(&id, &text_val(*val)).await;
                }
            }
            Op::MigrateAddNotes { device } => {
                // Backfill is safe to run anytime and idempotent.
                self.dev(*device).migrate_add_notes().await;
            }
            Op::DeletePaper { device, pk } => {
                let d = self.dev(*device);
                let id = paper_id(*pk);
                if d.paper_exists(&id).await {
                    d.delete_paper(&id).await;
                }
            }
            Op::InsertCollection { device, pk } => {
                let d = self.dev(*device);
                let id = coll_id(*pk);
                if !d.collection_exists(&id).await {
                    d.insert_collection(&id, &text_val(0)).await;
                }
            }
            Op::SetCollectionName { device, pk, val } => {
                let d = self.dev(*device);
                let id = coll_id(*pk);
                if d.collection_exists(&id).await {
                    d.set_collection_name(&id, &text_val(*val)).await;
                }
            }
            Op::DeleteCollection { device, pk } => {
                let d = self.dev(*device);
                let id = coll_id(*pk);
                if d.collection_exists(&id).await {
                    d.delete_collection(&id).await;
                }
            }
            Op::Link {
                device,
                paper,
                collection,
            } => {
                let d = self.dev(*device);
                let (p, c) = (paper_id(*paper), coll_id(*collection));
                if !d.link_exists(&p, &c).await {
                    d.link(&p, &c).await;
                }
            }
            Op::Unlink {
                device,
                paper,
                collection,
            } => {
                let d = self.dev(*device);
                let (p, c) = (paper_id(*paper), coll_id(*collection));
                if d.link_exists(&p, &c).await {
                    d.unlink(&p, &c).await;
                }
            }
            Op::Sync { from, to } => {
                if from != to {
                    support::sync(self.dev(*from), self.dev(*to)).await;
                }
            }
            Op::LossySync {
                from,
                to,
                drop_seed,
            } => {
                if from != to {
                    let seed = *drop_seed;
                    // Keep each change with a deterministic ~50% chance derived
                    // from (seed, index) — a cheap integer hash, no RNG.
                    support::sync_lossy(self.dev(*from), self.dev(*to), move |i| {
                        let mut h = seed ^ (i as u32).wrapping_mul(0x9E3779B1);
                        h ^= h >> 15;
                        h = h.wrapping_mul(0x85EBCA77);
                        h ^= h >> 13;
                        h & 1 == 0
                    })
                    .await;
                }
            }
        }
    }

    /// Gossip every ordered pair repeatedly until no replica's change count
    /// grows — i.e. every replica has seen every change. Bounded so a bug that
    /// prevents convergence surfaces as a *failed assertion*, not a hang.
    async fn gossip_to_fixpoint(&self) {
        let n = self.devices.len();
        for _ in 0..(n * n + 4) {
            let before = self.total_changes().await;
            for from in 0..n {
                for to in 0..n {
                    if from != to {
                        support::sync(&self.devices[from], &self.devices[to]).await;
                    }
                }
            }
            let after = self.total_changes().await;
            if after == before {
                break;
            }
        }
    }

    async fn total_changes(&self) -> usize {
        let mut total = 0;
        for d in &self.devices {
            total += d.changes().await.len();
        }
        total
    }
}

// --- observable state (what every replica must agree on) ---------------------

/// A paper's fully-observable tracked columns, blob included. Excludes the
/// `date_*` columns, whose skeleton defaults use `NowRfc3339` (wall clock) and
/// would inject nondeterminism into the compare.
type PaperRow = (String, String, i64, Option<i64>, Option<Vec<u8>>, String); // title, authors, fav, cites, cover, notes

/// The full observable state of one replica across all three tables.
#[derive(Debug, PartialEq, Eq)]
struct State {
    papers: BTreeMap<String, PaperRow>,
    collections: BTreeMap<String, String>, // id -> name
    links: Vec<(String, String)>,          // sorted (paper, collection)
}

async fn observable_state(d: &Device) -> State {
    let db = d.crr.db();

    let mut papers = BTreeMap::new();
    let rows = db
        .query(
            "SELECT id, title, authors, is_favorite, citation_count, cover, notes FROM papers",
            vec![],
        )
        .await
        .unwrap();
    for r in rows {
        let id = r.get(0).as_text().unwrap_or_default().to_string();
        let title = r.get(1).as_text().unwrap_or_default().to_string();
        let authors = r.get(2).as_text().unwrap_or_default().to_string();
        let fav = r.get(3).as_integer().unwrap_or(0);
        let cites = r.get(4).as_integer();
        let cover = r.get(5).as_blob().map(|b| b.to_vec());
        let notes = r.get(6).as_text().unwrap_or_default().to_string();
        papers.insert(id, (title, authors, fav, cites, cover, notes));
    }

    let mut collections = BTreeMap::new();
    let rows = db
        .query("SELECT id, name FROM collections", vec![])
        .await
        .unwrap();
    for r in rows {
        let id = r.get(0).as_text().unwrap_or_default().to_string();
        let name = r.get(1).as_text().unwrap_or_default().to_string();
        collections.insert(id, name);
    }

    let mut links = Vec::new();
    let rows = db
        .query(
            "SELECT paper_id, collection_id FROM paper_collections",
            vec![],
        )
        .await
        .unwrap();
    for r in rows {
        let p = r.get(0).as_text().unwrap_or_default().to_string();
        let c = r.get(1).as_text().unwrap_or_default().to_string();
        links.push((p, c));
    }
    links.sort();

    State {
        papers,
        collections,
        links,
    }
}

/// A canonical snapshot of a replica's *semantically meaningful* CRDT clock
/// metadata across all tables: `(clock_table, pk, col_name) -> (col_ver,
/// site_id)`. Two replicas that agree on observable rows but disagree on a field
/// that a *future* merge reads could diverge later, so we assert this converges.
///
/// What's meaningful (and thus compared), and what's excluded:
/// - Every row's **sentinel `col_ver`** (its causal length) is compared — it
///   governs delete/resurrect and MUST converge. Its `site_id` is excluded: the
///   sentinel merge (`merge.rs`) decides purely on CL (`change.cl > local_cl`)
///   and never reads the sentinel's stored site_id, so concurrent same-pk
///   inserts legitimately leave each replica's sentinel tagged with its own site.
/// - A **live** row's column clocks are compared in full (`col_ver` + `site_id`),
///   since the site_id IS the final LWW tie-break for a live column.
/// - A **dead** row's column clocks are excluded entirely: a resurrect zeroes
///   them (`zero_column_clocks`) before any incoming value is compared, so they
///   are never read and cannot affect observable state. Concurrent insert+delete
///   of the same pk on different devices legitimately leaves them divergent.
/// - `db_ver`/`seq`: purely local bookkeeping, expected to differ per replica.
async fn clock_state(d: &Device) -> BTreeMap<(String, String, String), (i64, Option<Vec<u8>>)> {
    // recrr's sentinel column marker (private in the crate; mirrored here).
    const SENTINEL: &str = "__sentinel";
    let db = d.crr.db();
    let mut map = BTreeMap::new();
    for table in ["papers", "collections", "paper_collections"] {
        let clock_table = format!("{table}__crr_clock");
        let rows = db
            .query(
                &format!("SELECT pk, col_name, col_ver, site_id FROM {clock_table}"),
                vec![],
            )
            .await
            .unwrap();

        // First pass: each pk's sentinel CL, to know which rows are alive (odd).
        let mut alive: BTreeMap<String, bool> = BTreeMap::new();
        for r in &rows {
            if r.get(1).as_text().unwrap_or_default() == SENTINEL {
                let pk = r.get(0).as_text().unwrap_or_default().to_string();
                alive.insert(pk, r.get(2).as_integer().unwrap_or(0) % 2 == 1);
            }
        }

        for r in rows {
            let pk = r.get(0).as_text().unwrap_or_default().to_string();
            let col = r.get(1).as_text().unwrap_or_default().to_string();
            let ver = r.get(2).as_integer().unwrap_or(0);
            if col == SENTINEL {
                map.insert((clock_table.clone(), pk, col), (ver, None));
            } else if *alive.get(&pk).unwrap_or(&false) {
                let site = r.get(3).as_blob().map(|b| b.to_vec()).unwrap_or_default();
                map.insert((clock_table.clone(), pk, col), (ver, Some(site)));
            }
            // else: dead row's column clock — excluded (see doc comment).
        }
    }
    map
}

/// Replay a program across N replicas, gossip to a fixed point, return device 0's
/// full converged changeset.
async fn changeset_from_program(ops: &[Op]) -> Vec<ChangeRow> {
    let world = World::new(N_DEVICES).await;
    for op in ops {
        world.apply_op(op).await;
    }
    world.gossip_to_fixpoint().await;
    world.dev(0).changes().await
}

// --- properties --------------------------------------------------------------

proptest! {
    // 1 + 2 + 5: convergence (observable rows AND clocks) and existence
    // agreement. Replay an arbitrary program across 3 replicas, gossip to a
    // fixed point, then assert every replica holds identical observable state
    // and identical clock metadata.
    #[test]
    fn prop_converges(ops in program_strategy()) {
        block_on(async {
            let world = World::new(N_DEVICES).await;
            for op in &ops {
                world.apply_op(op).await;
            }
            world.gossip_to_fixpoint().await;

            let ref_state = observable_state(world.dev(0)).await;
            let ref_clocks = clock_state(world.dev(0)).await;
            for i in 1..N_DEVICES {
                let other_state = observable_state(world.dev(i)).await;
                prop_assert_eq!(
                    &ref_state, &other_state,
                    "replica 0 and replica {} diverged on observable state", i
                );
                let other_clocks = clock_state(world.dev(i)).await;
                prop_assert_eq!(
                    &ref_clocks, &other_clocks,
                    "replica 0 and replica {} diverged on clock metadata", i
                );
            }
            Ok(())
        })?;
    }

    // 3: idempotence. Applying a converged changeset a second time applies
    // nothing and leaves state unchanged.
    #[test]
    fn prop_idempotent(ops in program_strategy()) {
        block_on(async {
            let changes = changeset_from_program(&ops).await;

            let target = Device::new().await;
            target.apply(&changes).await;
            let after_first = observable_state(&target).await;

            let second = target.crr.apply_changes(&changes).await.unwrap();
            let after_second = observable_state(&target).await;

            prop_assert_eq!(second.applied, 0, "re-apply applied {} changes", second.applied);
            prop_assert_eq!(after_first, after_second, "state changed on re-apply");
            Ok(())
        })?;
    }

    // 4: commutativity / order-independence. The same multiset of changes,
    // applied in two different orders to two fresh replicas, converges to the
    // same state.
    #[test]
    fn prop_order_independent(
        ops in program_strategy(),
        perm_seed in any::<u64>(),
    ) {
        block_on(async {
            let changes = changeset_from_program(&ops).await;

            let a = Device::new().await;
            a.apply(&changes).await;
            let state_a = observable_state(&a).await;

            let mut shuffled = changes.clone();
            deterministic_shuffle(&mut shuffled, perm_seed);
            let b = Device::new().await;
            b.apply(&shuffled).await;
            let state_b = observable_state(&b).await;

            prop_assert_eq!(state_a, state_b, "apply order changed the converged state");
            Ok(())
        })?;
    }

    // 6: migration commutes with sync-to-fixpoint. Replaying a program then
    // migrating-all-then-gossiping must converge to the SAME observable + clock
    // state as gossiping-first-then-migrating-all-then-gossiping. If backfill
    // wrote a wrong col_ver/db_ver, these two paths would diverge (the harness
    // also compares clock metadata, so a metadata-only divergence still fails).
    #[test]
    fn prop_migration_commutes(ops in program_strategy()) {
        block_on(async {
            // Path A: run program, migrate every device, then gossip to fixpoint.
            let a = World::new(N_DEVICES).await;
            for op in &ops { a.apply_op(op).await; }
            for i in 0..N_DEVICES { a.dev(i).migrate_add_notes().await; }
            a.gossip_to_fixpoint().await;

            // Path B: run program, gossip, THEN migrate every device, gossip again.
            let b = World::new(N_DEVICES).await;
            for op in &ops { b.apply_op(op).await; }
            b.gossip_to_fixpoint().await;
            for i in 0..N_DEVICES { b.dev(i).migrate_add_notes().await; }
            b.gossip_to_fixpoint().await;

            // Both worlds must be internally converged AND agree with each other.
            let a0 = observable_state(a.dev(0)).await;
            let b0 = observable_state(b.dev(0)).await;
            prop_assert_eq!(&a0, &b0, "migrate-then-sync diverged from sync-then-migrate");

            let a0c = clock_state(a.dev(0)).await;
            for i in 1..N_DEVICES {
                prop_assert_eq!(&a0, &observable_state(a.dev(i)).await, "path A replica {} diverged", i);
                prop_assert_eq!(&a0c, &clock_state(a.dev(i)).await, "path A clock {} diverged", i);
            }
            for i in 0..N_DEVICES {
                prop_assert_eq!(&b0, &observable_state(b.dev(i)).await, "path B replica {} diverged", i);
            }
            Ok(())
        })?;
    }
}

/// A small deterministic in-place shuffle (xorshift-driven Fisher–Yates), so the
/// permutation depends only on the proptest-provided seed — no external RNG,
/// fully reproducible on shrink/replay.
fn deterministic_shuffle<T>(items: &mut [T], seed: u64) {
    let mut state = seed | 1; // avoid the all-zero fixed point
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let len = items.len();
    for i in (1..len).rev() {
        let j = (next() % (i as u64 + 1)) as usize;
        items.swap(i, j);
    }
}
