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
//!
//! All run against the real in-memory rusqlite backend, over a small key space
//! (few pks, few values) so conflicts are frequent — the whole point.

mod support;

use std::collections::BTreeMap;

use proptest::prelude::*;
use recrr::ChangeRow;
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
const N_DEVICES: u8 = 3;
const N_PKS: u8 = 3;
const N_TITLES: u8 = 4;

/// A single step in a generated history: a local edit on one device, or a
/// one-way sync between two.
#[derive(Debug, Clone)]
enum Op {
    Insert { device: u8, pk: u8 },
    SetTitle { device: u8, pk: u8, val: u8 },
    SetFav { device: u8, pk: u8, val: bool },
    Delete { device: u8, pk: u8 },
    Sync { from: u8, to: u8 },
}

fn op_strategy() -> impl Strategy<Value = Op> {
    let dev = 0..N_DEVICES;
    let pk = 0..N_PKS;
    prop_oneof![
        (dev.clone(), pk.clone()).prop_map(|(device, pk)| Op::Insert { device, pk }),
        (dev.clone(), pk.clone(), 0..N_TITLES)
            .prop_map(|(device, pk, val)| Op::SetTitle { device, pk, val }),
        (dev.clone(), pk.clone(), any::<bool>())
            .prop_map(|(device, pk, val)| Op::SetFav { device, pk, val }),
        (dev.clone(), pk.clone()).prop_map(|(device, pk)| Op::Delete { device, pk }),
        (dev.clone(), dev).prop_map(|(from, to)| Op::Sync { from, to }),
    ]
}

fn program_strategy() -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(op_strategy(), 1..40)
}

fn pk_name(pk: u8) -> String {
    format!("p{pk}")
}

fn title_val(val: u8) -> String {
    format!("t{val}")
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

    /// Apply one generated op. Illegal ops (edit/delete a pk absent on that
    /// device, or a self-sync) become no-ops — the generator stays simple and
    /// the invariants still hold.
    async fn apply_op(&self, op: &Op) {
        match op {
            Op::Insert { device, pk } => {
                let d = self.dev(*device);
                let id = pk_name(*pk);
                // Only insert if not already present (INSERT would otherwise fail).
                if !d.paper_exists(&id).await {
                    d.insert_paper(&id, &title_val(0)).await;
                }
            }
            Op::SetTitle { device, pk, val } => {
                let d = self.dev(*device);
                let id = pk_name(*pk);
                if d.paper_exists(&id).await {
                    d.set_title(&id, &title_val(*val)).await;
                }
            }
            Op::SetFav { device, pk, val } => {
                let d = self.dev(*device);
                let id = pk_name(*pk);
                if d.paper_exists(&id).await {
                    d.set_favorite(&id, *val).await;
                }
            }
            Op::Delete { device, pk } => {
                let d = self.dev(*device);
                let id = pk_name(*pk);
                if d.paper_exists(&id).await {
                    d.delete_paper(&id).await;
                }
            }
            Op::Sync { from, to } => {
                if from != to {
                    support::sync(self.dev(*from), self.dev(*to)).await;
                }
            }
        }
    }

    /// Gossip every ordered pair repeatedly until no replica's change count
    /// grows — i.e. every replica has seen every change. Bounded so a bug that
    /// prevents convergence surfaces as a *failed assertion*, not a hang.
    async fn gossip_to_fixpoint(&self) {
        let n = self.devices.len();
        // Each full round is O(n^2) one-way syncs; convergence needs at most a
        // few rounds. Cap generously; the equality assert catches non-progress.
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

/// The canonical observable state of one replica: for every pk, `Some((title,
/// is_favorite))` if the row is live, or absent from the map if not.
///
/// Deliberately compares only the explicitly-tracked `title`/`is_favorite`
/// columns — not `date_added`/`date_modified`, whose skeleton defaults use
/// `NowRfc3339` and would introduce wall-clock nondeterminism into the compare.
async fn observable_state(d: &Device) -> BTreeMap<String, (String, i64)> {
    use recrr::Db;
    let rows = d
        .crr
        .db()
        .query("SELECT id, title, is_favorite FROM papers", vec![])
        .await
        .unwrap();
    let mut map = BTreeMap::new();
    for r in rows {
        let id = r.get(0).as_text().unwrap_or_default().to_string();
        let title = r.get(1).as_text().unwrap_or_default().to_string();
        let fav = r.get(2).as_integer().unwrap_or(0);
        map.insert(id, (title, fav));
    }
    map
}

/// Build a fresh device, replay a program on the given device index only, and
/// return that device's union changeset (everything it knows since v0).
async fn changeset_from_program(ops: &[Op]) -> Vec<ChangeRow> {
    let world = World::new(N_DEVICES).await;
    for op in ops {
        world.apply_op(op).await;
    }
    world.gossip_to_fixpoint().await;
    // Device 0 now knows the whole converged history.
    world.dev(0).changes().await
}

// --- properties --------------------------------------------------------------

proptest! {
    // 1 + 2: convergence and existence agreement.
    //
    // Replay an arbitrary program across 3 replicas, gossip to a fixed point,
    // then assert every replica holds byte-identical observable state. This
    // subsumes existence agreement: if a delete/resurrect race left a zombie,
    // the maps would differ and this fails with the diff.
    #[test]
    fn prop_converges(ops in program_strategy()) {
        block_on(async {
            let world = World::new(N_DEVICES).await;
            for op in &ops {
                world.apply_op(op).await;
            }
            world.gossip_to_fixpoint().await;

            let reference = observable_state(world.dev(0)).await;
            for i in 1..N_DEVICES {
                let other = observable_state(world.dev(i)).await;
                prop_assert_eq!(
                    &reference,
                    &other,
                    "replica 0 and replica {} diverged after full gossip",
                    i
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

            // Order A: as produced.
            let a = Device::new().await;
            a.apply(&changes).await;
            let state_a = observable_state(&a).await;

            // Order B: a deterministic shuffle of the same multiset.
            let mut shuffled = changes.clone();
            deterministic_shuffle(&mut shuffled, perm_seed);
            let b = Device::new().await;
            b.apply(&shuffled).await;
            let state_b = observable_state(&b).await;

            prop_assert_eq!(state_a, state_b, "apply order changed the converged state");
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
