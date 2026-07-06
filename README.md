# recrr

**Relational CRDT for SQLite-family databases.** Per-column last-write-wins sync
with causal-length delete/resurrect tracking — serverless, peer-to-peer, and
offline-first. It's ordinary application-level bookkeeping over plain SQL, not a
database extension, so it runs on any driver (turso, rusqlite, libSQL, sqlx)
behind a tiny trait.

```toml
[dependencies]
recrr = { version = "0.1", features = ["turso"] }   # or "rusqlite"
```

## Why

You want two devices to sync a SQLite database with **no central server** — each
edits offline, then they reconcile and converge to the same state. That needs a
CRDT: a deterministic, commutative merge so every replica reaches the same result
regardless of message order.

- **Turso's built-in sync** needs a central Turso Cloud primary and only does
  conflict *detection* (last-push-wins, whole-row).
- **cr-sqlite** does exactly this, but it's a C SQLite *extension* — incompatible
  with pure-Rust drivers like turso.

`recrr` fills that gap: a portable, column-level relational CRDT you can point at
whatever SQL backend you're already on, and sync over whatever transport you like
(a shared cloud folder, object storage, HTTP — `recrr` doesn't care).

## Model

Each tracked `(row, column)` carries a clock: a per-column version (`col_ver`),
the originating device (`site_id`), and ordering metadata, kept in a shadow
`{table}__crr_clock` table beside your real table. Row existence rides on a
synthetic `__sentinel` column via a **causal length** counter — odd = alive,
even = deleted — so delete and resurrect converge deterministically.

Conflicts on the same column resolve by, in order:

1. higher `col_ver` (more edits = later),
2. deterministic value comparison (a genuine-tie breaker),
3. larger `site_id` (final tie breaker).

Because every step is a total order, all replicas independently pick the same
winner. Edits to *different* columns of the same row both survive — merge is
per-column, not per-row.

> **LWW caveat:** for a true same-cell conflict the loser's value is silently
> dropped (no text merge, no conflict marker). That's the right trade for
> metadata like a title or a flag; it's the wrong tool for a collaboratively
> edited document body or a numeric counter.

## Usage

```rust
use recrr::backends::TursoDb;
use recrr::{Crr, Schema, TableSpec, SkeletonValue, Value};

// 1. Describe the tables to sync (pure data — no callbacks).
let schema = Schema::new(vec![
    TableSpec::new("papers", ["title", "authors", "is_favorite"])
        .with_skeleton([
            ("title",   SkeletonValue::Literal(Value::Text(String::new()))),
            ("authors", SkeletonValue::Literal(Value::Text("[]".into()))),
            ("is_favorite", SkeletonValue::Literal(Value::Integer(0))),
        ]),
]);

// 2. Bind a backend + schema, create the metadata tables once.
let crr = Crr::new(TursoDb::new(conn), schema);
crr.init().await?;

// 3. Track changes right after your own writes.
crr.track_insert("papers", &id, &["title", "authors", "is_favorite"]).await?;
crr.track_update("papers", &id, &["title"]).await?;
crr.track_delete("papers", &id).await?;

// 4. Sync: pull changes since a watermark, ship them, apply peers' changes.
let outgoing = crr.changeset_since(watermark).await?; // -> Changeset (versioned)
let result   = crr.apply_changeset(&incoming).await?; // deterministic merge
// result.skipped_unknown / result.peer_schema flag a cross-version peer.
```

The raw `changes_since` / `apply_changes` pair still exists (`Vec<ChangeRow>`),
but prefer the `Changeset` envelope — it carries the sender's schema identity so a
peer on a different schema is *detected*, not silently dropped.

## Schema migrations

Your app schema evolves. `recrr` keeps its CRDT metadata in step through explicit
migration primitives — you run your own `ALTER TABLE` (or rename) first, rebind the
handle to the updated [`Schema`], then call the matching primitive:

```rust
// Add a tracked column: ALTER first, then backfill clock entries for existing rows
// so they sync (existing rows would otherwise never emit the new column).
crr.db().execute("ALTER TABLE papers ADD COLUMN notes TEXT NOT NULL DEFAULT ''", vec![]).await?;
let crr = crr.with_schema(schema_with_notes);   // rebind to the evolved schema
crr.migrate_add_column("papers", "notes").await?;

crr.migrate_drop_column("papers", "old_col").await?;   // remove a column's clocks
crr.migrate_rename_table("papers", "articles").await?; // rename, preserving history
crr.migrate_change_pk("t", &new_pk, |old| Some(re_encode(old))).await?; // change PK shape
```

Each primitive bumps `crr.schema_version()` and re-stores the schema fingerprint, so
peers can tell they're on different versions. Backfill lands new columns at the base
version and skips deleted rows; if two replicas backfill the *same* cell with
*different* values, the tie resolves by the usual LWW rules — so use a deterministic
column default across devices. `migrate_change_pk` refuses mappings that would collide
two keys or produce a malformed composite key.

**Cross-version peers:** when a peer's [`Changeset`] carries a different fingerprint,
`apply_changeset` still merges every column it *knows*, and reports the rest in
`MergeResult { skipped_unknown, peer_schema }` — so a newer peer's edits to columns
you don't track yet are visibly held back (prompt an upgrade), never silently lost.

See [`examples/two_devices.rs`](crates/recrr/examples/two_devices.rs) for a
complete offline-edit-then-converge demo:

```sh
cargo run --example two_devices --features rusqlite
```

## Backends

Each backend is behind its own feature and off by default.

| Feature | Backend | Provides |
|---|---|---|
| `turso` | [turso](https://docs.turso.tech) (pure-Rust SQLite) | `backends::TursoDb` |
| `rusqlite` | [rusqlite](https://docs.rs/rusqlite) (bundled C SQLite) | `backends::SqliteDb` |
| `chrono` *(default)* | — | `SkeletonValue::NowRfc3339` timestamp defaults |

Writing your own backend is a few dozen lines — implement the `Db` trait (two
methods, `execute` and `query`) for your connection type.

## Design notes

- **Config is data, not code.** Tables, primary-key shapes (single or composite),
  and skeleton-row defaults are a plain `Schema` struct — serializable, testable
  without a live database, no trait to implement.
- **Blobs round-trip faithfully.** Blob column values encode as tagged base64 in
  the JSON changeset, so binary data survives sync byte-for-byte.
- **No transactions required.** The core uses only parameterized `execute`/`query`
  and reads back tagged values, keeping the backend trait minimal.

## License

Dual-licensed under either of [MIT](LICENSE-MIT) or
[Apache-2.0](LICENSE-APACHE), at your option.
