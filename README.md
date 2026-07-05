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
let outgoing = crr.changes_since(watermark).await?;   // -> Vec<ChangeRow> (serde)
let result   = crr.apply_changes(&incoming).await?;   // deterministic merge
```

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
