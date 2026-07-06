//! Relational CRDT: per-column last-write-wins sync over ordinary SQL tables.
//!
//! `recrr` tracks changes to your tables and merges changes from other replicas
//! so that every replica converges to the same state — with no central server
//! and no coordination. It is application-level bookkeeping over plain SQL, so
//! it runs on any [`Db`] backend (turso, rusqlite, libSQL, sqlx) rather than
//! requiring a database extension.
//!
//! # Model
//!
//! Each tracked `(row, column)` carries a clock: a per-column version
//! (`col_ver`), the originating device (`site_id`), and ordering metadata. Row
//! existence is tracked on a synthetic `__sentinel` column via a *causal length*
//! counter — odd means alive, even means deleted — which makes delete and
//! resurrect converge deterministically. Conflicts resolve by higher `col_ver`,
//! then by a deterministic value comparison, then by larger `site_id`.
//!
//! # Usage
//!
//! Describe your tables with a [`Schema`], wrap a [`Db`] in [`Crr`], call
//! [`Crr::init`] once, then call [`Crr::track_insert`] / [`Crr::track_update`] /
//! [`Crr::track_delete`] right after your own writes. To sync, exchange
//! [`ChangeRow`]s produced by [`Crr::changes_since`] and apply peers' rows with
//! [`Crr::apply_changes`]. The transport (files, cloud, HTTP) is up to you.

#![cfg_attr(docsrs, feature(doc_cfg))]

mod audit;
pub mod backends;
mod clock_tables;
mod db;
mod helpers;
mod merge;
mod migrate;
mod schema;
mod state;
mod tracking;

use serde::{Deserialize, Serialize};

pub use db::{Db, Error, Row, Value};
pub use schema::{CrdtTable, PkSpec, Schema, SkeletonValue, TableSpec};

/// Derive a [`CrdtTable`] impl and typed column constants for a struct.
///
/// See the crate README for the full attribute grammar. In brief:
///
/// ```ignore
/// use recrr::Crdt;
///
/// #[derive(Crdt)]
/// #[crdt(table = "papers")]
/// struct Paper {
///     #[crdt(pk)]
///     id: String,
///     title: String,
///     #[crdt(rename = "is_favorite")]
///     favorite: bool,
/// }
/// ```
///
/// generates `impl CrdtTable for Paper` plus the constants `Paper::TITLE`,
/// `Paper::FAVORITE` (= `"is_favorite"`), `Paper::ID`, and `Paper::ALL`.
pub use recrr_derive::Crdt;

/// Build a multi-table [`Schema`] from several `#[derive(Crdt)]` types.
///
/// ```ignore
/// let schema = recrr::schema![Paper, Collection, PaperCollection];
/// ```
///
/// Equivalent to `Schema::new(vec![Paper::table_spec(), ...])`.
#[macro_export]
macro_rules! schema {
    ($($t:ty),+ $(,)?) => {
        $crate::Schema::new(vec![ $( <$t as $crate::CrdtTable>::table_spec() ),+ ])
    };
}

/// The synthetic column name that tracks row existence (causal length).
pub(crate) const SENTINEL: &str = "__sentinel";

/// A single column-level change record for sync, carrying LWW metadata.
///
/// This is the wire format exchanged between replicas. Its serde representation
/// is stable — do not reorder or rename fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeRow {
    pub table_name: String,
    pub pk: String,
    /// The changed column, or [`SENTINEL`](crate::SENTINEL)'s value
    /// (`"__sentinel"`) for a row-existence change.
    pub col_name: String,
    pub col_val: serde_json::Value,
    pub col_ver: i64,
    pub db_ver: i64,
    pub site_id: Vec<u8>,
    pub seq: i64,
    /// Causal length: odd = alive, even = deleted.
    pub cl: i64,
}

/// A versioned changeset envelope: the changes plus the sender's schema identity.
///
/// This is the recommended wire unit between replicas. It carries the sender's
/// [`Schema`] version and fingerprint alongside the raw [`ChangeRow`]s, so the
/// receiver can detect a schema mismatch (a peer tracking a different set of
/// tables/columns) instead of silently dropping changes it doesn't recognize.
///
/// It is a superset of the older `Vec<ChangeRow>` payload: `rows` is exactly what
/// [`Crr::changes_since`] returns, so a peer that only understands the raw vector
/// can still consume `changeset.rows` directly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Changeset {
    /// The sender's schema migration version (see [`Crr::schema_version`]).
    pub schema_version: i64,
    /// The sender's schema fingerprint (see [`Schema::fingerprint`]).
    pub fingerprint: u64,
    /// The column-level changes, identical to [`Crr::changes_since`]'s output.
    pub rows: Vec<ChangeRow>,
}

/// Summary of a merge operation: how many changes were applied vs. skipped.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MergeResult {
    /// Changes that won their LWW merge and were written.
    pub applied: usize,
    /// Changes that lost their LWW merge (a newer local value already won). This
    /// is normal convergence, not data loss.
    pub skipped: usize,
    /// Changes skipped because their table/column is unknown to the *local*
    /// schema — i.e. the sender tracks something we do not. Unlike `skipped`,
    /// this signals a schema mismatch worth surfacing to the user (e.g. "peer is
    /// on a newer version; upgrade to receive these edits"). Non-zero here means
    /// the peer's data for those columns is being ignored, deliberately and
    /// visibly, rather than silently lost.
    pub skipped_unknown: usize,
    /// The sender's `(schema_version, fingerprint)` when applied via
    /// [`Crr::apply_changeset`] and it differed from ours; `None` when the
    /// schemas matched or the raw [`Crr::apply_changes`] path was used.
    pub peer_schema: Option<(i64, u64)>,
}

/// A change-tracking and merge handle bound to a [`Db`] and a [`Schema`].
///
/// Clone is cheap when the backing `D` is cheaply cloneable (e.g. a connection
/// handle); the [`Schema`] is shared.
pub struct Crr<D: Db> {
    db: D,
    schema: Schema,
}

impl<D: Db> Crr<D> {
    /// Bind a database and schema together.
    pub fn new(db: D, schema: Schema) -> Self {
        Self { db, schema }
    }

    /// The underlying database handle.
    pub fn db(&self) -> &D {
        &self.db
    }

    /// The schema this handle tracks.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Consume this handle and rebind the same database to a new [`Schema`].
    ///
    /// The typical migration flow: after running your real-table DDL, rebuild the
    /// handle with the evolved schema, then call the matching `migrate_*`
    /// primitive so the CRDT metadata catches up. Reuses the same underlying `D`,
    /// so no reconnection is needed.
    pub fn with_schema(self, schema: Schema) -> Self {
        Self {
            db: self.db,
            schema,
        }
    }
}
