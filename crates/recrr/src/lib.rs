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

pub mod backends;
mod clock_tables;
mod db;
mod helpers;
mod merge;
mod schema;
mod state;
mod tracking;

use serde::{Deserialize, Serialize};

pub use db::{Db, Error, Row, Value};
pub use schema::{PkSpec, Schema, SkeletonValue, TableSpec};

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

/// Summary of a merge operation: how many changes were applied vs. skipped.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MergeResult {
    pub applied: usize,
    pub skipped: usize,
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
}
