//! Debug-time auditing: catch the "forgot to call `track_*`" footgun.
//!
//! recrr never intercepts your writes, so if you run an `INSERT` (or `UPDATE`)
//! and forget the matching [`track_insert`](crate::Crr::track_insert) /
//! [`track_update`](crate::Crr::track_update), that row simply never emits from
//! [`changes_since`](crate::Crr::changes_since) and silently never syncs. A hook
//! *inside* `track_*` cannot catch this — the failure is that the call never
//! happens — so the detector is a **queryable audit** you run in your tests.
//!
//! These methods are for tests and debug assertions, not the hot path: each does
//! a full scan of the table.

use crate::db::{Db, Error, Value};
use crate::helpers::clock_table;
use crate::schema::PkSpec;
use crate::{Crr, SENTINEL};

impl<D: Db> Crr<D> {
    /// Primary keys of live real rows in `table` that have **no** clock entry —
    /// i.e. rows you inserted without calling
    /// [`track_insert`](crate::Crr::track_insert).
    ///
    /// A PK is "tracked" if the clock table has a **live** sentinel entry for it
    /// (causal length odd), matching how a real, existing row is represented. The
    /// returned PKs are encoded exactly as recrr stores them (for a
    /// [`PkSpec::Composite`] table, `"a{sep}b"`), so they line up with
    /// `{table}__crr_clock`.
    ///
    /// An empty result means every live row is tracked. Returns an error only if
    /// the underlying queries fail; an unknown table yields an empty result.
    pub async fn untracked_rows(&self, table: &str) -> Result<Vec<String>, Error> {
        let Some(spec) = self.schema.table(table) else {
            return Ok(Vec::new());
        };

        // Real-table PKs, encoded the way the clock stores them (a single value,
        // or the two composite parts joined by `sep`).
        let select = match &spec.pk {
            PkSpec::Single { column } => format!("SELECT {column} FROM {table}"),
            PkSpec::Composite { columns, .. } => {
                format!("SELECT {}, {} FROM {table}", columns.0, columns.1)
            }
        };
        let real_rows = self.db.query(&select, vec![]).await?;
        let real_pks: Vec<String> = match &spec.pk {
            PkSpec::Single { .. } => real_rows
                .iter()
                .map(|row| row.get(0).as_text().unwrap_or_default().to_string())
                .collect(),
            PkSpec::Composite { sep, .. } => real_rows
                .iter()
                .map(|row| {
                    let a = row.get(0);
                    let b = row.get(1);
                    format!(
                        "{}{sep}{}",
                        a.as_text().unwrap_or_default(),
                        b.as_text().unwrap_or_default()
                    )
                })
                .collect(),
        };

        // Live tracked PKs: sentinel entries with an odd (alive) causal length.
        let clock = clock_table(table);
        let clock_rows = self
            .db
            .query(
                &format!("SELECT pk, col_ver FROM {clock} WHERE col_name = ?1"),
                vec![Value::Text(SENTINEL.to_string())],
            )
            .await?;
        let tracked: std::collections::HashSet<String> = clock_rows
            .into_iter()
            .filter(|r| r.get(1).as_integer().unwrap_or(0) % 2 == 1)
            .map(|r| r.get(0).as_text().unwrap_or_default().to_string())
            .collect();

        Ok(real_pks
            .into_iter()
            .filter(|pk| !tracked.contains(pk))
            .collect())
    }

    /// In debug builds, panic if any live row in `table` is untracked; a no-op in
    /// release builds.
    ///
    /// Call this at the end of a test (or behind your own debug gate) to assert
    /// you never forgot a `track_*` call. The panic message lists the offending
    /// primary keys. Because it is gated on `debug_assertions`, it costs nothing
    /// in a release build.
    pub async fn debug_assert_all_tracked(&self, table: &str) -> Result<(), Error> {
        if cfg!(debug_assertions) {
            let untracked = self.untracked_rows(table).await?;
            assert!(
                untracked.is_empty(),
                "recrr: {} untracked row(s) in `{table}` (missing a track_* call): {untracked:?}",
                untracked.len(),
            );
        }
        Ok(())
    }
}
