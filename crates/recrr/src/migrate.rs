//! Explicit schema-migration primitives.
//!
//! `recrr`'s CRDT metadata (the per-table `{table}__crr_clock` shadow tables and
//! the persisted schema fingerprint) must be kept in step with your application
//! schema as it evolves. These primitives do exactly that — and *only* that: each
//! one mutates recrr's own metadata, never your real tables. You run your own
//! `ALTER TABLE` / rename first, update the in-memory [`Schema`](crate::Schema)
//! you pass to [`Crr::new`](crate::Crr::new), then call the matching primitive so
//! the clock metadata (and any backfill required for existing rows to sync)
//! catches up.
//!
//! Each primitive bumps the persisted schema version
//! ([`Crr::schema_version`](crate::Crr::schema_version)) and re-stores the schema
//! fingerprint, so peers on a different schema are detectable via
//! [`Crr::apply_changeset`](crate::Crr::apply_changeset).
//!
//! # Ordering contract
//!
//! Call migration primitives in the same order you evolve your schema, and call
//! them once per migration step. They are written to be safe to re-run (idempotent
//! where the underlying operation allows), but the canonical usage is one call per
//! schema change, right after the corresponding real-table DDL.

use crate::db::{Db, Error, Value};
use crate::helpers::clock_table;
use crate::schema::PkSpec;
use crate::{Crr, SENTINEL};

impl<D: Db> Crr<D> {
    /// Backfill clock entries so an added tracked column syncs for existing rows.
    ///
    /// Precondition: you have already run `ALTER TABLE {table} ADD COLUMN {column}`
    /// on the real table, and `column` is present in the in-memory
    /// [`TableSpec`](crate::TableSpec) for `table`.
    ///
    /// For every **live** row (sentinel causal length odd) in `{table}__crr_clock`
    /// that lacks a clock entry for `column`, this inserts one at `col_ver = 1`.
    /// The whole backfill shares a single `db_ver` (a batch), so it emits as one
    /// coherent chunk from [`changes_since`](crate::Crr::changes_since) and does
    /// not bloat the logical clock. Deleted rows are intentionally skipped: their
    /// column clocks are never read (a resurrect zeroes them first), so seeding
    /// them would be pure overhead.
    ///
    /// Idempotent: uses `INSERT OR IGNORE`, so re-running never clobbers a higher
    /// existing version and adds nothing once every live row is backfilled.
    ///
    /// # Convergence note
    ///
    /// Backfilled entries land at `col_ver = 1`; a genuine later edit (`col_ver`
    /// ≥ 2) always wins the LWW compare. If two replicas independently backfill
    /// the *same* `(row, column)` with *different* values (e.g. divergent
    /// `ADD COLUMN` defaults), the tie resolves deterministically by value then
    /// `site_id` — convergent, but one value is dropped. Use a deterministic
    /// default across replicas to avoid that.
    pub async fn migrate_add_column(&self, table: &str, column: &str) -> Result<(), Error> {
        if column == SENTINEL {
            return Err(Error::Backend(format!(
                "'{SENTINEL}' is reserved and cannot be added as a column"
            )));
        }
        let clock = clock_table(table);
        let site = self.site_id().await?;

        // Live pks: sentinel col_ver is odd. One query, then filter in-memory.
        let rows = self
            .db
            .query(
                &format!("SELECT pk, col_ver FROM {clock} WHERE col_name = '{SENTINEL}'"),
                vec![],
            )
            .await?;
        let live_pks: Vec<String> = rows
            .into_iter()
            .filter(|r| r.get(1).as_integer().unwrap_or(0) % 2 == 1)
            .map(|r| r.get(0).as_text().unwrap_or_default().to_string())
            .collect();

        if !live_pks.is_empty() {
            // One db_ver for the whole batch; distinct seq per row keeps a stable
            // order in the outgoing stream (ORDER BY db_ver, seq).
            let db_ver = self.next_db_version().await?;
            for (i, pk) in live_pks.iter().enumerate() {
                self.db
                    .execute(
                        &format!(
                            "INSERT OR IGNORE INTO {clock} (pk, col_name, col_ver, db_ver, site_id, seq)
                             VALUES (?1, ?2, 1, ?3, ?4, ?5)"
                        ),
                        vec![
                            Value::Text(pk.clone()),
                            Value::Text(column.to_string()),
                            Value::Integer(db_ver),
                            Value::Blob(site.clone()),
                            Value::Integer(i as i64),
                        ],
                    )
                    .await?;
            }
        }

        self.bump_schema_version().await?;
        Ok(())
    }

    /// Drop a removed tracked column's clock metadata.
    ///
    /// Precondition: `column` has been removed from the in-memory
    /// [`TableSpec`](crate::TableSpec). Dropping the real column is your job
    /// (SQLite may require a table rebuild for that).
    ///
    /// Deletes every `{table}__crr_clock` row for `column`. After this, inbound
    /// changes for the column are correctly unknown; applied via
    /// [`apply_changeset`](crate::Crr::apply_changeset) they are *reported* in
    /// [`MergeResult::skipped_unknown`](crate::MergeResult) rather than silently
    /// dropped. Idempotent.
    pub async fn migrate_drop_column(&self, table: &str, column: &str) -> Result<(), Error> {
        if column == SENTINEL {
            return Err(Error::Backend(format!(
                "refusing to drop the reserved '{SENTINEL}' column"
            )));
        }
        let clock = clock_table(table);
        self.db
            .execute(
                &format!("DELETE FROM {clock} WHERE col_name = ?1"),
                vec![Value::Text(column.to_string())],
            )
            .await?;
        self.bump_schema_version().await?;
        Ok(())
    }

    /// Rename a table's clock metadata to follow a real-table rename.
    ///
    /// Precondition: you have already renamed the real table, updated the
    /// in-memory [`Schema`](crate::Schema) to use `new`, and have **not** yet run
    /// [`init`](crate::Crr::init) under the new name (so `{new}__crr_clock` does
    /// not exist). This renames `{old}__crr_clock` to `{new}__crr_clock`,
    /// preserving all history (clocks, causal lengths, `db_ver`s) and the
    /// `(pk, col_name)` primary key.
    ///
    /// If `{new}__crr_clock` already exists this returns an error rather than
    /// risk a lossy merge — recreate the situation with the clock table absent,
    /// or migrate manually.
    pub async fn migrate_rename_table(&self, old: &str, new: &str) -> Result<(), Error> {
        let old_clock = clock_table(old);
        let new_clock = clock_table(new);

        if self.table_exists(&new_clock).await? {
            return Err(Error::Backend(format!(
                "{new_clock} already exists; rename the clock table before init() runs \
                 under the new name, or migrate manually"
            )));
        }
        self.db
            .execute(
                &format!("ALTER TABLE {old_clock} RENAME TO {new_clock}"),
                vec![],
            )
            .await?;
        self.bump_schema_version().await?;
        Ok(())
    }

    /// Re-encode every stored primary-key string in a table's clock metadata to
    /// follow a change in primary-key shape.
    ///
    /// Precondition: you have already migrated the real table's key columns to the
    /// new shape and updated the in-memory [`Schema`](crate::Schema)'s
    /// [`PkSpec`] to `new_pk`.
    ///
    /// `reencode` maps each distinct old `pk` string (as recrr stored it) to its
    /// new encoding; returning `None` drops that pk's clock rows entirely (use for
    /// rows with no valid new key). Only you know the semantic mapping, so you
    /// supply it.
    ///
    /// All clock rows sharing an old pk (its sentinel and every column) are
    /// rewritten atomically to the new pk in one statement.
    ///
    /// # Errors
    ///
    /// Rejects the migration *before* writing anything if:
    /// - two distinct old pks map to the same new pk (would violate the
    ///   `(pk, col_name)` uniqueness invariant), or
    /// - `new_pk` is [`PkSpec::Composite`] and any produced pk does not decode
    ///   into two components under its separator (it contains no separator char),
    ///   which would make `splitn` downstream yield a malformed key.
    pub async fn migrate_change_pk(
        &self,
        table: &str,
        new_pk: &PkSpec,
        reencode: impl Fn(&str) -> Option<String>,
    ) -> Result<(), Error> {
        let clock = clock_table(table);

        // Distinct old pks currently stored.
        let rows = self
            .db
            .query(&format!("SELECT DISTINCT pk FROM {clock}"), vec![])
            .await?;
        let old_pks: Vec<String> = rows
            .into_iter()
            .map(|r| r.get(0).as_text().unwrap_or_default().to_string())
            .collect();

        // Compute the full mapping and validate BEFORE any write.
        let mut mapping: Vec<(String, Option<String>)> = Vec::with_capacity(old_pks.len());
        let mut seen_new: std::collections::HashSet<String> = std::collections::HashSet::new();
        for old in &old_pks {
            let new = reencode(old);
            if let Some(new_pk_str) = &new {
                // Collision guard.
                if !seen_new.insert(new_pk_str.clone()) {
                    return Err(Error::Backend(format!(
                        "pk re-encode collision: two old keys map to {new_pk_str:?}"
                    )));
                }
                // Separator-ambiguity guard for composite targets.
                if let PkSpec::Composite { sep, .. } = new_pk {
                    if !pk_roundtrips(new_pk_str, *sep) {
                        return Err(Error::Backend(format!(
                            "re-encoded pk {new_pk_str:?} does not decode into two components \
                             under separator {sep:?}"
                        )));
                    }
                }
            }
            mapping.push((old.clone(), new));
        }

        // Apply: rewrite or delete per old pk. One UPDATE rewrites all its rows.
        for (old, new) in mapping {
            match new {
                Some(new_pk_str) => {
                    self.db
                        .execute(
                            &format!("UPDATE {clock} SET pk = ?1 WHERE pk = ?2"),
                            vec![Value::Text(new_pk_str), Value::Text(old)],
                        )
                        .await?;
                }
                None => {
                    self.db
                        .execute(
                            &format!("DELETE FROM {clock} WHERE pk = ?1"),
                            vec![Value::Text(old)],
                        )
                        .await?;
                }
            }
        }

        self.bump_schema_version().await?;
        Ok(())
    }

    /// Whether a table (or clock table) exists, via `sqlite_master`.
    async fn table_exists(&self, name: &str) -> Result<bool, Error> {
        let rows = self
            .db
            .query(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
                vec![Value::Text(name.to_string())],
            )
            .await?;
        Ok(!rows.is_empty())
    }
}

/// A composite pk string is well-formed iff it splits into two components under
/// `sep` — mirrors the `splitn(2, sep)` decoding used throughout the core, which
/// splits at the *first* separator. A string with no separator cannot represent a
/// two-column key and is rejected.
fn pk_roundtrips(pk: &str, sep: char) -> bool {
    let mut parts = pk.splitn(2, sep);
    matches!((parts.next(), parts.next()), (Some(_), Some(_)))
}
