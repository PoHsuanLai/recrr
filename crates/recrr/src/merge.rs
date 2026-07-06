//! LWW merge logic for applying remote changes.

use crate::db::{Db, Error, Value};
use crate::helpers::{clock_table, compare_json_values};
use crate::schema::PkSpec;
use crate::{ChangeRow, Crr, MergeResult, SENTINEL};

impl<D: Db> Crr<D> {
    /// Apply remote changes with LWW merge semantics.
    ///
    /// Idempotent and order-independent: applying the same changes twice, or in a
    /// different order, converges to the same state. Unknown tables/columns
    /// (per the [`Schema`](crate::Schema)) are skipped rather than trusted, and
    /// counted in [`MergeResult::skipped_unknown`].
    ///
    /// This is the raw entry point; prefer [`apply_changeset`](Self::apply_changeset)
    /// when the sender provides a [`Changeset`](crate::Changeset) envelope, to also
    /// learn the peer's schema identity on a mismatch.
    pub async fn apply_changes(&self, changes: &[ChangeRow]) -> Result<MergeResult, Error> {
        let mut result = MergeResult::default();
        self.apply_rows_inner(changes, &mut result).await?;
        Ok(result)
    }

    /// Apply a versioned [`Changeset`](crate::Changeset) envelope.
    ///
    /// Behaves exactly like [`apply_changes`](Self::apply_changes) for the merge
    /// itself (same shared core, so convergence semantics can never drift between
    /// the two entry points), and additionally records the sender's schema
    /// identity in [`MergeResult::peer_schema`] whenever it differs from ours —
    /// turning a silent cross-version skip into an actionable signal. The merge
    /// still applies every change it *can*; only genuinely-unknown ones are
    /// skipped (and counted in [`MergeResult::skipped_unknown`]).
    pub async fn apply_changeset(
        &self,
        changeset: &crate::Changeset,
    ) -> Result<MergeResult, Error> {
        let mut result = MergeResult::default();
        self.apply_rows_inner(&changeset.rows, &mut result).await?;
        let local_fp = self.schema.fingerprint();
        if changeset.fingerprint != local_fp {
            result.peer_schema = Some((changeset.schema_version, changeset.fingerprint));
        }
        Ok(result)
    }

    /// The shared merge core behind both [`apply_changes`](Self::apply_changes)
    /// and [`apply_changeset`](Self::apply_changeset). Mutates `result` in place.
    async fn apply_rows_inner(
        &self,
        changes: &[ChangeRow],
        result: &mut MergeResult,
    ) -> Result<(), Error> {
        let _local_site = self.site_id().await?;

        for change in changes {
            if !self
                .schema
                .is_valid_column(&change.table_name, &change.col_name)
            {
                result.skipped_unknown += 1;
                continue;
            }

            let clock_table = clock_table(&change.table_name);

            if change.col_name == SENTINEL {
                let local_cl = self.get_col_ver(&clock_table, &change.pk, SENTINEL).await;

                if change.cl <= local_cl {
                    result.skipped += 1;
                    continue;
                }

                let is_delete = change.cl % 2 == 0;
                let is_create = !is_delete && local_cl == 0;
                let is_resurrect = !is_delete && local_cl > 0 && local_cl % 2 == 0;

                if is_create {
                    self.create_skeleton_row(&change.table_name, &change.pk)
                        .await;
                } else if is_resurrect {
                    // Resurrect: re-create row and zero clocks so incoming values win.
                    self.create_skeleton_row(&change.table_name, &change.pk)
                        .await;
                    self.zero_column_clocks(&clock_table, &change.pk).await;
                } else if is_delete {
                    self.delete_row(&change.table_name, &change.pk).await;
                }

                let db_ver = self.next_db_version().await?;
                self.db
                    .execute(
                        &format!(
                            "INSERT OR REPLACE INTO {clock_table} (pk, col_name, col_ver, db_ver, site_id, seq)
                             VALUES (?1, '{SENTINEL}', ?2, ?3, ?4, 0)"
                        ),
                        vec![
                            Value::Text(change.pk.clone()),
                            Value::Integer(change.cl),
                            Value::Integer(db_ver),
                            Value::Blob(change.site_id.clone()),
                        ],
                    )
                    .await?;

                result.applied += 1;
            } else {
                // Column-level LWW merge.
                let local_sentinel_cl = self.get_col_ver(&clock_table, &change.pk, SENTINEL).await;

                if local_sentinel_cl == 0 {
                    // Out-of-order: column arrived before sentinel. Seed the
                    // sentinel clock at the column's CL so a later real sentinel
                    // of equal CL is correctly a no-op.
                    let db_ver = self.next_db_version().await?;
                    self.db
                        .execute(
                            &format!(
                                "INSERT OR IGNORE INTO {clock_table} (pk, col_name, col_ver, db_ver, site_id, seq)
                                 VALUES (?1, '{SENTINEL}', ?2, ?3, ?4, 0)"
                            ),
                            vec![
                                Value::Text(change.pk.clone()),
                                Value::Integer(change.cl),
                                Value::Integer(db_ver),
                                Value::Blob(change.site_id.clone()),
                            ],
                        )
                        .await?;

                    if change.cl % 2 == 0 {
                        // The column belongs to an already-deleted row (even CL,
                        // e.g. a row inserted and deleted before its first sync).
                        // Do not resurrect it as a skeleton — the row stays gone.
                        // Existence converges to "absent" regardless of whether
                        // the column or the sentinel arrives first.
                        //
                        // The column clock is intentionally NOT recorded: a dead
                        // row's column clocks are never read (a resurrect zeroes
                        // them first via `zero_column_clocks`), so they can't
                        // affect observable state. Recording them would only add
                        // work in the merge hot path.
                        result.skipped += 1;
                        continue;
                    }

                    // Genuinely-alive out-of-order column: materialize the row so
                    // the column write below has somewhere to land.
                    self.create_skeleton_row(&change.table_name, &change.pk)
                        .await;
                } else if local_sentinel_cl % 2 == 0
                    && change.cl % 2 == 1
                    && change.cl > local_sentinel_cl
                {
                    // Resurrect: column from newer alive state than local delete.
                    self.create_skeleton_row(&change.table_name, &change.pk)
                        .await;
                    self.zero_column_clocks(&clock_table, &change.pk).await;
                    let db_ver = self.next_db_version().await?;
                    self.db
                        .execute(
                            &format!(
                                "INSERT OR REPLACE INTO {clock_table} (pk, col_name, col_ver, db_ver, site_id, seq)
                                 VALUES (?1, '{SENTINEL}', ?2, ?3, ?4, 0)"
                            ),
                            vec![
                                Value::Text(change.pk.clone()),
                                Value::Integer(change.cl),
                                Value::Integer(db_ver),
                                Value::Blob(change.site_id.clone()),
                            ],
                        )
                        .await?;
                } else if local_sentinel_cl % 2 == 0 {
                    result.skipped += 1;
                    continue;
                }

                let (local_ver, local_clock_site) = self
                    .get_clock_entry(&clock_table, &change.pk, &change.col_name)
                    .await;

                let wins = if change.col_ver > local_ver {
                    true
                } else if change.col_ver < local_ver {
                    false
                } else {
                    // Tie-break: compare values, then site_id (of clock entry, not local device).
                    let local_val = self
                        .read_column_value(&change.table_name, &change.pk, &change.col_name)
                        .await;
                    let val_cmp = compare_json_values(&change.col_val, &local_val);
                    if val_cmp != std::cmp::Ordering::Equal {
                        val_cmp == std::cmp::Ordering::Greater
                    } else {
                        // Final tie-break by site_id; same site means duplicate.
                        change.site_id != local_clock_site && change.site_id > local_clock_site
                    }
                };

                if !wins {
                    result.skipped += 1;
                    continue;
                }

                self.write_column_value(
                    &change.table_name,
                    &change.pk,
                    &change.col_name,
                    &change.col_val,
                )
                .await?;

                let db_ver = self.next_db_version().await?;
                self.db
                    .execute(
                        &format!(
                            "INSERT OR REPLACE INTO {clock_table} (pk, col_name, col_ver, db_ver, site_id, seq)
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6)"
                        ),
                        vec![
                            Value::Text(change.pk.clone()),
                            Value::Text(change.col_name.clone()),
                            Value::Integer(change.col_ver),
                            Value::Integer(db_ver),
                            Value::Blob(change.site_id.clone()),
                            Value::Integer(change.seq),
                        ],
                    )
                    .await?;

                result.applied += 1;
            }
        }

        Ok(())
    }

    /// Delete a row by primary key, respecting the table's [`PkSpec`].
    async fn delete_row(&self, table: &str, pk: &str) {
        let Some(spec) = self.schema.table(table) else {
            return;
        };
        let (where_clause, params) = match &spec.pk {
            PkSpec::Single { column } => {
                (format!("{column} = ?1"), vec![Value::Text(pk.to_string())])
            }
            PkSpec::Composite { columns, sep } => {
                let parts: Vec<&str> = pk.splitn(2, *sep).collect();
                if parts.len() != 2 {
                    return;
                }
                (
                    format!("{} = ?1 AND {} = ?2", columns.0, columns.1),
                    vec![
                        Value::Text(parts[0].to_string()),
                        Value::Text(parts[1].to_string()),
                    ],
                )
            }
        };
        let sql = format!("DELETE FROM {table} WHERE {where_clause}");
        let _ = self.db.execute(&sql, params).await;
    }
}
