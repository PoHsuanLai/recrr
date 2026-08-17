//! Change tracking: called after each mutation to record changes for sync.

use crate::db::{Db, Error, Value};
use crate::helpers::clock_table;
use crate::{ChangeRow, Crr, SENTINEL};

impl<D: Db> Crr<D> {
    /// Record an INSERT: sentinel CL=1, all columns at col_ver=1.
    ///
    /// Call right after your own `INSERT`. `columns` are the tracked columns
    /// that were written.
    pub async fn track_insert(&self, table: &str, pk: &str, columns: &[&str]) -> Result<(), Error> {
        let site = self.site_id().await?;
        let db_ver = self.next_db_version().await?;
        let clock_table = clock_table(table);

        // Sentinel marks row as alive (CL=1, odd).
        self.db
            .execute(
                &format!(
                    "INSERT OR REPLACE INTO {clock_table} (pk, col_name, col_ver, db_ver, site_id, seq)
                     VALUES (?1, '{SENTINEL}', 1, ?2, ?3, 0)"
                ),
                vec![
                    Value::Text(pk.to_string()),
                    Value::Integer(db_ver),
                    Value::Blob(site.clone()),
                ],
            )
            .await?;

        for (i, col) in columns.iter().enumerate() {
            self.db
                .execute(
                    &format!(
                        "INSERT OR REPLACE INTO {clock_table} (pk, col_name, col_ver, db_ver, site_id, seq)
                         VALUES (?1, ?2, 1, ?3, ?4, ?5)"
                    ),
                    vec![
                        Value::Text(pk.to_string()),
                        Value::Text(col.to_string()),
                        Value::Integer(db_ver),
                        Value::Blob(site.clone()),
                        Value::Integer(i as i64 + 1),
                    ],
                )
                .await?;
        }

        Ok(())
    }

    /// Record an UPDATE: increments col_ver for each changed column.
    pub async fn track_update(
        &self,
        table: &str,
        pk: &str,
        changed_columns: &[&str],
    ) -> Result<(), Error> {
        let site = self.site_id().await?;
        let db_ver = self.next_db_version().await?;
        let clock_table = clock_table(table);

        for (i, col) in changed_columns.iter().enumerate() {
            let current_ver = self.get_col_ver(&clock_table, pk, col).await;
            self.db
                .execute(
                    &format!(
                        "INSERT OR REPLACE INTO {clock_table} (pk, col_name, col_ver, db_ver, site_id, seq)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6)"
                    ),
                    vec![
                        Value::Text(pk.to_string()),
                        Value::Text(col.to_string()),
                        Value::Integer(current_ver + 1),
                        Value::Integer(db_ver),
                        Value::Blob(site.clone()),
                        Value::Integer(i as i64),
                    ],
                )
                .await?;
        }

        Ok(())
    }

    /// Record a DELETE: increments sentinel CL to next even number.
    pub async fn track_delete(&self, table: &str, pk: &str) -> Result<(), Error> {
        let site = self.site_id().await?;
        let db_ver = self.next_db_version().await?;
        let clock_table = clock_table(table);

        let current_cl = self.get_col_ver(&clock_table, pk, SENTINEL).await;
        let new_cl = if current_cl % 2 == 1 {
            current_cl + 1
        } else {
            current_cl + 2
        };

        self.db
            .execute(
                &format!(
                    "INSERT OR REPLACE INTO {clock_table} (pk, col_name, col_ver, db_ver, site_id, seq)
                     VALUES (?1, '{SENTINEL}', ?2, ?3, ?4, 0)"
                ),
                vec![
                    Value::Text(pk.to_string()),
                    Value::Integer(new_cl),
                    Value::Integer(db_ver),
                    Value::Blob(site),
                ],
            )
            .await?;

        // Note: column clocks are intentionally preserved (not dropped) so that
        // resurrection can zero them and let incoming values win.

        Ok(())
    }

    /// Adopt a row that exists in the table but is not correctly tracked.
    ///
    /// This repairs libraries written by a build that never called
    /// [`init`](Self::init): those rows committed to the table while their clock
    /// entries were never written, so [`changes_since`](Self::changes_since)
    /// never emits them and they stay on the machine that created them.
    ///
    /// Unlike [`track_insert`](Self::track_insert), which unconditionally writes
    /// sentinel `col_ver = 1`, this reads the current sentinel first and does the
    /// right thing for each of the three states it can be in:
    ///
    /// - **absent** (`0`) — never tracked. Identical to `track_insert`.
    /// - **even** — [`track_delete`](Self::track_delete) bumped the sentinel to an
    ///   even value and left it in place, so the row reads as deleted even though
    ///   it is present locally. Writing `1` here would move the sentinel
    ///   *backwards* and peers would still see a delete, so the sentinel advances
    ///   to `current + 1` (odd, hence alive) instead.
    /// - **odd** — already alive and tracked. A no-op: re-seeding would reset
    ///   `col_ver` and discard edits a peer has not yet seen.
    ///
    /// That last case makes repeated adoption harmless, so callers may pass every
    /// row in a table without first working out which ones need it.
    ///
    /// A row that is absent from the table is also a no-op, so adoption can never
    /// resurrect something a peer legitimately deleted. Without that check this
    /// would be a footgun: a caller iterating clock entries, or racing a delete
    /// that arrived mid-scan, would silently bring deleted rows back.
    ///
    /// Column clocks are seeded at `col_ver = 1` rather than zeroed. Zeroing is
    /// correct in [`apply_changes`](Self::apply_changes), where an incoming remote
    /// value should win; here the local row *is* the value being adopted, so it
    /// has to carry a version a peer can actually receive. A genuine later edit
    /// on another device still wins, since it carries a higher `col_ver`.
    pub async fn track_adopt(&self, table: &str, pk: &str, columns: &[&str]) -> Result<(), Error> {
        // Only adopt rows that are really there; the clock alone cannot say.
        if !self.row_exists(table, pk).await {
            return Ok(());
        }

        let clock_table = clock_table(table);
        let current_cl = self.get_col_ver(&clock_table, pk, SENTINEL).await;

        // Already alive and tracked — leave its versions alone.
        if current_cl % 2 == 1 {
            return Ok(());
        }

        if current_cl == 0 {
            return self.track_insert(table, pk, columns).await;
        }

        let site = self.site_id().await?;
        let db_ver = self.next_db_version().await?;
        let new_cl = current_cl + 1;

        self.db
            .execute(
                &format!(
                    "INSERT OR REPLACE INTO {clock_table} (pk, col_name, col_ver, db_ver, site_id, seq)
                     VALUES (?1, '{SENTINEL}', ?2, ?3, ?4, 0)"
                ),
                vec![
                    Value::Text(pk.to_string()),
                    Value::Integer(new_cl),
                    Value::Integer(db_ver),
                    Value::Blob(site.clone()),
                ],
            )
            .await?;

        for (i, col) in columns.iter().enumerate() {
            self.db
                .execute(
                    &format!(
                        "INSERT OR REPLACE INTO {clock_table} (pk, col_name, col_ver, db_ver, site_id, seq)
                         VALUES (?1, ?2, 1, ?3, ?4, ?5)"
                    ),
                    vec![
                        Value::Text(pk.to_string()),
                        Value::Text(col.to_string()),
                        Value::Integer(db_ver),
                        Value::Blob(site.clone()),
                        Value::Integer(i as i64 + 1),
                    ],
                )
                .await?;
        }

        Ok(())
    }

    /// Read all changes since a given db_version, wrapped in a versioned
    /// [`Changeset`](crate::Changeset) envelope carrying this replica's schema
    /// identity. Prefer this over [`changes_since`](Self::changes_since) so the
    /// receiver can detect a schema mismatch via
    /// [`apply_changeset`](Self::apply_changeset).
    pub async fn changeset_since(&self, since_db_ver: i64) -> Result<crate::Changeset, Error> {
        let rows = self.changes_since(since_db_ver).await?;
        Ok(crate::Changeset {
            schema_version: self.schema_version().await,
            fingerprint: self.schema.fingerprint(),
            rows,
        })
    }

    /// Read all changes since a given db_version, ready to send to peers.
    pub async fn changes_since(&self, since_db_ver: i64) -> Result<Vec<ChangeRow>, Error> {
        let mut all_changes = Vec::new();

        for table in &self.schema.tables {
            let clock_table = clock_table(&table.name);

            let sql = format!(
                "SELECT pk, col_name, col_ver, db_ver, site_id, seq
                 FROM {clock_table}
                 WHERE db_ver > ?1
                 ORDER BY db_ver, seq"
            );
            let rows = self
                .db
                .query(&sql, vec![Value::Integer(since_db_ver)])
                .await?;

            for row in rows {
                let pk = row.get(0).as_text().unwrap_or_default().to_string();
                let col_name = row.get(1).as_text().unwrap_or_default().to_string();
                let col_ver = row.get(2).as_integer().unwrap_or(0);
                let db_ver = row.get(3).as_integer().unwrap_or(0);
                let site_id_blob = row.get(4).as_blob().map(|b| b.to_vec()).unwrap_or_default();
                let seq = row.get(5).as_integer().unwrap_or(0);

                let cl = self.get_col_ver(&clock_table, &pk, SENTINEL).await;

                let col_val = if col_name == SENTINEL {
                    serde_json::Value::Null
                } else {
                    self.read_column_value(&table.name, &pk, &col_name).await
                };

                all_changes.push(ChangeRow {
                    table_name: table.name.clone(),
                    pk,
                    col_name,
                    col_val,
                    col_ver,
                    db_ver,
                    site_id: site_id_blob,
                    seq,
                    cl,
                });
            }
        }

        Ok(all_changes)
    }
}
