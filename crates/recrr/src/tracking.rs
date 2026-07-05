//! Change tracking: called after each mutation to record changes for sync.

use crate::db::{Db, Error, Value};
use crate::{ChangeRow, Crr, SENTINEL};

impl<D: Db> Crr<D> {
    /// Record an INSERT: sentinel CL=1, all columns at col_ver=1.
    ///
    /// Call right after your own `INSERT`. `columns` are the tracked columns
    /// that were written.
    pub async fn track_insert(&self, table: &str, pk: &str, columns: &[&str]) -> Result<(), Error> {
        let site = self.site_id().await?;
        let db_ver = self.next_db_version().await?;
        let clock_table = format!("{table}__crr_clock");

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
        let clock_table = format!("{table}__crr_clock");

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
        let clock_table = format!("{table}__crr_clock");

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

    /// Read all changes since a given db_version, ready to send to peers.
    pub async fn changes_since(&self, since_db_ver: i64) -> Result<Vec<ChangeRow>, Error> {
        let mut all_changes = Vec::new();

        for table in &self.schema.tables {
            let clock_table = format!("{}__crr_clock", table.name);

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
