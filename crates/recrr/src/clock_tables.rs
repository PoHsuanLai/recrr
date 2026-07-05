//! CRR metadata and per-table clock table creation.

use crate::db::{Db, Error};
use crate::Crr;

impl<D: Db> Crr<D> {
    /// Create CRR metadata and clock tables (idempotent).
    ///
    /// Call once after your own schema migrations. Creates a `{table}__crr_clock`
    /// table for every table in the [`Schema`](crate::Schema), plus the global
    /// `crr_site_id`, `crr_db_version`, and `crr_sync_state` tables.
    pub async fn init(&self) -> Result<(), Error> {
        self.db
            .execute(
                "CREATE TABLE IF NOT EXISTS crr_site_id (site_id BLOB PRIMARY KEY)",
                vec![],
            )
            .await?;
        self.db
            .execute(
                "CREATE TABLE IF NOT EXISTS crr_db_version (version INTEGER NOT NULL)",
                vec![],
            )
            .await?;
        let rows = self
            .db
            .query("SELECT version FROM crr_db_version LIMIT 1", vec![])
            .await?;
        if rows.is_empty() {
            self.db
                .execute("INSERT INTO crr_db_version (version) VALUES (0)", vec![])
                .await?;
        }
        let rows = self
            .db
            .query("SELECT site_id FROM crr_site_id LIMIT 1", vec![])
            .await?;
        if rows.is_empty() {
            self.db
                .execute(
                    "INSERT INTO crr_site_id (site_id) VALUES (randomblob(16))",
                    vec![],
                )
                .await?;
        }

        // Transport-specific state (e.g. CloudKit server tokens).
        self.db
            .execute(
                "CREATE TABLE IF NOT EXISTS crr_sync_state (
                    key   TEXT PRIMARY KEY,
                    value BLOB
                )",
                vec![],
            )
            .await?;

        for table in &self.schema.tables {
            let sql = format!(
                "CREATE TABLE IF NOT EXISTS {}__crr_clock (
                    pk       TEXT NOT NULL,
                    col_name TEXT NOT NULL,
                    col_ver  INTEGER NOT NULL,
                    db_ver   INTEGER NOT NULL,
                    site_id  BLOB NOT NULL,
                    seq      INTEGER NOT NULL,
                    PRIMARY KEY (pk, col_name)
                )",
                table.name
            );
            self.db.execute(&sql, vec![]).await?;
        }
        Ok(())
    }
}
