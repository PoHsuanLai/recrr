//! Sync state: site ID, db version counter, and key-value store.

use crate::db::{Db, Error, Value};
use crate::Crr;

impl<D: Db> Crr<D> {
    /// Get this device's 16-byte site UUID, creating one if needed.
    pub async fn site_id(&self) -> Result<Vec<u8>, Error> {
        let rows = self
            .db
            .query("SELECT site_id FROM crr_site_id LIMIT 1", vec![])
            .await?;
        if let Some(row) = rows.into_iter().next() {
            if let Some(blob) = row.get(0).as_blob() {
                return Ok(blob.to_vec());
            }
        }
        // Fallback: create if init didn't run.
        self.db
            .execute(
                "INSERT OR IGNORE INTO crr_site_id (site_id) VALUES (randomblob(16))",
                vec![],
            )
            .await?;
        let rows = self
            .db
            .query("SELECT site_id FROM crr_site_id LIMIT 1", vec![])
            .await?;
        let row = rows.into_iter().next().ok_or(Error::NoRows)?;
        Ok(row.get(0).as_blob().map(|b| b.to_vec()).unwrap_or_default())
    }

    /// Atomically increment and return the global db_version.
    pub(crate) async fn next_db_version(&self) -> Result<i64, Error> {
        self.db
            .execute("UPDATE crr_db_version SET version = version + 1", vec![])
            .await?;
        let rows = self
            .db
            .query("SELECT version FROM crr_db_version LIMIT 1", vec![])
            .await?;
        let row = rows.into_iter().next().ok_or(Error::NoRows)?;
        Ok(row.get(0).as_integer().unwrap_or(1))
    }

    /// Read the current db_version without incrementing.
    pub async fn current_db_version(&self) -> Result<i64, Error> {
        let rows = self
            .db
            .query("SELECT version FROM crr_db_version LIMIT 1", vec![])
            .await?;
        let row = rows.into_iter().next().ok_or(Error::NoRows)?;
        Ok(row.get(0).as_integer().unwrap_or(0))
    }

    /// Read a blob value from the key-value sync state store (e.g. server change tokens).
    pub async fn get_sync_state(&self, key: &str) -> Option<Vec<u8>> {
        let rows = self
            .db
            .query(
                "SELECT value FROM crr_sync_state WHERE key = ?1",
                vec![Value::Text(key.to_string())],
            )
            .await
            .ok()?;
        rows.into_iter()
            .next()
            .and_then(|row| row.get(0).as_blob().map(|b| b.to_vec()))
    }

    /// Write a blob value to the key-value sync state store (upsert).
    pub async fn set_sync_state(&self, key: &str, value: &[u8]) -> Result<(), Error> {
        self.db
            .execute(
                "INSERT OR REPLACE INTO crr_sync_state (key, value) VALUES (?1, ?2)",
                vec![Value::Text(key.to_string()), Value::Blob(value.to_vec())],
            )
            .await?;
        Ok(())
    }
}
