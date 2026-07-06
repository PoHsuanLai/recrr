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

    /// The persisted schema version (monotonic migration counter), or 0 if none
    /// has been stored yet. Distinct from the logical `crr_db_version` clock —
    /// this counts *schema migrations*, that one counts *data changes*.
    pub async fn schema_version(&self) -> i64 {
        match self.get_sync_state(SCHEMA_VERSION_KEY).await {
            Some(bytes) => decode_i64(&bytes).unwrap_or(0),
            None => 0,
        }
    }

    /// The persisted schema fingerprint, or `None` if none has been stored yet.
    ///
    /// This is what the DB *was last initialized/migrated with*; compare it to
    /// [`Schema::fingerprint`](crate::Schema::fingerprint) of the in-memory schema
    /// to detect that the code's schema has drifted from the database.
    pub async fn schema_fingerprint(&self) -> Option<u64> {
        self.get_sync_state(SCHEMA_FINGERPRINT_KEY)
            .await
            .and_then(|bytes| decode_i64(&bytes).map(|v| v as u64))
    }

    /// Persist the current in-memory schema's fingerprint (without touching the
    /// version). Called by [`init`](crate::Crr::init); it seeds the version to 1
    /// only if unset so a fresh database starts at schema version 1.
    pub(crate) async fn store_schema_fingerprint(&self) -> Result<(), Error> {
        let fp = self.schema.fingerprint();
        self.set_sync_state(SCHEMA_FINGERPRINT_KEY, &(fp as i64).to_be_bytes())
            .await?;
        if self.get_sync_state(SCHEMA_VERSION_KEY).await.is_none() {
            self.set_sync_state(SCHEMA_VERSION_KEY, &1i64.to_be_bytes())
                .await?;
        }
        Ok(())
    }

    /// Bump the persisted schema version and re-store the current fingerprint.
    /// Called by each `migrate_*` primitive after it mutates clock metadata.
    pub(crate) async fn bump_schema_version(&self) -> Result<i64, Error> {
        let next = self.schema_version().await + 1;
        self.set_sync_state(SCHEMA_VERSION_KEY, &next.to_be_bytes())
            .await?;
        let fp = self.schema.fingerprint();
        self.set_sync_state(SCHEMA_FINGERPRINT_KEY, &(fp as i64).to_be_bytes())
            .await?;
        Ok(next)
    }
}

/// `crr_sync_state` key holding the schema migration version (big-endian i64).
const SCHEMA_VERSION_KEY: &str = "schema_version";
/// `crr_sync_state` key holding the schema fingerprint (big-endian i64 bits).
const SCHEMA_FINGERPRINT_KEY: &str = "schema_fingerprint";

/// Decode an 8-byte big-endian i64 from the sync-state store.
fn decode_i64(bytes: &[u8]) -> Option<i64> {
    bytes.try_into().ok().map(i64::from_be_bytes)
}
