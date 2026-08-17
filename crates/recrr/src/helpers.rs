//! Shared helpers for the CRR core.

use base64::Engine as _;

use crate::db::{Db, Error, Value};
use crate::schema::PkSpec;
use crate::{Crr, SENTINEL};

/// A tagged-JSON key marking a base64-encoded blob, so blob column values
/// round-trip through the `serde_json::Value` wire format without being confused
/// for plain text. Non-blob values are unaffected.
const BLOB_TAG: &str = "$blob";

/// The name of the shadow clock table for a tracked table.
///
/// The single source of truth for the `{table}__crr_clock` naming convention,
/// reused by tracking, merge, and migration code.
pub(crate) fn clock_table(table: &str) -> String {
    format!("{table}__crr_clock")
}

impl<D: Db> Crr<D> {
    /// Returns (col_ver, site_id) for a clock entry, or (0, empty) if not found.
    pub(crate) async fn get_clock_entry(
        &self,
        clock_table: &str,
        pk: &str,
        col_name: &str,
    ) -> (i64, Vec<u8>) {
        let sql =
            format!("SELECT col_ver, site_id FROM {clock_table} WHERE pk = ?1 AND col_name = ?2");
        let rows = self
            .db
            .query(
                &sql,
                vec![
                    Value::Text(pk.to_string()),
                    Value::Text(col_name.to_string()),
                ],
            )
            .await;
        match rows {
            Ok(rows) => match rows.into_iter().next() {
                Some(row) => {
                    let ver = row.get(0).as_integer().unwrap_or(0);
                    let site = row.get(1).as_blob().map(|b| b.to_vec()).unwrap_or_default();
                    (ver, site)
                }
                None => (0, Vec::new()),
            },
            Err(_) => (0, Vec::new()),
        }
    }

    /// Returns col_ver for a clock entry, or 0 if not found.
    pub(crate) async fn get_col_ver(&self, clock_table: &str, pk: &str, col_name: &str) -> i64 {
        let sql = format!("SELECT col_ver FROM {clock_table} WHERE pk = ?1 AND col_name = ?2");
        let rows = self
            .db
            .query(
                &sql,
                vec![
                    Value::Text(pk.to_string()),
                    Value::Text(col_name.to_string()),
                ],
            )
            .await;
        match rows {
            Ok(rows) => rows
                .into_iter()
                .next()
                .map(|row| row.get(0).as_integer().unwrap_or(0))
                .unwrap_or(0),
            Err(_) => 0,
        }
    }

    /// Read a single column value from a row by primary key, returning JSON.
    pub(crate) async fn read_column_value(
        &self,
        table: &str,
        pk: &str,
        col_name: &str,
    ) -> serde_json::Value {
        let Some(spec) = self.schema.table(table) else {
            return serde_json::Value::Null;
        };
        let (where_clause, params) = pk_where(&spec.pk, pk);
        let sql = format!("SELECT {col_name} FROM {table} WHERE {where_clause}");
        let rows = self.db.query(&sql, params).await;
        match rows {
            Ok(rows) => rows
                .into_iter()
                .next()
                .map(|row| value_to_json(&row.get(0)))
                .unwrap_or(serde_json::Value::Null),
            Err(_) => serde_json::Value::Null,
        }
    }

    /// Whether a row is physically present in its table.
    ///
    /// Distinct from the sentinel, which records what peers should *believe*
    /// about the row. The two disagree exactly when tracking failed, which is
    /// what [`track_adopt`](Crr::track_adopt) repairs — so it has to consult the
    /// table itself rather than trusting the clock. Unknown tables and malformed
    /// composite keys report `false`, matching the other helpers here.
    pub(crate) async fn row_exists(&self, table: &str, pk: &str) -> bool {
        let Some(spec) = self.schema.table(table) else {
            return false;
        };
        let (where_clause, params) = pk_where(&spec.pk, pk);
        let sql = format!("SELECT 1 FROM {table} WHERE {where_clause}");
        match self.db.query(&sql, params).await {
            Ok(rows) => !rows.is_empty(),
            Err(_) => false,
        }
    }

    /// Zero non-sentinel clocks so incoming values (col_ver >= 1) win on resurrect.
    pub(crate) async fn zero_column_clocks(&self, clock_table: &str, pk: &str) {
        let sql = format!(
            "UPDATE {clock_table} SET col_ver = 0 WHERE pk = ?1 AND col_name != '{SENTINEL}'"
        );
        let _ = self
            .db
            .execute(&sql, vec![Value::Text(pk.to_string())])
            .await;
    }

    /// Create a skeleton row with NOT NULL defaults; column-level changes fill
    /// actual values. Built generically from the table's [`PkSpec`] and skeleton
    /// defaults. A no-op for unknown tables or malformed composite keys.
    ///
    /// [`PkSpec`]: crate::PkSpec
    pub(crate) async fn create_skeleton_row(&self, table: &str, pk: &str) {
        let Some(spec) = self.schema.table(table) else {
            return;
        };

        // Key columns and their values come from the PkSpec.
        let mut cols: Vec<String> = Vec::new();
        let mut vals: Vec<Value> = Vec::new();
        match &spec.pk {
            PkSpec::Single { column } => {
                cols.push(column.clone());
                vals.push(Value::Text(pk.to_string()));
            }
            PkSpec::Composite { columns, sep } => {
                let parts: Vec<&str> = pk.splitn(2, *sep).collect();
                if parts.len() != 2 {
                    return;
                }
                cols.push(columns.0.clone());
                vals.push(Value::Text(parts[0].to_string()));
                cols.push(columns.1.clone());
                vals.push(Value::Text(parts[1].to_string()));
            }
        }

        // NOT NULL skeleton defaults.
        for (col, default) in &spec.skeleton {
            cols.push(col.clone());
            vals.push(default.resolve());
        }

        let placeholders: Vec<String> = (1..=vals.len()).map(|i| format!("?{i}")).collect();
        let sql = format!(
            "INSERT OR IGNORE INTO {table} ({}) VALUES ({})",
            cols.join(", "),
            placeholders.join(", ")
        );
        let _ = self.db.execute(&sql, vals).await;
    }

    /// Read the raw stored value at `col_name` and re-store the winning
    /// `col_val`, resolving a column-level LWW update.
    pub(crate) async fn write_column_value(
        &self,
        table: &str,
        pk: &str,
        col_name: &str,
        col_val: &serde_json::Value,
    ) -> Result<(), Error> {
        let Some(spec) = self.schema.table(table) else {
            return Ok(());
        };
        let (where_clause, mut params) = pk_where(&spec.pk, pk);
        // The value binds as ?1; shift the pk params after it.
        let val = json_to_value(col_val);
        let where_shifted = shift_placeholders(&where_clause, 1);
        let sql = format!("UPDATE {table} SET {col_name} = ?1 WHERE {where_shifted}");
        let mut all = Vec::with_capacity(params.len() + 1);
        all.push(val);
        all.append(&mut params);
        let _ = self.db.execute(&sql, all).await;
        Ok(())
    }
}

/// Build a `WHERE` clause (with `?1..` placeholders) and its params for a pk.
fn pk_where(pk_spec: &PkSpec, pk: &str) -> (String, Vec<Value>) {
    match pk_spec {
        PkSpec::Single { column } => (format!("{column} = ?1"), vec![Value::Text(pk.to_string())]),
        PkSpec::Composite { columns, sep } => {
            let parts: Vec<&str> = pk.splitn(2, *sep).collect();
            let (a, b) = if parts.len() == 2 {
                (parts[0], parts[1])
            } else {
                (pk, "")
            };
            (
                format!("{} = ?1 AND {} = ?2", columns.0, columns.1),
                vec![Value::Text(a.to_string()), Value::Text(b.to_string())],
            )
        }
    }
}

/// Renumber `?N` placeholders in a clause by `offset` (e.g. `?1` -> `?2`).
fn shift_placeholders(clause: &str, offset: usize) -> String {
    let mut out = String::with_capacity(clause.len());
    let mut chars = clause.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '?' {
            let mut num = String::new();
            while let Some(d) = chars.peek() {
                if d.is_ascii_digit() {
                    num.push(*d);
                    chars.next();
                } else {
                    break;
                }
            }
            if let Ok(n) = num.parse::<usize>() {
                out.push('?');
                out.push_str(&(n + offset).to_string());
                continue;
            }
            out.push('?');
            out.push_str(&num);
        } else {
            out.push(c);
        }
    }
    out
}

/// Convert a [`Value`] to a `serde_json::Value`. Blobs become a tagged
/// base64 object so they round-trip faithfully.
pub(crate) fn value_to_json(val: &Value) -> serde_json::Value {
    match val {
        Value::Text(s) => serde_json::Value::String(s.clone()),
        Value::Integer(i) => serde_json::Value::Number((*i).into()),
        Value::Real(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Null => serde_json::Value::Null,
        Value::Blob(b) => {
            let encoded = base64::engine::general_purpose::STANDARD.encode(b);
            serde_json::json!({ BLOB_TAG: encoded })
        }
    }
}

/// Convert a `serde_json::Value` back to a [`Value`], decoding tagged blobs.
pub(crate) fn json_to_value(val: &serde_json::Value) -> Value {
    match val {
        serde_json::Value::String(s) => Value::Text(s.clone()),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Integer(i)
            } else if let Some(f) = n.as_f64() {
                Value::Real(f)
            } else {
                Value::Null
            }
        }
        serde_json::Value::Bool(b) => Value::Integer(*b as i64),
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Object(map) => {
            // Tagged blob: {"$blob": "<base64>"}.
            if map.len() == 1 {
                if let Some(serde_json::Value::String(b64)) = map.get(BLOB_TAG) {
                    if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(b64) {
                        return Value::Blob(bytes);
                    }
                }
            }
            Value::Text(val.to_string())
        }
        _ => Value::Text(val.to_string()),
    }
}

/// Deterministic JSON comparison for LWW tie-breaking.
pub(crate) fn compare_json_values(
    a: &serde_json::Value,
    b: &serde_json::Value,
) -> std::cmp::Ordering {
    a.to_string().cmp(&b.to_string())
}
