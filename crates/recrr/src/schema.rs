//! The application-supplied description of which tables and columns to sync.
//!
//! This replaces what would otherwise be hardcoded knowledge in the CRDT core:
//! the set of synced tables and their columns, each table's primary-key shape,
//! and the NOT NULL defaults used to materialize a "skeleton" row when a change
//! arrives for a row that doesn't exist locally yet.

use crate::db::Value;

/// The set of tables `recrr` tracks and syncs.
#[derive(Clone, Debug)]
pub struct Schema {
    pub tables: Vec<TableSpec>,
}

impl Schema {
    /// Build a schema from its tables.
    pub fn new(tables: Vec<TableSpec>) -> Self {
        Self { tables }
    }

    /// The spec for `table`, if tracked.
    pub fn table(&self, table: &str) -> Option<&TableSpec> {
        self.tables.iter().find(|t| t.name == table)
    }

    /// Whether `col` is a valid tracked column of `table`.
    ///
    /// The sentinel column is valid for any tracked table.
    pub(crate) fn is_valid_column(&self, table: &str, col: &str) -> bool {
        match self.table(table) {
            Some(spec) => col == crate::SENTINEL || spec.columns.iter().any(|c| c == col),
            None => false,
        }
    }
}

/// How a table's primary key is represented in a [`ChangeRow`]'s `pk` field.
///
/// [`ChangeRow`]: crate::ChangeRow
#[derive(Clone, Debug)]
pub enum PkSpec {
    /// A single primary-key column, e.g. `id`. The `pk` string is its value.
    Single {
        /// The primary-key column name (used in `WHERE {col} = ?`).
        column: String,
    },
    /// A composite key of two columns encoded as `"a{sep}b"` in the `pk` string.
    ///
    /// Used for junction tables (e.g. `paper_collections` keyed by
    /// `paper_id` + `collection_id`).
    Composite {
        /// The two key column names, in order.
        columns: (String, String),
        /// The separator joining the two values in the `pk` string.
        sep: char,
    },
}

impl PkSpec {
    /// A single-column key named `column`.
    pub fn single(column: impl Into<String>) -> Self {
        PkSpec::Single {
            column: column.into(),
        }
    }

    /// A composite key of `a` + `b` joined by `sep`.
    pub fn composite(a: impl Into<String>, b: impl Into<String>, sep: char) -> Self {
        PkSpec::Composite {
            columns: (a.into(), b.into()),
            sep,
        }
    }
}

/// A default value for a NOT NULL column in a skeleton row.
///
/// A skeleton row is a placeholder inserted when a change references a row that
/// doesn't exist locally; column-level changes then fill in the real values.
/// Only NOT NULL columns without a database default need to appear here.
#[derive(Clone, Debug)]
pub enum SkeletonValue {
    /// A literal value.
    Literal(Value),
    /// The current time as an RFC 3339 string, filled in at insert time.
    ///
    /// Requires the `chrono` feature (enabled by default).
    #[cfg(feature = "chrono")]
    NowRfc3339,
}

impl SkeletonValue {
    /// Resolve to a concrete [`Value`] at insert time.
    pub(crate) fn resolve(&self) -> Value {
        match self {
            SkeletonValue::Literal(v) => v.clone(),
            #[cfg(feature = "chrono")]
            SkeletonValue::NowRfc3339 => Value::Text(chrono::Utc::now().to_rfc3339()),
        }
    }
}

/// The description of one synced table.
#[derive(Clone, Debug)]
pub struct TableSpec {
    /// The table name.
    pub name: String,
    /// The columns to track for LWW sync. Excludes the primary key(s) and any
    /// derived/re-computable columns you don't want to replicate.
    pub columns: Vec<String>,
    /// How the primary key is shaped.
    pub pk: PkSpec,
    /// NOT NULL columns and their skeleton-row defaults, in `(column, default)`
    /// pairs. Must cover every NOT NULL column lacking a database default, so an
    /// `INSERT` of a skeleton row succeeds. For [`PkSpec::Composite`] tables the
    /// key columns are supplied automatically and need not be listed here.
    pub skeleton: Vec<(String, SkeletonValue)>,
}

impl TableSpec {
    /// A table with a single `id` primary key and the given tracked columns.
    pub fn new(name: impl Into<String>, columns: impl IntoIterator<Item = &'static str>) -> Self {
        Self {
            name: name.into(),
            columns: columns.into_iter().map(String::from).collect(),
            pk: PkSpec::single("id"),
            skeleton: Vec::new(),
        }
    }

    /// Override the primary-key shape.
    pub fn with_pk(mut self, pk: PkSpec) -> Self {
        self.pk = pk;
        self
    }

    /// Set the skeleton-row NOT NULL defaults.
    pub fn with_skeleton(
        mut self,
        skeleton: impl IntoIterator<Item = (&'static str, SkeletonValue)>,
    ) -> Self {
        self.skeleton = skeleton
            .into_iter()
            .map(|(c, v)| (c.to_string(), v))
            .collect();
        self
    }
}
