//! The typed mutation log and conflict classification.
//!
//! Ops are recorded at classification grain — which schema-list entries
//! and which tables a commit touched — not at entity-payload grain; the
//! staged entity state lives in the transaction's working snapshot.

use std::collections::{BTreeMap, BTreeSet};

/// One staged mutation, at the grain conflict classification needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Operation {
    /// A schema was created.
    CreateSchema {
        /// The new schema's id.
        schema_id: u64,
        /// The new schema's name (the `changes_made` grammar carries
        /// names for created entries).
        name: String,
    },
    /// A schema was dropped.
    DropSchema {
        /// The dropped schema's id.
        schema_id: u64,
    },
    /// A table was created.
    CreateTable {
        /// The schema the table was created in.
        schema_id: u64,
        /// The new table's id.
        table_id: u64,
        /// The owning schema's name, serialized as `"schema"."table"`.
        schema_name: String,
        /// The new table's name.
        table_name: String,
    },
    /// An existing table was mutated (rename, move, or column DDL).
    AlterTable {
        /// The mutated table's id.
        table_id: u64,
    },
    /// A table was dropped.
    DropTable {
        /// The dropped table's id.
        table_id: u64,
    },
    /// Data was appended to a table.
    RegisterDataFile {
        /// The table rows were inserted into.
        table_id: u64,
    },
    /// Delete markers were appended to a table.
    RegisterDeleteFile {
        /// The table delete markers were appended to.
        table_id: u64,
    },
    /// Data file(s) became eligible for garbage collection via merge.
    ExpireDataFile {
        /// The table whose data files were merged.
        table_id: u64,
    },
    /// Delete marker file(s) became eligible for garbage collection.
    ExpireDeleteFile {
        /// The table whose delete marker files were cleaned up.
        table_id: u64,
    },
    /// Table statistics were updated.
    UpdateStats {
        /// The table whose statistics changed. Exists so a stats-only
        /// commit mints a snapshot; feeds no change-set entry and no
        /// conflict detection.
        table_id: u64,
    },
    /// A view was created.
    CreateView {
        /// The schema the view was created in.
        schema_id: u64,
        /// The new view's id.
        view_id: u64,
        /// The owning schema's name, serialized as `"schema"."view"`.
        schema_name: String,
        /// The new view's name.
        view_name: String,
    },
    /// An existing view was mutated.
    AlterView {
        /// The mutated view's id.
        view_id: u64,
    },
    /// A view was dropped.
    DropView {
        /// The dropped view's id.
        view_id: u64,
    },
    /// A macro was created.
    CreateMacro {
        /// The schema the macro was created in.
        schema_id: u64,
        /// The new macro's id.
        macro_id: u64,
        /// The owning schema's name, serialized as `"schema"."macro"`.
        schema_name: String,
        /// The new macro's name.
        macro_name: String,
        /// `"scalar"` or `"table"` — selects the change-set entry kind.
        macro_type: String,
    },
    /// A macro was dropped.
    DropMacro {
        /// The dropped macro's id.
        macro_id: u64,
        /// `"scalar"` or `"table"` — selects the change-set entry kind.
        macro_type: String,
    },
}

impl Operation {
    /// Whether this op changes the catalog's shape (and bumps the schema
    /// version). Explicit per op — never inferred from the write set.
    pub(crate) fn is_schema_changing(&self) -> bool {
        match self {
            Operation::CreateSchema { .. }
            | Operation::DropSchema { .. }
            | Operation::CreateTable { .. }
            | Operation::AlterTable { .. }
            | Operation::DropTable { .. }
            | Operation::CreateView { .. }
            | Operation::AlterView { .. }
            | Operation::DropView { .. }
            | Operation::CreateMacro { .. }
            | Operation::DropMacro { .. } => true,
            Operation::RegisterDataFile { .. }
            | Operation::RegisterDeleteFile { .. }
            | Operation::ExpireDataFile { .. }
            | Operation::ExpireDeleteFile { .. }
            | Operation::UpdateStats { .. } => false,
        }
    }

    /// The table or view whose shape this op changes, if any — the ids a
    /// snapshot records as its `ducklake_schema_versions` rows. Tables and
    /// views share the catalog id space; drops mint no new shape.
    pub(crate) fn schema_changed_table_id(&self) -> Option<u64> {
        match self {
            Operation::CreateTable { table_id, .. } | Operation::AlterTable { table_id } => {
                Some(*table_id)
            }
            Operation::CreateView { view_id, .. } | Operation::AlterView { view_id } => {
                Some(*view_id)
            }
            // Macros carry no per-table schema version: DuckLake writes no
            // `ducklake_schema_versions` row for macro DDL.
            Operation::CreateSchema { .. }
            | Operation::DropSchema { .. }
            | Operation::DropTable { .. }
            | Operation::DropView { .. }
            | Operation::CreateMacro { .. }
            | Operation::DropMacro { .. }
            | Operation::RegisterDataFile { .. }
            | Operation::RegisterDeleteFile { .. }
            | Operation::ExpireDataFile { .. }
            | Operation::ExpireDeleteFile { .. }
            | Operation::UpdateStats { .. } => None,
        }
    }
}

/// Wraps `s` in double quotes, doubling any embedded quote — the SQL
/// identifier quoting rule for names in `changes_made`.
fn quote_ident(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        if c == '"' {
            out.push('"');
        }
        out.push(c);
    }
    out.push('"');

    out
}

/// Parses one SQL-quoted identifier at the start of `s`, undoubling
/// embedded quotes; returns the value and the remainder, or `None` if
/// unterminated.
fn parse_quoted(s: &str) -> Option<(String, &str)> {
    let rest = s.strip_prefix('"')?;
    let mut value = String::new();
    let mut chars = rest.char_indices();
    while let Some((i, c)) = chars.next() {
        if c != '"' {
            value.push(c);
            continue;
        }
        if let Some(b'"') = rest.as_bytes().get(i + 1) {
            value.push('"');
            chars.next();
        } else {
            return Some((value, &rest[i + 1..]));
        }
    }

    None
}

/// Parses a fully quoted `created_schema` payload: a single quoted name
/// consuming the entire payload.
fn parse_created_schema_payload(payload: &str) -> Option<String> {
    let (name, rest) = parse_quoted(payload)?;
    rest.is_empty().then_some(name)
}

/// Parses a `created_table` payload: `"schema"."table"`, each name
/// independently quoted and joined by a bare dot.
fn parse_created_table_payload(payload: &str) -> Option<(String, String)> {
    let (schema, rest) = parse_quoted(payload)?;
    let rest = rest.strip_prefix('.')?;
    let (table, rest) = parse_quoted(rest)?;
    rest.is_empty().then_some((schema, table))
}

/// Splits `changes_made` on top-level commas: a `"` toggles an in-quotes
/// flag, and a comma is only an entry separator while the flag is clear.
fn split_entries(changes_made: &str) -> Vec<&str> {
    let mut entries = Vec::new();
    let mut in_quotes = false;
    let mut start = 0;
    for (i, c) in changes_made.char_indices() {
        match c {
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                entries.push(&changes_made[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    entries.push(&changes_made[start..]);
    entries.retain(|e| !e.is_empty());

    entries
}

/// What one commit touched, comparable against another commit's set.
/// Serialized into the snapshot record's `changes_made` field in
/// DuckLake's own wire grammar: comma-joined `kind:payload` entries,
/// created entries carrying SQL-quoted names and all other entries
/// carrying numeric ids, e.g.
/// `dropped_schema:5,dropped_table:4,created_schema:"s1",created_table:"s1"."orders",altered_table:3`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ChangeSet {
    /// Names of schemas created by this commit, unquoted.
    pub(crate) created_schemas: BTreeSet<String>,
    pub(crate) dropped_schemas: BTreeSet<u64>,
    /// `(schema name, table name)` pairs, unquoted — the grammar carries
    /// names, not ids, for created entries.
    pub(crate) created_tables: BTreeSet<(String, String)>,
    /// Schema ids a table was created in. Populated only by
    /// [`Self::from_ops`] (the wire grammar has no ids for created
    /// entries); feeds the create-inside-dropped-schema check for our own
    /// side only — the other direction re-validates on retry.
    pub(crate) created_table_schema_ids: BTreeSet<u64>,
    pub(crate) altered_tables: BTreeSet<u64>,
    pub(crate) dropped_tables: BTreeSet<u64>,
    /// `(schema name, view name)` pairs, unquoted — the view twin of
    /// [`Self::created_tables`].
    pub(crate) created_views: BTreeSet<(String, String)>,
    /// Schema ids a view was created in — the view twin of
    /// [`Self::created_table_schema_ids`].
    pub(crate) created_view_schema_ids: BTreeSet<u64>,
    pub(crate) altered_views: BTreeSet<u64>,
    pub(crate) dropped_views: BTreeSet<u64>,
    /// `(schema name, macro name)` pairs, unquoted — one set per macro
    /// type because the wire grammar distinguishes them.
    pub(crate) created_scalar_macros: BTreeSet<(String, String)>,
    /// The table-macro twin of [`Self::created_scalar_macros`].
    pub(crate) created_table_macros: BTreeSet<(String, String)>,
    /// Schema ids a macro was created in — the macro twin of
    /// [`Self::created_table_schema_ids`].
    pub(crate) created_macro_schema_ids: BTreeSet<u64>,
    pub(crate) dropped_scalar_macros: BTreeSet<u64>,
    pub(crate) dropped_table_macros: BTreeSet<u64>,
    /// Tables data was appended to.
    pub(crate) inserted_tables: BTreeSet<u64>,
    /// Tables delete markers were appended to.
    pub(crate) deleted_from_tables: BTreeSet<u64>,
    /// Tables whose data files were merged away.
    pub(crate) merge_adjacent_tables: BTreeSet<u64>,
    /// Tables whose delete files were rewritten away.
    pub(crate) rewrite_delete_tables: BTreeSet<u64>,
    /// Parse-only legacy `compacted_table` kind; never emitted,
    /// classifies as compaction.
    pub(crate) compacted_tables: BTreeSet<u64>,
    /// Set when parsing met a kind or payload this binary does not
    /// model. Unknown changes classify as conflicting, never benign.
    pub(crate) has_unknown: bool,
}

impl ChangeSet {
    pub(crate) fn from_operations(operations: &[Operation]) -> Self {
        let mut set = Self::default();
        for op in operations {
            match op {
                Operation::CreateSchema { name, .. } => {
                    set.created_schemas.insert(name.clone());
                }
                Operation::DropSchema { schema_id } => {
                    set.dropped_schemas.insert(*schema_id);
                }
                Operation::CreateTable {
                    schema_id,
                    schema_name,
                    table_name,
                    ..
                } => {
                    set.created_tables
                        .insert((schema_name.clone(), table_name.clone()));
                    set.created_table_schema_ids.insert(*schema_id);
                }
                Operation::AlterTable { table_id } => {
                    set.altered_tables.insert(*table_id);
                }
                Operation::DropTable { table_id } => {
                    set.dropped_tables.insert(*table_id);
                }
                Operation::RegisterDataFile { table_id } => {
                    set.inserted_tables.insert(*table_id);
                }
                Operation::RegisterDeleteFile { table_id } => {
                    set.deleted_from_tables.insert(*table_id);
                }
                Operation::ExpireDataFile { table_id } => {
                    set.merge_adjacent_tables.insert(*table_id);
                }
                Operation::ExpireDeleteFile { table_id } => {
                    set.rewrite_delete_tables.insert(*table_id);
                }
                Operation::UpdateStats { .. } => {
                    // UpdateStats does not populate any set; it exists so a
                    // stats-only commit is non-empty and mints a snapshot.
                }
                Operation::CreateView {
                    schema_id,
                    schema_name,
                    view_name,
                    ..
                } => {
                    set.created_views
                        .insert((schema_name.clone(), view_name.clone()));
                    set.created_view_schema_ids.insert(*schema_id);
                }
                Operation::AlterView { view_id } => {
                    set.altered_views.insert(*view_id);
                }
                Operation::DropView { view_id } => {
                    set.dropped_views.insert(*view_id);
                }
                Operation::CreateMacro {
                    schema_id,
                    schema_name,
                    macro_name,
                    macro_type,
                    ..
                } => {
                    let pair = (schema_name.clone(), macro_name.clone());
                    if macro_type == "table" {
                        set.created_table_macros.insert(pair);
                    } else {
                        set.created_scalar_macros.insert(pair);
                    }
                    set.created_macro_schema_ids.insert(*schema_id);
                }
                Operation::DropMacro {
                    macro_id,
                    macro_type,
                } => {
                    if macro_type == "table" {
                        set.dropped_table_macros.insert(*macro_id);
                    } else {
                        set.dropped_scalar_macros.insert(*macro_id);
                    }
                }
            }
        }
        set
    }

    /// Emits entries in DuckLake's writer order (the subset moraine
    /// emits): dropped schemas, dropped tables, dropped views, created
    /// schemas, created tables, created views, created scalar/table
    /// macros, dropped scalar/table macros, `inserted_into_table`,
    /// `deleted_from_table`, altered tables, altered views,
    /// `merge_adjacent`, `rewrite_delete`.
    pub(crate) fn to_changes_made(&self) -> String {
        let mut entries = Vec::new();
        entries.extend(
            self.dropped_schemas
                .iter()
                .map(|id| format!("dropped_schema:{id}")),
        );
        entries.extend(
            self.dropped_tables
                .iter()
                .map(|id| format!("dropped_table:{id}")),
        );
        entries.extend(
            self.dropped_views
                .iter()
                .map(|id| format!("dropped_view:{id}")),
        );
        entries.extend(
            self.created_schemas
                .iter()
                .map(|name| format!("created_schema:{}", quote_ident(name))),
        );
        entries.extend(
            self.created_tables
                .iter()
                .map(|(s, t)| format!("created_table:{}.{}", quote_ident(s), quote_ident(t))),
        );
        entries.extend(
            self.created_views
                .iter()
                .map(|(s, v)| format!("created_view:{}.{}", quote_ident(s), quote_ident(v))),
        );
        entries.extend(
            self.created_scalar_macros.iter().map(|(s, m)| {
                format!("created_scalar_macro:{}.{}", quote_ident(s), quote_ident(m))
            }),
        );
        entries.extend(
            self.created_table_macros
                .iter()
                .map(|(s, m)| format!("created_table_macro:{}.{}", quote_ident(s), quote_ident(m))),
        );
        entries.extend(
            self.dropped_scalar_macros
                .iter()
                .map(|id| format!("dropped_scalar_macro:{id}")),
        );
        entries.extend(
            self.dropped_table_macros
                .iter()
                .map(|id| format!("dropped_table_macro:{id}")),
        );
        entries.extend(
            self.inserted_tables
                .iter()
                .map(|id| format!("inserted_into_table:{id}")),
        );
        entries.extend(
            self.deleted_from_tables
                .iter()
                .map(|id| format!("deleted_from_table:{id}")),
        );
        entries.extend(
            self.altered_tables
                .iter()
                .map(|id| format!("altered_table:{id}")),
        );
        entries.extend(
            self.altered_views
                .iter()
                .map(|id| format!("altered_view:{id}")),
        );
        entries.extend(
            self.merge_adjacent_tables
                .iter()
                .map(|id| format!("merge_adjacent:{id}")),
        );
        entries.extend(
            self.rewrite_delete_tables
                .iter()
                .map(|id| format!("rewrite_delete:{id}")),
        );
        entries.join(",")
    }

    /// Parses a stored `changes_made` string. Kind matching is
    /// case-insensitive.
    pub(crate) fn parse(changes_made: &str) -> Self {
        let mut set = Self::default();
        for entry in split_entries(changes_made) {
            let Some((kind, payload)) = entry.split_once(':') else {
                set.has_unknown = true;
                continue;
            };
            let known = if kind.eq_ignore_ascii_case("created_schema") {
                parse_created_schema_payload(payload)
                    .map(|name| set.created_schemas.insert(name))
                    .is_some()
            } else if kind.eq_ignore_ascii_case("dropped_schema") {
                payload
                    .parse()
                    .map(|id| set.dropped_schemas.insert(id))
                    .is_ok()
            } else if kind.eq_ignore_ascii_case("created_table") {
                parse_created_table_payload(payload)
                    .map(|pair| set.created_tables.insert(pair))
                    .is_some()
            } else if kind.eq_ignore_ascii_case("altered_table") {
                payload
                    .parse()
                    .map(|id| set.altered_tables.insert(id))
                    .is_ok()
            } else if kind.eq_ignore_ascii_case("dropped_table") {
                payload
                    .parse()
                    .map(|id| set.dropped_tables.insert(id))
                    .is_ok()
            } else if kind.eq_ignore_ascii_case("created_view") {
                parse_created_table_payload(payload)
                    .map(|pair| set.created_views.insert(pair))
                    .is_some()
            } else if kind.eq_ignore_ascii_case("altered_view") {
                payload
                    .parse()
                    .map(|id| set.altered_views.insert(id))
                    .is_ok()
            } else if kind.eq_ignore_ascii_case("dropped_view") {
                payload
                    .parse()
                    .map(|id| set.dropped_views.insert(id))
                    .is_ok()
            } else if kind.eq_ignore_ascii_case("created_scalar_macro") {
                parse_created_table_payload(payload)
                    .map(|pair| set.created_scalar_macros.insert(pair))
                    .is_some()
            } else if kind.eq_ignore_ascii_case("created_table_macro") {
                parse_created_table_payload(payload)
                    .map(|pair| set.created_table_macros.insert(pair))
                    .is_some()
            } else if kind.eq_ignore_ascii_case("dropped_scalar_macro") {
                payload
                    .parse()
                    .map(|id| set.dropped_scalar_macros.insert(id))
                    .is_ok()
            } else if kind.eq_ignore_ascii_case("dropped_table_macro") {
                payload
                    .parse()
                    .map(|id| set.dropped_table_macros.insert(id))
                    .is_ok()
            } else if kind.eq_ignore_ascii_case("inserted_into_table") {
                payload
                    .parse()
                    .map(|id| set.inserted_tables.insert(id))
                    .is_ok()
            } else if kind.eq_ignore_ascii_case("deleted_from_table") {
                payload
                    .parse()
                    .map(|id| set.deleted_from_tables.insert(id))
                    .is_ok()
            } else if kind.eq_ignore_ascii_case("merge_adjacent") {
                payload
                    .parse()
                    .map(|id| set.merge_adjacent_tables.insert(id))
                    .is_ok()
            } else if kind.eq_ignore_ascii_case("rewrite_delete") {
                payload
                    .parse()
                    .map(|id| set.rewrite_delete_tables.insert(id))
                    .is_ok()
            } else if kind.eq_ignore_ascii_case("compacted_table") {
                payload
                    .parse()
                    .map(|id| set.compacted_tables.insert(id))
                    .is_ok()
            } else {
                false
            };

            if !known {
                set.has_unknown = true;
            }
        }

        set
    }

    /// True when the set records no changes at all.
    pub(crate) fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    fn touches_schema_list(&self) -> bool {
        !self.created_schemas.is_empty() || !self.dropped_schemas.is_empty()
    }

    fn creates_table_in(&self, schema_id: u64) -> bool {
        self.created_table_schema_ids.contains(&schema_id)
            || self.created_view_schema_ids.contains(&schema_id)
            || self.created_macro_schema_ids.contains(&schema_id)
    }

    fn table_kinds(&self) -> BTreeMap<u64, TableKinds> {
        let mut kinds: BTreeMap<u64, TableKinds> = BTreeMap::new();
        for &table_id in &self.inserted_tables {
            kinds.entry(table_id).or_default().inserted = true;
        }
        for &table_id in &self.deleted_from_tables {
            kinds.entry(table_id).or_default().deleted = true;
        }
        for &table_id in self.altered_tables.iter().chain(self.altered_views.iter()) {
            kinds.entry(table_id).or_default().altered = true;
        }
        for &table_id in self.dropped_tables.iter().chain(self.dropped_views.iter()) {
            kinds.entry(table_id).or_default().dropped = true;
        }
        let compacted: BTreeSet<u64> = self
            .merge_adjacent_tables
            .iter()
            .chain(self.rewrite_delete_tables.iter())
            .chain(self.compacted_tables.iter())
            .copied()
            .collect();
        for &table_id in &compacted {
            kinds.entry(table_id).or_default().compacted = true;
        }

        kinds
    }
}

#[derive(Default, Clone, Copy)]
#[allow(clippy::struct_excessive_bools)]
struct TableKinds {
    inserted: bool,
    deleted: bool,
    altered: bool,
    dropped: bool,
    compacted: bool,
}

/// DuckLake's per-table conflict matrix, symmetric closure.
fn kinds_conflict(a: TableKinds, b: TableKinds) -> bool {
    let one_way = |x: TableKinds, y: TableKinds| {
        (x.inserted && (y.altered || y.deleted || y.dropped))
            || (x.deleted && (y.altered || y.deleted || y.compacted || y.dropped || y.inserted))
            || (x.altered && (y.altered || y.dropped))
            || (x.compacted && (y.deleted || y.dropped || y.compacted))
    };

    one_way(a, b) || one_way(b, a)
}

/// Whether two concurrent commits are a true conflict. Symmetric.
///
/// Benign unless: either side has unknown changes; both touch the schema
/// list (coarse by design); a common table has incompatible kinds; or one
/// created a table inside a schema the other dropped. Name uniqueness is
/// re-validated by the closure re-run, not by set comparison.
pub(crate) fn conflicts(ours: &ChangeSet, theirs: &ChangeSet) -> bool {
    if ours.has_unknown || theirs.has_unknown {
        return true;
    }
    if ours.touches_schema_list() && theirs.touches_schema_list() {
        return true;
    }

    let our_kinds = ours.table_kinds();
    let their_kinds = theirs.table_kinds();
    for (&table_id, &our_table_kinds) in &our_kinds {
        if let Some(&their_table_kinds) = their_kinds.get(&table_id) {
            if kinds_conflict(our_table_kinds, their_table_kinds) {
                return true;
            }
        }
    }

    let created_in_dropped =
        |a: &ChangeSet, b: &ChangeSet| b.dropped_schemas.iter().any(|s| a.creates_table_in(*s));
    created_in_dropped(ours, theirs) || created_in_dropped(theirs, ours)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_schema(schema_id: u64, name: &str) -> Operation {
        Operation::CreateSchema {
            schema_id,
            name: name.to_owned(),
        }
    }

    fn create_table(
        schema_id: u64,
        table_id: u64,
        schema_name: &str,
        table_name: &str,
    ) -> Operation {
        Operation::CreateTable {
            schema_id,
            table_id,
            schema_name: schema_name.to_owned(),
            table_name: table_name.to_owned(),
        }
    }

    #[test]
    fn data_plane_ops_serialize_in_ducklake_order() {
        let ops = [
            create_schema(1, "s1"),
            Operation::RegisterDataFile { table_id: 7 },
            Operation::RegisterDeleteFile { table_id: 8 },
            Operation::ExpireDataFile { table_id: 9 },
            Operation::ExpireDeleteFile { table_id: 9 },
            Operation::AlterTable { table_id: 3 },
            Operation::UpdateStats { table_id: 7 },
        ];
        let set = ChangeSet::from_operations(&ops);
        assert_eq!(
            set.to_changes_made(),
            r#"created_schema:"s1",inserted_into_table:7,deleted_from_table:8,altered_table:3,merge_adjacent:9,rewrite_delete:9"#
        );
        assert_eq!(ChangeSet::parse(&set.to_changes_made()), {
            let mut expect = set.clone();
            expect.created_table_schema_ids.clear();
            expect
        });
    }

    #[test]
    fn stats_ops_emit_nothing_but_are_ops() {
        let ops = [Operation::UpdateStats { table_id: 7 }];
        let set = ChangeSet::from_operations(&ops);
        assert_eq!(set.to_changes_made(), "");
        assert!(!ops[0].is_schema_changing());
        // An empty change set conflicts with nothing.
        let drop = ChangeSet::from_operations(&[Operation::DropTable { table_id: 7 }]);
        assert!(!conflicts(&set, &drop));
    }

    #[test]
    fn append_append_is_benign() {
        let a = ChangeSet::from_operations(&[Operation::RegisterDataFile { table_id: 1 }]);
        let b = ChangeSet::from_operations(&[Operation::RegisterDataFile { table_id: 1 }]);
        assert!(!conflicts(&a, &b));
        // Appends are also benign against compactions of the same table.
        let c = ChangeSet::from_operations(&[Operation::ExpireDataFile { table_id: 1 }]);
        assert!(!conflicts(&a, &c));
    }

    #[test]
    fn the_conflict_matrix() {
        let insert = |t| ChangeSet::from_operations(&[Operation::RegisterDataFile { table_id: t }]);
        let delete =
            |t| ChangeSet::from_operations(&[Operation::RegisterDeleteFile { table_id: t }]);
        let alter = |t| ChangeSet::from_operations(&[Operation::AlterTable { table_id: t }]);
        let drop = |t| ChangeSet::from_operations(&[Operation::DropTable { table_id: t }]);
        let compact =
            |t| ChangeSet::from_operations(&[Operation::ExpireDeleteFile { table_id: t }]);

        // Conflicting pairs.
        assert!(conflicts(&insert(1), &alter(1)));
        assert!(conflicts(&insert(1), &delete(1)));
        assert!(conflicts(&insert(1), &drop(1)));
        assert!(conflicts(&delete(1), &delete(1)));
        assert!(conflicts(&delete(1), &compact(1)));
        assert!(conflicts(&delete(1), &alter(1)));
        assert!(conflicts(&delete(1), &drop(1)));
        assert!(conflicts(&alter(1), &alter(1)));
        assert!(conflicts(&alter(1), &drop(1)));
        assert!(conflicts(&compact(1), &compact(1)));
        assert!(conflicts(&compact(1), &drop(1)));
        // Benign pairs.
        assert!(!conflicts(&insert(1), &insert(1)));
        assert!(!conflicts(&insert(1), &compact(1)));
        assert!(!conflicts(&alter(1), &compact(1)));
        assert!(!conflicts(&drop(1), &drop(1)));
        // Different tables never conflict.
        assert!(!conflicts(&delete(1), &delete(2)));
    }

    #[test]
    fn parsed_compaction_kinds_classify_as_compaction() {
        let theirs = ChangeSet::parse("compacted_table:1");
        assert!(!theirs.has_unknown);
        let ours = ChangeSet::from_operations(&[Operation::RegisterDeleteFile { table_id: 1 }]);
        assert!(conflicts(&ours, &theirs));
        let benign = ChangeSet::from_operations(&[Operation::RegisterDataFile { table_id: 1 }]);
        assert!(!conflicts(&benign, &theirs));
    }

    #[test]
    fn changes_made_exact_serialization() {
        let ops = [
            create_schema(1, "s1"),
            create_table(1, 2, "s1", "orders"),
            Operation::AlterTable { table_id: 3 },
            Operation::DropTable { table_id: 4 },
            Operation::DropSchema { schema_id: 5 },
        ];
        let set = ChangeSet::from_operations(&ops);
        let text = set.to_changes_made();
        assert_eq!(
            text,
            r#"dropped_schema:5,dropped_table:4,created_schema:"s1",created_table:"s1"."orders",altered_table:3"#
        );
    }

    #[test]
    fn round_trip_clears_created_table_schema_ids() {
        let ops = [
            create_schema(1, "s1"),
            create_table(1, 2, "s1", "orders"),
            Operation::AlterTable { table_id: 3 },
            Operation::DropTable { table_id: 4 },
            Operation::DropSchema { schema_id: 5 },
        ];
        let set = ChangeSet::from_operations(&ops);
        let text = set.to_changes_made();
        let parsed = ChangeSet::parse(&text);
        let expected = ChangeSet {
            created_table_schema_ids: BTreeSet::new(),
            ..set
        };
        assert_eq!(parsed, expected);
        assert_eq!(ChangeSet::parse(""), ChangeSet::default());
    }

    #[test]
    fn quoting_edges_round_trip() {
        // Names containing a comma, a dot, and an embedded quote must all
        // round-trip through the quoted grammar unchanged.
        let ops = [create_schema(1, "s,1"), create_table(2, 3, "a.b", r#"c"d"#)];
        let set = ChangeSet::from_operations(&ops);
        let text = set.to_changes_made();
        assert_eq!(text, r#"created_schema:"s,1",created_table:"a.b"."c""d""#);
        let parsed = ChangeSet::parse(&text);
        assert_eq!(parsed.created_schemas, set.created_schemas);
        assert_eq!(parsed.created_tables, set.created_tables);
        assert!(!parsed.has_unknown);
    }

    #[test]
    fn kind_matching_is_case_insensitive() {
        let parsed = ChangeSet::parse(r#"CREATED_SCHEMA:"x""#);
        assert!(!parsed.has_unknown);
        assert!(parsed.created_schemas.contains("x"));
    }

    #[test]
    fn known_ducklake_kind_moraine_does_not_model_is_unknown() {
        let parsed = ChangeSet::parse("inline_flush:7");
        assert!(parsed.has_unknown);
        assert!(conflicts(&ChangeSet::default(), &parsed));
    }

    #[test]
    fn unknown_entries_are_conservative() {
        let parsed = ChangeSet::parse("flushed_inline:7");
        assert!(parsed.has_unknown);
        assert!(conflicts(&ChangeSet::default(), &parsed));
    }

    #[test]
    fn malformed_payload_is_unknown() {
        // A created_schema payload missing the closing quote is malformed.
        let parsed = ChangeSet::parse(r#"created_schema:"unterminated"#);
        assert!(parsed.has_unknown);
        // A dropped_table payload that is not numeric is malformed.
        let parsed = ChangeSet::parse("dropped_table:not_a_number");
        assert!(parsed.has_unknown);
    }

    #[test]
    fn disjoint_tables_are_benign() {
        let ours = ChangeSet::from_operations(&[Operation::AlterTable { table_id: 1 }]);
        let theirs = ChangeSet::from_operations(&[Operation::AlterTable { table_id: 2 }]);
        assert!(!conflicts(&ours, &theirs));
    }

    #[test]
    fn overlapping_tables_conflict() {
        let ours = ChangeSet::from_operations(&[Operation::AlterTable { table_id: 1 }]);
        let dropped = ChangeSet::from_operations(&[Operation::DropTable { table_id: 1 }]);
        assert!(conflicts(&ours, &dropped));
        assert!(conflicts(&dropped, &ours));
    }

    #[test]
    fn schema_list_is_coarse_grained() {
        let create = ChangeSet::from_operations(&[create_schema(1, "s1")]);
        let drop = ChangeSet::from_operations(&[Operation::DropSchema { schema_id: 9 }]);
        assert!(conflicts(&create, &drop));
        // A table-only commit does not touch the schema list.
        let alter = ChangeSet::from_operations(&[Operation::AlterTable { table_id: 1 }]);
        assert!(!conflicts(&alter, &drop));
    }

    #[test]
    fn create_inside_dropped_schema_conflicts() {
        let ours = ChangeSet::from_operations(&[create_table(3, 8, "s3", "t8")]);
        let theirs = ChangeSet::from_operations(&[Operation::DropSchema { schema_id: 3 }]);
        assert!(conflicts(&ours, &theirs));
        assert!(conflicts(&theirs, &ours));
        // Creation in a surviving schema is benign.
        let elsewhere = ChangeSet::from_operations(&[create_table(4, 9, "s4", "t9")]);
        assert!(!conflicts(&elsewhere, &theirs));
    }

    #[test]
    fn parsed_created_table_schema_ids_stay_empty() {
        // The wire grammar carries names, not schema ids, for created
        // entries, so a parsed ChangeSet can never populate
        // created_table_schema_ids — the create-inside-dropped-schema
        // check is a from_ops-only capability for our own side.
        let ours = ChangeSet::from_operations(&[create_table(3, 8, "s3", "t8")]);
        let parsed = ChangeSet::parse(&ours.to_changes_made());
        assert!(parsed.created_table_schema_ids.is_empty());
        let theirs = ChangeSet::from_operations(&[Operation::DropSchema { schema_id: 3 }]);
        // The parsed side can no longer detect its own creation inside
        // the dropped schema by this mechanism; the closure re-run on
        // retry covers that risk instead (see the field's doc comment).
        assert!(!conflicts(&parsed, &theirs));
    }

    #[test]
    fn fresh_created_tables_never_conflict_by_id() {
        let a = ChangeSet::from_operations(&[create_table(1, 7, "s1", "t7")]);
        let b = ChangeSet::from_operations(&[create_table(1, 7, "s1", "t7")]);
        // Same ids cannot happen for real (ids are allocated above head),
        // but creation is not a mutation of existing state either way.
        assert!(!conflicts(&a, &b));
    }

    #[test]
    fn view_ops_serialize_and_round_trip() {
        let ops = [
            Operation::CreateView {
                schema_id: 1,
                view_id: 4,
                schema_name: "s".into(),
                view_name: "v".into(),
            },
            Operation::DropView { view_id: 5 },
            Operation::AlterView { view_id: 6 },
        ];
        let set = ChangeSet::from_operations(&ops);
        assert_eq!(
            set.to_changes_made(),
            r#"dropped_view:5,created_view:"s"."v",altered_view:6"#
        );
        assert_eq!(ChangeSet::parse(&set.to_changes_made()), {
            let mut e = set.clone();
            e.created_view_schema_ids.clear();
            e.created_table_schema_ids.clear();
            e
        });
        assert!(ops.iter().all(Operation::is_schema_changing));
    }

    #[test]
    fn macro_ops_serialize_and_round_trip() {
        let ops = [
            Operation::CreateMacro {
                schema_id: 1,
                macro_id: 4,
                schema_name: "s".into(),
                macro_name: "m".into(),
                macro_type: "scalar".into(),
            },
            Operation::CreateMacro {
                schema_id: 1,
                macro_id: 5,
                schema_name: "s".into(),
                macro_name: "tm".into(),
                macro_type: "table".into(),
            },
            Operation::DropMacro {
                macro_id: 6,
                macro_type: "scalar".into(),
            },
            Operation::DropMacro {
                macro_id: 7,
                macro_type: "table".into(),
            },
        ];
        let set = ChangeSet::from_operations(&ops);
        assert_eq!(
            set.to_changes_made(),
            r#"created_scalar_macro:"s"."m",created_table_macro:"s"."tm",dropped_scalar_macro:6,dropped_table_macro:7"#
        );
        assert_eq!(ChangeSet::parse(&set.to_changes_made()), {
            let mut e = set.clone();
            e.created_macro_schema_ids.clear();
            e
        });
        assert!(ops.iter().all(Operation::is_schema_changing));
    }

    #[test]
    fn macro_conflicts_classify_like_views() {
        // Two drops of one macro classify benign: like tables and views,
        // the loser's closure re-run sees the macro gone and surfaces
        // NotFound — set comparison never has to catch it.
        let drop = ChangeSet::from_operations(&[Operation::DropMacro {
            macro_id: 9,
            macro_type: "scalar".into(),
        }]);
        assert!(!conflicts(&drop, &drop));
        // Creating a macro inside a schema another commit dropped conflicts.
        let create = ChangeSet::from_operations(&[Operation::CreateMacro {
            schema_id: 3,
            macro_id: 7,
            schema_name: "s".into(),
            macro_name: "m".into(),
            macro_type: "scalar".into(),
        }]);
        let drop_schema = ChangeSet::from_operations(&[Operation::DropSchema { schema_id: 3 }]);
        assert!(conflicts(&create, &drop_schema));
        assert!(conflicts(&drop_schema, &create));
    }

    #[test]
    fn view_conflicts_classify_at_id_grain() {
        let alter = ChangeSet::from_operations(&[Operation::AlterView { view_id: 9 }]);
        let drop = ChangeSet::from_operations(&[Operation::DropView { view_id: 9 }]);
        assert!(conflicts(&alter, &drop));
        assert!(conflicts(&alter, &alter));
        let other = ChangeSet::from_operations(&[Operation::AlterView { view_id: 8 }]);
        assert!(!conflicts(&alter, &other));
        // Creating a view inside a schema another commit dropped conflicts.
        let create = ChangeSet::from_operations(&[Operation::CreateView {
            schema_id: 3,
            view_id: 7,
            schema_name: "s".into(),
            view_name: "v".into(),
        }]);
        let drop_schema = ChangeSet::from_operations(&[Operation::DropSchema { schema_id: 3 }]);
        assert!(conflicts(&create, &drop_schema));
    }

    #[test]
    fn ddl_ops_are_schema_changing() {
        assert!(create_schema(0, "s").is_schema_changing());
        assert!(Operation::AlterTable { table_id: 0 }.is_schema_changing());
        assert!(Operation::DropTable { table_id: 0 }.is_schema_changing());
    }

    #[test]
    fn data_plane_ops_are_not_schema_changing() {
        assert!(!Operation::RegisterDataFile { table_id: 0 }.is_schema_changing());
        assert!(!Operation::RegisterDeleteFile { table_id: 0 }.is_schema_changing());
        assert!(!Operation::ExpireDataFile { table_id: 0 }.is_schema_changing());
        assert!(!Operation::ExpireDeleteFile { table_id: 0 }.is_schema_changing());
        assert!(!Operation::UpdateStats { table_id: 0 }.is_schema_changing());
    }
}
