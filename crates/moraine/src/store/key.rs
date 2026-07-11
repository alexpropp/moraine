//! Key layout.
//!
//! A key is a typed tree — subspace, kind, components — and its on-disk
//! bytes are `storekey`'s derived order-preserving encoding of that tree:
//! one discriminant byte per enum level (assigned by declaration order),
//! then fixed-width big-endian `u64` components in field order. The
//! structure *is* the format: variant order is permanent once written,
//! and the golden-vector tests pin the exact bytes so any drift —
//! reordered variants, a changed derive — fails loudly in CI.

use storekey::{Decode, Encode};

use crate::error::{Error, Result};

/// Length in bytes of the encoded subspace prefix — one discriminant
/// byte. The segment extractor and the prefix builders must agree on
/// this.
pub(crate) const TAG_PREFIX_LEN: usize = 1;

/// A fully addressed store key. The five variants are the five
/// subspaces; each is also a SlateDB segment (the store is created with a
/// one-byte segment extractor over the leading discriminant).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub(crate) enum Key {
    /// Store-level singletons: format version, head pointer, migration
    /// marker. Overwritten in place.
    Sys(SysKey),
    /// One record per snapshot — `ducklake_snapshot` +
    /// `ducklake_snapshot_changes`, merged. Append-only.
    Snap {
        /// Snapshot id.
        snapshot_id: u64,
    },
    /// Live catalog state.
    Cur(CurKey),
    /// Ended entity versions; append-only.
    Hist(HistKey),
    /// Inlined data and its archive forms.
    Inline(InlineKey),
}

/// Store-level singleton keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub(crate) enum SysKey {
    /// Layout format version + moraine version that wrote the store.
    Format,
    /// Latest committed snapshot id.
    Head,
    /// Structural-migration marker. Reserved from format v1.
    Migration,
}

/// A live record: a temporally versioned entity, or the `cur`-only
/// gc-file bookkeeping (which has no begin/end lifecycle and therefore no
/// `hist` mirror — that stays unrepresentable by construction).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub(crate) enum CurKey {
    /// A live entity version.
    Entity(EntityKey),
    /// `ducklake_files_scheduled_for_deletion`.
    GcFile {
        /// Deletion id (global counter).
        deletion_id: u64,
    },
}

/// An ended entity version; `end_snapshot` is the final key component,
/// so a single entity's versions sort by when they ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub(crate) struct HistKey {
    /// The ended entity.
    pub(crate) entity: EntityKey,
    /// Snapshot at which the version ended.
    pub(crate) end_snapshot: u64,
}

/// A temporally versioned catalog entity. Lives in `cur` while live; its
/// `hist` mirror appends `end_snapshot`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub(crate) enum EntityKey {
    /// `ducklake_schema`.
    Schema {
        /// Global catalog id.
        schema_id: u64,
    },
    /// `ducklake_table`.
    Table {
        /// Global catalog id.
        table_id: u64,
    },
    /// `ducklake_view`.
    View {
        /// Global catalog id.
        view_id: u64,
    },
    /// `ducklake_column` — one record per row, nested fields included.
    Column {
        /// Owning table.
        table_id: u64,
        /// Per-table field id (never reused across the table's history).
        column_id: u64,
    },
    /// `ducklake_partition_info` (+ partition columns embedded).
    Partition {
        /// Owning table.
        table_id: u64,
        /// Partition spec id.
        partition_id: u64,
    },
    /// `ducklake_data_file`.
    File {
        /// Owning table.
        table_id: u64,
        /// Global file id.
        data_file_id: u64,
    },
    /// `ducklake_delete_file`.
    DeleteFile {
        /// Owning table.
        table_id: u64,
        /// Global file id.
        delete_file_id: u64,
    },
    /// `ducklake_file_column_stats`, keyed file-major.
    FileColumnStats {
        /// Owning table.
        table_id: u64,
        /// Data file the stats describe.
        data_file_id: u64,
        /// Column the stats describe.
        column_id: u64,
    },
    /// `ducklake_table_stats`.
    TableStats {
        /// Owning table.
        table_id: u64,
    },
    /// `ducklake_table_column_stats`.
    TableColumnStats {
        /// Owning table.
        table_id: u64,
        /// Column the stats describe.
        column_id: u64,
    },
    /// `ducklake_tag` — object ids are unique across entity types.
    Tag {
        /// The tagged object's id.
        object_id: u64,
    },
    /// `ducklake_metadata` / `set_option` scopes — one record per scope.
    Option {
        /// Scope kind: global = 0, schema = 1, table = 2.
        scope_kind: u64,
        /// Scope id (0 for global).
        scope_id: u64,
    },
}

/// An inlined-data key: the per-schema-version Arrow schema, a live
/// record, or the archived (post-flush) form of a live record. Archive
/// keys are invisible to catalog reads and served only by the recent-row
/// archive; sharing [`InlineOp`] between `Live` and `Arch` makes "an
/// archive key has exactly the components of its live form"
/// compiler-enforced.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub(crate) enum InlineKey {
    /// Arrow IPC schema message, once per `(table, schema_version)`. Has
    /// no archive form (schemas are never flushed away).
    Schema {
        /// Owning table.
        table_id: u64,
        /// Schema version the layout is pinned to.
        schema_version: u64,
    },
    /// A live inlined-data record.
    Live(InlineOp),
    /// The archived form of an inlined-data record.
    Arch(InlineOp),
}

/// An inlined-data record that exists in both live and archived form.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub(crate) enum InlineOp {
    /// One inlined-insert chunk (Arrow record-batch body).
    Ins {
        /// Owning table.
        table_id: u64,
        /// Schema version the chunk was written under.
        schema_version: u64,
        /// Commit snapshot of the insert.
        begin_snapshot: u64,
        /// Disambiguates multiple chunks in one commit.
        chunk_seq: u64,
    },
    /// Tombstone for an inlined insert row.
    Idel {
        /// Owning table.
        table_id: u64,
        /// Deleted row.
        row_id: u64,
    },
    /// Inlined delete against a Parquet-file row.
    Fdel {
        /// Owning table.
        table_id: u64,
        /// Targeted data file.
        data_file_id: u64,
        /// Deleted row.
        row_id: u64,
    },
}

impl Key {
    /// A live entity key.
    pub(crate) const fn cur(entity: EntityKey) -> Self {
        Self::Cur(CurKey::Entity(entity))
    }

    /// An ended entity-version key.
    pub(crate) const fn hist(entity: EntityKey, end_snapshot: u64) -> Self {
        Self::Hist(HistKey {
            entity,
            end_snapshot,
        })
    }

    /// Encode to the on-disk byte form.
    pub(crate) fn encode(&self) -> Vec<u8> {
        // Infallible by construction: a `Vec` sink cannot raise io errors
        // and the derived `Encode` raises no custom errors
        // (`storekey::encode_vec` documents the same reasoning).
        #[allow(clippy::expect_used)]
        storekey::encode_vec(self).expect("storekey encode into a Vec cannot fail")
    }

    /// Decode from the on-disk byte form. Fails as `Corruption` on an
    /// unknown discriminant, wrong arity, or trailing bytes
    /// (`storekey::decode` verifies the reader is exhausted).
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        storekey::decode(bytes).map_err(|err| Error::Corruption(format!("key: {err}")))
    }
}

/// A subspace, fieldless — for building scan prefixes without naming
/// discriminant bytes (prefixes are derived by encoding a sample key and
/// truncating, so the derive stays the single source of the bytes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Subspace {
    /// Store-level singletons.
    // Sys keys are read/written by exact key today; no caller prefix-scans
    // the whole subspace yet.
    #[allow(dead_code)]
    Sys,
    /// Snapshot records.
    // No production caller prefix-scans every snapshot yet (snapshot
    // listing lands with expiry/GC); a store-open test exercises it.
    #[allow(dead_code)]
    Snap,
    /// Live catalog state.
    Cur,
    /// Ended entity versions.
    Hist,
    /// Inlined data.
    // Data inlining is not implemented yet.
    #[allow(dead_code)]
    Inline,
}

impl Subspace {
    /// A minimal key inside this subspace, for prefix derivation.
    const fn sample(self) -> Key {
        match self {
            Self::Sys => Key::Sys(SysKey::Format),
            Self::Snap => Key::Snap { snapshot_id: 0 },
            Self::Cur => Key::cur(EntityKey::Schema { schema_id: 0 }),
            Self::Hist => Key::hist(EntityKey::Schema { schema_id: 0 }, 0),
            Self::Inline => Key::Inline(InlineKey::Schema {
                table_id: 0,
                schema_version: 0,
            }),
        }
    }
}

/// The kinds whose live keys are scoped `table_id`-first — the only kinds
/// [`cur_table_prefix`] accepts, so "which kinds are table-scoped" is a
/// type, not caller knowledge.
// No caller needs "everything about table T" yet (cascading table drop and
// per-table GC land in later slices); the prefix math is pinned by tests.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TableScopedKind {
    /// `ducklake_column`.
    Column,
    /// `ducklake_partition_info`.
    Partition,
    /// `ducklake_data_file`.
    File,
    /// `ducklake_delete_file`.
    DeleteFile,
    /// `ducklake_file_column_stats`.
    FileColumnStats,
    /// `ducklake_table_stats`.
    TableStats,
    /// `ducklake_table_column_stats`.
    TableColumnStats,
}

impl TableScopedKind {
    /// An entity of this kind with its table id set and every other
    /// component zeroed, for prefix derivation.
    #[allow(dead_code)]
    const fn sample(self, table_id: u64) -> EntityKey {
        match self {
            Self::Column => EntityKey::Column {
                table_id,
                column_id: 0,
            },
            Self::Partition => EntityKey::Partition {
                table_id,
                partition_id: 0,
            },
            Self::File => EntityKey::File {
                table_id,
                data_file_id: 0,
            },
            Self::DeleteFile => EntityKey::DeleteFile {
                table_id,
                delete_file_id: 0,
            },
            Self::FileColumnStats => EntityKey::FileColumnStats {
                table_id,
                data_file_id: 0,
                column_id: 0,
            },
            Self::TableStats => EntityKey::TableStats { table_id },
            Self::TableColumnStats => EntityKey::TableColumnStats {
                table_id,
                column_id: 0,
            },
        }
    }
}

/// Discriminant bytes preceding an entity's components in a live key:
/// subspace, `CurKey::Entity`, entity kind.
// Only `cur_table_prefix` consumes this, and it has no caller yet.
#[allow(dead_code)]
const CUR_KIND_PREFIX_LEN: usize = 3;

/// Byte prefix of every key in a subspace (exactly `TAG_PREFIX_LEN`
/// bytes — the same prefix the segment extractor derives).
pub(crate) fn subspace_prefix(subspace: Subspace) -> Vec<u8> {
    let mut bytes = subspace.sample().encode();
    bytes.truncate(TAG_PREFIX_LEN);
    bytes
}

/// Byte prefix of every live key of `kind` scoped to `table_id` —
/// "everything about table T" for one kind.
// No caller yet; pinned by the prefix tests below against the day
// cascading table drop or per-table GC needs it.
#[allow(dead_code)]
pub(crate) fn cur_table_prefix(kind: TableScopedKind, table_id: u64) -> Vec<u8> {
    let mut bytes = Key::cur(kind.sample(table_id)).encode();
    bytes.truncate(CUR_KIND_PREFIX_LEN + size_of::<u64>());
    bytes
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn be(v: u64) -> Vec<u8> {
        v.to_be_bytes().to_vec()
    }

    // Golden vectors: the exact on-disk bytes, pinned per kind. The
    // derive assigns each enum level's discriminant as declaration index
    // + 2; these tests are what make that assignment — and variant
    // order — part of the on-disk format rather than an accident.

    #[test]
    fn golden_sys_keys() {
        assert_eq!(Key::Sys(SysKey::Format).encode(), vec![0x02, 0x02]);
        assert_eq!(Key::Sys(SysKey::Head).encode(), vec![0x02, 0x03]);
        assert_eq!(Key::Sys(SysKey::Migration).encode(), vec![0x02, 0x04]);
    }

    #[test]
    fn golden_snapshot_key() {
        let mut expect = vec![0x03];
        expect.extend(be(42));
        assert_eq!(Key::Snap { snapshot_id: 42 }.encode(), expect);
    }

    #[test]
    fn golden_cur_file_key() {
        let mut expect = vec![0x04, 0x02, 0x07];
        expect.extend(be(1));
        expect.extend(be(2));
        let key = Key::cur(EntityKey::File {
            table_id: 1,
            data_file_id: 2,
        });
        assert_eq!(key.encode(), expect);
    }

    #[test]
    fn golden_hist_file_key_appends_end_snapshot() {
        let mut expect = vec![0x05, 0x07];
        expect.extend(be(1));
        expect.extend(be(2));
        expect.extend(be(9));
        let key = Key::hist(
            EntityKey::File {
                table_id: 1,
                data_file_id: 2,
            },
            9,
        );
        assert_eq!(key.encode(), expect);
    }

    #[test]
    fn golden_gcfile_key() {
        let mut expect = vec![0x04, 0x03];
        expect.extend(be(5));
        let key = Key::Cur(CurKey::GcFile { deletion_id: 5 });
        assert_eq!(key.encode(), expect);
    }

    #[test]
    fn golden_inline_ins_key() {
        let mut expect = vec![0x06, 0x03, 0x02];
        for v in [7, 3, 11, 0] {
            expect.extend(be(v));
        }
        let key = Key::Inline(InlineKey::Live(InlineOp::Ins {
            table_id: 7,
            schema_version: 3,
            begin_snapshot: 11,
            chunk_seq: 0,
        }));
        assert_eq!(key.encode(), expect);
    }

    #[test]
    fn golden_arch_kinds_are_distinct_from_live() {
        let ops = [
            InlineOp::Ins {
                table_id: 7,
                schema_version: 3,
                begin_snapshot: 11,
                chunk_seq: 0,
            },
            InlineOp::Idel {
                table_id: 7,
                row_id: 9,
            },
            InlineOp::Fdel {
                table_id: 7,
                data_file_id: 2,
                row_id: 9,
            },
        ];
        for op in ops {
            let live = Key::Inline(InlineKey::Live(op)).encode();
            let arch = Key::Inline(InlineKey::Arch(op)).encode();
            // Same subspace and components; only the form byte differs,
            // and every archived key sorts after every live key.
            assert_eq!(live[0], 0x06);
            assert_eq!(arch[0], 0x06);
            assert_ne!(live[1], arch[1]);
            assert_eq!(live[2..], arch[2..]);
            assert!(arch > live);
        }
    }

    // Decode: exact inverse, loud failure.

    #[test]
    fn decode_roundtrips_representative_keys() {
        let keys = [
            Key::Sys(SysKey::Head),
            Key::Snap {
                snapshot_id: u64::MAX,
            },
            Key::cur(EntityKey::Column {
                table_id: 3,
                column_id: 4,
            }),
            Key::hist(
                EntityKey::Option {
                    scope_kind: 2,
                    scope_id: 3,
                },
                8,
            ),
            Key::Cur(CurKey::GcFile { deletion_id: 0 }),
            Key::Inline(InlineKey::Live(InlineOp::Fdel {
                table_id: 1,
                data_file_id: 2,
                row_id: 3,
            })),
            Key::Inline(InlineKey::Arch(InlineOp::Fdel {
                table_id: 1,
                data_file_id: 2,
                row_id: 3,
            })),
        ];
        for key in keys {
            let decoded = Key::decode(&key.encode()).unwrap();
            assert_eq!(decoded, key);
        }
    }

    #[test]
    fn decode_rejects_unknown_discriminants_and_bad_arity() {
        // Unknown subspace discriminant (below and above the valid range).
        assert!(Key::decode(&[0x00, 0x02]).is_err());
        assert!(Key::decode(&[0x7f, 0x02]).is_err());
        // Unknown kind within a valid subspace.
        assert!(Key::decode(&[0x02, 0x7f]).is_err());
        assert!(Key::decode(&[0x06, 0x7f]).is_err());
        // Truncated components.
        let mut short = Key::Snap { snapshot_id: 1 }.encode();
        short.pop();
        assert!(Key::decode(&short).is_err());
        // Trailing bytes.
        let mut long = Key::Sys(SysKey::Head).encode();
        long.push(0);
        assert!(Key::decode(&long).is_err());
        // Empty.
        assert!(Key::decode(&[]).is_err());
    }

    // Prefixes: derived by encode-and-truncate, so they are byte prefixes
    // of full keys by construction — these tests pin the truncation
    // lengths against the structure.

    #[test]
    fn prefixes_are_byte_prefixes_of_full_keys() {
        let key = Key::cur(EntityKey::File {
            table_id: 1,
            data_file_id: 2,
        });
        let bytes = key.encode();
        assert!(bytes.starts_with(&subspace_prefix(Subspace::Cur)));
        assert!(bytes.starts_with(&cur_table_prefix(TableScopedKind::File, 1)));
        // A different table's prefix must not match.
        assert!(!bytes.starts_with(&cur_table_prefix(TableScopedKind::File, 2)));
        // A different kind's prefix must not match.
        assert!(!bytes.starts_with(&cur_table_prefix(TableScopedKind::DeleteFile, 1)));
    }

    /// Every table-scoped kind's prefix matches a live key of that kind
    /// with the same table id and rejects a different table id.
    #[test]
    fn table_scoped_prefixes_cover_all_kinds() {
        let kinds = [
            TableScopedKind::Column,
            TableScopedKind::Partition,
            TableScopedKind::File,
            TableScopedKind::DeleteFile,
            TableScopedKind::FileColumnStats,
            TableScopedKind::TableStats,
            TableScopedKind::TableColumnStats,
        ];
        for kind in kinds {
            let key = Key::cur(kind.sample(9));
            let bytes = key.encode();
            assert!(
                bytes.starts_with(&cur_table_prefix(kind, 9)),
                "{kind:?} prefix must match its own key"
            );
            assert!(
                !bytes.starts_with(&cur_table_prefix(kind, 10)),
                "{kind:?} prefix must not match another table"
            );
        }
    }

    // Property tests: roundtrip, order preservation, uniqueness.

    fn arb_entity() -> impl Strategy<Value = EntityKey> {
        prop_oneof![
            any::<u64>().prop_map(|schema_id| EntityKey::Schema { schema_id }),
            any::<u64>().prop_map(|table_id| EntityKey::Table { table_id }),
            any::<u64>().prop_map(|view_id| EntityKey::View { view_id }),
            any::<(u64, u64)>().prop_map(|(table_id, column_id)| EntityKey::Column {
                table_id,
                column_id
            }),
            any::<(u64, u64)>().prop_map(|(table_id, partition_id)| EntityKey::Partition {
                table_id,
                partition_id
            }),
            any::<(u64, u64)>().prop_map(|(table_id, data_file_id)| EntityKey::File {
                table_id,
                data_file_id
            }),
            any::<(u64, u64)>().prop_map(|(table_id, delete_file_id)| EntityKey::DeleteFile {
                table_id,
                delete_file_id
            }),
            any::<(u64, u64, u64)>().prop_map(|(table_id, data_file_id, column_id)| {
                EntityKey::FileColumnStats {
                    table_id,
                    data_file_id,
                    column_id,
                }
            }),
            any::<u64>().prop_map(|table_id| EntityKey::TableStats { table_id }),
            any::<(u64, u64)>().prop_map(|(table_id, column_id)| EntityKey::TableColumnStats {
                table_id,
                column_id
            }),
            any::<u64>().prop_map(|object_id| EntityKey::Tag { object_id }),
            any::<(u64, u64)>().prop_map(|(scope_kind, scope_id)| EntityKey::Option {
                scope_kind,
                scope_id
            }),
        ]
    }

    fn arb_inline_op() -> impl Strategy<Value = InlineOp> {
        prop_oneof![
            any::<(u64, u64, u64, u64)>().prop_map(
                |(table_id, schema_version, begin_snapshot, chunk_seq)| InlineOp::Ins {
                    table_id,
                    schema_version,
                    begin_snapshot,
                    chunk_seq
                }
            ),
            any::<(u64, u64)>().prop_map(|(table_id, row_id)| InlineOp::Idel { table_id, row_id }),
            any::<(u64, u64, u64)>().prop_map(|(table_id, data_file_id, row_id)| InlineOp::Fdel {
                table_id,
                data_file_id,
                row_id,
            }),
        ]
    }

    fn arb_key() -> impl Strategy<Value = Key> {
        prop_oneof![
            Just(Key::Sys(SysKey::Format)),
            Just(Key::Sys(SysKey::Head)),
            Just(Key::Sys(SysKey::Migration)),
            any::<u64>().prop_map(|snapshot_id| Key::Snap { snapshot_id }),
            arb_entity().prop_map(Key::cur),
            (arb_entity(), any::<u64>()).prop_map(|(entity, end)| Key::hist(entity, end)),
            any::<u64>().prop_map(|deletion_id| Key::Cur(CurKey::GcFile { deletion_id })),
            any::<(u64, u64)>().prop_map(|(table_id, schema_version)| {
                Key::Inline(InlineKey::Schema {
                    table_id,
                    schema_version,
                })
            }),
            arb_inline_op().prop_map(|op| Key::Inline(InlineKey::Live(op))),
            arb_inline_op().prop_map(|op| Key::Inline(InlineKey::Arch(op))),
        ]
    }

    proptest! {
        #[test]
        fn roundtrip(key in arb_key()) {
            let decoded = Key::decode(&key.encode()).unwrap();
            prop_assert_eq!(decoded, key);
        }

        #[test]
        fn encoded_keys_are_unique(a in arb_key(), b in arb_key()) {
            if a != b {
                prop_assert_ne!(a.encode(), b.encode());
            }
        }
    }
}
