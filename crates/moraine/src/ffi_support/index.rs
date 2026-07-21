//! Index type policy for the ABI: which DuckLake column types equality
//! indexes cover, and how a lookup value coerces to a column's canonical
//! stored form. Lives in the core so the type vocabulary cannot drift
//! from index maintenance, which derives stored keys from the same types.

use crate::{
    error::{Error, Result},
    store::index_encoding::{IndexKeyValue, IntWidth},
};

/// The base name of a DuckLake column type — `DECIMAL` from
/// `DECIMAL(18,3)` — uppercased.
#[must_use]
pub fn ducklake_base_type(column_type: &str) -> String {
    column_type
        .split('(')
        .next()
        .unwrap_or(column_type)
        .trim()
        .to_ascii_uppercase()
}

/// Refuses a column equality indexes cannot cover faithfully. DuckDB
/// writes a 128-bit integer to Parquet as a lossy `double`, so distinct
/// values could collide and the column's data-file and inline forms
/// disagree — refused rather than indexed silently wrong.
pub fn ensure_indexable(column_name: &str, column_type: &str) -> Result<()> {
    if matches!(
        ducklake_base_type(column_type).as_str(),
        "HUGEINT" | "UHUGEINT" | "INT128" | "UINT128"
    ) {
        return Err(Error::Constraint(format!(
            "column {column_name} is {column_type}; a 128-bit integer is not indexable because \
             DuckDB stores it as a lossy double in Parquet"
        )));
    }
    Ok(())
}

/// A lookup value as the ABI delivers it, owned, before coercion to the
/// indexed column's canonical form.
#[derive(Debug, Clone, PartialEq)]
pub enum LookupInput {
    /// A signed integer.
    Int(i64),
    /// An unsigned integer.
    UInt(u64),
    /// A float; carries `FLOAT` lookups too, narrowed at coercion.
    Float(f64),
    /// A boolean.
    Bool(bool),
    /// A string.
    Str(String),
    /// A UUID or blob.
    Bytes(Vec<u8>),
}

/// Coerces a lookup value to the canonical [`IndexKeyValue`] for a column
/// of DuckLake type `ducklake_type`, matching how index maintenance
/// derives the stored key from that column's Parquet/Arrow values. Errors
/// (as plain message text — the ABI owns the error-code mapping) on a
/// column type equality indexes do not cover, or a value kind that cannot
/// represent it.
// Distinct type names with a coincidentally identical body (e.g. `INTEGER`
// and `DATE` both index as an `i32`) are kept as separate arms for clarity.
// The `f64 as f32` narrowing for a `FLOAT` column is intended: the value came
// from a single-precision column.
#[allow(clippy::match_same_arms, clippy::cast_possible_truncation)]
pub fn coerce_lookup_value(
    input: &LookupInput,
    ducklake_type: &str,
) -> std::result::Result<IndexKeyValue, String> {
    let want = |what: &str| -> String {
        format!("index lookup: expected {what} value for a `{ducklake_type}` column")
    };
    let signed_input = || -> std::result::Result<i128, String> {
        match input {
            LookupInput::Int(value) => Ok(i128::from(*value)),
            LookupInput::UInt(value) => Ok(i128::from(*value)),
            _ => Err(want("an integer")),
        }
    };
    let unsigned_input = || -> std::result::Result<u128, String> {
        match input {
            LookupInput::UInt(value) => Ok(u128::from(*value)),
            LookupInput::Int(value) if *value >= 0 => Ok(value.unsigned_abs().into()),
            _ => Err(want("an unsigned integer")),
        }
    };
    let signed = |value, width| IndexKeyValue::Int { value, width };
    let unsigned = |value, width| IndexKeyValue::UInt { value, width };

    // Names as DuckLake records them (see the shim's `MapColumnType`): the
    // bit-width spellings (`INT64`) alongside the SQL names (`BIGINT`).
    let value = match ducklake_base_type(ducklake_type).as_str() {
        "TINYINT" | "INT8" => signed(signed_input()?, IntWidth::I8),
        "SMALLINT" | "INT16" => signed(signed_input()?, IntWidth::I16),
        "INTEGER" | "INT32" => signed(signed_input()?, IntWidth::I32),
        "BIGINT" | "INT64" => signed(signed_input()?, IntWidth::I64),
        "UTINYINT" | "UINT8" => unsigned(unsigned_input()?, IntWidth::I8),
        "USMALLINT" | "UINT16" => unsigned(unsigned_input()?, IntWidth::I16),
        "UINTEGER" | "UINT32" => unsigned(unsigned_input()?, IntWidth::I32),
        "UBIGINT" | "UINT64" => unsigned(unsigned_input()?, IntWidth::I64),
        // 128-bit integers are intentionally absent: [`ensure_indexable`]
        // refuses them at creation, so no index covers such a column and
        // this coercion falls through to the unsupported-type error.
        // Temporal types index by their underlying integer, as the scoped
        // read derives them: `DATE` as an `i32` day count, the rest as `i64`.
        "DATE" => signed(signed_input()?, IntWidth::I32),
        "TIME"
        | "TIME_NS"
        | "TIMETZ"
        | "TIME WITH TIME ZONE"
        | "TIMESTAMP"
        | "TIMESTAMP_S"
        | "TIMESTAMP_MS"
        | "TIMESTAMP_NS"
        | "TIMESTAMP_US"
        | "TIMESTAMPTZ"
        | "TIMESTAMP WITH TIME ZONE" => signed(signed_input()?, IntWidth::I64),
        "BOOLEAN" => {
            let LookupInput::Bool(value) = input else {
                return Err(want("a boolean"));
            };
            IndexKeyValue::Bool(*value)
        }
        "FLOAT" | "FLOAT32" | "REAL" => {
            let LookupInput::Float(value) = input else {
                return Err(want("a float"));
            };
            IndexKeyValue::F32(*value as f32)
        }
        "DOUBLE" | "FLOAT64" => {
            let LookupInput::Float(value) = input else {
                return Err(want("a float"));
            };
            IndexKeyValue::F64(*value)
        }
        "VARCHAR" | "TEXT" | "JSON" => {
            let LookupInput::Str(value) = input else {
                return Err(want("a string"));
            };
            IndexKeyValue::Str(value.clone())
        }
        "UUID" | "BLOB" => {
            let LookupInput::Bytes(value) = input else {
                return Err(want("a UUID or blob"));
            };
            IndexKeyValue::Bytes(value.clone())
        }
        other => {
            return Err(format!(
                "index lookup: column type `{other}` is not supported"
            ));
        }
    };
    Ok(value)
}
