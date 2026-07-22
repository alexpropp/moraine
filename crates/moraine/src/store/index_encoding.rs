//! Canonical value encoding for equality-index keys.
//!
//! An index entry key embeds the indexed column values as bytes. The
//! contract is canonical bytes for equality: one logical value always
//! encodes to one byte string, distinct values to distinct byte strings,
//! per DuckLake column type. Where it is free the encoding is also
//! order-compatible (byte order matches value order), so a future range
//! contract is an upgrade, not a rewrite — but only equality is promised.

use storekey::{Decode, Encode};

use crate::error::{Error, Result};

/// Maximum size of a composite index key, summed over its component
/// canonical encodings. Values past this are refused: huge keys degrade
/// the whole segment, and equality over megabyte values is not this
/// feature's job.
pub(crate) const MAX_INDEX_KEY_BYTES: usize = 1024;

/// Byte width of a fixed-width integer column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntWidth {
    /// One byte (`TINYINT`, `UTINYINT`).
    I8,
    /// Two bytes (`SMALLINT`, `USMALLINT`).
    I16,
    /// Four bytes (`INTEGER`, `UINTEGER`).
    I32,
    /// Eight bytes (`BIGINT`, `UBIGINT`, and temporal types by their
    /// underlying representation).
    I64,
    /// Sixteen bytes. Reserved: no column type currently derives this width
    /// — `HUGEINT`/`UHUGEINT` are not indexable, since DuckDB writes them to
    /// Parquet as a lossy double. Kept encodable for if that changes.
    I128,
}

impl IntWidth {
    /// Width in bytes.
    const fn bytes(self) -> usize {
        match self {
            Self::I8 => 1,
            Self::I16 => 2,
            Self::I32 => 4,
            Self::I64 => 8,
            Self::I128 => 16,
        }
    }

    /// Whether `value` is representable as a signed integer of this width.
    const fn holds_signed(self, value: i128) -> bool {
        if matches!(self, Self::I128) {
            return true;
        }
        let bits = self.bytes() * 8;
        value >= -(1_i128 << (bits - 1)) && value < (1_i128 << (bits - 1))
    }

    /// Whether `value` is representable as an unsigned integer of this
    /// width.
    const fn holds_unsigned(self, value: u128) -> bool {
        matches!(self, Self::I128) || value < (1_u128 << (self.bytes() * 8))
    }
}

/// A single non-null indexed column value, typed by the column's category.
/// NULL is not a variant here — it is the `None` of a column's optional
/// value, which encodes to a leading null flag.
#[derive(Debug, Clone, PartialEq)]
pub enum IndexKeyValue {
    /// A signed integer of a fixed width.
    Int {
        /// The value, sign-extended into an `i128`.
        value: i128,
        /// The column's byte width.
        width: IntWidth,
    },
    /// An unsigned integer of a fixed width.
    UInt {
        /// The value, zero-extended into a `u128`.
        value: u128,
        /// The column's byte width.
        width: IntWidth,
    },
    /// A single-precision float.
    F32(f32),
    /// A double-precision float.
    F64(f64),
    /// A boolean.
    Bool(bool),
    /// A UTF-8 string.
    Str(String),
    /// A raw byte string (blob).
    Bytes(Vec<u8>),
}

/// The one quiet-NaN bit pattern every `f32` NaN collapses to.
const F32_CANONICAL_NAN: u32 = 0x7fc0_0000;
/// The one quiet-NaN bit pattern every `f64` NaN collapses to.
const F64_CANONICAL_NAN: u64 = 0x7ff8_0000_0000_0000;

impl IndexKeyValue {
    /// Canonical bytes for this value.
    pub(crate) fn encode(&self) -> Vec<u8> {
        match self {
            Self::Int { value, width } => {
                let width = width.bytes();
                let mut bytes = value.to_be_bytes()[size_of::<i128>() - width..].to_vec();
                // Flip the sign bit so two's-complement values sort as
                // unsigned bytes in numeric order.
                bytes[0] ^= 0x80;
                bytes
            }
            Self::UInt { value, width } => {
                let width = width.bytes();
                value.to_be_bytes()[size_of::<u128>() - width..].to_vec()
            }
            Self::F32(value) => {
                let bits = if value.is_nan() {
                    F32_CANONICAL_NAN
                } else {
                    // Adding 0.0 folds -0.0 to +0.0 without touching any
                    // other value.
                    (value + 0.0).to_bits()
                };
                // Total-order transform: flip the sign bit of a non-negative
                // value, every bit of a negative one, so the big-endian bytes
                // sort in numeric order (NaN, collapsed above, sorts greatest).
                let mask = 0x8000_0000_u32 | 0_u32.wrapping_sub(bits >> 31);
                (bits ^ mask).to_be_bytes().to_vec()
            }
            Self::F64(value) => {
                let bits = if value.is_nan() {
                    F64_CANONICAL_NAN
                } else {
                    (value + 0.0).to_bits()
                };
                let mask = 0x8000_0000_0000_0000_u64 | 0_u64.wrapping_sub(bits >> 63);
                (bits ^ mask).to_be_bytes().to_vec()
            }
            Self::Bool(value) => vec![u8::from(*value)],
            // The per-component escaping that keeps composite boundaries
            // unambiguous is applied by the storekey framing of the
            // enclosing [`CanonicalKey`], not here.
            Self::Str(value) => value.as_bytes().to_vec(),
            Self::Bytes(value) => value.clone(),
        }
    }
}

/// Sort direction of an indexed column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Ascending — smaller values first.
    Ascending,
    /// Descending — larger values first, realized by complementing the
    /// column's framed bytes so that one forward scan yields reverse value
    /// order without a reverse iterator.
    Descending,
}

/// Where NULLs sort relative to the non-null values of an indexed column,
/// independent of the column's [`Direction`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullOrder {
    /// NULLs sort before every non-null value.
    First,
    /// NULLs sort after every non-null value.
    Last,
}

/// One column of an ordered index key: its value (`None` is SQL NULL) and
/// the ordering the column was declared with.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderedColumn {
    /// The column value, or `None` for NULL.
    pub value: Option<IndexKeyValue>,
    /// Ascending or descending.
    pub direction: Direction,
    /// NULL placement relative to non-null values.
    pub nulls: NullOrder,
}

/// The canonical encoding of an index's ordered column values: the
/// storekey-framed concatenation of the per-column component encodings,
/// held as one opaque byte string. Framing at construction keeps distinct
/// component splits distinct (`("ab","c") ≠ ("a","bc")`); holding the
/// result as a single byte string lets the enclosing entry key embed it —
/// and append a trailing row id — with an unambiguous, self-delimiting
/// storekey encoding.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct CanonicalKey(Vec<u8>);

// Coded as a storekey byte string rather than by the derive, which routes a
// `Vec<u8>` through the generic sequence codec. Both emit the same bytes —
// low bytes escaped behind `0x01`, a `0x00` terminator — but the sequence
// decoder leaves "the next byte may be escaped" set after consuming that
// terminator, so a following fixed-width field whose leading byte is `0x01`
// loses it. A non-unique entry key is exactly that shape: the value, then a
// raw row id.
impl<F> Encode<F> for CanonicalKey {
    fn encode<W: std::io::Write>(
        &self,
        w: &mut storekey::Writer<W>,
    ) -> std::result::Result<(), storekey::EncodeError> {
        w.write_slice(&self.0)
    }
}

impl<F> Decode<F> for CanonicalKey {
    fn decode<R: std::io::BufRead>(
        r: &mut storekey::Reader<R>,
    ) -> std::result::Result<Self, storekey::DecodeError> {
        Ok(Self(r.read_vec()?))
    }
}

impl CanonicalKey {
    /// A key with no framed content, for subspace-prefix derivation only —
    /// the derived prefix keeps just the leading discriminant byte, so the
    /// content is never inspected.
    pub(crate) const fn empty() -> Self {
        Self(Vec::new())
    }

    /// The framed bytes embedded in an entry key.
    #[cfg(test)]
    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Canonically encode an equality lookup's column values — the degenerate
/// all-ascending, non-null case of [`encode_ordered_key`]. A test-only
/// convenience; production paths call [`encode_ordered_values`] with the
/// index's declared orders.
#[cfg(test)]
pub(crate) fn encode_key(values: &[IndexKeyValue]) -> Result<CanonicalKey> {
    let columns: Vec<OrderedColumn> = values
        .iter()
        .map(|value| OrderedColumn {
            value: Some(value.clone()),
            direction: Direction::Ascending,
            nulls: NullOrder::Last,
        })
        .collect();
    encode_ordered_key(&columns)
}

/// Canonically encode an index's ordered columns into a [`CanonicalKey`].
///
/// Each column contributes a self-delimiting, order-preserving blob: a
/// leading flag byte separating NULL from non-null (placed per the column's
/// [`NullOrder`], never complemented, so null placement is independent of
/// direction), then — for a non-null value — its canonical bytes framed as a
/// storekey byte string, complemented in full (terminator included) when the
/// column is [`Direction::Descending`] so variable-length values reverse
/// correctly. Concatenating the blobs makes byte order equal SQL tuple order.
///
/// Fails as [`Error::Constraint`] when the summed non-null value size exceeds
/// [`MAX_INDEX_KEY_BYTES`] or an integer value does not fit its declared
/// width (truncating would map distinct values to one key).
pub(crate) fn encode_ordered_key(columns: &[OrderedColumn]) -> Result<CanonicalKey> {
    let mut total = 0usize;
    let mut out = Vec::new();
    for column in columns {
        let (null_flag, non_null_flag) = match column.nulls {
            NullOrder::First => (0x00u8, 0x01u8),
            NullOrder::Last => (0x01u8, 0x00u8),
        };
        let Some(value) = &column.value else {
            out.push(null_flag);
            continue;
        };
        let fits = match value {
            IndexKeyValue::Int { value, width } => width.holds_signed(*value),
            IndexKeyValue::UInt { value, width } => width.holds_unsigned(*value),
            _ => true,
        };
        if !fits {
            return Err(Error::Constraint(format!(
                "index key value {value:?} does not fit its declared integer width"
            )));
        }
        let raw = value.encode();
        total += raw.len();
        let framed = frame_bytes(&raw);
        out.push(non_null_flag);
        match column.direction {
            Direction::Ascending => out.extend_from_slice(&framed),
            Direction::Descending => out.extend(framed.iter().map(|byte| !byte)),
        }
    }
    if total > MAX_INDEX_KEY_BYTES {
        return Err(Error::Constraint(format!(
            "index key of {total} bytes exceeds the {MAX_INDEX_KEY_BYTES}-byte limit"
        )));
    }
    Ok(CanonicalKey(out))
}

/// Encode index values in their columns' declared orders. `directions` and
/// `nulls` run parallel to `values`; where either is shorter (or empty) the
/// column defaults to ascending / NULLS LAST, so an empty slice means the
/// all-ascending equality shape. A `None` value is SQL NULL.
pub(crate) fn encode_ordered_values(
    values: &[Option<IndexKeyValue>],
    directions: &[Direction],
    nulls: &[NullOrder],
) -> Result<CanonicalKey> {
    let columns: Vec<OrderedColumn> = values
        .iter()
        .enumerate()
        .map(|(index, value)| OrderedColumn {
            value: value.clone(),
            direction: directions
                .get(index)
                .copied()
                .unwrap_or(Direction::Ascending),
            nulls: nulls.get(index).copied().unwrap_or(NullOrder::Last),
        })
        .collect();
    encode_ordered_key(&columns)
}

/// A single slice framed as a storekey byte string: low bytes escaped behind
/// `0x01`, a `0x00` terminator — order-preserving and prefix-free, so a
/// shorter value sorts before its extension and component boundaries stay
/// unambiguous under concatenation.
fn frame_bytes(raw: &[u8]) -> Vec<u8> {
    // Infallible by construction: a `Vec` sink raises no io error and
    // storekey's `Vec<u8>` encoder (a byte string, not the generic sequence
    // codec) raises no custom error.
    #[allow(clippy::expect_used)]
    storekey::encode_vec(&raw.to_vec()).expect("storekey encode into a Vec cannot fail")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn golden_signed_int_flips_sign_bit() {
        // Zero sits at the midpoint; the high bit is set.
        assert_eq!(
            IndexKeyValue::Int {
                value: 0,
                width: IntWidth::I64,
            }
            .encode(),
            vec![0x80, 0, 0, 0, 0, 0, 0, 0],
        );
        // One is just above the midpoint.
        assert_eq!(
            IndexKeyValue::Int {
                value: 1,
                width: IntWidth::I64,
            }
            .encode(),
            vec![0x80, 0, 0, 0, 0, 0, 0, 1],
        );
        // Minus one sits just below the midpoint.
        assert_eq!(
            IndexKeyValue::Int {
                value: -1,
                width: IntWidth::I64,
            }
            .encode(),
            vec![0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
        );
    }

    #[test]
    fn golden_signed_int_widths_are_normalized() {
        assert_eq!(
            IndexKeyValue::Int {
                value: 1,
                width: IntWidth::I8,
            }
            .encode(),
            vec![0x81],
        );
        assert_eq!(
            IndexKeyValue::Int {
                value: 1,
                width: IntWidth::I16,
            }
            .encode(),
            vec![0x80, 0x01],
        );
        assert_eq!(
            IndexKeyValue::Int {
                value: 1,
                width: IntWidth::I32,
            }
            .encode(),
            vec![0x80, 0, 0, 0x01],
        );
        assert_eq!(
            IndexKeyValue::Int {
                value: 1,
                width: IntWidth::I128,
            }
            .encode()
            .len(),
            16,
        );
    }

    #[test]
    fn golden_unsigned_int_is_plain_big_endian() {
        assert_eq!(
            IndexKeyValue::UInt {
                value: 5,
                width: IntWidth::I32,
            }
            .encode(),
            vec![0, 0, 0, 5],
        );
        assert_eq!(
            IndexKeyValue::UInt {
                value: u128::from(u64::MAX),
                width: IntWidth::I64,
            }
            .encode(),
            vec![0xff; 8],
        );
    }

    // Equality and inequality of the encoding, and — for the widths where
    // it is free — order-compatibility with the numeric order.

    fn signed(value: i128) -> Vec<u8> {
        IndexKeyValue::Int {
            value,
            width: IntWidth::I64,
        }
        .encode()
    }

    #[test]
    fn equal_values_encode_equally_distinct_values_distinctly() {
        assert_eq!(signed(42), signed(42));
        assert_ne!(signed(42), signed(43));
    }

    #[test]
    fn signed_encoding_is_order_compatible() {
        let ordered = [i64::MIN, -2, -1, 0, 1, 2, i64::MAX];
        for pair in ordered.windows(2) {
            let lower = signed(i128::from(pair[0]));
            let higher = signed(i128::from(pair[1]));
            assert!(lower < higher, "{:?} !< {:?}", pair[0], pair[1]);
        }
    }

    #[test]
    fn unsigned_encoding_is_order_compatible() {
        let value = |v: u128| {
            IndexKeyValue::UInt {
                value: v,
                width: IntWidth::I64,
            }
            .encode()
        };
        assert!(value(0) < value(1));
        assert!(value(1) < value(u128::from(u64::MAX)));
    }

    #[test]
    fn golden_bool_is_one_byte() {
        assert_eq!(IndexKeyValue::Bool(false).encode(), vec![0]);
        assert_eq!(IndexKeyValue::Bool(true).encode(), vec![1]);
    }

    #[test]
    fn float_negative_zero_normalizes_to_positive_zero() {
        assert_eq!(
            IndexKeyValue::F64(-0.0).encode(),
            IndexKeyValue::F64(0.0).encode(),
        );
        assert_eq!(
            IndexKeyValue::F32(-0.0).encode(),
            IndexKeyValue::F32(0.0).encode(),
        );
    }

    #[test]
    fn float_nans_collapse_to_one_pattern() {
        // Distinct NaN bit patterns (quiet, signalling, sign-set) all
        // encode identically — DuckDB treats NaN = NaN.
        let quiet = IndexKeyValue::F64(f64::from_bits(0x7ff8_0000_0000_0001)).encode();
        let signalling = IndexKeyValue::F64(f64::from_bits(0xfff0_0000_0000_0001)).encode();
        let canonical = IndexKeyValue::F64(f64::NAN).encode();
        assert_eq!(quiet, signalling);
        assert_eq!(signalling, canonical);

        let f32_quiet = IndexKeyValue::F32(f32::from_bits(0x7fc0_0001)).encode();
        let f32_signed = IndexKeyValue::F32(f32::from_bits(0xffc0_0001)).encode();
        assert_eq!(f32_quiet, f32_signed);
    }

    #[test]
    fn distinct_floats_encode_distinctly() {
        assert_ne!(
            IndexKeyValue::F64(1.0).encode(),
            IndexKeyValue::F64(2.0).encode(),
        );
        assert_ne!(
            IndexKeyValue::F32(1.5).encode(),
            IndexKeyValue::F32(-1.5).encode(),
        );
    }

    #[test]
    fn golden_string_and_blob_are_raw_bytes() {
        assert_eq!(IndexKeyValue::Str("abc".into()).encode(), b"abc".to_vec(),);
        assert_eq!(
            IndexKeyValue::Bytes(vec![0, 1, 2, 0xff]).encode(),
            vec![0, 1, 2, 0xff],
        );
    }

    #[test]
    fn encode_key_is_deterministic_and_injective() {
        let value = |s: &str| {
            encode_key(&[
                IndexKeyValue::Int {
                    value: 7,
                    width: IntWidth::I64,
                },
                IndexKeyValue::Str(s.into()),
            ])
            .unwrap()
        };
        assert_eq!(value("x"), value("x"));
        assert_ne!(value("x"), value("y"));
        // Framing produces non-empty bytes even for a short key.
        assert!(!value("x").as_bytes().is_empty());
    }

    #[test]
    fn composite_framing_distinguishes_component_splits() {
        // The classic ambiguity: without framing, ("ab","c") and
        // ("a","bc") would share a byte string. The storekey framing must
        // keep them distinct.
        let ab_c = encode_key(&[
            IndexKeyValue::Str("ab".into()),
            IndexKeyValue::Str("c".into()),
        ])
        .unwrap();
        let a_bc = encode_key(&[
            IndexKeyValue::Str("a".into()),
            IndexKeyValue::Str("bc".into()),
        ])
        .unwrap();
        assert_ne!(ab_c, a_bc);
    }

    #[test]
    fn oversized_key_is_refused() {
        let big = IndexKeyValue::Bytes(vec![0; MAX_INDEX_KEY_BYTES + 1]);
        let err = encode_key(std::slice::from_ref(&big)).unwrap_err();
        assert!(matches!(err, Error::Constraint(_)), "got {err:?}");
    }

    #[test]
    fn key_at_the_cap_is_accepted() {
        let at_cap = IndexKeyValue::Bytes(vec![0; MAX_INDEX_KEY_BYTES]);
        assert!(encode_key(std::slice::from_ref(&at_cap)).is_ok());
    }

    #[test]
    fn out_of_width_integers_are_refused() {
        // Truncating 300 to one byte would collide with 44; refuse instead.
        let narrow = |value| {
            encode_key(&[IndexKeyValue::Int {
                value,
                width: IntWidth::I8,
            }])
        };
        assert!(matches!(narrow(300).unwrap_err(), Error::Constraint(_)));
        assert!(matches!(narrow(-129).unwrap_err(), Error::Constraint(_)));
        assert!(narrow(i128::from(i8::MIN)).is_ok());
        assert!(narrow(i128::from(i8::MAX)).is_ok());

        let unsigned = |value| {
            encode_key(&[IndexKeyValue::UInt {
                value,
                width: IntWidth::I8,
            }])
        };
        assert!(matches!(unsigned(256).unwrap_err(), Error::Constraint(_)));
        assert!(unsigned(u128::from(u8::MAX)).is_ok());

        // Full width admits the extremes.
        assert!(
            encode_key(&[IndexKeyValue::Int {
                value: i128::MIN,
                width: IntWidth::I128,
            }])
            .is_ok()
        );
        assert!(
            encode_key(&[IndexKeyValue::UInt {
                value: u128::MAX,
                width: IntWidth::I128,
            }])
            .is_ok()
        );
    }

    fn int(value: i128) -> IndexKeyValue {
        IndexKeyValue::Int {
            value,
            width: IntWidth::I64,
        }
    }

    fn ordered(value: Option<IndexKeyValue>, direction: Direction, nulls: NullOrder) -> Vec<u8> {
        encode_ordered_key(&[OrderedColumn {
            value,
            direction,
            nulls,
        }])
        .unwrap()
        .as_bytes()
        .to_vec()
    }

    #[test]
    fn float_encoding_is_order_preserving() {
        let enc = |v: f64| IndexKeyValue::F64(v).encode();
        let rising = [
            f64::NEG_INFINITY,
            -1e300,
            -1.0,
            -0.0,
            0.0,
            1.0,
            1e300,
            f64::INFINITY,
        ];
        for pair in rising.windows(2) {
            assert!(enc(pair[0]) <= enc(pair[1]), "{} !<= {}", pair[0], pair[1]);
        }
        // Strict where the values differ, and NaN sorts greatest of all.
        assert!(enc(-1.0) < enc(1.0));
        assert!(enc(f64::NEG_INFINITY) < enc(-1e300));
        assert!(enc(f64::INFINITY) < enc(f64::NAN));
        // -0.0 and 0.0 collapse to one encoding.
        assert_eq!(enc(-0.0), enc(0.0));
    }

    #[test]
    fn descending_reverses_numeric_order() {
        let asc = |v: i128| ordered(Some(int(v)), Direction::Ascending, NullOrder::Last);
        let desc = |v: i128| ordered(Some(int(v)), Direction::Descending, NullOrder::Last);
        assert!(asc(1) < asc(2));
        // The larger value sorts first under descending.
        assert!(desc(2) < desc(1));
    }

    #[test]
    fn descending_reverses_variable_length_strings() {
        let string = |s: &str| IndexKeyValue::Str(s.to_owned());
        let asc = |s: &str| ordered(Some(string(s)), Direction::Ascending, NullOrder::Last);
        let desc = |s: &str| ordered(Some(string(s)), Direction::Descending, NullOrder::Last);
        // Ascending "a" < "ab"; descending must reverse it, length included —
        // the property that breaks if only the raw bytes are complemented.
        assert!(asc("a") < asc("ab"));
        assert!(desc("ab") < desc("a"));
    }

    #[test]
    fn null_placement_respects_first_and_last_under_both_directions() {
        for direction in [Direction::Ascending, Direction::Descending] {
            let null_first = ordered(None, direction, NullOrder::First);
            let value_first = ordered(Some(int(0)), direction, NullOrder::First);
            assert!(null_first < value_first, "NULLS FIRST under {direction:?}");

            let null_last = ordered(None, direction, NullOrder::Last);
            let value_last = ordered(Some(int(0)), direction, NullOrder::Last);
            assert!(value_last < null_last, "NULLS LAST under {direction:?}");
        }
    }

    #[test]
    fn composite_order_matches_declared_per_column_directions() {
        // Index (a ASC, b DESC).
        let key = |a: i128, b: i128| {
            encode_ordered_key(&[
                OrderedColumn {
                    value: Some(int(a)),
                    direction: Direction::Ascending,
                    nulls: NullOrder::Last,
                },
                OrderedColumn {
                    value: Some(int(b)),
                    direction: Direction::Descending,
                    nulls: NullOrder::Last,
                },
            ])
            .unwrap()
        };
        // `a` ascending is the primary sort, whatever `b` holds.
        assert!(key(1, 100) < key(2, 100));
        assert!(key(1, 0) < key(2, 100));
        // Within one `a`, `b` sorts descending.
        assert!(key(1, 100) < key(1, 50));
        assert!(key(1, 50) < key(1, 10));
    }

    #[test]
    fn ordered_encoding_is_deterministic_for_uniqueness() {
        // A unique index keys on the value alone; the same value must encode
        // identically under any direction so racing inserts collide.
        let one = ordered(Some(int(7)), Direction::Descending, NullOrder::First);
        assert_eq!(
            one,
            ordered(Some(int(7)), Direction::Descending, NullOrder::First)
        );
        assert_ne!(
            one,
            ordered(Some(int(8)), Direction::Descending, NullOrder::First)
        );
    }

    mod properties {
        use proptest::prelude::*;

        use super::*;

        proptest! {
            /// Determinism and injectivity: equal values share one byte
            /// string, distinct values never collide.
            #[test]
            fn signed_encoding_is_injective(a: i64, b: i64) {
                let one = signed(i128::from(a));
                let two = signed(i128::from(b));
                prop_assert_eq!(one.clone(), signed(i128::from(a)));
                prop_assert_eq!(a == b, one == two);
            }

            /// Byte order matches numeric order at every width the codec
            /// promises order-compatibility for.
            #[test]
            fn signed_encoding_preserves_order(a: i64, b: i64) {
                let one = signed(i128::from(a));
                let two = signed(i128::from(b));
                prop_assert_eq!(a.cmp(&b), one.cmp(&two));
            }

            #[test]
            fn unsigned_encoding_preserves_order(a: u64, b: u64) {
                let encode = |value| {
                    IndexKeyValue::UInt {
                        value,
                        width: IntWidth::I64,
                    }
                    .encode()
                };
                let one = encode(u128::from(a));
                let two = encode(u128::from(b));
                prop_assert_eq!(a.cmp(&b), one.cmp(&two));
            }

            /// Non-NaN float bytes sort in numeric total order, with -0.0 and
            /// +0.0 collapsed to one value.
            #[test]
            fn float_encoding_preserves_total_order(a: f64, b: f64) {
                prop_assume!(!a.is_nan() && !b.is_nan());
                let one = IndexKeyValue::F64(a).encode();
                let two = IndexKeyValue::F64(b).encode();
                let norm = |v: f64| if v == 0.0 { 0.0 } else { v };
                let expected = norm(a).partial_cmp(&norm(b)).unwrap();
                prop_assert_eq!(expected, one.cmp(&two));
            }

            /// In-range narrow values encode injectively — the property the
            /// width validation in `encode_key` protects.
            #[test]
            fn narrow_signed_encoding_is_injective(a: i16, b: i16) {
                let encode = |value: i16| {
                    encode_key(&[IndexKeyValue::Int {
                        value: i128::from(value),
                        width: IntWidth::I16,
                    }])
                    .unwrap()
                };
                prop_assert_eq!(a == b, encode(a) == encode(b));
            }

            /// Composite framing keeps distinct component splits distinct
            /// for arbitrary string pairs.
            #[test]
            fn composite_framing_is_injective(
                a in prop::collection::vec(".{0,8}", 1..3),
                b in prop::collection::vec(".{0,8}", 1..3),
            ) {
                let encode = |parts: &[String]| {
                    encode_key(
                        &parts
                            .iter()
                            .map(|part| IndexKeyValue::Str(part.clone()))
                            .collect::<Vec<_>>(),
                    )
                    .unwrap()
                };
                prop_assert_eq!(a == b, encode(&a) == encode(&b));
            }
        }
    }
}
