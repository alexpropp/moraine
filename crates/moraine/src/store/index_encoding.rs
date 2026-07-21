//! Canonical value encoding for equality-index keys.
//!
//! An index entry key embeds the indexed column values as bytes. The
//! contract is canonical bytes for equality: one logical value always
//! encodes to one byte string, distinct values to distinct byte strings,
//! per DuckLake column type. Where it is free the encoding is also
//! order-compatible (byte order matches value order), so a future range
//! contract is an upgrade, not a rewrite — but only equality is promised.

// The codec is consumed by index maintenance and lookups in later slices;
// until those land only tests exercise it.
#![allow(dead_code)]

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
}

/// A single indexed column value, typed by the column's category. NULL is
/// never represented here: a NULL in any indexed column yields no entry,
/// so the maintenance layer skips such rows before encoding.
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
                bits.to_be_bytes().to_vec()
            }
            Self::F64(value) => {
                let bits = if value.is_nan() {
                    F64_CANONICAL_NAN
                } else {
                    (value + 0.0).to_bits()
                };
                bits.to_be_bytes().to_vec()
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

/// Canonically encode an index's ordered column values into a
/// [`CanonicalKey`]. Fails as [`Error::Constraint`] when the summed
/// component size exceeds [`MAX_INDEX_KEY_BYTES`]. Every value must be
/// non-null: a NULL in any indexed column yields no entry, so the caller
/// skips such rows before encoding.
pub(crate) fn encode_key(values: &[IndexKeyValue]) -> Result<CanonicalKey> {
    let components: Vec<Vec<u8>> = values.iter().map(IndexKeyValue::encode).collect();
    let total: usize = components.iter().map(Vec::len).sum();
    if total > MAX_INDEX_KEY_BYTES {
        return Err(Error::Constraint(format!(
            "index key of {total} bytes exceeds the {MAX_INDEX_KEY_BYTES}-byte limit"
        )));
    }
    // This inner framing keeps storekey's sequence codec, which the entry
    // key's own framing deliberately avoids: the two are different layers,
    // and only this one is write-only — nothing decodes components back out,
    // and a sequence of sequences ends in a terminator, never in a
    // fixed-width field that could swallow the escape state. Adding a
    // trailing fixed-width component here would reintroduce that hazard.
    //
    // Infallible by construction: a `Vec` sink raises no io error and
    // storekey's `Vec` encoder raises no custom error.
    #[allow(clippy::expect_used)]
    let framed = storekey::encode_vec(&components).expect("storekey encode into a Vec cannot fail");
    Ok(CanonicalKey(framed))
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
}
