//! Typed value codec: protobuf messages behind the framing header.

use prost::Message;

use crate::{
    error::{Error, Result},
    store::frame,
};

/// Encode a message behind the framing header, directly into the framed
/// buffer with no payload copy.
pub(crate) fn encode_value<M: Message>(msg: &M) -> Vec<u8> {
    let mut buf = frame::header_buf(msg.encoded_len());
    // Infallible by construction: insufficient capacity is the only error
    // `encode` returns, and a `Vec`'s `BufMut` capacity is unbounded.
    #[allow(clippy::expect_used)]
    msg.encode(&mut buf)
        .expect("prost encode into a Vec cannot fail");
    buf
}

/// Validate the framing header and decode the payload as `M`. Fails as
/// [`Error::Corruption`] on any framing or protobuf decode failure.
pub(crate) fn decode_value<M: Message + Default>(bytes: &[u8]) -> Result<M> {
    let payload = frame::unframe(bytes)?;
    M::decode(payload).map_err(|err| Error::Corruption(format!("value: {err}")))
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::store::proto::*;

    #[test]
    fn decode_rejects_unframed_bytes() {
        let err = decode_value::<HeadValue>(b"not framed").unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
    }

    #[test]
    fn decode_rejects_framed_garbage_payload() {
        // A valid frame whose payload is not a valid message for the
        // requested type: field 1 wire-type 2 with a length running past
        // the end of the buffer.
        let framed = frame::frame(&[0x0a, 0xff]);
        let err = decode_value::<HeadValue>(&framed).unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
    }

    macro_rules! roundtrip {
        ($name:ident, $ty:ty) => {
            proptest! {
                #[test]
                fn $name(msg in any::<$ty>()) {
                    let encoded = encode_value(&msg);
                    let decoded: $ty = decode_value(&encoded).unwrap();
                    prop_assert_eq!(decoded, msg);
                }
            }
        };
    }

    roundtrip!(roundtrip_schema, SchemaValue);
    roundtrip!(roundtrip_table, TableValue);
    roundtrip!(roundtrip_view, ViewValue);
    roundtrip!(roundtrip_column, ColumnValue);
    roundtrip!(roundtrip_partition, PartitionValue);
    roundtrip!(roundtrip_sort, SortValue);
    roundtrip!(roundtrip_data_file, DataFileValue);
    roundtrip!(roundtrip_delete_file, DeleteFileValue);
    roundtrip!(roundtrip_file_column_stats, FileColumnStatsValue);
    roundtrip!(roundtrip_table_stats, TableStatsValue);
    roundtrip!(roundtrip_table_column_stats, TableColumnStatsValue);
    roundtrip!(roundtrip_tag, TagValue);
    roundtrip!(roundtrip_option_scope, OptionScopeValue);
    roundtrip!(roundtrip_snapshot, SnapshotValue);
    roundtrip!(roundtrip_gcfile, GcFileValue);
    roundtrip!(roundtrip_format, FormatValue);
    roundtrip!(roundtrip_head, HeadValue);
    roundtrip!(roundtrip_migration, MigrationValue);
    roundtrip!(roundtrip_inline_schema, InlineSchemaValue);
    roundtrip!(roundtrip_inline_chunk, InlineChunkValue);
    roundtrip!(roundtrip_inline_inline_delete, InlineInlineDeleteValue);
    roundtrip!(roundtrip_inline_file_delete, InlineFileDeleteValue);
}
