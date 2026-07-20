//! Scoped Parquet read: the extension path derives index entries by
//! reading only the indexed columns, the row positions, and — when the
//! file carries one — the row-id column of a registered data file. A
//! bounded, merge-free projection, not the scan path the no-Parquet-read
//! rule guards.

use arrow::{
    array::{
        Array, BinaryArray, BooleanArray, Date32Array, Date64Array, FixedSizeBinaryArray,
        Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
        LargeBinaryArray, LargeStringArray, RecordBatch, StringArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
        UInt16Array, UInt32Array, UInt64Array,
    },
    buffer::Buffer,
    datatypes::{DataType, TimeUnit},
    ipc::{
        reader::{StreamReader, read_record_batch},
        root_as_message,
    },
};
use bytes::Bytes;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use parquet::arrow::{ProjectionMask, arrow_reader::ParquetRecordBatchReaderBuilder};

use crate::{
    error::{Error, Result},
    store::index_encoding::{IndexKeyValue, IntWidth},
};

/// One entry derived from a registered file: the row id and the canonical
/// values of the indexed columns, in the index's column order.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ScopedReadEntry {
    /// The row this entry points at.
    pub(crate) row_id: u64,
    /// The indexed column values; a `None` is SQL NULL (no entry).
    pub(crate) values: Vec<Option<IndexKeyValue>>,
}

fn downcast<A: 'static>(array: &dyn Array) -> Result<&A> {
    array
        .as_any()
        .downcast_ref::<A>()
        .ok_or_else(|| Error::Corruption("scoped read: parquet column type mismatch".to_owned()))
}

/// The canonical value of `array` at `row`, or `None` for NULL.
fn array_value(array: &dyn Array, row: usize) -> Result<Option<IndexKeyValue>> {
    if array.is_null(row) {
        return Ok(None);
    }

    let signed = |value: i128, width| IndexKeyValue::Int { value, width };
    let unsigned = |value: u128, width| IndexKeyValue::UInt { value, width };
    let value = match array.data_type() {
        DataType::Int8 => signed(
            i128::from(downcast::<Int8Array>(array)?.value(row)),
            IntWidth::I8,
        ),
        DataType::Int16 => signed(
            i128::from(downcast::<Int16Array>(array)?.value(row)),
            IntWidth::I16,
        ),
        DataType::Int32 => signed(
            i128::from(downcast::<Int32Array>(array)?.value(row)),
            IntWidth::I32,
        ),
        DataType::Int64 => signed(
            i128::from(downcast::<Int64Array>(array)?.value(row)),
            IntWidth::I64,
        ),
        DataType::UInt8 => unsigned(
            u128::from(downcast::<UInt8Array>(array)?.value(row)),
            IntWidth::I8,
        ),
        DataType::UInt16 => unsigned(
            u128::from(downcast::<UInt16Array>(array)?.value(row)),
            IntWidth::I16,
        ),
        DataType::UInt32 => unsigned(
            u128::from(downcast::<UInt32Array>(array)?.value(row)),
            IntWidth::I32,
        ),
        DataType::UInt64 => unsigned(
            u128::from(downcast::<UInt64Array>(array)?.value(row)),
            IntWidth::I64,
        ),
        DataType::Float32 => IndexKeyValue::F32(downcast::<Float32Array>(array)?.value(row)),
        DataType::Float64 => IndexKeyValue::F64(downcast::<Float64Array>(array)?.value(row)),
        DataType::Boolean => IndexKeyValue::Bool(downcast::<BooleanArray>(array)?.value(row)),
        DataType::Utf8 => IndexKeyValue::Str(downcast::<StringArray>(array)?.value(row).to_owned()),
        DataType::LargeUtf8 => {
            IndexKeyValue::Str(downcast::<LargeStringArray>(array)?.value(row).to_owned())
        }
        DataType::LargeBinary => {
            IndexKeyValue::Bytes(downcast::<LargeBinaryArray>(array)?.value(row).to_vec())
        }
        DataType::Binary => {
            IndexKeyValue::Bytes(downcast::<BinaryArray>(array)?.value(row).to_vec())
        }
        // Fixed-width blobs, e.g. a 16-byte `UUID`.
        DataType::FixedSizeBinary(_) => {
            IndexKeyValue::Bytes(downcast::<FixedSizeBinaryArray>(array)?.value(row).to_vec())
        }
        // Temporal types index by their underlying integer representation.
        DataType::Date32 => signed(
            i128::from(downcast::<Date32Array>(array)?.value(row)),
            IntWidth::I32,
        ),
        DataType::Date64 => signed(
            i128::from(downcast::<Date64Array>(array)?.value(row)),
            IntWidth::I64,
        ),
        // Each timestamp width indexes by its own underlying `i64` count —
        // seconds, milli-, micro-, or nanoseconds — read from the array of
        // the matching unit. The count is the same on the inline path (whose
        // schema carries the same unit), so the two paths agree.
        DataType::Timestamp(TimeUnit::Second, _) => signed(
            i128::from(downcast::<TimestampSecondArray>(array)?.value(row)),
            IntWidth::I64,
        ),
        DataType::Timestamp(TimeUnit::Millisecond, _) => signed(
            i128::from(downcast::<TimestampMillisecondArray>(array)?.value(row)),
            IntWidth::I64,
        ),
        DataType::Timestamp(TimeUnit::Microsecond, _) => signed(
            i128::from(downcast::<TimestampMicrosecondArray>(array)?.value(row)),
            IntWidth::I64,
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => signed(
            i128::from(downcast::<TimestampNanosecondArray>(array)?.value(row)),
            IntWidth::I64,
        ),
        other => {
            return Err(Error::Constraint(format!(
                "scoped read: column type {other:?} is not indexable"
            )));
        }
    };
    Ok(Some(value))
}

/// Reads the row-id column value at `row` as a `u64` (`Int64`/`UInt64`).
fn row_id_value(array: &dyn Array, row: usize) -> Result<u64> {
    match array.data_type() {
        DataType::Int64 => u64::try_from(downcast::<Int64Array>(array)?.value(row))
            .map_err(|_| Error::Corruption("scoped read: negative row id".to_owned())),
        DataType::UInt64 => Ok(downcast::<UInt64Array>(array)?.value(row)),
        other => Err(Error::Corruption(format!(
            "scoped read: row-id column has non-integer type {other:?}"
        ))),
    }
}

/// Derives one [`ScopedReadEntry`] per row of the Parquet file at `path`,
/// reading only `indexed_positions` (the indexed columns, in the index's
/// column order) and `row_id_position` when present. Row ids come from the
/// embedded row-id column if `row_id_position` is set — rewrite files from
/// UPDATE and compaction preserve old ids there — else `row_id_start +
/// ordinal`.
pub(crate) async fn scoped_read_entries(
    object_store: &dyn ObjectStore,
    path: &Path,
    indexed_positions: &[usize],
    row_id_position: Option<usize>,
    row_id_start: u64,
) -> Result<Vec<ScopedReadEntry>> {
    let bytes: Bytes = object_store
        .get(path)
        .await
        .map_err(|err| Error::Corruption(format!("scoped read: {err}")))?
        .bytes()
        .await
        .map_err(|err| Error::Corruption(format!("scoped read: {err}")))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|err| Error::Corruption(format!("scoped read: {err}")))?;

    // Project only the columns we need, so no non-indexed column is read.
    let mut projected: Vec<usize> = indexed_positions.to_vec();
    if let Some(position) = row_id_position {
        projected.push(position);
    }
    projected.sort_unstable();
    projected.dedup();
    let mask = ProjectionMask::roots(builder.parquet_schema(), projected.iter().copied());
    // Output-batch column index for an original file position.
    let output_index = |position: usize| {
        projected
            .iter()
            .position(|&candidate| candidate == position)
            .ok_or_else(|| Error::Corruption("scoped read: projected column vanished".to_owned()))
    };

    let reader = builder
        .with_projection(mask)
        .build()
        .map_err(|err| Error::Corruption(format!("scoped read: {err}")))?;

    let batches = reader
        .into_iter()
        .map(|batch| batch.map_err(|err| Error::Corruption(format!("scoped read: {err}"))))
        .collect::<Result<Vec<_>>>()?;

    // Map the indexed and row-id columns to their positions in the projected
    // output batch.
    let indexed_output = indexed_positions
        .iter()
        .map(|&position| output_index(position))
        .collect::<Result<Vec<_>>>()?;
    let row_id_output = row_id_position.map(&output_index).transpose()?;

    let mut entries = Vec::new();
    let mut ordinal_base = 0u64;
    for batch in &batches {
        entries.extend(record_batch_entries(
            batch,
            &indexed_output,
            row_id_output,
            row_id_start,
            ordinal_base,
        )?);
        ordinal_base =
            ordinal_base.saturating_add(u64::try_from(batch.num_rows()).unwrap_or(u64::MAX));
    }
    Ok(entries)
}

/// Derives one entry per row of `batch`: the values of the columns at
/// `positions` (direct indices into `batch`, in the index's column order),
/// and a row id read from `row_id_position` when present, else
/// `row_id_start + ordinal_base + row`.
fn record_batch_entries(
    batch: &RecordBatch,
    positions: &[usize],
    row_id_position: Option<usize>,
    row_id_start: u64,
    ordinal_base: u64,
) -> Result<Vec<ScopedReadEntry>> {
    (0..batch.num_rows())
        .map(|row| {
            let values = positions
                .iter()
                .map(|&position| array_value(batch.column(position).as_ref(), row))
                .collect::<Result<Vec<_>>>()?;
            let row_id = match row_id_position {
                Some(position) => row_id_value(batch.column(position).as_ref(), row)?,
                None => row_id_start
                    .saturating_add(ordinal_base)
                    .saturating_add(u64::try_from(row).unwrap_or(u64::MAX)),
            };
            Ok(ScopedReadEntry { row_id, values })
        })
        .collect()
}

/// Decodes an inline-insert Arrow body — `[u32-le message length][record-
/// batch message][arrow data buffers]` — against `schema_ipc`, the table's
/// schema-only IPC stream, into a record batch. Inline chunks store the
/// schema once per version, so the body carries none of its own.
fn decode_inline_batch(schema_ipc: &[u8], body: &[u8]) -> Result<RecordBatch> {
    let schema = StreamReader::try_new(std::io::Cursor::new(schema_ipc.to_vec()), None)
        .map_err(|err| Error::Corruption(format!("inline schema: {err}")))?
        .schema();
    if body.len() < 4 {
        return Err(Error::Corruption("inline body truncated".to_owned()));
    }
    let message_len = u32::from_le_bytes(
        body[0..4]
            .try_into()
            .map_err(|_| Error::Corruption("inline body length prefix".to_owned()))?,
    ) as usize;
    let message_end = 4 + message_len;
    if message_end > body.len() {
        return Err(Error::Corruption("inline body truncated".to_owned()));
    }
    let message = root_as_message(&body[4..message_end])
        .map_err(|err| Error::Corruption(format!("inline message: {err}")))?;
    let record_batch = message
        .header_as_record_batch()
        .ok_or_else(|| Error::Corruption("inline body is not a record batch".to_owned()))?;
    let version = message.version();
    let buffer = Buffer::from_vec(body[message_end..].to_vec());
    read_record_batch(
        &buffer,
        record_batch,
        schema,
        &std::collections::HashMap::new(),
        None,
        &version,
    )
    .map_err(|err| Error::Corruption(format!("inline batch: {err}")))
}

/// Derives entries from an inline-insert chunk: decodes its body against the
/// schema, then reads the columns at `positions` per row with dense
/// `row_id_start + ordinal` ids (inline chunks carry no row-id column).
pub(crate) fn inline_batch_entries(
    schema_ipc: &[u8],
    body: &[u8],
    positions: &[usize],
    row_id_start: u64,
) -> Result<Vec<ScopedReadEntry>> {
    let batch = decode_inline_batch(schema_ipc, body)?;
    record_batch_entries(&batch, positions, None, row_id_start, 0)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use object_store::{ObjectStoreExt, memory::InMemory};
    use parquet::arrow::ArrowWriter;

    use super::*;

    async fn write_fixture(object_store: &InMemory, path: &Path, batch: &RecordBatch) {
        let mut buffer = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
            writer.write(batch).unwrap();
            writer.close().unwrap();
        }
        object_store.put(path, buffer.into()).await.unwrap();
    }

    fn fixture_batch() -> RecordBatch {
        // Columns: id (indexed), name (indexed, one NULL), row_id, and an
        // unindexed `payload` column the read must not touch.
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("row_id", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
                Arc::new(Int64Array::from(vec![100, 101, 102])),
                Arc::new(StringArray::from(vec!["x", "y", "z"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn sub_microsecond_timestamps_index_by_their_own_count() {
        // A millisecond timestamp indexes by its millisecond count, a
        // nanosecond one by its nanosecond count — not misread as micros.
        let millis = arrow::array::TimestampMillisecondArray::from(vec![1_700_000_000_123i64]);
        assert_eq!(
            array_value(&millis, 0).unwrap(),
            Some(IndexKeyValue::Int {
                value: 1_700_000_000_123,
                width: IntWidth::I64
            }),
        );
        let nanos =
            arrow::array::TimestampNanosecondArray::from(vec![1_700_000_000_123_456_789i64]);
        assert_eq!(
            array_value(&nanos, 0).unwrap(),
            Some(IndexKeyValue::Int {
                value: 1_700_000_000_123_456_789,
                width: IntWidth::I64
            }),
        );
    }

    #[test]
    fn fixed_size_binary_indexes_as_bytes() {
        // A `UUID` reaches the read as a 16-byte `FixedSizeBinary`.
        let uuid = [0xABu8; 16];
        let array = arrow::array::FixedSizeBinaryArray::try_from_iter([uuid].into_iter()).unwrap();
        assert_eq!(
            array_value(&array, 0).unwrap(),
            Some(IndexKeyValue::Bytes(uuid.to_vec())),
        );
    }

    #[tokio::test]
    async fn reads_indexed_columns_and_embedded_row_ids() {
        let store = InMemory::new();
        let path = Path::from("data.parquet");
        write_fixture(&store, &path, &fixture_batch()).await;

        // Index over (id, name); the file carries a row-id column at 2.
        let entries = scoped_read_entries(&store, &path, &[0, 1], Some(2), 0)
            .await
            .unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(
            entries[0],
            ScopedReadEntry {
                row_id: 100,
                values: vec![
                    Some(IndexKeyValue::Int {
                        value: 10,
                        width: IntWidth::I64,
                    }),
                    Some(IndexKeyValue::Str("a".to_owned())),
                ],
            }
        );
        // Row 1's name is NULL → no value for that component.
        assert_eq!(entries[1].row_id, 101);
        assert_eq!(entries[1].values[1], None);
        assert_eq!(entries[2].row_id, 102);
    }

    #[tokio::test]
    async fn derives_row_ids_from_start_plus_ordinal_when_absent() {
        let store = InMemory::new();
        let path = Path::from("data.parquet");
        write_fixture(&store, &path, &fixture_batch()).await;

        // No row-id column: ids are row_id_start (500) + ordinal.
        let entries = scoped_read_entries(&store, &path, &[0], None, 500)
            .await
            .unwrap();
        assert_eq!(
            entries.iter().map(|e| e.row_id).collect::<Vec<_>>(),
            vec![500, 501, 502]
        );
    }
}
