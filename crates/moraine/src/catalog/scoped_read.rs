//! Scoped Parquet read: the extension path derives index entries by
//! reading only the indexed columns, the row positions, and — when the
//! file carries one — the row-id column of a registered data file. A
//! bounded, merge-free projection, not the scan path the no-Parquet-read
//! rule guards.

use std::sync::Arc;

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
use futures::{
    StreamExt,
    future::{BoxFuture, FutureExt},
};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use parquet::{
    arrow::{
        ProjectionMask,
        arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder},
        async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder},
    },
    errors::{ParquetError, Result as ParquetResult},
    file::metadata::{ParquetMetaData, ParquetMetaDataReader},
};

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
    /// The indexed column values; a `None` is SQL NULL (stored multi-shaped).
    pub(crate) values: Vec<Option<IndexKeyValue>>,
}

/// DuckDB's reserved Parquet field id tagging the embedded row-id column
/// (`_ducklake_internal_row_id`) that rewrite and flush files carry.
const ROW_ID_FIELD_ID: i32 = 2_147_483_540;

/// How a scoped read resolves each row's id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RowIdSource {
    /// The embedded row-id column when the file carries one — preferred
    /// even over a recorded dense start, since flushed files carry both
    /// and their embedded ids may hold gaps — else `start + ordinal`,
    /// else refusal.
    Resolve {
        /// The catalog row's dense start, if it records one.
        row_id_start: Option<u64>,
    },
    /// Row ids are file ordinals from 0. A file carrying the embedded
    /// column is refused: its rows already have ids, and renumbering
    /// them would fork the file's identity.
    Ordinal,
}

/// The root position of the embedded row-id column, if the file has one.
fn embedded_row_id_position(schema: &parquet::schema::types::SchemaDescriptor) -> Option<usize> {
    schema.root_schema().get_fields().iter().position(|field| {
        let info = field.get_basic_info();
        info.has_id() && info.id() == ROW_ID_FIELD_ID
    })
}

/// Resolves `source` against the file's schema into the projection
/// inputs: the row-id column's position (when it is to be read) and the
/// dense start.
fn resolve_row_id_source(
    schema: &parquet::schema::types::SchemaDescriptor,
    source: RowIdSource,
    path: &Path,
) -> Result<(Option<usize>, u64)> {
    match (source, embedded_row_id_position(schema)) {
        (RowIdSource::Ordinal, Some(_)) => Err(Error::Constraint(format!(
            "scoped read: {path} carries an embedded row-id column and cannot be read by ordinal"
        ))),
        (RowIdSource::Ordinal, None) => Ok((None, 0)),
        (RowIdSource::Resolve { .. }, Some(position)) => Ok((Some(position), 0)),
        (
            RowIdSource::Resolve {
                row_id_start: Some(start),
            },
            None,
        ) => Ok((None, start)),
        (RowIdSource::Resolve { row_id_start: None }, None) => Err(Error::Corruption(format!(
            "scoped read: {path} is recorded as carrying per-row ids but has no embedded \
             row-id column"
        ))),
    }
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

/// Reads a row id or row position at `row` as a `u64` (`Int64`/`UInt64`).
fn row_id_value(array: &dyn Array, row: usize) -> Result<u64> {
    if array.is_null(row) {
        return Err(Error::Corruption(
            "scoped read: row-id column holds a NULL".to_owned(),
        ));
    }
    match array.data_type() {
        DataType::Int64 => u64::try_from(downcast::<Int64Array>(array)?.value(row))
            .map_err(|_| Error::Corruption("scoped read: negative row id".to_owned())),
        DataType::UInt64 => Ok(downcast::<UInt64Array>(array)?.value(row)),
        other => Err(Error::Corruption(format!(
            "scoped read: row-id column has non-integer type {other:?}"
        ))),
    }
}

/// An [`AsyncFileReader`] over moraine's own object store: the Parquet
/// footer and the projected column chunks arrive as byte-range reads, so a
/// scoped read never downloads the whole data file. Deliberately not
/// `parquet`'s built-in `object_store` integration, which pins a different
/// `object_store` major than the workspace.
struct ObjectStoreReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
    /// The object's total length, required to locate the footer.
    file_size: u64,
}

impl AsyncFileReader for ObjectStoreReader {
    fn get_bytes(&mut self, range: std::ops::Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        let store = Arc::clone(&self.store);
        let path = self.path.clone();
        async move {
            store
                .get_range(&path, range)
                .await
                .map_err(|err| ParquetError::External(Box::new(err)))
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<std::ops::Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        let store = Arc::clone(&self.store);
        let path = self.path.clone();
        // One `get_ranges` call, so the store can coalesce adjacent chunks.
        async move {
            store
                .get_ranges(&path, &ranges)
                .await
                .map_err(|err| ParquetError::External(Box::new(err)))
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        let file_size = self.file_size;
        async move {
            ParquetMetaDataReader::new()
                .load_and_finish(&mut *self, file_size)
                .await
                .map(Arc::new)
        }
        .boxed()
    }
}

/// Derives one [`ScopedReadEntry`] per row of the Parquet file at `path`,
/// fetching only the footer, the columns at `indexed_positions` (the
/// indexed columns, in the index's column order), and the embedded row-id
/// column when the file carries one — byte-range reads, never the whole
/// object. Row ids resolve per `row_id_source`: the field-id-tagged
/// embedded column if present — rewrite files from UPDATE and compaction
/// preserve old ids there — else `row_id_start + ordinal`, else refusal.
/// `file_size` is the object's length when the caller knows it (DuckLake
/// records it per data file); `None` costs one `head` request.
pub(crate) async fn scoped_read_entries(
    object_store: Arc<dyn ObjectStore>,
    path: &Path,
    indexed_positions: &[usize],
    row_id_source: RowIdSource,
    file_size: Option<u64>,
) -> Result<Vec<ScopedReadEntry>> {
    let file_size = match file_size {
        Some(size) => size,
        None => {
            object_store
                .head(path)
                .await
                .map_err(|err| Error::Corruption(format!("scoped read: {err}")))?
                .size
        }
    };
    if file_size < WHOLE_OBJECT_THRESHOLD {
        return whole_object_entries(
            object_store.as_ref(),
            path,
            indexed_positions,
            row_id_source,
        )
        .await;
    }

    let reader = ObjectStoreReader {
        store: object_store,
        path: path.clone(),
        file_size,
    };
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(|err| Error::Corruption(format!("scoped read: {err}")))?;
    let (row_id_position, row_id_start) =
        resolve_row_id_source(builder.parquet_schema(), row_id_source, path)?;
    let (mask, indexed_output, row_id_output) =
        projection(builder.parquet_schema(), indexed_positions, row_id_position)?;
    let mut stream = builder
        .with_projection(mask)
        .build()
        .map_err(|err| Error::Corruption(format!("scoped read: {err}")))?;

    let mut entries = Vec::new();
    let mut ordinal_base = 0u64;
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(|err| Error::Corruption(format!("scoped read: {err}")))?;
        entries.extend(record_batch_entries(
            &batch,
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

/// Below this object size the whole file is fetched in one request and
/// decoded from memory: on a remote store the range reader's footer and
/// chunk round trips cost more than the bytes they save (measured crossover
/// ≈ 6 MiB at 30 ms per request and 100 MB/s, and DuckLake's small
/// per-insert files sit far below it).
const WHOLE_OBJECT_THRESHOLD: u64 = 4 * 1024 * 1024;

/// The pre-threshold read path: one whole-object fetch, then decode only
/// the projected columns. The batches decode from memory, so this stays
/// bounded by the threshold above.
async fn whole_object_entries(
    object_store: &dyn ObjectStore,
    path: &Path,
    indexed_positions: &[usize],
    row_id_source: RowIdSource,
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
    let (row_id_position, row_id_start) =
        resolve_row_id_source(builder.parquet_schema(), row_id_source, path)?;
    let (mask, indexed_output, row_id_output) =
        projection(builder.parquet_schema(), indexed_positions, row_id_position)?;
    let reader = builder
        .with_projection(mask)
        .build()
        .map_err(|err| Error::Corruption(format!("scoped read: {err}")))?;

    let mut entries = Vec::new();
    let mut ordinal_base = 0u64;
    for batch in reader {
        let batch = batch.map_err(|err| Error::Corruption(format!("scoped read: {err}")))?;
        entries.extend(record_batch_entries(
            &batch,
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

/// The projection over `schema` covering only the indexed and row-id
/// columns, with each requested position mapped to its index in the
/// projected output batch.
fn projection(
    schema: &parquet::schema::types::SchemaDescriptor,
    indexed_positions: &[usize],
    row_id_position: Option<usize>,
) -> Result<(ProjectionMask, Vec<usize>, Option<usize>)> {
    let mut projected: Vec<usize> = indexed_positions.to_vec();
    if let Some(position) = row_id_position {
        projected.push(position);
    }
    projected.sort_unstable();
    projected.dedup();
    let mask = ProjectionMask::roots(schema, projected.iter().copied());
    // Output-batch column index for an original file position.
    let output_index = |position: usize| {
        projected
            .iter()
            .position(|&candidate| candidate == position)
            .ok_or_else(|| Error::Corruption("scoped read: projected column vanished".to_owned()))
    };
    let indexed_output = indexed_positions
        .iter()
        .map(|&position| output_index(position))
        .collect::<Result<Vec<_>>>()?;
    let row_id_output = row_id_position.map(output_index).transpose()?;
    Ok((mask, indexed_output, row_id_output))
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

/// The row positions a DuckLake delete file marks dead, read from its `pos`
/// column. A delete file names positions within one data file, so its
/// `file_path` column carries no information the caller lacks.
pub(crate) async fn delete_file_positions(
    object_store: &dyn ObjectStore,
    path: &Path,
) -> Result<Vec<u64>> {
    let bytes: Bytes = object_store
        .get(path)
        .await
        .map_err(|err| Error::Corruption(format!("delete-file read: {err}")))?
        .bytes()
        .await
        .map_err(|err| Error::Corruption(format!("delete-file read: {err}")))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|err| Error::Corruption(format!("delete-file read: {err}")))?;

    let position = builder
        .schema()
        .fields()
        .iter()
        .position(|field| field.name() == "pos")
        .ok_or_else(|| Error::Corruption("delete file has no `pos` column".to_owned()))?;

    let mask = ProjectionMask::roots(builder.parquet_schema(), [position]);
    let reader = builder
        .with_projection(mask)
        .build()
        .map_err(|err| Error::Corruption(format!("delete-file read: {err}")))?;

    let mut positions = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|err| Error::Corruption(format!("delete-file read: {err}")))?;
        let column = batch.column(0).as_ref();
        for row in 0..batch.num_rows() {
            if column.is_null(row) {
                return Err(Error::Corruption(
                    "delete file has a NULL position".to_owned(),
                ));
            }
            positions.push(row_id_value(column, row)?);
        }
    }
    Ok(positions)
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
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };

    use arrow::{
        array::{Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use futures::stream::BoxStream;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory,
    };
    use parquet::arrow::ArrowWriter;

    use super::*;

    /// Wraps an [`InMemory`] store, counting the payload bytes and requests
    /// served by object/range reads — so a test can assert how much of a
    /// file the scoped read actually fetched. `head` probes are not counted:
    /// they carry no payload.
    #[derive(Debug)]
    struct CountingStore {
        inner: InMemory,
        fetched_bytes: AtomicU64,
        fetch_requests: AtomicU64,
    }

    impl CountingStore {
        fn new() -> Self {
            Self {
                inner: InMemory::new(),
                fetched_bytes: AtomicU64::new(0),
                fetch_requests: AtomicU64::new(0),
            }
        }

        fn fetched_bytes(&self) -> u64 {
            self.fetched_bytes.load(Ordering::Relaxed)
        }

        fn fetch_requests(&self) -> u64 {
            self.fetch_requests.load(Ordering::Relaxed)
        }
    }

    impl std::fmt::Display for CountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CountingStore({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for CountingStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            let head = options.head;
            let result = self.inner.get_opts(location, options).await?;
            if !head {
                self.fetch_requests.fetch_add(1, Ordering::Relaxed);
                self.fetched_bytes
                    .fetch_add(result.range.end - result.range.start, Ordering::Relaxed);
            }
            Ok(result)
        }

        async fn get_ranges(
            &self,
            location: &Path,
            ranges: &[std::ops::Range<u64>],
        ) -> object_store::Result<Vec<Bytes>> {
            let results = self.inner.get_ranges(location, ranges).await?;
            self.fetch_requests.fetch_add(1, Ordering::Relaxed);
            let total: u64 = results.iter().map(|bytes| bytes.len() as u64).sum();
            self.fetched_bytes.fetch_add(total, Ordering::Relaxed);
            Ok(results)
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// A file wide enough that its indexed column is a small fraction of its
    /// bytes: one `Int64` id plus seven fat `Utf8` payload columns, `rows`
    /// rows. Returns the written object's size.
    async fn write_wide_fixture(store: &dyn ObjectStore, path: &Path, rows: usize) -> u64 {
        let mut fields = vec![Field::new("id", DataType::Int64, false)];
        for i in 0..7 {
            fields.push(Field::new(format!("payload{i}"), DataType::Utf8, false));
        }
        let schema = Arc::new(Schema::new(fields));

        let ids: Vec<i64> = (0..i64::try_from(rows).unwrap()).collect();
        let mut columns: Vec<Arc<dyn Array>> = vec![Arc::new(Int64Array::from(ids))];
        for i in 0..7 {
            // Row-unique text so the payload columns stay large on disk.
            let values: Vec<String> = (0..rows)
                .map(|row| format!("payload-{i}-row-{row:08}-abcdefghijklmnopqrstuvwxyz"))
                .collect();
            columns.push(Arc::new(StringArray::from(values)));
        }
        let batch = RecordBatch::try_new(schema, columns).unwrap();

        let mut buffer = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        let object_len = buffer.len() as u64;
        store.put(path, buffer.into()).await.unwrap();
        object_len
    }

    /// Above the whole-object threshold the read must fetch only the footer
    /// and the projected columns' chunks, not the whole object — and values
    /// must follow the requested position order on this path too.
    #[tokio::test]
    async fn scoped_read_fetches_only_projected_columns() {
        let store = Arc::new(CountingStore::new());
        let path = Path::from("wide.parquet");
        // 20k rows ≈ 7.9 MB: above `WHOLE_OBJECT_THRESHOLD`, so the range
        // reader (not the small-file whole-object fallback) serves this.
        let object_len = write_wide_fixture(store.as_ref(), &path, 20_000).await;
        assert!(object_len >= WHOLE_OBJECT_THRESHOLD);

        let entries =
            scoped_read_entries(store.clone(), &path, &[1, 0], RowIdSource::Ordinal, None)
                .await
                .unwrap();
        assert_eq!(entries.len(), 20_000);
        assert_eq!(
            entries[19_999].values,
            vec![
                Some(IndexKeyValue::Str(
                    "payload-0-row-00019999-abcdefghijklmnopqrstuvwxyz".to_owned()
                )),
                Some(IndexKeyValue::Int {
                    value: 19_999,
                    width: IntWidth::I64,
                }),
            ],
        );

        let fetched = store.fetched_bytes();
        assert!(
            fetched < object_len * 2 / 5,
            "fetched {fetched} of {object_len} bytes ({} requests) — the scoped read should \
             range-read only the footer and the projected columns",
            store.fetch_requests(),
        );
    }

    /// A narrow file shaped like DuckLake's small per-insert output: an
    /// `Int64` id and one short `Utf8` column, `rows` rows. Returns the
    /// written object's size.
    async fn write_narrow_fixture(store: &dyn ObjectStore, path: &Path, rows: usize) -> u64 {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let ids: Vec<i64> = (0..i64::try_from(rows).unwrap()).collect();
        let names: Vec<String> = (0..rows).map(|row| format!("name-{row}")).collect();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap();

        let mut buffer = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        let object_len = u64::try_from(buffer.len()).unwrap();
        store.put(path, buffer.into()).await.unwrap();
        object_len
    }

    /// Prints the exact bytes and requests the range reader issues per
    /// fixture shape. Run with:
    /// `cargo test -p moraine --lib -- --ignored --nocapture prints_fetch_profile`
    #[tokio::test]
    #[ignore = "fetch-profile probe; run manually with --nocapture"]
    async fn prints_fetch_profile() {
        // (label, wide fixture?, rows, indexed positions)
        let shapes: [(&str, bool, usize, &[usize]); 4] = [
            ("wide 8-col x 20k rows, 1 indexed col ", true, 20_000, &[0]),
            (
                "wide 8-col x 20k rows, 2 indexed cols",
                true,
                20_000,
                &[0, 1],
            ),
            ("narrow 2-col x 100 rows, 1 indexed   ", false, 100, &[0]),
            ("narrow 2-col x 10 rows, 1 indexed    ", false, 10, &[0]),
        ];
        for (label, wide, rows, positions) in shapes {
            let store = Arc::new(CountingStore::new());
            let path = Path::from("probe.parquet");
            let object_len = if wide {
                write_wide_fixture(store.as_ref(), &path, rows).await
            } else {
                write_narrow_fixture(store.as_ref(), &path, rows).await
            };
            let entries =
                scoped_read_entries(store.clone(), &path, positions, RowIdSource::Ordinal, None)
                    .await
                    .unwrap();
            assert_eq!(entries.len(), rows);
            println!(
                "{label}: object {object_len:>8} B, fetched {:>7} B ({:>2}%), {} requests",
                store.fetched_bytes(),
                store.fetched_bytes() * 100 / object_len,
                store.fetch_requests(),
            );
        }
    }

    /// Per-request latency a [`LatencyStore`] charges, modelling a remote
    /// store round trip.
    const REQUEST_LATENCY: std::time::Duration = std::time::Duration::from_millis(30);

    /// Wraps an [`InMemory`] store, modelling a remote one: every read
    /// request pays [`REQUEST_LATENCY`] plus transfer at ~100 MB/s (10 ns
    /// per byte).
    #[derive(Debug)]
    struct LatencyStore {
        inner: InMemory,
    }

    impl LatencyStore {
        async fn charge(bytes: u64) {
            tokio::time::sleep(REQUEST_LATENCY + std::time::Duration::from_nanos(bytes * 10)).await;
        }
    }

    impl std::fmt::Display for LatencyStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "LatencyStore({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for LatencyStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            let head = options.head;
            let result = self.inner.get_opts(location, options).await?;
            let bytes = if head {
                0
            } else {
                result.range.end - result.range.start
            };
            Self::charge(bytes).await;
            Ok(result)
        }

        async fn get_ranges(
            &self,
            location: &Path,
            ranges: &[std::ops::Range<u64>],
        ) -> object_store::Result<Vec<Bytes>> {
            let results = self.inner.get_ranges(location, ranges).await?;
            Self::charge(results.iter().map(|bytes| bytes.len() as u64).sum()).await;
            Ok(results)
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// The pre-range-reader read, reproduced for comparison: fetch the whole
    /// object, then decode only the projected columns.
    async fn whole_file_entries(
        store: &dyn ObjectStore,
        path: &Path,
        indexed_positions: &[usize],
    ) -> Vec<ScopedReadEntry> {
        let bytes: Bytes = store.get(path).await.unwrap().bytes().await.unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        let mask =
            ProjectionMask::roots(builder.parquet_schema(), indexed_positions.iter().copied());
        let reader = builder.with_projection(mask).build().unwrap();

        let output_positions: Vec<usize> = (0..indexed_positions.len()).collect();
        let mut entries = Vec::new();
        let mut ordinal_base = 0u64;
        for batch in reader {
            let batch = batch.unwrap();
            entries.extend(
                record_batch_entries(&batch, &output_positions, None, 0, ordinal_base).unwrap(),
            );
            ordinal_base += u64::try_from(batch.num_rows()).unwrap();
        }
        entries
    }

    /// Wall-clock comparison of the whole-object read against the range
    /// reader on a simulated remote store (30 ms per request, ~100 MB/s).
    /// Run with:
    /// `cargo test -p moraine --lib -- --ignored --nocapture simulated_remote`
    #[tokio::test]
    #[ignore = "timing probe; run manually with --nocapture"]
    async fn simulated_remote_store_bench() {
        // (label, wide fixture?, rows) — narrow-small is DuckLake's typical
        // per-insert file; wide-large is the backfill/bulk-maintenance case.
        let shapes = [
            ("narrow 2-col x 100 rows ", false, 100),
            ("wide 8-col x 50k rows   ", true, 50_000),
        ];
        for (label, wide, rows) in shapes {
            let store = Arc::new(LatencyStore {
                inner: InMemory::new(),
            });
            let path = Path::from("bench.parquet");
            let object_len = if wide {
                write_wide_fixture(store.as_ref(), &path, rows).await
            } else {
                write_narrow_fixture(store.as_ref(), &path, rows).await
            };

            let started = std::time::Instant::now();
            let old_entries = whole_file_entries(store.as_ref(), &path, &[0]).await;
            let whole_file = started.elapsed();

            let started = std::time::Instant::now();
            let new_entries =
                scoped_read_entries(store.clone(), &path, &[0], RowIdSource::Ordinal, None)
                    .await
                    .unwrap();
            let range_read = started.elapsed();

            assert_eq!(
                old_entries, new_entries,
                "both paths derive the same entries"
            );
            println!(
                "{label} ({object_len:>8} B): whole-file {whole_file:>9.2?}, range-read \
                 {range_read:>9.2?}"
            );
        }
    }

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

    /// The row-id column DuckLake's rewrite and flush writers append:
    /// BIGINT, tagged with the reserved field id — at any position.
    fn tagged_row_id_field(nullable: bool) -> Field {
        Field::new("_ducklake_internal_row_id", DataType::Int64, nullable).with_metadata(
            std::collections::HashMap::from([(
                parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_string(),
                "2147483540".to_string(),
            )]),
        )
    }

    /// `fixture_batch` with the row-id column carrying the field id, so
    /// discovery finds it (at position 2, not trailing).
    fn tagged_fixture_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            tagged_row_id_field(false),
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

    #[tokio::test]
    async fn reads_indexed_columns_and_embedded_row_ids() {
        let store = Arc::new(InMemory::new());
        let path = Path::from("data.parquet");
        write_fixture(&store, &path, &tagged_fixture_batch()).await;

        // Index over (id, name); the file carries the field-id-tagged
        // row-id column at position 2, found without a caller hint.
        let entries = scoped_read_entries(
            store.clone(),
            &path,
            &[0, 1],
            RowIdSource::Resolve { row_id_start: None },
            None,
        )
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

    /// Values come back ordered as the requested positions — duplicates and
    /// all. The merged multi-index read (one fetch per file, split back per
    /// index) relies on exactly this.
    #[tokio::test]
    async fn values_follow_requested_position_order() {
        let store = Arc::new(InMemory::new());
        let path = Path::from("data.parquet");
        write_fixture(&store, &path, &fixture_batch()).await;

        let entries =
            scoped_read_entries(store.clone(), &path, &[1, 0, 0], RowIdSource::Ordinal, None)
                .await
                .unwrap();
        assert_eq!(
            entries[0].values,
            vec![
                Some(IndexKeyValue::Str("a".to_owned())),
                Some(IndexKeyValue::Int {
                    value: 10,
                    width: IntWidth::I64,
                }),
                Some(IndexKeyValue::Int {
                    value: 10,
                    width: IntWidth::I64,
                }),
            ],
        );
    }

    #[tokio::test]
    async fn derives_row_ids_from_start_plus_ordinal_when_absent() {
        let store = Arc::new(InMemory::new());
        let path = Path::from("data.parquet");
        write_fixture(&store, &path, &fixture_batch()).await;

        // `fixture_batch`'s "row_id" column carries no field id, so it is
        // not the embedded column — names mean nothing to discovery — and
        // ids fall back to row_id_start (500) + ordinal.
        let entries = scoped_read_entries(
            store.clone(),
            &path,
            &[0],
            RowIdSource::Resolve {
                row_id_start: Some(500),
            },
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            entries.iter().map(|e| e.row_id).collect::<Vec<_>>(),
            vec![500, 501, 502]
        );
    }

    /// The embedded column wins even when a dense start is recorded:
    /// flushed files carry both, and their ids may hold gaps.
    #[tokio::test]
    async fn embedded_row_id_column_wins_over_dense_start() {
        let store = Arc::new(InMemory::new());
        let path = Path::from("rewrite.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            tagged_row_id_field(false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(Int64Array::from(vec![5, 9, 12])),
            ],
        )
        .unwrap();
        write_fixture(&store, &path, &batch).await;

        let entries = scoped_read_entries(
            store.clone(),
            &path,
            &[0],
            RowIdSource::Resolve {
                row_id_start: Some(100),
            },
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            entries.iter().map(|e| e.row_id).collect::<Vec<_>>(),
            vec![5, 9, 12],
            "ids come from the column, not 100 + ordinal"
        );
    }

    /// A per-row-id catalog row over a file lacking the column is a
    /// disagreement between catalog and file.
    #[tokio::test]
    async fn resolve_with_neither_source_fails_corruption() {
        let store = Arc::new(InMemory::new());
        let path = Path::from("plain.parquet");
        write_fixture(&store, &path, &fixture_batch()).await;

        let err = scoped_read_entries(
            store.clone(),
            &path,
            &[0],
            RowIdSource::Resolve { row_id_start: None },
            None,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, Error::Corruption(_)), "{err}");
    }

    /// Ordinal mode refuses a file that already carries row ids —
    /// renumbering its rows would fork their identity.
    #[tokio::test]
    async fn ordinal_mode_refuses_an_embedded_row_id_column() {
        let store = Arc::new(InMemory::new());
        let path = Path::from("rewrite.parquet");
        write_fixture(&store, &path, &tagged_fixture_batch()).await;

        let err = scoped_read_entries(store.clone(), &path, &[0], RowIdSource::Ordinal, None)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Constraint(_)), "{err}");
    }

    /// A NULL embedded row id has no dense fallback to hide behind.
    #[tokio::test]
    async fn null_embedded_row_id_fails_corruption() {
        let store = Arc::new(InMemory::new());
        let path = Path::from("null-id.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            tagged_row_id_field(true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![10, 20])),
                Arc::new(Int64Array::from(vec![Some(5), None])),
            ],
        )
        .unwrap();
        write_fixture(&store, &path, &batch).await;

        let err = scoped_read_entries(
            store.clone(),
            &path,
            &[0],
            RowIdSource::Resolve { row_id_start: None },
            None,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, Error::Corruption(_)), "{err}");
    }
}
