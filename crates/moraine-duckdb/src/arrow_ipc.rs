//! Arrow IPC serialization for inlined-data chunks.
//!
//! The C++ shim converts a DuckDB `DataChunk` to the Arrow C Data
//! Interface (`ArrowArray`/`ArrowSchema`) using DuckDB's own converter,
//! then hands those structs here; this module serializes them to Arrow
//! IPC bytes with `arrow-rs` (the layer DuckDB's C++ lacks). Decoding
//! reverses it: IPC bytes become C Data Interface structs the shim feeds
//! back to DuckDB's Arrow import.
//!
//! Ownership across the boundary is one rule in each direction. On encode,
//! this crate **consumes** the structs the shim passes (reads them out and
//! releases DuckDB's buffers) — the shim must not release them afterward.
//! On decode, this crate **produces** structs it owns via their release
//! callbacks and writes them into the shim's out-pointers — the shim (via
//! DuckDB's importer) owns calling those callbacks.

use std::{collections::HashMap, io::Cursor, ptr};

use arrow::{
    array::{Array, RecordBatch, StructArray},
    buffer::Buffer,
    datatypes::{Schema, SchemaRef},
    ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi, to_ffi},
    ipc::{
        MetadataVersion,
        reader::{StreamReader, read_record_batch},
        root_as_message,
        writer::{
            DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions, StreamWriter,
        },
    },
};

/// A byte buffer owned by Rust and freed via [`moraine_arrow_bytes_free`].
#[repr(C)]
pub struct MoraineArrowBytes {
    /// Pointer to the buffer, or null on failure.
    pub data: *mut u8,
    /// Length in bytes.
    pub len: usize,
    /// Capacity, retained so the buffer can be reconstructed for freeing.
    pub cap: usize,
}

impl MoraineArrowBytes {
    fn from_vec(mut v: Vec<u8>) -> Self {
        v.shrink_to_fit();
        let out = Self {
            data: v.as_mut_ptr(),
            len: v.len(),
            cap: v.capacity(),
        };
        std::mem::forget(v);
        out
    }

    fn empty() -> Self {
        Self {
            data: ptr::null_mut(),
            len: 0,
            cap: 0,
        }
    }
}

/// A short status/message pair mirroring the shim's error convention.
#[repr(C)]
pub struct MoraineArrowError {
    /// Non-zero on failure.
    pub failed: i32,
    /// Heap `CString` message (owned by this struct), or null.
    pub message: *mut std::os::raw::c_char,
}

fn set_error(err: *mut MoraineArrowError, message: &str) -> i32 {
    if !err.is_null() {
        let c = std::ffi::CString::new(message.replace('\0', " "))
            .unwrap_or_else(|_| std::ffi::CString::new("arrow ipc error").unwrap_or_default());
        // SAFETY: `err` is a valid, writable pointer the caller supplies for
        // every fallible entry point.
        unsafe {
            (*err).failed = 1;
            (*err).message = c.into_raw();
        }
    }
    1
}

fn clear_error(err: *mut MoraineArrowError) -> i32 {
    if !err.is_null() {
        // SAFETY: as above; on success we leave the message null.
        unsafe {
            (*err).failed = 0;
            (*err).message = ptr::null_mut();
        }
    }
    0
}

/// Frees a message allocated by a failed call.
///
/// # Safety
/// `message` must be null or a pointer returned in [`MoraineArrowError`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_arrow_error_free(message: *mut std::os::raw::c_char) {
    if !message.is_null() {
        // SAFETY: caller guarantees `message` came from a `CString::into_raw`
        // in this module and is freed once.
        drop(unsafe { std::ffi::CString::from_raw(message) });
    }
}

/// Frees a buffer returned by an encode call.
///
/// # Safety
/// `bytes` must be a value returned by a `moraine_arrow_encode_*` call and
/// not previously freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_arrow_bytes_free(bytes: MoraineArrowBytes) {
    if !bytes.data.is_null() {
        // SAFETY: caller guarantees the fields came from `from_vec` and are
        // freed once.
        drop(unsafe { Vec::from_raw_parts(bytes.data, bytes.len, bytes.cap) });
    }
}

fn write_ffi_result(
    out_schema: *mut FFI_ArrowSchema,
    out_array: *mut FFI_ArrowArray,
    err: *mut MoraineArrowError,
    result: Result<(FFI_ArrowArray, FFI_ArrowSchema), String>,
) -> i32 {
    match result {
        Ok((ffi_array, ffi_schema)) => {
            // SAFETY: `out_*` are valid writable slots per the caller contract.
            unsafe {
                ptr::write(out_array, ffi_array);
                ptr::write(out_schema, ffi_schema);
            }
            clear_error(err)
        }
        Err(e) => set_error(err, &e),
    }
}

fn write_bytes_result(
    out: *mut MoraineArrowBytes,
    err: *mut MoraineArrowError,
    result: Result<Vec<u8>, String>,
) -> i32 {
    match result {
        Ok(buf) => {
            // SAFETY: `out` is a valid writable slot per the caller contract.
            unsafe { ptr::write(out, MoraineArrowBytes::from_vec(buf)) };
            clear_error(err)
        }
        Err(e) => {
            // SAFETY: as above.
            unsafe { ptr::write(out, MoraineArrowBytes::empty()) };
            set_error(err, &e)
        }
    }
}

/// Serializes just the Arrow schema (an IPC stream with the schema and no
/// batches), stored once per inline schema version so an empty scan can
/// reconstruct the column layout.
///
/// # Safety
/// `schema` is an exported `ArrowSchema` consumed by this call; `out`/`err`
/// are valid writable pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_arrow_encode_schema(
    schema: *mut FFI_ArrowSchema,
    out: *mut MoraineArrowBytes,
    err: *mut MoraineArrowError,
) -> i32 {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // SAFETY: caller cedes ownership of the exported schema.
        let schema = unsafe { consume_schema(schema) }?;
        let mut buf = Vec::new();
        {
            let mut writer =
                StreamWriter::try_new(&mut buf, &schema).map_err(|e| format!("writer: {e}"))?;
            writer.finish().map_err(|e| format!("finish: {e}"))?;
        }
        Ok::<Vec<u8>, String>(buf)
    }))
    .unwrap_or_else(|_| Err("panic encoding arrow schema".to_string()));
    write_bytes_result(out, err, result)
}

/// Serializes one inlined chunk to a record-batch **body** only — the IPC
/// record-batch message and its buffers, without a schema message. The
/// schema is stored once per version by [`moraine_arrow_encode_schema`] and
/// supplied back at decode by [`moraine_arrow_decode_body`], so the WAL
/// append for a tiny commit never re-serializes the schema. The layout is a
/// little-endian `u32` message length, the record-batch flatbuffer message,
/// then the arrow data buffers.
///
/// Dictionary-encoded columns are rejected: the body carries no dictionary
/// messages. Inlined user columns are not dictionary-encoded in practice.
///
/// # Safety
/// `schema`/`array` are exported structs consumed by this call; `out`/`err`
/// are valid writable pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_arrow_encode_chunk(
    schema: *mut FFI_ArrowSchema,
    array: *mut FFI_ArrowArray,
    out: *mut MoraineArrowBytes,
    err: *mut MoraineArrowError,
) -> i32 {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if schema.is_null() || array.is_null() {
            return Err("null schema or array".to_string());
        }
        // SAFETY: caller cedes ownership of both exported structs.
        let (owned_schema, owned_array) = unsafe { (ptr::read(schema), ptr::read(array)) };
        // SAFETY: `owned_array` is a valid exported array matching `owned_schema`.
        let data = unsafe { from_ffi(owned_array, &owned_schema) }
            .map_err(|e| format!("array import: {e}"))?;
        drop(owned_schema);
        let batch = RecordBatch::from(StructArray::from(data));

        let generator = IpcDataGenerator::default();
        let mut tracker = DictionaryTracker::new(false);
        let options = IpcWriteOptions::default();
        let mut context = IpcWriteContext::default();
        let (dictionaries, encoded) = generator
            .encode(&batch, &mut tracker, &options, &mut context)
            .map_err(|e| format!("encode batch: {e}"))?;
        if !dictionaries.is_empty() {
            return Err("dictionary-encoded inline columns are not supported".to_string());
        }

        let mut buf = Vec::with_capacity(4 + encoded.ipc_message.len() + encoded.arrow_data.len());
        let message_len = u32::try_from(encoded.ipc_message.len())
            .map_err(|_| "inline chunk message too large".to_string())?;
        buf.extend_from_slice(&message_len.to_le_bytes());
        buf.extend_from_slice(&encoded.ipc_message);
        buf.extend_from_slice(&encoded.arrow_data);
        Ok::<Vec<u8>, String>(buf)
    }))
    .unwrap_or_else(|_| Err("panic encoding arrow chunk".to_string()));
    write_bytes_result(out, err, result)
}

/// Decodes a chunk body from [`moraine_arrow_encode_chunk`] against the
/// schema stored for its version (`schema_ipc` is that version's schema-only
/// IPC stream). Produces exported C Data Interface structs the shim feeds to
/// DuckDB's Arrow import; the caller (via DuckDB's importer) owns releasing
/// them.
///
/// # Safety
/// `schema_ipc`/`body` point to `schema_ipc_len`/`body_len` readable bytes;
/// `out_schema`/`out_array` are writable slots the caller releases; `err` is
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_arrow_decode_body(
    schema_ipc: *const u8,
    schema_ipc_len: usize,
    body: *const u8,
    body_len: usize,
    out_schema: *mut FFI_ArrowSchema,
    out_array: *mut FFI_ArrowArray,
    err: *mut MoraineArrowError,
) -> i32 {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if schema_ipc.is_null() || body.is_null() {
            return Err("null schema or body".to_string());
        }
        // SAFETY: caller guarantees `schema_ipc_len` bytes are readable at `schema_ipc`.
        let schema_bytes =
            unsafe { std::slice::from_raw_parts(schema_ipc, schema_ipc_len) }.to_vec();
        // SAFETY: caller guarantees `body_len` bytes are readable at `body`.
        let body_bytes = unsafe { std::slice::from_raw_parts(body, body_len) };

        let schema = StreamReader::try_new(Cursor::new(schema_bytes), None)
            .map_err(|e| format!("schema reader: {e}"))?
            .schema();

        if body_bytes.len() < 4 {
            return Err("inline chunk body truncated".to_string());
        }
        let len_bytes: [u8; 4] = body_bytes[0..4]
            .try_into()
            .map_err(|_| "inline chunk length prefix".to_string())?;
        let message_len = u32::from_le_bytes(len_bytes) as usize;
        let message_end = 4 + message_len;
        if message_end > body_bytes.len() {
            return Err("inline chunk body truncated".to_string());
        }
        let message = root_as_message(&body_bytes[4..message_end])
            .map_err(|e| format!("message parse: {e}"))?;
        let record_batch = message
            .header_as_record_batch()
            .ok_or_else(|| "inline chunk body is not a record batch".to_string())?;
        let version: MetadataVersion = message.version();
        let buffer = Buffer::from_vec(body_bytes[message_end..].to_vec());

        let batch = read_record_batch(
            &buffer,
            record_batch,
            schema,
            &HashMap::new(),
            None,
            &version,
        )
        .map_err(|e| format!("read batch: {e}"))?;
        to_ffi(&StructArray::from(batch).to_data()).map_err(|e| format!("array export: {e}"))
    }))
    .unwrap_or_else(|_| Err("panic decoding inline chunk body".to_string()));
    write_ffi_result(out_schema, out_array, err, result)
}

/// Decodes a self-contained IPC stream (from [`moraine_arrow_encode_chunk`],
/// or a schema-only stream from [`moraine_arrow_encode_schema`], which
/// yields an empty batch) into exported C Data Interface structs the shim
/// feeds to DuckDB's Arrow import.
///
/// # Safety
/// `body` points to `body_len` readable bytes; `out_schema`/`out_array` are
/// writable slots the caller (DuckDB) will release; `err` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_arrow_decode_stream(
    body: *const u8,
    body_len: usize,
    out_schema: *mut FFI_ArrowSchema,
    out_array: *mut FFI_ArrowArray,
    err: *mut MoraineArrowError,
) -> i32 {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if body.is_null() {
            return Err("null body".to_string());
        }
        // SAFETY: caller guarantees `body_len` readable bytes at `body`.
        let bytes = unsafe { std::slice::from_raw_parts(body, body_len) }.to_vec();
        let mut reader =
            StreamReader::try_new(Cursor::new(bytes), None).map_err(|e| format!("reader: {e}"))?;
        let schema = reader.schema();
        let batch = match reader.next() {
            Some(b) => b.map_err(|e| format!("read batch: {e}"))?,
            None => RecordBatch::new_empty(schema),
        };
        to_ffi(&StructArray::from(batch).to_data()).map_err(|e| format!("array export: {e}"))
    }))
    .unwrap_or_else(|_| Err("panic decoding arrow stream".to_string()));
    write_ffi_result(out_schema, out_array, err, result)
}

/// Reads and takes ownership of an exported schema struct.
///
/// # Safety
/// `schema` points to a valid exported `ArrowSchema` whose ownership the
/// caller cedes to this call.
unsafe fn consume_schema(schema: *mut FFI_ArrowSchema) -> Result<SchemaRef, String> {
    if schema.is_null() {
        return Err("null schema".to_string());
    }
    // SAFETY: caller cedes ownership of the exported struct.
    let owned = unsafe { ptr::read(schema) };
    let schema = Schema::try_from(&owned).map_err(|e| format!("schema import: {e}"));
    drop(owned);
    Ok(SchemaRef::new(schema?))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Int64Array, ListArray, StringArray},
        buffer::OffsetBuffer,
        datatypes::{DataType, Field},
    };

    use super::*;

    /// Encodes a struct array's schema and body separately, then decodes the
    /// body against that stored schema — the exact FFI round trip the C++ shim
    /// drives (schema once per version, body per chunk). The returned structs
    /// are Rust-owned and release on drop.
    fn encode_then_decode(source: &StructArray) -> (FFI_ArrowArray, FFI_ArrowSchema) {
        let mut err = MoraineArrowError {
            failed: 0,
            message: ptr::null_mut(),
        };

        // Schema-only stream (stored once per version as `inline/schema`).
        let batch = RecordBatch::from(source.clone());
        let mut schema_ffi =
            FFI_ArrowSchema::try_from(batch.schema().as_ref()).expect("export schema");
        let mut schema_bytes = MoraineArrowBytes::empty();
        // SAFETY: `schema_ffi` is a valid exported schema consumed by the call;
        // out-pointers are valid. Forget our copy after to avoid a double free.
        let code = unsafe {
            moraine_arrow_encode_schema(&raw mut schema_ffi, &raw mut schema_bytes, &raw mut err)
        };
        std::mem::forget(schema_ffi);
        assert_eq!(code, 0, "encode schema failed");

        // Body-only chunk.
        let (mut in_array, mut in_schema) = to_ffi(&source.to_data()).expect("export source");
        let mut body_bytes = MoraineArrowBytes::empty();
        // SAFETY: the exported structs and out-pointers are valid; encode
        // consumes the structs, so we `forget` our copies below.
        let code = unsafe {
            moraine_arrow_encode_chunk(
                &raw mut in_schema,
                &raw mut in_array,
                &raw mut body_bytes,
                &raw mut err,
            )
        };
        std::mem::forget(in_array);
        std::mem::forget(in_schema);
        assert_eq!(code, 0, "encode chunk failed");

        let mut out_schema = FFI_ArrowSchema::empty();
        let mut out_array = FFI_ArrowArray::empty();
        // SAFETY: the byte buffers are valid; the out-pointers are writable
        // slots this call fills with owned structs.
        let code = unsafe {
            moraine_arrow_decode_body(
                schema_bytes.data,
                schema_bytes.len,
                body_bytes.data,
                body_bytes.len,
                &raw mut out_schema,
                &raw mut out_array,
                &raw mut err,
            )
        };
        assert_eq!(code, 0, "decode body failed");
        // SAFETY: both buffers came from encode calls above and are freed once.
        unsafe {
            moraine_arrow_bytes_free(schema_bytes);
            moraine_arrow_bytes_free(body_bytes);
        }
        (out_array, out_schema)
    }

    fn round_trip(batch: &RecordBatch) -> RecordBatch {
        let (out_array, out_schema) = encode_then_decode(&StructArray::from(batch.clone()));
        // SAFETY: the decoded structs are valid and owned; `from_ffi` consumes
        // them, taking over their release.
        let data = unsafe { from_ffi(out_array, &out_schema) }.expect("import decoded");
        RecordBatch::from(StructArray::from(data))
    }

    #[test]
    fn scalar_columns_round_trip_with_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])),
            ],
        )
        .unwrap();
        assert_eq!(round_trip(&batch), batch);
    }

    #[test]
    fn decoded_children_report_null_count() {
        // DuckDB's arrow importer skips a column's validity buffer unless the
        // C-Data-Interface array reports `null_count != 0`. arrow-rs's own
        // `from_ffi` ignores `null_count` (it always reads the buffer), so a
        // dropped child `null_count` is invisible to the other round-trip
        // tests but silently strips every null on the DuckDB side.
        let schema = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
        ]));
        let source = StructArray::from(
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
                    Arc::new(StringArray::from(vec![Some("a"), None, None])),
                ],
            )
            .unwrap(),
        );
        let (out_array, _out_schema) = encode_then_decode(&source);
        assert_eq!(out_array.child(0).null_count(), 1, "int column null_count");
        assert_eq!(
            out_array.child(1).null_count(),
            2,
            "string column null_count"
        );
    }

    #[test]
    fn nested_list_column_round_trips() {
        let values = Int64Array::from(vec![10, 20, 30, 40]);
        let offsets = OffsetBuffer::new(vec![0, 2, 2, 4].into());
        let field = Arc::new(Field::new("item", DataType::Int64, true));
        let list = ListArray::new(field, offsets, Arc::new(values), None);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(list)]).unwrap();
        assert_eq!(round_trip(&batch), batch);
    }
}
