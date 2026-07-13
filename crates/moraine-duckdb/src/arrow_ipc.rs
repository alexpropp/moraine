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

use std::io::Cursor;
use std::ptr;

use arrow::array::{Array, RecordBatch, StructArray};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi, to_ffi};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;

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

/// Serializes one inlined chunk to a self-contained IPC stream (schema +
/// one record batch), so decode never depends on a separately stored
/// schema.
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
        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())
                .map_err(|e| format!("writer: {e}"))?;
            writer.write(&batch).map_err(|e| format!("write: {e}"))?;
            writer.finish().map_err(|e| format!("finish: {e}"))?;
        }
        Ok::<Vec<u8>, String>(buf)
    }))
    .unwrap_or_else(|_| Err("panic encoding arrow chunk".to_string()));
    write_bytes_result(out, err, result)
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

    use arrow::array::{Int64Array, ListArray, StringArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field};

    use super::*;

    /// Encodes a struct array through the chunk bridge and decodes it back to
    /// owned C Data Interface structs — the exact FFI round trip the C++ shim
    /// drives. The returned structs are Rust-owned and release on drop.
    fn encode_then_decode(source: &StructArray) -> (FFI_ArrowArray, FFI_ArrowSchema) {
        let (mut in_array, mut in_schema) = to_ffi(&source.to_data()).expect("export source");
        let mut bytes = MoraineArrowBytes::empty();
        let mut err = MoraineArrowError {
            failed: 0,
            message: ptr::null_mut(),
        };
        // SAFETY: the exported structs and out-pointers are valid; encode
        // consumes the structs, so we `forget` our copies below to avoid a
        // double release (mirrors the C++ contract).
        let code = unsafe {
            moraine_arrow_encode_chunk(
                &raw mut in_schema,
                &raw mut in_array,
                &raw mut bytes,
                &raw mut err,
            )
        };
        std::mem::forget(in_array);
        std::mem::forget(in_schema);
        assert_eq!(code, 0, "encode failed");

        let mut out_schema = FFI_ArrowSchema::empty();
        let mut out_array = FFI_ArrowArray::empty();
        // SAFETY: `bytes` holds the encoded stream; the out-pointers are valid
        // writable slots this call fills with owned structs.
        let code = unsafe {
            moraine_arrow_decode_stream(
                bytes.data,
                bytes.len,
                &raw mut out_schema,
                &raw mut out_array,
                &raw mut err,
            )
        };
        assert_eq!(code, 0, "decode failed");
        // SAFETY: `bytes` came from the encode call above and is freed once.
        unsafe { moraine_arrow_bytes_free(bytes) };
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
