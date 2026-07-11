//! C-ABI error codes and the `(code, message)` pair carried across the
//! boundary.
//!
//! Every `extern "C"` entry point returns an [`i32`] code (`codes::OK` is
//! the only success value) and, on failure, may fill a caller-owned
//! [`MoraineError`] with the same code plus a heap-allocated message,
//! freed exactly once via
//! [`moraine_error_free`](crate::abi::moraine_error_free) — never `free()`.

use std::ffi::{CString, c_char};

/// Named error codes returned by every `moraine_*` entry point.
///
/// The C++ shim maps these to DuckDB exception kinds:
///
/// | Code | Meaning | Shim maps to |
/// |---|---|---|
/// | [`OK`](codes::OK) | success | — |
/// | [`NOT_FOUND`](codes::NOT_FOUND) | referenced entity does not exist | `CatalogException` |
/// | [`ALREADY_EXISTS`](codes::ALREADY_EXISTS) | name uniqueness violated | `CatalogException` |
/// | [`CONSTRAINT`](codes::CONSTRAINT) | structural constraint violated | `CatalogException` |
/// | [`COMMIT_CONFLICT`](codes::COMMIT_CONFLICT) | concurrent commit conflict; message contains the substring `conflict` | `TransactionException` |
/// | [`CORRUPTION`](codes::CORRUPTION) | stored bytes failed to decode, or a catalog string cannot round-trip through a C string | `IOException` |
/// | [`STORE`](codes::STORE) | the underlying object store / SlateDB failed | `IOException` |
/// | [`INVALID_ARGUMENT`](codes::INVALID_ARGUMENT) | a null pointer, non-UTF-8 string, or unsupported ABI input | `InvalidInputException` |
/// | [`INTERNAL`](codes::INTERNAL) | a panic was caught at the FFI boundary | `InternalException` |
/// | [`INTERRUPTED`](codes::INTERRUPTED) | [`moraine_interrupt`](crate::abi::moraine_interrupt) cancelled the read in flight (or about to start) on this handle | `InterruptException` |
pub mod codes {
    /// Success; no error occurred.
    pub const OK: i32 = 0;
    /// [`moraine::Error::NotFound`].
    pub const NOT_FOUND: i32 = 1;
    /// [`moraine::Error::AlreadyExists`].
    pub const ALREADY_EXISTS: i32 = 2;
    /// [`moraine::Error::Constraint`].
    pub const CONSTRAINT: i32 = 3;
    /// [`moraine::Error::CommitConflict`].
    pub const COMMIT_CONFLICT: i32 = 4;
    /// [`moraine::Error::Corruption`], and ABI-level string encoding
    /// failures (an embedded NUL byte cannot be represented as a C
    /// string).
    pub const CORRUPTION: i32 = 5;
    /// [`moraine::Error::Store`].
    pub const STORE: i32 = 6;
    /// A null pointer, invalid UTF-8, or unsupported argument value
    /// (e.g. an unrecognized `object_store_uri` scheme). Never produced
    /// by the `moraine` core — an ABI-layer validation failure.
    pub const INVALID_ARGUMENT: i32 = 7;
    /// A panic was caught at the FFI boundary and converted to an error
    /// instead of unwinding into C++.
    pub const INTERNAL: i32 = 8;
    /// [`moraine_interrupt`](crate::abi::moraine_interrupt) cancelled the
    /// read in flight (or about to start) on this handle. Never produced
    /// by the `moraine` core — an ABI-layer cancellation signal.
    pub const INTERRUPTED: i32 = 9;
}

/// Fixed message for a caught panic; never derived from the panic
/// payload.
pub(crate) const INTERNAL_PANIC_MESSAGE: &str =
    "moraine-duckdb: internal error (a panic was caught at the FFI boundary)";

/// An error to report back across the FFI boundary: a code plus an owned
/// message. Internal to the crate; [`write_into`](AbiError::write_into)
/// turns it into the C representation.
#[derive(Debug)]
pub(crate) struct AbiError {
    pub code: i32,
    pub message: String,
}

impl AbiError {
    pub(crate) fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub(crate) fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(codes::INVALID_ARGUMENT, message)
    }

    /// The fixed error a `select!`-guarded read reports when
    /// [`moraine_interrupt`](crate::abi::moraine_interrupt) cancelled it.
    pub(crate) fn interrupted() -> Self {
        Self::new(
            codes::INTERRUPTED,
            "moraine-duckdb: operation was interrupted",
        )
    }

    /// Writes `self` into a caller-owned [`MoraineError`], if `err` is
    /// non-null. The message is sanitized (embedded NUL bytes stripped) so
    /// the `CString` construction below cannot fail.
    ///
    /// # Safety
    ///
    /// `err`, if non-null, must point to a valid, writable [`MoraineError`]
    /// for the duration of this call.
    pub(crate) unsafe fn write_into(self, err: *mut MoraineError) {
        if err.is_null() {
            return;
        }
        // Embedded NULs are stripped: diagnostic text, not protocol data.
        let sanitized = self.message.replace('\0', "");
        // Always succeeds after sanitization above; falls back to an
        // empty message otherwise.
        let c_message = CString::new(sanitized).unwrap_or_default();
        // SAFETY: caller contract above; checked non-null just above.
        unsafe {
            (*err).code = self.code;
            (*err).message = c_message.into_raw();
        }
    }
}

impl From<moraine::Error> for AbiError {
    fn from(err: moraine::Error) -> Self {
        let code = match &err {
            moraine::Error::NotFound(_) => codes::NOT_FOUND,
            moraine::Error::AlreadyExists(_) => codes::ALREADY_EXISTS,
            moraine::Error::Constraint(_) => codes::CONSTRAINT,
            // The shim's retry loop matches the literal substring
            // "conflict" in the message; core's `Display` already includes
            // it.
            moraine::Error::CommitConflict(_) => codes::COMMIT_CONFLICT,
            moraine::Error::Corruption(_) => codes::CORRUPTION,
            // Covers `Store` and any future `#[non_exhaustive]` variant.
            _ => codes::STORE,
        };
        Self::new(code, err.to_string())
    }
}

/// The `(code, message)` pair carried across the FFI boundary.
///
/// Caller-allocated and passed by pointer to every fallible `moraine_*`
/// entry point; on failure the callee fills in both fields. `message` is
/// null when there is nothing to free, and must be passed to
/// [`moraine_error_free`](crate::abi::moraine_error_free) exactly once —
/// the entry point never frees a previous message, so reuse without
/// freeing in between leaks.
#[repr(C)]
#[derive(Debug)]
pub struct MoraineError {
    /// One of the [`codes`] constants.
    pub code: i32,
    /// A UTF-8, NUL-terminated, heap-allocated message, or null.
    pub message: *mut c_char,
}

impl Default for MoraineError {
    fn default() -> Self {
        Self {
            code: codes::OK,
            message: std::ptr::null_mut(),
        }
    }
}
