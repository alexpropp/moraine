//! The DuckDB extension entry point.
//!
//! After `dlopen`ing the extension file, DuckDB's loader resolves
//! `<filebase>_duckdb_cpp_init` (base name `moraine_duckdb`, so
//! `moraine_duckdb_duckdb_cpp_init`) and calls it with an `ExtensionLoader`.
//! That symbol must live in the shared object's dynamic symbol table.
//!
//! On ELF, rustc's cdylib link emits a version script that binds every
//! symbol it does not own to `local`, and no additive linker flag overrides
//! that wildcard — so an entry point exported only from the C++ shim is
//! hidden and the load fails. Defining the entry point here, as a
//! `#[no_mangle]` Rust function rustc lists among the cdylib's global
//! exports, puts it in the dynamic symbol table on every platform. It
//! forwards straight to the C++ shim, which does the registration.

use core::ffi::c_void;

unsafe extern "C" {
    /// Registers moraine's `StorageExtension` on the loading database.
    /// Defined in the C++ shim (`cpp/extension.cpp`) and resolved at
    /// static-link time; takes the `duckdb::ExtensionLoader *` this entry
    /// point receives.
    fn moraine_duckdb_register(loader: *mut c_void);
}

/// Extension entry point DuckDB calls after `dlopen`. The name is fixed by
/// DuckDB's loader (`<filebase>_duckdb_cpp_init` for artifact base name
/// `moraine_duckdb`). Forwards the `ExtensionLoader` to the C++ shim, whose
/// reference parameter has the same ABI as this pointer.
///
/// # Safety
/// `loader` must be the non-null `duckdb::ExtensionLoader *` DuckDB passes;
/// the C++ side dereferences it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_duckdb_duckdb_cpp_init(loader: *mut c_void) {
    // SAFETY: `loader` is forwarded unchanged under this function's contract.
    unsafe { moraine_duckdb_register(loader) }
}
