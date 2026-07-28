//! Carrying [`moraine`]'s `tracing` events into DuckDB's logger.
//!
//! The core emits `tracing` events; nothing in a loaded extension consumes
//! them by default. The extension is a separate dynamically-loaded library
//! with its own statically-linked `tracing`, so even a host process that
//! installs a subscriber never sees them — the extension must consume its
//! own.
//!
//! Events fire on the handle's tokio worker threads, where no DuckDB
//! `ClientContext` is in scope, so they are not written through to DuckDB
//! as they happen. A `tracing` layer buffers them; the shim drains it on
//! the calling thread, which does hold a context, and writes each record
//! through `Logger::Get(context)`. Records therefore surface when the
//! operation that produced them returns — which for a commit is exactly
//! when a caller wants them.
//!
//! The buffer is bounded and drops oldest-first: diagnostics must never
//! grow without limit behind a caller that stops draining.

use std::{
    collections::VecDeque,
    ffi::{CString, c_char, c_void},
    fmt::Write as _,
    sync::{Mutex, OnceLock},
};

use tracing::{Level, Subscriber, field::Visit};
use tracing_subscriber::{layer::Context, prelude::*, registry::LookupSpan};

/// How many records the buffer holds before dropping the oldest. One
/// commit's retry trace is a handful of events, so this spans many
/// operations' worth of history.
const LOG_BUFFER_CAPACITY: usize = 512;

/// DuckDB's `LogLevel` values, which the sink forwards unchanged.
mod levels {
    pub const TRACE: i32 = 10;
    pub const DEBUG: i32 = 20;
    pub const INFO: i32 = 30;
    pub const WARNING: i32 = 40;
    pub const ERROR: i32 = 50;
}

/// One buffered event.
struct LogRecord {
    level: i32,
    message: String,
}

/// The process-wide buffer, plus how many records were dropped since the
/// last drain.
struct LogBuffer {
    records: VecDeque<LogRecord>,
    dropped: u64,
}

fn buffer() -> &'static Mutex<LogBuffer> {
    static BUFFER: OnceLock<Mutex<LogBuffer>> = OnceLock::new();
    BUFFER.get_or_init(|| {
        Mutex::new(LogBuffer {
            records: VecDeque::new(),
            dropped: 0,
        })
    })
}

/// The lowest level captured, from `MORAINE_LOG` (`trace`, `debug`, `info`,
/// `warn`, `error`); defaults to `info`. Events below it are never
/// buffered, so a chatty level cannot evict the warnings that matter.
fn capture_level() -> Level {
    static LEVEL: OnceLock<Level> = OnceLock::new();
    *LEVEL.get_or_init(|| match std::env::var("MORAINE_LOG") {
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "trace" => Level::TRACE,
            "debug" => Level::DEBUG,
            "warn" | "warning" => Level::WARN,
            "error" => Level::ERROR,
            _ => Level::INFO,
        },
        Err(_) => Level::INFO,
    })
}

fn duckdb_level(level: Level) -> i32 {
    match level {
        Level::TRACE => levels::TRACE,
        Level::DEBUG => levels::DEBUG,
        Level::INFO => levels::INFO,
        Level::WARN => levels::WARNING,
        Level::ERROR => levels::ERROR,
    }
}

/// Renders an event's `message` field plus its remaining fields as
/// `message (key=value, key=value)`.
#[derive(Default)]
struct MessageVisitor {
    message: String,
    fields: String,
}

impl MessageVisitor {
    fn record(&mut self, name: &str, value: &dyn std::fmt::Debug) {
        if name == "message" {
            self.message = format!("{value:?}");
            return;
        }
        if !self.fields.is_empty() {
            self.fields.push_str(", ");
        }
        self.fields.push_str(name);
        self.fields.push('=');
        // Writing into a `String` is infallible; the result is ignored
        // rather than unwrapped.
        let _ = write!(self.fields, "{value:?}");
    }

    fn finish(self, target: &str) -> String {
        let Self { message, fields } = self;
        match (message.is_empty(), fields.is_empty()) {
            (true, true) => target.to_string(),
            (true, false) => format!("{target}: {fields}"),
            (false, true) => format!("{target}: {message}"),
            (false, false) => format!("{target}: {message} ({fields})"),
        }
    }
}

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.record(field.name(), value);
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.record(field.name(), &value);
    }
}

/// Buffers every event at or above [`capture_level`].
struct BufferLayer;

impl<S> tracing_subscriber::Layer<S> for BufferLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn enabled(&self, metadata: &tracing::Metadata<'_>, _: Context<'_, S>) -> bool {
        metadata.level() <= &capture_level()
    }

    fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
        let metadata = event.metadata();
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);
        let record = LogRecord {
            level: duckdb_level(*metadata.level()),
            message: visitor.finish(metadata.target()),
        };

        let Ok(mut buffer) = buffer().lock() else {
            return;
        };
        if buffer.records.len() >= LOG_BUFFER_CAPACITY {
            buffer.records.pop_front();
            buffer.dropped = buffer.dropped.saturating_add(1);
        }
        buffer.records.push_back(record);
    }
}

/// Installs the buffering subscriber, once per process. Called on every
/// `ATTACH`; every call after the first is a no-op, and a host that already
/// installed a global subscriber wins — this never displaces one.
pub fn install() {
    static INSTALLED: OnceLock<()> = OnceLock::new();
    INSTALLED.get_or_init(|| {
        // `try_init` fails only when a global subscriber already exists,
        // which is a valid state, not an error to report.
        let _ = tracing_subscriber::registry().with(BufferLayer).try_init();
    });
}

/// Receives one buffered log record: its DuckDB `LogLevel` value and a
/// UTF-8, NUL-terminated message valid only for the duration of the call.
pub type MoraineLogSink =
    Option<unsafe extern "C" fn(ctx: *mut c_void, level: i32, message: *const c_char)>;

/// Drains every buffered log record into `sink`, oldest first.
///
/// Called by the shim on a thread that holds a DuckDB `ClientContext`, so
/// the records can be written through DuckDB's own logger. Never fails and
/// never allocates on the caller's behalf: each message is borrowed for the
/// duration of its `sink` call and freed after it returns.
///
/// # Safety
///
/// `sink`, if non-null, must be callable with `sink_ctx` and must not
/// unwind. It must not re-enter any `moraine_*` entry point.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moraine_drain_logs(sink: MoraineLogSink, sink_ctx: *mut c_void) {
    let Some(sink) = sink else {
        return;
    };
    let Ok(mut buffer) = buffer().lock() else {
        return;
    };
    let dropped = std::mem::take(&mut buffer.dropped);
    let records: Vec<LogRecord> = buffer.records.drain(..).collect();
    // Released before calling out, so a sink that logs cannot deadlock on
    // an event this same thread emits.
    drop(buffer);

    if dropped > 0
        && let Ok(message) = CString::new(format!(
            "moraine: {dropped} diagnostic record(s) dropped; the log buffer filled between drains"
        ))
    {
        // SAFETY: caller contract; pointer valid for this call only.
        unsafe { sink(sink_ctx, levels::WARNING, message.as_ptr()) };
    }
    for record in records {
        // A message with an embedded NUL cannot cross as a C string; the
        // level still tells the reader something happened.
        let Ok(message) = CString::new(record.message.replace('\0', "")) else {
            continue;
        };
        // SAFETY: caller contract; pointer valid for this call only.
        unsafe { sink(sink_ctx, record.level, message.as_ptr()) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The visitor renders the message and the remaining fields in the
    /// shape the shim forwards to DuckDB.
    #[test]
    fn visitor_renders_message_and_fields() {
        let mut visitor = MessageVisitor::default();
        visitor.record("message", &"commit exhausted its retry budget");
        visitor.record("attempts", &10);
        let rendered = visitor.finish("moraine::transaction::commit");
        assert_eq!(
            rendered,
            "moraine::transaction::commit: \"commit exhausted its retry budget\" (attempts=10)"
        );
    }

    #[test]
    fn visitor_renders_a_bare_message() {
        let mut visitor = MessageVisitor::default();
        visitor.record("message", &"plain");
        assert_eq!(visitor.finish("target"), "target: \"plain\"");
    }

    /// `warn!` maps to DuckDB's `LOG_WARNING`, the level the exhausted-budget
    /// diagnostic is emitted at.
    #[test]
    fn levels_map_to_duckdb_values() {
        assert_eq!(duckdb_level(Level::WARN), 40);
        assert_eq!(duckdb_level(Level::DEBUG), 20);
    }

    /// A drain with no sink is a no-op rather than a crash.
    #[test]
    fn draining_without_a_sink_is_a_no_op() {
        // SAFETY: a null sink is the documented no-op case.
        unsafe { moraine_drain_logs(None, std::ptr::null_mut()) };
    }
}
