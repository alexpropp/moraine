//! Crash-injection seams for the migration and crash test matrix. Compiled
//! only under `test` or the `fault-injection` feature, so production builds
//! carry no seam code and no fault surface.

use std::cell::Cell;

use crate::error::{Error, Result};

/// A named seam the migration driver consults between durable batches.
/// Each variant is one row of the crash matrix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CrashPoint {
    /// After the start batch (marker + initial cursor) is durable.
    AfterStart,
    /// After a step batch (data rewrite + cursor advance) is durable.
    AfterStep,
    /// After the last step, before the finish flip.
    BeforeFinish,
    /// After the finish batch (format flip + marker clear) is durable.
    AfterFinish,
}

thread_local! {
    static ARMED: Cell<Option<CrashPoint>> = const { Cell::new(None) };
}

/// Arms (or disarms with `None`) the seam that trips on its next pass.
pub(crate) fn inject_crash(point: Option<CrashPoint>) {
    ARMED.with(|armed| armed.set(point));
}

/// Errors the first time it is called at the armed point, then disarms. The
/// driver treats the error as a simulated crash: it stops, leaving the last
/// durable state intact.
pub(crate) fn crash_seam(point: CrashPoint) -> Result<()> {
    let tripped = ARMED.with(|armed| {
        if armed.get() == Some(point) {
            armed.set(None);
            true
        } else {
            false
        }
    });
    if tripped {
        return Err(Error::Migration(
            "fault-injection: simulated crash at a migration seam".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn armed_point_trips_its_seam_once() {
        inject_crash(Some(CrashPoint::AfterStart));
        assert!(crash_seam(CrashPoint::AfterStart).is_err());
        // Disarmed after firing: a second pass proceeds.
        assert!(crash_seam(CrashPoint::AfterStart).is_ok());
    }

    #[test]
    fn unarmed_seam_is_a_noop() {
        inject_crash(None);
        assert!(crash_seam(CrashPoint::AfterFinish).is_ok());
    }

    #[test]
    fn other_points_do_not_trip() {
        inject_crash(Some(CrashPoint::BeforeFinish));
        assert!(crash_seam(CrashPoint::AfterStep).is_ok());
    }
}
