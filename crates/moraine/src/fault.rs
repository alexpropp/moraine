//! Crash-injection seams for the format-migration driver. The driver calls
//! a seam unconditionally between its durable batches; the arming machinery
//! behind it compiles only under `test` or the `fault-injection` feature, so
//! a production build carries an empty function and no fault surface.

/// A named seam the migration driver consults between durable batches.
/// These are that driver's own batch boundaries; the enumerated crash cases
/// are a separate set of rows, driven by the integration suite.
///
/// Reachable outside the crate only under the `fault-injection` feature,
/// which re-exports it alongside [`inject_crash`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashPoint {
    /// After the start batch (marker + initial cursor) is durable.
    AfterStart,
    /// After a step batch (data rewrite + cursor advance) is durable.
    AfterStep,
    /// After the last step, before the finish flip.
    BeforeFinish,
    /// After the finish batch (format flip + marker clear) is durable.
    AfterFinish,
}

#[cfg(any(test, feature = "fault-injection"))]
mod armed {
    use std::cell::Cell;

    use super::CrashPoint;
    use crate::error::{Error, Result};

    thread_local! {
        static ARMED: Cell<Option<CrashPoint>> = const { Cell::new(None) };
    }

    /// Arms (or disarms with `None`) the seam that trips on its next pass.
    /// Thread-local, so tests arming different seams do not collide.
    pub fn inject_crash(point: Option<CrashPoint>) {
        ARMED.with(|armed| armed.set(point));
    }

    /// Errors the first time it is called at the armed point, then disarms.
    /// The driver treats the error as a simulated crash: it stops, leaving
    /// the last durable state intact.
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
}

#[cfg(any(test, feature = "fault-injection"))]
pub(crate) use armed::crash_seam;
#[cfg(any(test, feature = "fault-injection"))]
pub use armed::inject_crash;

/// The seam with fault injection compiled out: an empty function, so the
/// driver's call sites cost a production build nothing.
///
/// The `Result` it can never fail with is what keeps those call sites
/// identical to the armed build's, which is the point of the pair.
#[cfg(not(any(test, feature = "fault-injection")))]
#[inline]
#[allow(clippy::unnecessary_wraps)]
pub(crate) fn crash_seam(_point: CrashPoint) -> crate::error::Result<()> {
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
