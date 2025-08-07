//! Simple profiling utilities for ingest performance measurement.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::OnceLock;

static PROFILING_ENABLED: AtomicBool = AtomicBool::new(false);
static EVENT_COUNT: AtomicU64 = AtomicU64::new(0);

#[cfg(feature = "profiling")]
static PROFILER_GUARD: OnceLock<pprof::ProfilerGuard> = OnceLock::new();

const MAX_EVENTS: u64 = 1_000_000; // Changed to 1M for the test

/// Start profiling if the MOONLINK_PROFILE_INGEST environment variable is set.
pub fn start_profiling() {
    if std::env::var("MOONLINK_PROFILE_INGEST").is_ok() {
        #[cfg(feature = "profiling")]
        {
            if let Ok(guard) = pprof::ProfilerGuard::new(100) {
                if PROFILER_GUARD.set(guard).is_ok() {
                    PROFILING_ENABLED.store(true, Ordering::Release);
                    tracing::debug!("started ingest profiling");
                } else {
                    tracing::warn!("profiler already started");
                }
            } else {
                tracing::warn!("failed to start profiler");
            }
        }
        #[cfg(not(feature = "profiling"))]
        {
            tracing::warn!("profiling requested but not compiled with profiling feature");
        }
    }
}

/// Increment event count and check if we should stop profiling.
/// Returns true if profiling should continue, false if it should stop.
pub fn increment_event_count() -> bool {
    if !PROFILING_ENABLED.load(Ordering::Acquire) {
        return true;
    }

    let count = EVENT_COUNT.fetch_add(1, Ordering::Relaxed);
    if count >= MAX_EVENTS {
        stop_profiling();
        false
    } else {
        true
    }
}

/// Stop profiling and generate flamegraph.
pub fn stop_profiling() {
    if !PROFILING_ENABLED.load(Ordering::Acquire) {
        return;
    }

    PROFILING_ENABLED.store(false, Ordering::Release);
    let count = EVENT_COUNT.load(Ordering::Relaxed);

    #[cfg(feature = "profiling")]
    {
        if let Some(guard) = PROFILER_GUARD.get() {
            if let Ok(report) = guard.report().build() {
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                let filename = format!("ingest_flamegraph_{}_events_{}.svg", count, timestamp);

                if let Ok(file) = std::fs::File::create(&filename) {
                    if let Err(e) = report.flamegraph(file) {
                        tracing::warn!(error = ?e, filename, "failed to write flamegraph");
                    } else {
                        tracing::info!(filename, event_count = count, "flamegraph saved");
                    }
                } else {
                    tracing::warn!(filename, "failed to create flamegraph file");
                }
            } else {
                tracing::warn!("failed to build profiling report");
            }
        }
    }

    tracing::debug!(event_count = count, "stopped ingest profiling");
}

/// Check if profiling is currently enabled.
pub fn is_profiling_enabled() -> bool {
    PROFILING_ENABLED.load(Ordering::Acquire)
}
