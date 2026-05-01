// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! One-shot run summary reporter (Windows-only).
//!
//! Captures cumulative process CPU time and the `log_records_uploaded` counter
//! at process start, and prints a single summary block when
//! [`RunReporter::print_summary`] is called. Designed for A/B comparisons of a
//! known-duration run.
//!
//! - CPU time comes from `cpu_time::ProcessTime` (calls `GetProcessTimes` on
//!   Windows). It only counts CPU consumed by `df_engine.exe`.
//! - Memory counters come from `K32GetProcessMemoryInfo` with
//!   `PROCESS_MEMORY_COUNTERS_EX2` (Windows 10 1903+).
//! - `log_records_uploaded` is fetched from the admin endpoint at start and at
//!   end so the same source feeds both A and B (it's the same counter the
//!   dashboard shows).

#![cfg(windows)]

use cpu_time::ProcessTime;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Snapshot of cumulative counters at a single instant.
struct Snapshot {
    wall: Instant,
    cpu: ProcessTime,
    records: Option<u64>,
}

#[derive(Default, Clone, Copy)]
struct MemorySnapshot {
    /// Total Working Set in bytes (matches `dashboard.engine.memory_rss` and
    /// PowerShell `WorkingSet64`). Sampled at end-of-run.
    working_set: u64,
    /// Private Working Set in bytes (matches Task Manager's "Memory" column
    /// and WMI `WorkingSetPrivate`). Sampled at end-of-run.
    private_working_set: u64,
    /// Peak Working Set in bytes since process start. Tracked by the OS — this
    /// is the high-water mark, regardless of when sampling occurs.
    peak_working_set: u64,
}

/// Reporter that captures a start snapshot at construction time and prints a
/// summary on demand. Safe to call `print_summary` from multiple threads — only
/// the first call prints.
pub struct RunReporter {
    start: Snapshot,
    admin_bind: String,
    printed: AtomicBool,
}

impl RunReporter {
    /// Capture the start-of-run snapshot. `admin_bind` is used to fetch the
    /// `log_records_uploaded` counter at start (and again at end of run).
    ///
    /// At process start the admin HTTP server hasn't bound yet, so the start
    /// fetch normally fails — but the records counter is 0 by definition at
    /// that moment, so we treat a failed start fetch as 0. The end fetch is
    /// the meaningful one.
    #[must_use]
    pub fn new(admin_bind: String) -> Self {
        let start = Snapshot {
            wall: Instant::now(),
            cpu: ProcessTime::now(),
            records: Some(fetch_records_uploaded(&admin_bind).unwrap_or(0)),
        };
        Self {
            start,
            admin_bind,
            printed: AtomicBool::new(false),
        }
    }

    /// Print the summary block. Idempotent — subsequent calls are no-ops.
    pub fn print_summary(&self) {
        if self.printed.swap(true, Ordering::SeqCst) {
            return;
        }
        let end = Snapshot {
            wall: Instant::now(),
            cpu: ProcessTime::now(),
            records: fetch_records_uploaded(&self.admin_bind),
        };
        let mem = read_memory_counters();

        let wall_secs = end.wall.duration_since(self.start.wall).as_secs_f64();
        let cpu_secs = end.cpu.duration_since(self.start.cpu).as_secs_f64();
        let cores = if wall_secs > 0.0 {
            cpu_secs / wall_secs
        } else {
            0.0
        };

        println!();
        println!("=== Run summary ===");
        println!("  duration             : {wall_secs:>10.2} s");
        println!(
            "  cpu_time             : {cpu_secs:>10.3} s ({pct:>5.2}% of one core)",
            pct = cores * 100.0
        );
        println!(
            "  rss (working set)    : {:>10.1} MiB",
            bytes_to_mib(mem.working_set)
        );
        println!(
            "  peak working set     : {:>10.1} MiB",
            bytes_to_mib(mem.peak_working_set)
        );
        println!(
            "  private working set  : {:>10.1} MiB",
            bytes_to_mib(mem.private_working_set)
        );

        match (self.start.records, end.records) {
            (Some(s), Some(e)) if e >= s => {
                let drec = e - s;
                let rate = if wall_secs > 0.0 {
                    drec as f64 / wall_secs
                } else {
                    0.0
                };
                let ns_per = if drec > 0 {
                    cpu_secs * 1.0e9 / drec as f64
                } else {
                    0.0
                };
                println!("  log_records_uploaded : {drec:>10} ({rate:>9.0}/s)");
                println!("  cpu_time per record  : {ns_per:>10.0} ns/log");
            }
            _ => {
                println!("  log_records_uploaded : <unavailable>");
            }
        }
    }
}

fn bytes_to_mib(b: u64) -> f64 {
    (b as f64) / (1024.0 * 1024.0)
}

// -----------------------------------------------------------------------------
// Memory: K32GetProcessMemoryInfo with PROCESS_MEMORY_COUNTERS_EX2.
// -----------------------------------------------------------------------------

#[allow(non_snake_case)]
#[repr(C)]
struct ProcessMemoryCountersEx2 {
    cb: u32,
    PageFaultCount: u32,
    PeakWorkingSetSize: usize,
    WorkingSetSize: usize,
    QuotaPeakPagedPoolUsage: usize,
    QuotaPagedPoolUsage: usize,
    QuotaPeakNonPagedPoolUsage: usize,
    QuotaNonPagedPoolUsage: usize,
    PagefileUsage: usize,
    PeakPagefileUsage: usize,
    PrivateUsage: usize,
    PrivateWorkingSetSize: usize,
    SharedCommitUsage: u64,
}

unsafe extern "system" {
    fn GetCurrentProcess() -> *mut core::ffi::c_void;
    fn K32GetProcessMemoryInfo(
        h_process: *mut core::ffi::c_void,
        ppsmem_counters: *mut ProcessMemoryCountersEx2,
        cb: u32,
    ) -> i32;
}

fn read_memory_counters() -> MemorySnapshot {
    // SAFETY: zero-initialized struct of POD types is valid; cb tells the OS
    // which size we expect, and we own the pointer for the duration of the
    // call. Falling back to zeros on failure is safe and only affects display.
    unsafe {
        let mut info = ProcessMemoryCountersEx2 {
            cb: core::mem::size_of::<ProcessMemoryCountersEx2>() as u32,
            PageFaultCount: 0,
            PeakWorkingSetSize: 0,
            WorkingSetSize: 0,
            QuotaPeakPagedPoolUsage: 0,
            QuotaPagedPoolUsage: 0,
            QuotaPeakNonPagedPoolUsage: 0,
            QuotaNonPagedPoolUsage: 0,
            PagefileUsage: 0,
            PeakPagefileUsage: 0,
            PrivateUsage: 0,
            PrivateWorkingSetSize: 0,
            SharedCommitUsage: 0,
        };
        let ok = K32GetProcessMemoryInfo(GetCurrentProcess(), &mut info, info.cb);
        if ok == 0 {
            MemorySnapshot::default()
        } else {
            MemorySnapshot {
                working_set: info.WorkingSetSize as u64,
                private_working_set: info.PrivateWorkingSetSize as u64,
                peak_working_set: info.PeakWorkingSetSize as u64,
            }
        }
    }
}

// -----------------------------------------------------------------------------
// Tiny HTTP/1.0 GET to the admin endpoint to fetch a single Prometheus counter.
// -----------------------------------------------------------------------------

fn fetch_records_uploaded(addr: &str) -> Option<u64> {
    let body = http_get(addr, "/api/v1/telemetry/metrics/aggregate")?;
    parse_prom_counter(&body, "log_records_uploaded")
}

fn http_get(addr: &str, path: &str) -> Option<String> {
    let mut s = TcpStream::connect(addr).ok()?;
    s.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
    s.set_write_timeout(Some(Duration::from_secs(2))).ok()?;
    let req = format!(
        "GET {path} HTTP/1.0\r\nHost: {addr}\r\nUser-Agent: df_engine-stats\r\nConnection: close\r\nAccept: text/plain\r\n\r\n"
    );
    s.write_all(req.as_bytes()).ok()?;
    let mut buf = String::new();
    s.read_to_string(&mut buf).ok()?;
    let body_start = buf.find("\r\n\r\n")? + 4;
    Some(buf[body_start..].to_string())
}

/// Parse a Prometheus-style line like
/// `log_records_uploaded{set="otap.exporter.geneva"} 31000 1777581738705`
/// and return the numeric value. Sums across all label sets if the metric
/// appears multiple times.
fn parse_prom_counter(text: &str, name: &str) -> Option<u64> {
    let mut total: u64 = 0;
    let mut found = false;
    for line in text.lines() {
        if line.starts_with('#') {
            continue;
        }
        // Match "name{...}" or "name " at the start.
        let Some(after) = line.strip_prefix(name) else {
            continue;
        };
        if !after.starts_with('{') && !after.starts_with(' ') {
            continue;
        }
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 2 {
            continue;
        }
        // Format: NAME{labels} VALUE [TIMESTAMP]
        // VALUE is the second-to-last token when timestamp present, or last otherwise.
        let value_tok = if parts.len() >= 3 {
            parts[parts.len() - 2]
        } else {
            parts[parts.len() - 1]
        };
        if let Ok(v) = value_tok.parse::<u64>() {
            total = total.saturating_add(v);
            found = true;
        } else if let Ok(v) = value_tok.parse::<f64>() {
            total = total.saturating_add(v as u64);
            found = true;
        }
    }
    if found { Some(total) } else { None }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_counter_with_labels_and_timestamp() {
        let txt = "# HELP log_records_uploaded foo\n\
                   # TYPE log_records_uploaded counter\n\
                   log_records_uploaded{set=\"otap.exporter.geneva\"} 31000 1777581738705\n";
        assert_eq!(parse_prom_counter(txt, "log_records_uploaded"), Some(31000));
    }

    #[test]
    fn parse_counter_sums_multiple_label_sets() {
        let txt = "log_records_uploaded{set=\"a\"} 100\n\
                   log_records_uploaded{set=\"b\"} 50 1234\n";
        assert_eq!(parse_prom_counter(txt, "log_records_uploaded"), Some(150));
    }

    #[test]
    fn parse_counter_missing_returns_none() {
        let txt = "other_metric{x=\"y\"} 1\n";
        assert_eq!(parse_prom_counter(txt, "log_records_uploaded"), None);
    }

    #[test]
    fn read_memory_returns_nonzero_working_set() {
        let m = read_memory_counters();
        assert!(m.working_set > 0, "working set should be > 0");
    }
}
