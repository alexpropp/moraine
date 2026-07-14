//! Bench report shaping: an aligned text table for stdout and a JSON
//! document for `target/bench/report.json`, both computed from raw
//! per-repeat phase timings.

use std::fmt::Write as _;

use super::timing::{median, spread};

/// Per-repeat seconds for one (workload, phase, backend) cell.
pub struct Row {
    pub workload: String,
    pub phase: String,
    pub backend: String,
    pub seconds: Vec<f64>,
}

/// A full bench run: the knobs it ran with and every timed cell.
pub struct Report {
    pub scale: String,
    pub repeat: usize,
    pub rows: Vec<Row>,
}

/// Renders `seconds` with an adaptive unit: µs below a millisecond, ms
/// below a second, seconds above.
pub fn format_duration(seconds: f64) -> String {
    if seconds < 0.001 {
        format!("{:.1}µs", seconds * 1_000_000.0)
    } else if seconds < 1.0 {
        format!("{:.1}ms", seconds * 1000.0)
    } else {
        format!("{seconds:.2}s")
    }
}

/// One rendered cell: `median (min…max)` over repeats, or `skipped` when
/// the backend didn't run this workload.
fn cell(report: &Report, workload: &str, phase: &str, backend: &str) -> anyhow::Result<String> {
    let Some(row) = report
        .rows
        .iter()
        .find(|row| row.workload == workload && row.phase == phase && row.backend == backend)
    else {
        return Ok("skipped".to_owned());
    };
    let mut samples = row.seconds.clone();
    let mid = median(&mut samples)?;
    let (min, max) = spread(&samples)?;
    Ok(format!(
        "{} ({}…{})",
        format_duration(mid),
        format_duration(min),
        format_duration(max)
    ))
}

/// Median for a cell, if that backend ran it.
fn cell_median(report: &Report, workload: &str, phase: &str, backend: &str) -> Option<f64> {
    let row = report
        .rows
        .iter()
        .find(|row| row.workload == workload && row.phase == phase && row.backend == backend)?;
    let mut samples = row.seconds.clone();
    median(&mut samples).ok()
}

/// Renders the aligned comparison table: one row per `workload/phase`,
/// one column per backend (`median (min…max)`), and — when moraine is a
/// column — one `<backend>/moraine` ratio column per other backend.
pub fn render_table(report: &Report, backends: &[String]) -> anyhow::Result<String> {
    let has_moraine = backends.iter().any(|backend| backend == "moraine");

    let mut header = vec!["workload/phase".to_owned()];
    header.extend(backends.iter().cloned());
    if has_moraine {
        for backend in backends {
            if backend != "moraine" {
                header.push(format!("{backend}/moraine"));
            }
        }
    }

    // Phases appear in run order and each (workload, phase) is unique per
    // backend, so deduplicating the first backend column's keys preserves
    // the report's row order.
    let mut keys: Vec<(String, String)> = Vec::new();
    for row in &report.rows {
        let key = (row.workload.clone(), row.phase.clone());
        if !keys.contains(&key) {
            keys.push(key);
        }
    }

    let mut table = vec![header];
    for (workload, phase) in &keys {
        let mut line = vec![format!("{workload}/{phase}")];
        for backend in backends {
            line.push(cell(report, workload, phase, backend)?);
        }
        if has_moraine {
            let moraine = cell_median(report, workload, phase, "moraine");
            for backend in backends {
                if backend == "moraine" {
                    continue;
                }
                let other = cell_median(report, workload, phase, backend);
                line.push(match (moraine, other) {
                    (Some(base), Some(other)) if base > 0.0 => {
                        format!("{:.2}×", other / base)
                    }
                    _ => "—".to_owned(),
                });
            }
        }
        table.push(line);
    }

    let columns = table[0].len();
    let mut widths = vec![0usize; columns];
    for line in &table {
        for (index, text) in line.iter().enumerate() {
            widths[index] = widths[index].max(text.chars().count());
        }
    }

    let mut rendered = String::new();
    for line in &table {
        for (index, text) in line.iter().enumerate() {
            if index > 0 {
                rendered.push_str("  ");
            }
            rendered.push_str(text);
            let pad = widths[index] - text.chars().count();
            if index + 1 < columns {
                rendered.push_str(&" ".repeat(pad));
            }
        }
        rendered.push('\n');
    }
    Ok(rendered)
}

fn json_escape(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\t' => escaped.push_str("\\t"),
            '\r' => escaped.push_str("\\r"),
            control if (control as u32) < 0x20 => {
                // write! to a String cannot fail.
                let _ = write!(escaped, "\\u{:04x}", control as u32);
            }
            other => escaped.push(other),
        }
    }
    escaped
}

/// Serializes the report as JSON for diffing across checkouts.
pub fn render_json(report: &Report) -> String {
    let mut results = String::new();
    for (index, row) in report.rows.iter().enumerate() {
        if index > 0 {
            results.push_str(",\n");
        }
        let seconds: Vec<String> = row.seconds.iter().map(ToString::to_string).collect();
        // write! to a String cannot fail.
        let _ = write!(
            results,
            "    {{\"workload\": \"{}\", \"phase\": \"{}\", \"backend\": \"{}\", \
             \"seconds\": [{}]}}",
            json_escape(&row.workload),
            json_escape(&row.phase),
            json_escape(&row.backend),
            seconds.join(", ")
        );
    }
    format!(
        "{{\n  \"scale\": \"{}\",\n  \"repeat\": {},\n  \"results\": [\n{}\n  ]\n}}\n",
        json_escape(&report.scale),
        report.repeat,
        results
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_units_adapt() {
        assert_eq!(format_duration(0.000_000_5), "0.5µs");
        assert_eq!(format_duration(0.0123), "12.3ms");
        assert_eq!(format_duration(1.5), "1.50s");
    }

    fn sample_report() -> Report {
        Report {
            scale: "small".to_owned(),
            repeat: 2,
            rows: vec![
                Row {
                    workload: "small_commits".to_owned(),
                    phase: "inserts".to_owned(),
                    backend: "moraine".to_owned(),
                    seconds: vec![0.010, 0.020],
                },
                Row {
                    workload: "small_commits".to_owned(),
                    phase: "inserts".to_owned(),
                    backend: "duckdb".to_owned(),
                    seconds: vec![0.030, 0.030],
                },
            ],
        }
    }

    #[test]
    fn table_has_backend_columns_ratio_and_skipped_cells() {
        let backends = vec![
            "moraine".to_owned(),
            "duckdb".to_owned(),
            "postgres".to_owned(),
        ];
        let table = render_table(&sample_report(), &backends).unwrap();
        let lines: Vec<&str> = table.lines().collect();
        assert!(lines[0].contains("workload/phase"));
        assert!(lines[0].contains("duckdb/moraine"));
        assert!(lines[0].contains("postgres/moraine"));
        assert!(lines[1].starts_with("small_commits/inserts"));
        assert!(lines[1].contains("15.0ms (10.0ms…20.0ms)"));
        assert!(lines[1].contains("30.0ms (30.0ms…30.0ms)"));
        assert!(lines[1].contains("2.00×"));
        assert!(lines[1].contains("skipped"));
    }

    #[test]
    fn json_carries_metadata_and_per_repeat_seconds() {
        let json = render_json(&sample_report());
        assert!(json.contains("\"scale\": \"small\""));
        assert!(json.contains("\"repeat\": 2"));
        assert!(json.contains(
            "{\"workload\": \"small_commits\", \"phase\": \"inserts\", \
             \"backend\": \"moraine\", \"seconds\": [0.01, 0.02]}"
        ));
    }
}
