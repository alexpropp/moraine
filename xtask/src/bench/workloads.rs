//! Workload definitions: identical statement streams every backend runs.
//! A workload is an optional untimed seeding session plus a measured
//! session; the session runner prepends the warm-up and `ATTACH`
//! statements, so definitions here start from an attached `lake`.

use anyhow::bail;

use super::timing::Statement;

/// Knobs a scale name expands to.
pub struct Scale {
    pub name: &'static str,
    pub bulk_rows: u64,
    pub small_commits: u64,
    pub tables: u64,
}

impl Scale {
    pub fn parse(name: &str) -> anyhow::Result<Self> {
        let (name, bulk_rows, small_commits, tables) = match name {
            "small" => ("small", 100_000, 20, 10),
            "medium" => ("medium", 1_000_000, 50, 25),
            "large" => ("large", 10_000_000, 200, 100),
            other => bail!("unknown scale `{other}`; valid: small, medium, large"),
        };
        Ok(Self {
            name,
            bulk_rows,
            small_commits,
            tables,
        })
    }
}

/// One benchmark workload: `seed` runs in its own untimed session first
/// (empty for write workloads), then `measured` runs in a fresh session
/// whose per-phase timings are reported.
pub struct Workload {
    pub name: &'static str,
    pub seed: Vec<String>,
    pub measured: Vec<Statement>,
}

fn bulk_create_and_insert(rows: u64) -> [String; 2] {
    [
        "CREATE TABLE lake.main.items (id BIGINT, amount DOUBLE);".to_owned(),
        format!(
            "INSERT INTO lake.main.items \
             SELECT i::BIGINT, (i * 1.5)::DOUBLE FROM range({rows}) t(i);"
        ),
    ]
}

fn small_commit_inserts(count: u64) -> impl Iterator<Item = String> {
    (0..count)
        .map(|index| format!("INSERT INTO lake.main.events VALUES ({index}, 'event-{index}');"))
}

/// Every workload at `scale`, in report order.
pub fn workloads(scale: &Scale) -> Vec<Workload> {
    let [create_items, insert_items] = bulk_create_and_insert(scale.bulk_rows);

    let bulk_load = Workload {
        name: "bulk_load",
        seed: Vec::new(),
        measured: vec![
            Statement::measured("create_table", create_items.clone()),
            Statement::measured("insert", insert_items.clone()),
        ],
    };

    // Each autocommitted single-row insert is one catalog commit; the
    // sum across all of them is the headline catalog-latency phase.
    let small_commits = Workload {
        name: "small_commits",
        seed: Vec::new(),
        measured: std::iter::once(Statement::setup(
            "CREATE TABLE lake.main.events (id BIGINT, note VARCHAR);",
        ))
        .chain(
            small_commit_inserts(scale.small_commits)
                .map(|sql| Statement::measured("inserts", sql)),
        )
        .collect(),
    };

    let many_tables = Workload {
        name: "many_tables",
        seed: Vec::new(),
        measured: (0..scale.tables)
            .map(|index| {
                Statement::measured(
                    "creates",
                    format!("CREATE TABLE lake.main.table_{index} (id BIGINT, name VARCHAR);"),
                )
            })
            .collect(),
    };

    let scan = Workload {
        name: "scan",
        seed: vec![create_items, insert_items],
        measured: vec![
            Statement::measured("full_scan", "SELECT sum(amount) FROM lake.main.items;"),
            Statement::measured(
                "filtered_scan",
                format!(
                    "SELECT count(*) FROM lake.main.items WHERE id = {};",
                    scale.bulk_rows / 2
                ),
            ),
            Statement::measured(
                "time_travel",
                "SELECT count(*) FROM lake.main.items AT (VERSION => 1);",
            ),
            Statement::measured(
                "snapshots",
                "SELECT count(*) FROM ducklake_snapshots('lake');",
            ),
        ],
    };

    let maintenance = Workload {
        name: "maintenance",
        seed: std::iter::once(
            "CREATE TABLE lake.main.events (id BIGINT, note VARCHAR);".to_owned(),
        )
        .chain(small_commit_inserts(scale.small_commits))
        .collect(),
        measured: vec![
            Statement::measured("merge", "CALL ducklake_merge_adjacent_files('lake');"),
            Statement::measured(
                "expire",
                "CALL ducklake_expire_snapshots('lake', older_than => now());",
            ),
            Statement::measured(
                "cleanup",
                "CALL ducklake_cleanup_old_files('lake', cleanup_all => true);",
            ),
        ],
    };

    vec![bulk_load, small_commits, many_tables, scan, maintenance]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn small() -> Scale {
        Scale::parse("small").unwrap()
    }

    #[test]
    fn scale_parses_known_names_and_rejects_others() {
        assert_eq!(small().bulk_rows, 100_000);
        assert_eq!(Scale::parse("large").unwrap().tables, 100);
        assert!(Scale::parse("huge").is_err());
    }

    #[test]
    fn workload_names_and_phases_match_the_report_contract() {
        let all = workloads(&small());
        let names: Vec<&str> = all.iter().map(|workload| workload.name).collect();
        assert_eq!(
            names,
            [
                "bulk_load",
                "small_commits",
                "many_tables",
                "scan",
                "maintenance"
            ]
        );

        let phases = |name: &str| -> Vec<&'static str> {
            let workload = all.iter().find(|workload| workload.name == name).unwrap();
            let mut seen = Vec::new();
            for statement in &workload.measured {
                if let Some(phase) = statement.phase
                    && !seen.contains(&phase)
                {
                    seen.push(phase);
                }
            }
            seen
        };
        assert_eq!(phases("bulk_load"), ["create_table", "insert"]);
        assert_eq!(phases("small_commits"), ["inserts"]);
        assert_eq!(phases("many_tables"), ["creates"]);
        assert_eq!(
            phases("scan"),
            ["full_scan", "filtered_scan", "time_travel", "snapshots"]
        );
        assert_eq!(phases("maintenance"), ["merge", "expire", "cleanup"]);
    }

    #[test]
    fn small_commits_counts_match_scale() {
        let all = workloads(&small());
        let commits = all
            .iter()
            .find(|workload| workload.name == "small_commits")
            .unwrap();
        let inserts = commits
            .measured
            .iter()
            .filter(|statement| statement.phase == Some("inserts"))
            .count();
        assert_eq!(inserts as u64, small().small_commits);
    }

    #[test]
    fn read_workloads_seed_the_tables_they_query() {
        let all = workloads(&small());
        let scan = all.iter().find(|workload| workload.name == "scan").unwrap();
        assert!(scan.seed[0].contains("CREATE TABLE lake.main.items"));
        assert!(scan.measured[0].sql.contains("FROM lake.main.items"));

        let maintenance = all
            .iter()
            .find(|workload| workload.name == "maintenance")
            .unwrap();
        assert!(maintenance.seed[0].contains("CREATE TABLE lake.main.events"));
        assert!(maintenance.seed.len() as u64 == 1 + small().small_commits);
    }
}
