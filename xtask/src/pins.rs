//! The `check-pins` task: every place that names a DuckDB version must
//! name the one `.github/duckdb-versions` calls primary.
//!
//! A C++-ABI extension is refused by any DuckDB whose version string
//! differs from the one in its metadata footer, so the pin is not a
//! preference — it decides which engine can load the artifact at all. It
//! is also named in six places that cannot be derived from one another
//! (two git submodules, a Rust constant, two workflow files, a README
//! table), and a bump that misses one produces an artifact that builds,
//! passes CI, and then fails to load. This makes that a build failure
//! instead.

use std::process::Command;

use anyhow::{Context, bail, ensure};

use crate::duckdb::{
    duckdb_pin, primary_submodule_pins, supported_duckdb_versions, workspace_root,
};

/// Checks every pinned DuckDB version reference against the primary entry
/// in `.github/duckdb-versions`, reporting all mismatches at once rather
/// than the first.
pub fn check_pins() -> anyhow::Result<()> {
    let pin = duckdb_pin();
    let supported = supported_duckdb_versions();
    ensure!(
        !pin.is_empty(),
        ".github/duckdb-versions lists no DuckDB version"
    );
    println!("primary pin: {pin}");
    println!("supported:   {}", supported.join(", "));

    let mut problems = Vec::new();
    // Compared by commit rather than tag: a submodule is routinely a
    // shallow clone with no tags fetched, where `git describe` has nothing
    // to say.
    let pinned_submodules = primary_submodule_pins();
    ensure!(
        pinned_submodules.len() == 2,
        "the primary entry in .github/duckdb-versions must pin both submodules \
         (`duckdb=<commit> extension-ci-tools=<commit>`); it pins {}",
        pinned_submodules.len()
    );

    for (path, expected) in pinned_submodules {
        match submodule_commit(&path) {
            Ok(commit) if commit == expected => {}
            Ok(commit) => problems.push(format!(
                "the `{path}` submodule is at `{commit}`, but .github/duckdb-versions \
                 pins `{expected}` for {pin} — `git -C {path} checkout {expected}`"
            )),
            Err(error) => problems.push(format!(
                "could not read the `{path}` submodule's commit: {error} \
                 (run `git submodule update --init`)"
            )),
        }
    }

    // Two levels of check, because the files differ in kind. In a workflow
    // every DuckDB version is a pin, so an unlisted one is a mistake; in
    // prose a version is often an example ("a v1.5.3 user cannot load a
    // v1.5.4 build"), so only the primary's presence is required.
    for file in [
        ".github/workflows/extension.yml",
        ".github/workflows/release.yml",
    ] {
        let contents = read(file)?;
        if !contents.contains(pin) {
            problems.push(format!("{file} never names the primary pin `{pin}`"));
        }
        for stale in stale_versions(&contents, &supported) {
            problems.push(format!(
                "{file} pins DuckDB `{stale}`, which is not in .github/duckdb-versions"
            ));
        }
    }

    for file in ["crates/moraine-duckdb/README.md"] {
        if !read(file)?.contains(pin) {
            problems.push(format!(
                "{file}'s pin table never names the primary pin `{pin}`"
            ));
        }
    }

    ensure!(
        problems.is_empty(),
        "the DuckDB pin is inconsistent:\n  - {}",
        problems.join("\n  - ")
    );
    println!("ok: every DuckDB version reference matches .github/duckdb-versions");
    Ok(())
}

/// A repo-relative file, read whole.
fn read(file: &str) -> anyhow::Result<String> {
    std::fs::read_to_string(workspace_root().join(file)).with_context(|| format!("reading {file}"))
}

/// The commit the submodule at `path` is checked out on.
fn submodule_commit(path: &str) -> anyhow::Result<String> {
    let output = Command::new("git")
        .args(["-C", path, "rev-parse", "HEAD"])
        .current_dir(workspace_root())
        .output()
        .with_context(|| format!("running git rev-parse in {path}"))?;
    if !output.status.success() {
        bail!(
            "git rev-parse exited with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Every `vN.N.N`-shaped DuckDB version in `contents` that the manifest
/// does not list. Scoped to the `v1.` prefix so a Rust crate version, an
/// action's `@v4`, or a DuckLake branch name is not mistaken for one.
fn stale_versions(contents: &str, supported: &[String]) -> Vec<String> {
    let mut found: Vec<String> = Vec::new();
    for (index, _) in contents.match_indices("v1.") {
        let rest = &contents[index..];
        let end = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.' || c == 'v'))
            .unwrap_or(rest.len());
        let candidate = rest[..end].trim_end_matches('.');
        // Three components: a DuckDB release, not a `v1.5` series name.
        if candidate.split('.').count() != 3 {
            continue;
        }
        if !supported.iter().any(|version| version == candidate)
            && !found.iter().any(|seen| seen == candidate)
        {
            found.push(candidate.to_string());
        }
    }
    found
}

/// The versions manifest as the JSON array the release workflows feed to
/// their build matrix, printed to stdout.
pub fn print_version_matrix() {
    let entries: Vec<String> = supported_duckdb_versions()
        .iter()
        .map(|version| format!("\"{version}\""))
        .collect();
    println!("[{}]", entries.join(","));
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The manifest parses to at least one version, and the primary is the
    /// first of them.
    #[test]
    fn the_manifest_parses_and_leads_with_the_primary_pin() {
        let supported = supported_duckdb_versions();
        assert!(!supported.is_empty(), "the manifest lists no versions");
        assert_eq!(supported[0], duckdb_pin());
        for version in &supported {
            assert!(
                version.starts_with('v') && version.split('.').count() == 3,
                "`{version}` is not a `vMAJOR.MINOR.PATCH` DuckDB release"
            );
        }
    }

    #[test]
    fn stale_versions_ignores_series_names_and_known_releases() {
        let supported = vec!["v1.5.4".to_string()];
        assert!(stale_versions("duckdb_version: v1.5.4", &supported).is_empty());
        // A DuckLake branch name is a series, not a release.
        assert!(stale_versions("branch v1.5-variegata", &supported).is_empty());
        assert_eq!(
            stale_versions("duckdb_version: v1.5.3", &supported),
            vec!["v1.5.3".to_string()]
        );
    }
}
