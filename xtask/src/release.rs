//! The `check-release-assets` task: a release carries a build for every
//! supported DuckDB version on every platform the workflows publish.
//!
//! A build that never happened leaves almost nothing behind to read. The
//! matrix is generated inside a reusable upstream workflow, and a leg it
//! declines to start produces no job, no check run and no annotation: the
//! run goes red with every visible job green. This is what names the
//! builds that are missing — and what a release is missing is a DuckDB
//! version that cannot load moraine on that platform at all, since a
//! C++-ABI extension is refused by any DuckDB but the one it names.

use std::fs;

use anyhow::{Context, bail, ensure};

use crate::duckdb::supported_duckdb_versions;

/// The platforms the extension workflows publish: extension-ci-tools'
/// distribution matrix, minus the entries that are opt-in there and the
/// ones `exclude_archs` names in `extension.yml` and `release.yml`.
const PUBLISHED_PLATFORMS: [&str; 4] = ["linux_amd64", "linux_arm64", "osx_amd64", "osx_arm64"];

/// The workflows that build the extension, each naming what it excludes.
/// Read only by the test holding `PUBLISHED_PLATFORMS` to them.
#[cfg(test)]
const BUILD_WORKFLOWS: [&str; 2] = [
    ".github/workflows/extension.yml",
    ".github/workflows/release.yml",
];

/// Fails unless `directory` holds one extension per supported DuckDB
/// version per published platform, naming every build that is missing.
pub fn check_release_assets(arguments: &[String]) -> anyhow::Result<()> {
    let Some(directory) = arguments.first() else {
        bail!(
            "usage: cargo xtask check-release-assets <directory>, e.g. `… check-release-assets dist`"
        );
    };

    let mut present = Vec::new();
    for entry in fs::read_dir(directory)
        .with_context(|| format!("reading the release directory {directory}"))?
    {
        let entry = entry.with_context(|| format!("reading an entry of {directory}"))?;
        present.push(entry.file_name().to_string_lossy().into_owned());
    }

    let versions = supported_duckdb_versions();
    let missing = missing_assets(&present, &versions);
    ensure!(
        missing.is_empty(),
        "{directory} holds {} of the {} builds a release needs; missing:\n  - {}\n\
         A matrix leg that never starts leaves no failed job, so check that every \
         version's build jobs exist before re-running.",
        expected_assets(&versions).len() - missing.len(),
        expected_assets(&versions).len(),
        missing.join("\n  - ")
    );

    println!(
        "ok: {} builds present — {} on {}",
        present.len(),
        versions.join(", "),
        PUBLISHED_PLATFORMS.join(", ")
    );
    Ok(())
}

/// Every asset a release must carry, in the name the publish step gives it.
fn expected_assets(versions: &[String]) -> Vec<String> {
    versions
        .iter()
        .flat_map(|version| {
            PUBLISHED_PLATFORMS
                .iter()
                .map(move |platform| format!("moraine.{version}.{platform}.duckdb_extension"))
        })
        .collect()
}

/// The expected assets that `present` does not name.
fn missing_assets(present: &[String], versions: &[String]) -> Vec<String> {
    expected_assets(versions)
        .into_iter()
        .filter(|asset| !present.iter().any(|candidate| candidate == asset))
        .collect()
}

/// The architectures a workflow's `exclude_archs` input names.
#[cfg(test)]
fn excluded_architectures(contents: &str) -> Vec<&str> {
    let marker = "exclude_archs: \"";
    let Some(start) = contents.find(marker).map(|index| index + marker.len()) else {
        return Vec::new();
    };
    let Some(rest) = contents.get(start..) else {
        return Vec::new();
    };
    let Some(end) = rest.find('"') else {
        return Vec::new();
    };
    rest[..end]
        .split(';')
        .filter(|arch| !arch.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn versions() -> Vec<String> {
        vec!["v1.5.5".to_owned(), "v1.5.4".to_owned()]
    }

    #[test]
    fn a_complete_set_is_accepted_and_a_gap_names_what_is_absent() {
        let complete = expected_assets(&versions());
        assert_eq!(complete.len(), 8);
        assert!(missing_assets(&complete, &versions()).is_empty());

        // The failure this exists for: one version built for Linux only.
        let partial: Vec<String> = complete
            .iter()
            .filter(|asset| !(asset.contains("v1.5.4") && asset.contains("osx")))
            .cloned()
            .collect();
        assert_eq!(
            missing_assets(&partial, &versions()),
            vec![
                "moraine.v1.5.4.osx_amd64.duckdb_extension".to_owned(),
                "moraine.v1.5.4.osx_arm64.duckdb_extension".to_owned(),
            ]
        );
    }

    /// Assets are matched by their whole name: another version's build is
    /// not one of these, however many of them there are.
    #[test]
    fn a_build_of_another_version_does_not_stand_in() {
        let present = vec![
            "moraine.v1.5.3.osx_arm64.duckdb_extension".to_owned(),
            "moraine.v1.5.5.osx_arm64.duckdb_extension".to_owned(),
        ];
        let missing = missing_assets(&present, &["v1.5.5".to_owned()]);
        assert_eq!(missing.len(), PUBLISHED_PLATFORMS.len() - 1);
        assert!(!missing.contains(&"moraine.v1.5.5.osx_arm64.duckdb_extension".to_owned()));
    }

    #[test]
    fn exclusions_are_read_out_of_the_workflow_input() {
        assert_eq!(
            excluded_architectures("      exclude_archs: \"wasm_mvp;windows_amd64\"\n"),
            vec!["wasm_mvp", "windows_amd64"]
        );
        assert!(excluded_architectures("no exclusions here").is_empty());
    }

    /// The platform list and the workflows' exclusions describe one set:
    /// a platform cannot be both published and excluded, and the two
    /// workflows must exclude the same thing or they publish different
    /// releases.
    #[test]
    fn the_published_platforms_are_the_ones_the_workflows_do_not_exclude() {
        let mut exclusions = Vec::new();
        for file in BUILD_WORKFLOWS {
            let contents = fs::read_to_string(crate::duckdb::workspace_root().join(file))
                .expect("reading a build workflow");
            let excluded: Vec<String> = excluded_architectures(&contents)
                .iter()
                .map(|arch| (*arch).to_owned())
                .collect();
            assert!(!excluded.is_empty(), "{file} names no exclude_archs");
            for platform in PUBLISHED_PLATFORMS {
                assert!(
                    !excluded.iter().any(|arch| arch == platform),
                    "{file} excludes `{platform}`, which check-release-assets requires"
                );
            }
            exclusions.push(excluded);
        }
        assert_eq!(
            exclusions[0], exclusions[1],
            "{} and {} exclude different architectures",
            BUILD_WORKFLOWS[0], BUILD_WORKFLOWS[1]
        );
    }
}
