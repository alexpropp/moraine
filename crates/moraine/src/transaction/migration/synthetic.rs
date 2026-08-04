//! Synthetic migration units, installed into the driver's registry by a
//! fault-injection build.
//!
//! Every format this binary ships is additive, so the real registry is empty
//! and no public path can put a store mid-migration. These units are what a
//! test drives instead — installed into the registry the shipped planner
//! reads, so the coverage is of that planner and not a parallel copy of it.

use futures::{FutureExt, future::BoxFuture};
use slatedb::DbTransaction;

use crate::{
    error::{Error, Result},
    store::{
        handle::ReadHandle,
        key::{EntityKey, Key},
        proto,
        read::{EntityRecord, scan_current_entities},
        value,
    },
    transaction::{
        commit::{FORMAT_VERSION, MAX_FORMAT_VERSION},
        migration::{MigrationUnit, StepOutcome},
    },
};

/// The option scope kind the rewritten records start under, and the one the
/// rewrite moves them to. Moving a record between scope kinds changes where
/// its key sorts, which is exactly what makes a change structural rather than
/// a value edit.
///
/// They are `OptionScope::Schema` and `OptionScope::Table` respectively, so a
/// caller with only the public API can plant the records and read them back
/// where the rewrite left them.
pub(crate) const SOURCE_SCOPE: u64 = 1;
pub(crate) const TARGET_SCOPE: u64 = 2;

/// A rewriting migration in miniature: it walks the option records under
/// [`SOURCE_SCOPE`] in key order and moves each to [`TARGET_SCOPE`], one
/// record per batch, writing the new key before deleting the old. Shaped
/// exactly like a real unit — idempotent, and resumable from the cursor it
/// returns.
fn move_scope_step<'a>(
    tx: &'a DbTransaction,
    cursor: &'a [u8],
) -> BoxFuture<'a, Result<StepOutcome>> {
    async move {
        let start = decode_cursor(cursor)?;

        let Some((scope_id, value)) = next_source_record(tx, start).await? else {
            return Ok(None);
        };

        // New key first, old key second. Within one batch the pair is
        // atomic; the ordering is the discipline a unit that ever split its
        // work across batches would need, so it is written that way here.
        tx.put(
            Key::current(EntityKey::Option {
                scope_kind: TARGET_SCOPE,
                scope_id,
            })
            .encode(),
            value::encode_value(&value),
        )
        .map_err(Error::from)?;
        tx.delete(
            Key::current(EntityKey::Option {
                scope_kind: SOURCE_SCOPE,
                scope_id,
            })
            .encode(),
        )
        .map_err(Error::from)?;

        Ok(Some(scope_id.to_be_bytes().to_vec()))
    }
    .boxed()
}

/// The scope id a cursor names, or `None` at the start of the walk.
fn decode_cursor(cursor: &[u8]) -> Result<Option<u64>> {
    if cursor.is_empty() {
        return Ok(None);
    }
    let bytes: [u8; 8] = cursor
        .try_into()
        .map_err(|_| Error::Corruption("migration cursor is not a scope id".to_string()))?;
    Ok(Some(u64::from_be_bytes(bytes)))
}

/// The first record still under [`SOURCE_SCOPE`] past `start`. Records
/// already moved sort under the target scope and are never returned, which
/// is what makes re-running an applied step a no-op.
async fn next_source_record(
    tx: &DbTransaction,
    start: Option<u64>,
) -> Result<Option<(u64, proto::OptionScopeValue)>> {
    let mut found: Vec<(u64, proto::OptionScopeValue)> = scan_current_entities(ReadHandle::Tx(tx))
        .await?
        .into_iter()
        .filter_map(|record| match record {
            EntityRecord::Option {
                scope_kind,
                scope_id,
                value,
            } if scope_kind == SOURCE_SCOPE => Some((scope_id, value)),
            _ => None,
        })
        .filter(|(scope_id, _)| start.is_none_or(|start| *scope_id > start))
        .collect();
    found.sort_by_key(|(scope_id, _)| *scope_id);

    Ok(found.into_iter().next())
}

/// The rewriting unit. Its target is the newest format this binary reads, so
/// a store it migrates still attaches — the end state a driven crash case
/// has to be able to observe through the public API.
const MOVE_SCOPE: MigrationUnit = MigrationUnit {
    name: "move-option-scope",
    from_format: FORMAT_VERSION,
    to_format: MAX_FORMAT_VERSION,
    step: move_scope_step,
};

/// A second link, so a multi-version jump has something to compose. It walks
/// nothing: its whole job is to prove the driver runs a chain, each link with
/// its own start, steps, and finish.
fn no_work_step<'a>(
    _tx: &'a DbTransaction,
    _cursor: &'a [u8],
) -> BoxFuture<'a, Result<StepOutcome>> {
    async move { Ok(None) }.boxed()
}

/// Past the newest format this binary reads, so a store carried all the way
/// through the chain is one no attach will open — which is what makes the
/// jump's own end state distinguishable from the rewriting unit's.
const SECOND_LINK: MigrationUnit = MigrationUnit {
    name: "second-link",
    from_format: MAX_FORMAT_VERSION,
    to_format: MAX_FORMAT_VERSION + 1,
    step: no_work_step,
};

/// The registry [`SyntheticMigration::MoveOptionScope`] installs.
///
/// [`SyntheticMigration::MoveOptionScope`]: crate::SyntheticMigration::MoveOptionScope
pub(crate) const REWRITE: &[&MigrationUnit] = &[&MOVE_SCOPE];

/// The registry [`SyntheticMigration::MoveOptionScopeThenLink`] installs.
///
/// [`SyntheticMigration::MoveOptionScopeThenLink`]: crate::SyntheticMigration::MoveOptionScopeThenLink
pub(crate) const REWRITE_THEN_LINK: &[&MigrationUnit] = &[&MOVE_SCOPE, &SECOND_LINK];
