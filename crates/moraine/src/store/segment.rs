//! The tag-byte segment extractor.
//!
//! Every store is created with this fixed-length one-byte extractor, so
//! each subspace is a SlateDB segment with its own LSM state. Fixed-length
//! extraction satisfies SlateDB's no-nesting (antichain) rule by
//! construction, and SlateDB persists the extractor identity in its
//! manifest, refusing a mismatched open.

use slatedb::{PrefixExtractor, PrefixTarget};

use crate::store::key::TAG_PREFIX_LEN;

/// Extracts the leading byte — the subspace discriminant — as the
/// segment prefix.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct TagSegmentExtractor;

/// Stable extractor identity, persisted by SlateDB in the manifest.
/// Changing it orphans every existing store; it is part of the on-disk
/// commitment.
pub(crate) const EXTRACTOR_NAME: &str = "moraine-tag-v1";

impl PrefixExtractor for TagSegmentExtractor {
    fn name(&self) -> &str {
        EXTRACTOR_NAME
    }

    fn prefix_len(&self, target: &PrefixTarget) -> Option<usize> {
        // Fixed-length one-byte extraction: the answer depends only on
        // the first byte existing, so `Point` and `Prefix` agree and
        // prefix-scan filtering stays enabled.
        let bytes = match target {
            PrefixTarget::Point(bytes) | PrefixTarget::Prefix(bytes) => bytes,
        };
        (!bytes.is_empty()).then_some(TAG_PREFIX_LEN)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn extracts_one_byte_for_any_nonempty_target() {
        let extractor = TagSegmentExtractor;
        for target in [
            PrefixTarget::Point(Bytes::from_static(&[0x02, 0x05, 1, 2, 3])),
            PrefixTarget::Prefix(Bytes::from_static(&[0x02])),
            PrefixTarget::Point(Bytes::from_static(&[0xff])),
        ] {
            assert_eq!(extractor.prefix_len(&target), Some(1));
        }
    }

    /// The extractor and the prefix builders derive the same segment
    /// prefix: extraction length equals the subspace-prefix length, for
    /// real encoded keys.
    #[test]
    fn extractor_agrees_with_subspace_prefix() {
        use crate::store::key::{self, Key};
        let key = Key::cur(key::EntityKey::Table { table_id: 7 });
        let encoded = key.encode();
        let len = TagSegmentExtractor
            .prefix_len(&PrefixTarget::Point(Bytes::from(encoded.clone())))
            .unwrap();
        let prefix = key::subspace_prefix(key::Subspace::Cur);
        assert_eq!(len, prefix.len());
        assert_eq!(&encoded[..len], prefix.as_slice());
    }

    #[test]
    fn empty_targets_have_no_prefix() {
        let extractor = TagSegmentExtractor;
        assert_eq!(
            extractor.prefix_len(&PrefixTarget::Point(Bytes::new())),
            None
        );
        assert_eq!(
            extractor.prefix_len(&PrefixTarget::Prefix(Bytes::new())),
            None
        );
    }

    #[test]
    fn name_is_the_pinned_identity() {
        assert_eq!(TagSegmentExtractor.name(), EXTRACTOR_NAME);
    }
}
