use crate::PayloadTotals;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// What a part holds.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum PartKind {
    /// Spans, in the files of a span part. The default, and what a manifest
    /// written before annotations existed means.
    #[default]
    Spans,
    /// Annotations (scores, labels, comments) in one `annotations.nwa` file.
    Annotations,
}

/// Metadata describing one immutable part, as recorded in the manifest and
/// next to the part's files.
///
/// For a part of kind [`PartKind::Annotations`] the fields mean: `rows` is the
/// number of records, `traces` the number of distinct trace ids,
/// `min_start_ns` and `max_end_ns` the smallest and largest `time_ns` of the
/// records, `window_start_ns` and `window_ns` the window the part belongs to,
/// `files` its one file, `payload` zero totals, and `format_version` the
/// annotation format version.
///
/// Field names are part of the on-disk format and must not change. Unknown
/// fields are ignored when reading, so a reader built before a field was added
/// still understands newer files.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartMeta {
    /// Unique, increasing identifier of the part.
    pub part_id: u64,
    /// Compaction level; 0 is a fresh flush.
    pub level: u8,
    /// Start of the time window the part covers, Unix nanoseconds.
    pub window_start_ns: i64,
    /// Width of the window, nanoseconds.
    pub window_ns: i64,
    /// Number of span rows (records, for an annotation part).
    pub rows: u64,
    /// Number of distinct traces.
    pub traces: u64,
    /// Smallest span start in the part, Unix nanoseconds.
    pub min_start_ns: i64,
    /// Largest span end in the part, Unix nanoseconds.
    pub max_end_ns: i64,
    /// File name to size in bytes, for every file of the part.
    pub files: BTreeMap<String, u64>,
    /// Creation time, Unix milliseconds.
    pub created_unix_ms: u64,
    /// Version of the part's file format.
    pub format_version: u32,
    /// What the part's piece store holds. Absent in manifests written before
    /// the store existed, where it reads as all zeros.
    #[serde(default)]
    pub payload: PayloadTotals,
    /// What the part holds. Absent in manifests written before annotations
    /// existed, where it reads as spans; an unknown value is an error.
    #[serde(default)]
    pub kind: PartKind,
}

impl PartMeta {
    /// Bytes the part takes on disk: the sum of its files. Compaction sizes
    /// its choices by this.
    pub fn size_bytes(&self) -> u64 {
        self.files.values().fold(0u64, |a, b| a.saturating_add(*b))
    }
}

/// Directory name of a part: the id zero-padded to 20 digits, the width of
/// `u64::MAX`, so names sort in id order.
pub fn part_dir_name(part_id: u64) -> String {
    format!("{part_id:020}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> PartMeta {
        PartMeta {
            part_id: 42,
            level: 1,
            window_start_ns: -900_000_000_000,
            window_ns: 900_000_000_000,
            rows: 10,
            traces: 3,
            min_start_ns: -5,
            max_end_ns: 99,
            files: BTreeMap::from([
                ("spans.parquet".to_string(), 1234),
                ("traces.idx".to_string(), 56),
            ]),
            created_unix_ms: 1_700_000_000_000,
            format_version: 1,
            payload: PayloadTotals::default(),
            kind: PartKind::Spans,
        }
    }

    #[test]
    fn json_round_trip_and_stable_names() {
        let m = sample();
        let json = serde_json::to_string(&m).unwrap();
        assert_eq!(serde_json::from_str::<PartMeta>(&json).unwrap(), m);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        let mut keys: Vec<_> = v.as_object().unwrap().keys().cloned().collect();
        keys.sort();
        assert_eq!(
            keys,
            [
                "created_unix_ms",
                "files",
                "format_version",
                "kind",
                "level",
                "max_end_ns",
                "min_start_ns",
                "part_id",
                "payload",
                "rows",
                "traces",
                "window_ns",
                "window_start_ns"
            ]
        );
    }

    #[test]
    fn a_part_without_kind_reads_as_spans() {
        let mut v = serde_json::to_value(sample()).unwrap();
        assert_eq!(v["kind"], "spans");
        v.as_object_mut().unwrap().remove("kind");
        let m: PartMeta = serde_json::from_value(v).unwrap();
        assert_eq!(m.kind, PartKind::Spans);
        assert_eq!(m, sample());
    }

    #[test]
    fn annotation_kind_round_trips_and_an_unknown_kind_is_refused() {
        let mut m = sample();
        m.kind = PartKind::Annotations;
        let json = serde_json::to_string(&m).unwrap();
        assert!(json.contains(r#""kind":"annotations""#));
        assert_eq!(serde_json::from_str::<PartMeta>(&json).unwrap(), m);
        let mut v = serde_json::to_value(sample()).unwrap();
        v["kind"] = serde_json::json!("scores");
        assert!(serde_json::from_value::<PartMeta>(v).is_err());
    }

    #[test]
    fn unknown_fields_are_ignored() {
        let mut v = serde_json::to_value(sample()).unwrap();
        v["added_by_a_newer_writer"] = serde_json::json!({"x": 1});
        assert_eq!(serde_json::from_value::<PartMeta>(v).unwrap(), sample());
    }

    #[test]
    fn a_manifest_without_payload_totals_reads_as_zeros() {
        let mut v = serde_json::to_value(sample()).unwrap();
        v.as_object_mut().unwrap().remove("payload");
        assert_eq!(serde_json::from_value::<PartMeta>(v).unwrap(), sample());
        let mut m = sample();
        m.payload.pieces = 3;
        let back: PartMeta = serde_json::from_str(&serde_json::to_string(&m).unwrap()).unwrap();
        assert_eq!(back.payload.pieces, 3);
    }

    #[test]
    fn dir_names_sort() {
        assert_eq!(part_dir_name(7), "00000000000000000007");
        assert_eq!(part_dir_name(u64::MAX).len(), 20);
        assert!(part_dir_name(9) < part_dir_name(10));
    }
}
