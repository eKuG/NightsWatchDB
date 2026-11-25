use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Metadata describing one immutable part, as recorded in the manifest and
/// next to the part's files.
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
    /// Number of span rows.
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
                "level",
                "max_end_ns",
                "min_start_ns",
                "part_id",
                "rows",
                "traces",
                "window_ns",
                "window_start_ns"
            ]
        );
    }

    #[test]
    fn unknown_fields_are_ignored() {
        let mut v = serde_json::to_value(sample()).unwrap();
        v["added_by_a_newer_writer"] = serde_json::json!({"x": 1});
        assert_eq!(serde_json::from_value::<PartMeta>(v).unwrap(), sample());
    }

    #[test]
    fn dir_names_sort() {
        assert_eq!(part_dir_name(7), "00000000000000000007");
        assert_eq!(part_dir_name(u64::MAX).len(), 20);
        assert!(part_dir_name(9) < part_dir_name(10));
    }
}
