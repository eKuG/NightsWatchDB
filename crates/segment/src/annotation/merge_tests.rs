//! The stop flag of a merge, set at points a test can name.

use super::*;
use crate::annotation::Annotation;
use nwdb_core::TraceId;
use std::cell::Cell;
use std::rc::Rc;

fn rec(trace: u128, arrival: u64) -> Annotation {
    Annotation {
        trace_id: TraceId(trace.to_be_bytes()),
        span_id: None,
        name: "n".into(),
        source: String::new(),
        time_ns: 5,
        value: Some(1.0),
        label: None,
        // Four of these fill a 16 KiB block.
        comment: Some("c".repeat(4096)),
        delete: false,
        arrival,
    }
}

fn input(dir: &Path, id: u64, traces: std::ops::Range<u128>, arrival: u64) -> AnnotationReader {
    let mut w = AnnotationWriter::create(dir, id, 0, 1_000).unwrap();
    for t in traces {
        w.append(&rec(t, arrival)).unwrap();
    }
    let meta = w.finish(0).unwrap();
    AnnotationReader::open(dir.join(nwdb_core::part_dir_name(id)), &meta).unwrap()
}

fn names(dir: &Path) -> Vec<String> {
    let mut v: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    v.sort();
    v
}

fn set_hook(f: impl Fn(u64) + 'static) {
    ON_OUTPUT_BLOCK.with(|h| *h.borrow_mut() = Some(Box::new(f)));
}

#[test]
fn a_stop_before_the_merge_stops_it_before_any_input_is_read() {
    let tmp = tempfile::tempdir().unwrap();
    let a = input(tmp.path(), 1, 1..10, 1);
    let flag = AtomicBool::new(true);
    let err = merge_annotation_parts(&[&a], tmp.path(), 9, 0, 1_000, 1, Some(&flag)).unwrap_err();
    assert!(matches!(&err, Error::Unavailable(m) if m == "the merge was stopped"));
    assert_eq!(a.blocks_read(), 0, "an input was verified");
    assert_eq!(names(tmp.path()), ["00000000000000000001"]);
}

#[test]
fn a_stop_half_way_is_seen_after_the_next_64_blocks_and_leaves_nothing() {
    // 640 records of 4 KiB: 160 output blocks. The hook runs each time the
    // merge closes an output block, on this thread, so the flag is set at an
    // exact point; the merge looks at it after blocks 64, 128, ...
    for (set_at, stops_at) in [(1u64, 64u64), (64, 64), (65, 128), (100, 128)] {
        let tmp = tempfile::tempdir().unwrap();
        let a = input(tmp.path(), 1, 1..321, 1);
        let b = input(tmp.path(), 2, 321..641, 1);
        let flag = Rc::new(AtomicBool::new(false));
        let last = Rc::new(Cell::new(0));
        {
            let (flag, last) = (flag.clone(), last.clone());
            set_hook(move |n| {
                last.set(n);
                if n == set_at {
                    flag.store(true, Ordering::Relaxed);
                }
            });
        }
        let err =
            merge_annotation_parts(&[&a, &b], tmp.path(), 9, 0, 1_000, 1, Some(&flag)).unwrap_err();
        assert!(matches!(&err, Error::Unavailable(m) if m == "the merge was stopped"));
        assert_eq!(last.get(), stops_at, "flag set at block {set_at}");
        assert_eq!(
            names(tmp.path()),
            ["00000000000000000001", "00000000000000000002"]
        );
    }
    // Never set: the same merge runs to the end, through 160 blocks.
    let tmp = tempfile::tempdir().unwrap();
    let a = input(tmp.path(), 1, 1..321, 1);
    let b = input(tmp.path(), 2, 321..641, 1);
    let last = Rc::new(Cell::new(0));
    let l = last.clone();
    set_hook(move |n| l.set(n));
    let flag = AtomicBool::new(false);
    let (meta, _) =
        merge_annotation_parts(&[&a, &b], tmp.path(), 9, 0, 1_000, 1, Some(&flag)).unwrap();
    assert_eq!((meta.rows, last.get()), (640, 160));
}
