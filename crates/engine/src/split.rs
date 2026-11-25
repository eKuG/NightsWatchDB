//! Which traces are spread over more than one part.
//!
//! A trace can have rows in several parts (late spans, a flush in the middle
//! of a trace). A query that merges a trace's partial summaries needs to know
//! which parts those are. Asking every part's trace index for every candidate
//! costs candidates times parts, nearly all answers being "no", so the answer
//! is worked out once, when the set of parts changes, and kept with the
//! snapshot state.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;

use nwdb_core::TraceId;
use nwdb_segment::TraceEntry;

use crate::engine::LivePart;

/// The traces that are in more than one part, and the parts that hold them.
///
/// Part numbers are positions in the part list of the same snapshot, in
/// ascending order. A trace that is in at most one part is not in the map, so
/// the map's memory is the number of split traces times about 40 bytes (the
/// id, the box and a few part numbers), not the number of traces.
#[derive(Default)]
pub(crate) struct SplitMap {
    map: HashMap<TraceId, Box<[u32]>>,
}

impl SplitMap {
    /// The parts that hold `id`, if it is in more than one.
    pub(crate) fn holders(&self, id: &TraceId) -> Option<&[u32]> {
        self.map.get(id).map(|h| &h[..])
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&TraceId, &[u32])> {
        self.map.iter().map(|(id, h)| (id, &h[..]))
    }

    /// The map for `parts`, given the map for `parts[..old_len]` (an empty map
    /// and 0 at start-up).
    ///
    /// The new parts `parts[old_len..]` are merged with each other in one pass
    /// over their trace indexes, which are sorted by trace id. Each trace id
    /// found there is then looked for in the old parts, whose indexes are
    /// walked forward with a galloping search, so the cost per old part is at
    /// most the new traces times the log of the old part's size over the new
    /// traces, and no more than a linear pass over the old part. The old
    /// parts are not otherwise touched: a trace that is in no new part keeps
    /// its entry, copied from the old map, whose size is the number of split
    /// traces.
    ///
    /// Everything here is in memory and does no I/O, so it runs outside the
    /// core lock.
    pub(crate) fn extend(&self, parts: &[Arc<LivePart>], old_len: usize) -> SplitMap {
        let mut map = self.map.clone();
        let index = |p: usize| parts[p].reader.trace_entries();

        // The next entry of each new part, smallest trace id first.
        let mut heap = Heap::new();
        for p in old_len..parts.len() {
            if let Some(e) = index(p).first() {
                heap.push(Reverse((e.trace_id, p, 0)));
            }
        }
        // How far each old part's index has been walked.
        let mut at = vec![0usize; old_len];
        let mut holders: Vec<u32> = Vec::new();
        while let Some(Reverse((id, p, i))) = heap.pop() {
            push_next(index(p), p, i, &mut heap);
            // Parts come out in ascending order for equal ids.
            holders.clear();
            holders.push(p as u32);
            while let Some(&Reverse((same, q, j))) = heap.peek() {
                if same != id {
                    break;
                }
                heap.pop();
                push_next(index(q), q, j, &mut heap);
                holders.push(q as u32);
            }
            let mut all: Vec<u32> = Vec::new();
            for (o, pos) in at.iter_mut().enumerate() {
                let entries = index(o);
                *pos = gallop(entries, *pos, id);
                if entries.get(*pos).is_some_and(|e| e.trace_id == id) {
                    all.push(o as u32);
                }
            }
            all.extend_from_slice(&holders);
            if all.len() > 1 {
                map.insert(id, all.into_boxed_slice());
            }
        }
        SplitMap { map }
    }
}

type Heap = BinaryHeap<Reverse<(TraceId, usize, usize)>>;

/// Queues entry `i + 1` of part `p`, if there is one.
fn push_next(entries: &[TraceEntry], p: usize, i: usize, heap: &mut Heap) {
    if let Some(e) = entries.get(i + 1) {
        heap.push(Reverse((e.trace_id, p, i + 1)));
    }
}

/// The position of the first entry at or after `from` whose trace id is not
/// below `id`, found by doubling the step from `from`.
fn gallop(entries: &[TraceEntry], from: usize, id: TraceId) -> usize {
    let mut bound = 1;
    while from + bound <= entries.len() && entries[from + bound - 1].trace_id < id {
        bound *= 2;
    }
    let lo = from + bound / 2;
    let hi = (from + bound).min(entries.len());
    lo + entries[lo..hi].partition_point(|e| e.trace_id < id)
}
