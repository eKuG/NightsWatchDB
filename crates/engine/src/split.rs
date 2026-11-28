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

    /// The map for `parts`, given an empty map (at start-up): all the parts
    /// are new.
    pub(crate) fn extend(&self, parts: &[Arc<LivePart>]) -> SplitMap {
        let added: Vec<usize> = (0..parts.len()).collect();
        self.extend_at(parts, &added)
    }

    /// The map for `parts` after parts were taken out of the list and others
    /// put in, given the map of the old list. `new_pos[i]` is where old part
    /// `i` is in `parts` (`None` if it was removed) and `added` holds, in
    /// ascending order, the positions in `parts` of the parts that are new.
    /// Survivors keep their relative order.
    ///
    /// The old map is renumbered (a trace left with fewer than two holders
    /// drops out), which costs the number of split traces, and nothing is
    /// read from any part for that. Then [`extend_at`](Self::extend_at) adds
    /// what the new parts bring. With nothing added, as in a removal by
    /// retention, the first step is all there is.
    pub(crate) fn replace(
        &self,
        new_pos: &[Option<u32>],
        parts: &[Arc<LivePart>],
        added: &[usize],
    ) -> SplitMap {
        let map = self
            .map
            .iter()
            .filter_map(|(id, holders)| {
                let kept: Vec<u32> = holders
                    .iter()
                    .filter_map(|&h| new_pos[h as usize])
                    .collect();
                (kept.len() > 1).then(|| (*id, kept.into_boxed_slice()))
            })
            .collect();
        SplitMap { map }.extend_at(parts, added)
    }

    /// Adds to the map what the parts at positions `added` (ascending) bring;
    /// every other part of `parts` is old and its holders are already right.
    ///
    /// The new parts are merged with each other in one pass over their trace
    /// indexes, which are sorted by trace id. Each trace id found there is
    /// then looked for in the other parts, whose indexes are walked forward
    /// with a galloping search, so the cost per old part is at most the new
    /// traces times the log of the old part's size over the new traces, and
    /// no more than a linear pass over the old part. The old parts are not
    /// otherwise touched: a trace that is in no new part keeps its entry,
    /// copied from the old map, whose size is the number of split traces.
    ///
    /// Everything here is in memory and does no I/O, so it runs outside the
    /// core lock.
    fn extend_at(&self, parts: &[Arc<LivePart>], added: &[usize]) -> SplitMap {
        let mut map = self.map.clone();
        let index = |p: usize| parts[p].reader.trace_entries();
        let others: Vec<usize> = (0..parts.len())
            .filter(|p| added.binary_search(p).is_err())
            .collect();

        // The next entry of each new part, smallest trace id first.
        let mut heap = Heap::new();
        for &p in added {
            if let Some(e) = index(p).first() {
                heap.push(Reverse((e.trace_id, p, 0)));
            }
        }
        // How far each old part's index has been walked.
        let mut at = vec![0usize; others.len()];
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
            for (pos, &o) in at.iter_mut().zip(&others) {
                let entries = index(o);
                *pos = gallop(entries, *pos, id);
                if entries.get(*pos).is_some_and(|e| e.trace_id == id) {
                    all.push(o as u32);
                }
            }
            all.extend_from_slice(&holders);
            // New parts can sit between old ones in the list.
            all.sort_unstable();
            if all.len() > 1 {
                map.insert(id, all.into_boxed_slice());
            }
        }
        SplitMap { map }
    }
}

/// The part list after `remove` (ids of live parts) left it and `add` joined
/// it, kept in part id order, and the split map that matches it.
///
/// The cost is the old list, the old map's split traces, and, for the added
/// parts, their traces times a galloping step in each other part; see
/// [`SplitMap::replace`]. It does no I/O.
pub(crate) fn change_parts(
    old: &[Arc<LivePart>],
    old_split: &SplitMap,
    remove: &[u64],
    add: Vec<Arc<LivePart>>,
) -> (Vec<Arc<LivePart>>, SplitMap) {
    let gone: std::collections::HashSet<u64> = remove.iter().copied().collect();
    // (part id, index in `old` or None for a new part, the part)
    let mut all: Vec<(u64, Option<usize>, Arc<LivePart>)> = old
        .iter()
        .enumerate()
        .filter(|(_, p)| !gone.contains(&p.meta.part_id))
        .map(|(i, p)| (p.meta.part_id, Some(i), p.clone()))
        .chain(add.into_iter().map(|p| (p.meta.part_id, None, p)))
        .collect();
    // Part ids are unique, and a flush and a replacement can publish out of
    // the order in which they reserved their ids.
    all.sort_by_key(|(id, _, _)| *id);
    let mut new_pos = vec![None; old.len()];
    let mut added = Vec::new();
    for (pos, (_, from, _)) in all.iter().enumerate() {
        match from {
            Some(i) => new_pos[*i] = Some(pos as u32),
            None => added.push(pos),
        }
    }
    let parts: Vec<Arc<LivePart>> = all.into_iter().map(|(_, _, p)| p).collect();
    let split = old_split.replace(&new_pos, &parts, &added);
    (parts, split)
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
pub(crate) fn gallop(entries: &[TraceEntry], from: usize, id: TraceId) -> usize {
    let mut bound = 1;
    while from + bound <= entries.len() && entries[from + bound - 1].trace_id < id {
        bound *= 2;
    }
    let lo = from + bound / 2;
    let hi = (from + bound).min(entries.len());
    lo + entries[lo..hi].partition_point(|e| e.trace_id < id)
}
