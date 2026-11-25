//! Ascending lists of document ids and the set operations over them.
//!
//! Each operation allocates its output and nothing else that grows with its
//! inputs: the memory it needs is bounded by the size of its result.

use std::ops::Deref;

/// An ascending list of distinct document ids.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DocIds(Vec<u32>);

impl DocIds {
    /// Wraps ids that the caller knows are strictly ascending. Returns `None`
    /// if they are not.
    pub fn new(ids: Vec<u32>) -> Option<Self> {
        is_ascending(&ids).then_some(Self(ids))
    }

    /// For callers inside the crate that have just built an ascending list.
    pub(crate) fn from_ascending(ids: Vec<u32>) -> Self {
        debug_assert!(is_ascending(&ids));
        Self(ids)
    }

    /// The ids as a slice.
    pub fn as_slice(&self) -> &[u32] {
        &self.0
    }

    /// Takes the ids.
    pub fn into_vec(self) -> Vec<u32> {
        self.0
    }

    /// True if `id` is in the list.
    pub fn contains(&self, id: u32) -> bool {
        self.0.binary_search(&id).is_ok()
    }
}

impl Deref for DocIds {
    type Target = [u32];

    fn deref(&self) -> &[u32] {
        &self.0
    }
}

impl IntoIterator for DocIds {
    type Item = u32;
    type IntoIter = std::vec::IntoIter<u32>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a DocIds {
    type Item = u32;
    type IntoIter = std::iter::Copied<std::slice::Iter<'a, u32>>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter().copied()
    }
}

impl From<DocIds> for Vec<u32> {
    fn from(d: DocIds) -> Self {
        d.0
    }
}

/// True if `ids` is strictly ascending.
pub fn is_ascending(ids: &[u32]) -> bool {
    ids.windows(2).all(|w| w[0] < w[1])
}

/// How much longer a list must be than the other before the intersection
/// searches it instead of walking it.
const GALLOP_RATIO: usize = 16;

/// Ids in both lists. When one list is much shorter, each of its ids is
/// searched for in the longer one with an exponential then binary search, so
/// the work follows the short list.
pub fn intersect(a: &[u32], b: &[u32]) -> Vec<u32> {
    let (short, long) = if a.len() <= b.len() { (a, b) } else { (b, a) };
    let mut out = Vec::with_capacity(short.len());
    if short.len() * GALLOP_RATIO < long.len() {
        let mut rest = long;
        for &x in short {
            let mut step = 1;
            while step < rest.len() && rest[step - 1] < x {
                step *= 2;
            }
            let hi = step.min(rest.len());
            let at = rest[..hi].partition_point(|&y| y < x);
            rest = &rest[at..];
            match rest.first() {
                Some(&y) if y == x => out.push(x),
                Some(_) => {}
                None => break,
            }
        }
    } else {
        let (mut i, mut j) = (0, 0);
        while i < short.len() && j < long.len() {
            match short[i].cmp(&long[j]) {
                std::cmp::Ordering::Less => i += 1,
                std::cmp::Ordering::Greater => j += 1,
                std::cmp::Ordering::Equal => {
                    out.push(short[i]);
                    i += 1;
                    j += 1;
                }
            }
        }
    }
    out
}

/// Ids in either list.
pub fn union(a: &[u32], b: &[u32]) -> Vec<u32> {
    let mut out = Vec::with_capacity(a.len() + b.len());
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => {
                out.push(a[i]);
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                out.push(b[j]);
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    out.extend_from_slice(&a[i..]);
    out.extend_from_slice(&b[j..]);
    out
}

/// Ids in `a` and not in `b`.
pub fn difference(a: &[u32], b: &[u32]) -> Vec<u32> {
    let mut out = Vec::with_capacity(a.len());
    let mut j = 0;
    for &x in a {
        j += b[j..].partition_point(|&y| y < x);
        if b.get(j) != Some(&x) {
            out.push(x);
        }
    }
    out
}
