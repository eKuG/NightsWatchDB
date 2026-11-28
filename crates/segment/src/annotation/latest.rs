//! The winners over several inputs: per identity the record with the largest
//! `arrival`.

use super::Annotation;
use super::part::AnnotationCursor;
use nwdb_core::Result;
use std::borrow::Cow;
use std::cmp::Ordering;

/// Winners over several inputs: per identity the record with the largest
/// `arrival` (on a tie the earlier input), deletes included, in identity
/// order. Holds one cursor and one record per input.
pub struct Latest<'a> {
    cursors: Vec<AnnotationCursor<'a>>,
    extra: std::vec::IntoIter<&'a Annotation>,
    /// The next record of each cursor.
    heads: Vec<Option<Annotation>>,
    /// The next record of `extra`, the last input.
    extra_head: Option<&'a Annotation>,
    primed: bool,
}

impl<'a> Latest<'a> {
    /// `extra` is one more input held in memory, borrowed, sorted by identity
    /// (several records of one identity are allowed, in any order). It counts
    /// as the last input. A record of it is copied only if it wins.
    pub fn new(cursors: Vec<AnnotationCursor<'a>>, extra: Vec<&'a Annotation>) -> Self {
        // One record per identity, so that `extra` is an input like the others.
        let mut reduced: Vec<&'a Annotation> = Vec::with_capacity(extra.len());
        for a in extra {
            match reduced.last_mut() {
                Some(last) if last.identity_cmp(a) == Ordering::Equal => {
                    if a.arrival > last.arrival {
                        *last = a;
                    }
                }
                _ => reduced.push(a),
            }
        }
        Self {
            heads: Vec::new(),
            cursors,
            extra: reduced.into_iter(),
            extra_head: None,
            primed: false,
        }
    }

    fn refill(&mut self, i: usize) -> Result<()> {
        if i < self.cursors.len() {
            self.heads[i] = self.cursors[i].next()?;
        } else {
            self.extra_head = self.extra.next();
        }
        Ok(())
    }

    /// The head of input `i`; the last input is `extra`.
    fn head(&self, i: usize) -> Option<&Annotation> {
        if i < self.cursors.len() {
            self.heads[i].as_ref()
        } else {
            self.extra_head
        }
    }

    /// The next winner, or `None` at the end, copied if it came from the
    /// borrowed input.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<Annotation>> {
        Ok(self.next_ref()?.map(Cow::into_owned))
    }

    /// [`next`](Self::next) without a copy of a winner from the borrowed
    /// input: it is returned borrowed.
    pub fn next_ref(&mut self) -> Result<Option<Cow<'a, Annotation>>> {
        if !self.primed {
            self.heads = vec![None; self.cursors.len()];
            for i in 0..=self.cursors.len() {
                self.refill(i)?;
            }
            self.primed = true;
        }
        let inputs = self.cursors.len() + 1;
        // A linear scan over the inputs; a heap would pay only for dozens of
        // inputs.
        let mut min: Option<usize> = None;
        for i in 0..inputs {
            let Some(h) = self.head(i) else { continue };
            if min.is_none_or(|m| h.identity_cmp(self.head(m).expect("set")).is_lt()) {
                min = Some(i);
            }
        }
        let Some(min) = min else { return Ok(None) };
        // `min` is the first input holding the smallest identity: the inputs
        // before it hold larger ones.
        let mut winner = min;
        let mut same = Vec::new();
        for i in min..inputs {
            let Some(h) = self.head(i) else { continue };
            if h.identity_cmp(self.head(min).expect("set")).is_eq() {
                same.push(i);
                if h.arrival > self.head(winner).expect("set").arrival {
                    winner = i;
                }
            }
        }
        let out = if winner < self.cursors.len() {
            self.heads[winner].take().map(Cow::Owned)
        } else {
            self.extra_head.map(Cow::Borrowed)
        };
        for i in same {
            self.refill(i)?;
        }
        Ok(out)
    }
}
