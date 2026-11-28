//! The winners over several inputs: per identity the record with the largest
//! `arrival`.

use super::Annotation;
use super::part::AnnotationCursor;
use nwdb_core::Result;
use std::cmp::Ordering;

/// Winners over several inputs: per identity the record with the largest
/// `arrival` (on a tie the earlier input), deletes included, in identity
/// order. Holds one cursor and one record per input.
pub struct Latest<'a> {
    cursors: Vec<AnnotationCursor<'a>>,
    extra: std::vec::IntoIter<Annotation>,
    /// The next record of each cursor, then of `extra`.
    heads: Vec<Option<Annotation>>,
    primed: bool,
}

impl<'a> Latest<'a> {
    /// `extra` is one more input held in memory, sorted by identity (several
    /// records of one identity are allowed, in any order). It counts as the
    /// last input.
    pub fn new(cursors: Vec<AnnotationCursor<'a>>, extra: Vec<Annotation>) -> Self {
        // One record per identity, so that `extra` is an input like the others.
        let mut reduced: Vec<Annotation> = Vec::with_capacity(extra.len());
        for a in extra {
            match reduced.last_mut() {
                Some(last) if last.identity_cmp(&a) == Ordering::Equal => {
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
            primed: false,
        }
    }

    fn refill(&mut self, i: usize) -> Result<()> {
        self.heads[i] = if i < self.cursors.len() {
            self.cursors[i].next()?
        } else {
            self.extra.next()
        };
        Ok(())
    }

    /// The next winner, or `None` at the end.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<Annotation>> {
        if !self.primed {
            self.heads = vec![None; self.cursors.len() + 1];
            for i in 0..self.heads.len() {
                self.refill(i)?;
            }
            self.primed = true;
        }
        // A linear scan over the inputs; a heap would pay only for dozens of
        // inputs.
        let mut min: Option<usize> = None;
        for (i, h) in self.heads.iter().enumerate() {
            let Some(h) = h else { continue };
            if min.is_none_or(|m| h.identity_cmp(self.heads[m].as_ref().expect("set")).is_lt()) {
                min = Some(i);
            }
        }
        let Some(min) = min else { return Ok(None) };
        // `min` is the first input holding the smallest identity: the inputs
        // before it hold larger ones.
        let mut winner = min;
        let mut same = Vec::new();
        for i in min..self.heads.len() {
            let Some(h) = &self.heads[i] else { continue };
            if h.identity_cmp(self.heads[min].as_ref().expect("set"))
                .is_eq()
            {
                same.push(i);
                if h.arrival > self.heads[winner].as_ref().expect("set").arrival {
                    winner = i;
                }
            }
        }
        let out = self.heads[winner].take();
        for i in same {
            self.refill(i)?;
        }
        Ok(out)
    }
}
