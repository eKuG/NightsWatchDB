//! The query model: a small boolean language over words, prefixes and
//! phrases, a parser for it, and a generic evaluator over id lists.
//!
//! The crate does not run a whole query against documents. A caller maps
//! documents to something else (spans made of several documents), and a
//! boolean operator has to be applied to that something, not to documents. So
//! the crate gives the caller the pieces: the leaves of a query, a way to find
//! the documents a leaf can match ([`IndexReader::term`], [`IndexReader::prefix`],
//! [`phrase_candidates`] followed by [`phrase_matches`]), and [`evaluate`],
//! which combines the id lists the caller computes for each leaf. A leaf for
//! which [`Leaf::needs_confirmation`] is true (a phrase, or a word longer than
//! 64 bytes) returns candidates that the caller checks against each
//! document's text first.
//!
//! # Grammar
//!
//! ```text
//! query   = and
//! and     = or { or }                 operands side by side are ANDed
//! or      = unary { "OR" unary }      OR binds tighter than the implicit AND
//! unary   = "-" unary | primary
//! primary = "(" and ")" | phrase | word
//! phrase  = '"' any bytes but '"' '"'
//! word    = a run of bytes other than white space, '(', ')' and '"'
//! ```
//!
//! - `a b OR c` is `a AND (b OR c)`; `OR` must be upper case, `or` is a word.
//! - A `-` negates the operand that follows it directly (`-foo`, `-"a b"`,
//!   `-(a OR b)`). A `-` followed by white space, `)` or the end is not a
//!   negation; it is an ordinary byte of a word.
//! - A word is cut into tokens by the tokeniser. One token is a [`Query::Term`].
//!   Several (for example `foo-bar`) are a [`Query::Phrase`]. None (for
//!   example `!!!`): the word is ignored, as are an empty phrase and an empty
//!   group.
//! - A phrase in double quotes is cut into tokens the same way.
//! - A word that ends in `*` is a prefix: what precedes the last `*` must be
//!   one token of at least two bytes (and at most 64).
//! - `OR` with no operand on either side, an unterminated `"` or `(`, and a
//!   stray `)` are errors.
//!
//! A query must have at least one operand, and at least one that is not
//! negated: negation is relative to the universe the caller passes to
//! [`evaluate`], and a query that only subtracts has no starting set to
//! subtract from. Limits: nesting (parentheses and `-` together) deeper than
//! 32, more than 64 operands, a phrase of more than 64 tokens, and text longer
//! than 16 KiB are `InvalidArgument`.

use nwdb_core::{Error, Result};

use crate::ids::{difference, intersect, is_ascending, union};
use crate::reader::{IndexReader, Matches};
use crate::source::ReadAt;
use crate::tokenizer::{MAX_TOKEN_LEN, tokens};

const MAX_NESTING: usize = 32;
const MAX_OPERANDS: usize = 64;
const MAX_PHRASE_TOKENS: usize = 64;
const MAX_QUERY_BYTES: usize = 16 * 1024;

/// A parsed query. Words are already tokenised and lowercased.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Query {
    /// Documents containing this token.
    Term(String),
    /// Documents containing a token that starts with this.
    Prefix(String),
    /// The tokens, consecutively and in order.
    Phrase(Vec<String>),
    /// All of the operands.
    And(Vec<Query>),
    /// Any of the operands.
    Or(Vec<Query>),
    /// Not the operand, relative to the universe given to [`evaluate`].
    Not(Box<Query>),
}

/// A leaf of a query: something an index can be asked about.
///
/// Not every leaf is answered exactly by the index alone; see
/// [`Leaf::needs_confirmation`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Leaf<'a> {
    /// See [`Query::Term`].
    Term(&'a str),
    /// See [`Query::Prefix`].
    Prefix(&'a str),
    /// See [`Query::Phrase`].
    Phrase(&'a [String]),
}

impl Leaf<'_> {
    /// True if the ids the index returns for this leaf are candidates that
    /// the caller must check against each document's text:
    ///
    /// - a `Term` longer than 64 bytes: [`IndexReader::term`] returns the
    ///   documents whose token shares its first 64 bytes; confirm with
    ///   [`contains_token`];
    /// - a `Phrase`: [`phrase_candidates`] returns the documents that hold the
    ///   tokens, not necessarily in order; confirm with [`phrase_matches`].
    ///
    /// A `Prefix` and a `Term` of up to 64 bytes are exact.
    pub fn needs_confirmation(&self) -> bool {
        match self {
            Leaf::Term(t) => t.len() > MAX_TOKEN_LEN,
            Leaf::Prefix(_) => false,
            Leaf::Phrase(_) => true,
        }
    }
}

/// The leaves of `query`, in the order they appear.
pub fn leaves(query: &Query) -> Vec<Leaf<'_>> {
    let mut out = Vec::new();
    let mut stack = vec![query];
    while let Some(q) = stack.pop() {
        match q {
            Query::Term(t) => out.push(Leaf::Term(t)),
            Query::Prefix(p) => out.push(Leaf::Prefix(p)),
            Query::Phrase(p) => out.push(Leaf::Phrase(p)),
            Query::And(v) | Query::Or(v) => stack.extend(v.iter().rev()),
            Query::Not(q) => stack.push(q),
        }
    }
    out
}

// ---------------------------------------------------------------- parsing

struct Parser<'a> {
    src: &'a str,
    b: &'a [u8],
    pos: usize,
    operands: usize,
}

fn bad<T>(msg: impl std::fmt::Display) -> Result<T> {
    Err(Error::invalid_argument(msg))
}

/// Parses `text` into a [`Query`].
pub fn parse(text: &str) -> Result<Query> {
    if text.len() > MAX_QUERY_BYTES {
        return bad("query is longer than 16 KiB");
    }
    let mut p = Parser {
        src: text,
        b: text.as_bytes(),
        pos: 0,
        operands: 0,
    };
    let q = p.and(0)?;
    if p.pos < p.b.len() {
        return bad("unmatched )");
    }
    let Some(q) = q else {
        return bad("query has no usable operand");
    };
    if !has_positive(&q) {
        return bad("query has only negated operands");
    }
    Ok(q)
}

fn has_positive(q: &Query) -> bool {
    match q {
        Query::Term(_) | Query::Prefix(_) | Query::Phrase(_) => true,
        Query::And(v) | Query::Or(v) => v.iter().any(has_positive),
        Query::Not(_) => false,
    }
}

impl Parser<'_> {
    fn skip_space(&mut self) {
        while self.b.get(self.pos).is_some_and(u8::is_ascii_whitespace) {
            self.pos += 1;
        }
    }

    fn is_word_byte(b: u8) -> bool {
        !b.is_ascii_whitespace() && !matches!(b, b'(' | b')' | b'"')
    }

    /// True if an `OR` keyword starts here.
    fn at_or(&self) -> bool {
        self.b[self.pos..].starts_with(b"OR")
            && !self
                .b
                .get(self.pos + 2)
                .copied()
                .is_some_and(Self::is_word_byte)
    }

    /// Operands side by side.
    fn and(&mut self, depth: usize) -> Result<Option<Query>> {
        let mut items = Vec::new();
        loop {
            self.skip_space();
            if matches!(self.b.get(self.pos), None | Some(b')')) {
                break;
            }
            items.extend(self.or(depth)?);
        }
        Ok(collapse(items, Query::And))
    }

    /// Operands joined by `OR`.
    fn or(&mut self, depth: usize) -> Result<Option<Query>> {
        let mut items: Vec<Query> = self.unary(depth)?.into_iter().collect();
        loop {
            self.skip_space();
            if self.pos >= self.b.len() || !self.at_or() {
                break;
            }
            self.pos += 2;
            items.extend(self.unary(depth)?);
        }
        Ok(collapse(items, Query::Or))
    }

    /// An operand with an optional leading `-`. `None` means the operand
    /// had no tokens and is ignored.
    fn unary(&mut self, depth: usize) -> Result<Option<Query>> {
        self.skip_space();
        let Some(&c) = self.b.get(self.pos) else {
            return bad("an operand is missing (OR needs one on each side)");
        };
        match c {
            b')' => bad("an operand is missing before )"),
            b'-' if self
                .b
                .get(self.pos + 1)
                .is_some_and(|n| !n.is_ascii_whitespace() && *n != b')') =>
            {
                self.pos += 1;
                let inner = self.unary(self.enter(depth)?)?;
                Ok(inner.map(|q| Query::Not(Box::new(q))))
            }
            b'(' => {
                self.pos += 1;
                let inner = self.and(self.enter(depth)?)?;
                if self.b.get(self.pos) != Some(&b')') {
                    return bad("missing )");
                }
                self.pos += 1;
                Ok(inner)
            }
            b'"' => {
                let start = self.pos + 1;
                let Some(len) = self.b[start..].iter().position(|&x| x == b'"') else {
                    return bad("unterminated \"");
                };
                self.pos = start + len + 1;
                let src = self.src;
                self.words(&src[start..start + len], false)
            }
            _ => {
                if self.at_or() {
                    return bad("OR needs an operand on each side");
                }
                let start = self.pos;
                while self
                    .b
                    .get(self.pos)
                    .copied()
                    .is_some_and(Self::is_word_byte)
                {
                    self.pos += 1;
                }
                let src = self.src;
                self.words(&src[start..self.pos], true)
            }
        }
    }

    fn enter(&self, depth: usize) -> Result<usize> {
        if depth >= MAX_NESTING {
            return bad("nesting is deeper than 32");
        }
        Ok(depth + 1)
    }

    /// Turns the text of a word or phrase into a leaf.
    fn words(&mut self, text: &str, star_allowed: bool) -> Result<Option<Query>> {
        let (text, prefix) = match text.strip_suffix('*') {
            Some(stem) if star_allowed => (stem, true),
            _ => (text, false),
        };
        let mut toks = Vec::new();
        for t in tokens(text.as_bytes()) {
            if toks.len() == MAX_PHRASE_TOKENS {
                return bad("a phrase is longer than 64 tokens");
            }
            let s = std::str::from_utf8(t.raw())
                .map_err(|_| Error::invalid_argument("token is not valid UTF-8"))?;
            toks.push(s.to_ascii_lowercase());
        }
        let leaf = match (toks.len(), prefix) {
            (0, _) => return Ok(None),
            (1, true) => {
                let t = toks.remove(0);
                if t.len() < 2 || t.len() > MAX_TOKEN_LEN {
                    return bad("a prefix needs between 2 and 64 bytes before the *");
                }
                Query::Prefix(t)
            }
            (_, true) => return bad("a prefix must be a single token before the *"),
            (1, false) => Query::Term(toks.remove(0)),
            (_, false) => Query::Phrase(toks),
        };
        self.operands += 1;
        if self.operands > MAX_OPERANDS {
            return bad("more than 64 operands");
        }
        Ok(Some(leaf))
    }
}

fn collapse(mut items: Vec<Query>, make: fn(Vec<Query>) -> Query) -> Option<Query> {
    match items.len() {
        0 => None,
        1 => items.pop(),
        _ => Some(make(items)),
    }
}

// ------------------------------------------------------------- evaluation

/// The deepest query [`evaluate`] accepts: 128 levels of nodes, the top one
/// being level 0. A `Not` that is a direct operand of an `And` adds no level
/// of its own (the `And` evaluates what is inside it one level down), so an
/// `And` and its `Not` count as one level; every other nesting counts one per
/// node. A parsed query nests at most 32 levels of parentheses and `-`, each
/// of which adds at most two nodes (an `And` and an `Or`, or a `Not`), so
/// every parsed query is within the limit. A query built by hand that is
/// deeper is `InvalidArgument`.
pub const MAX_EVAL_DEPTH: usize = 128;

/// Combines ascending id lists the caller supplies for each leaf.
///
/// `leaf` returns the ascending ids that match one leaf. `universe` is the
/// ascending list of every id the caller considers: `Not` is the universe
/// minus the operand, and an `And` whose operands are all negated starts from
/// the universe. Leaf results are not intersected with the universe, so a
/// caller that wants results inside it should return ids inside it.
///
/// `And([])` is the universe and `Or([])` is empty. Lists that are not
/// strictly ascending are `InvalidArgument`. An `And` stops asking for leaves
/// once its running result is empty. Memory is bounded by the lists involved.
pub fn evaluate(
    query: &Query,
    universe: &[u32],
    mut leaf: impl FnMut(&Leaf<'_>) -> Result<Vec<u32>>,
) -> Result<Vec<u32>> {
    if !is_ascending(universe) {
        return bad("the universe is not strictly ascending");
    }
    eval(query, universe, &mut leaf, 0)
}

type LeafFn<'f> = dyn FnMut(&Leaf<'_>) -> Result<Vec<u32>> + 'f;

fn leaf_ids(l: Leaf<'_>, leaf: &mut LeafFn<'_>) -> Result<Vec<u32>> {
    let ids = leaf(&l)?;
    if !is_ascending(&ids) {
        return bad("a leaf returned ids that are not strictly ascending");
    }
    Ok(ids)
}

fn eval(q: &Query, universe: &[u32], leaf: &mut LeafFn<'_>, depth: usize) -> Result<Vec<u32>> {
    if depth > MAX_EVAL_DEPTH {
        return bad("query is nested too deeply");
    }
    match q {
        Query::Term(t) => leaf_ids(Leaf::Term(t), leaf),
        Query::Prefix(p) => leaf_ids(Leaf::Prefix(p), leaf),
        Query::Phrase(p) => leaf_ids(Leaf::Phrase(p), leaf),
        Query::Not(inner) => Ok(difference(
            universe,
            &eval(inner, universe, leaf, depth + 1)?,
        )),
        Query::Or(items) => {
            let mut acc: Vec<u32> = Vec::new();
            for item in items {
                acc = union(&acc, &eval(item, universe, leaf, depth + 1)?);
            }
            Ok(acc)
        }
        Query::And(items) => {
            let mut acc: Option<Vec<u32>> = None;
            let mut negatives = Vec::new();
            for item in items {
                match item {
                    Query::Not(inner) => negatives.push(&**inner),
                    _ => {
                        let ids = eval(item, universe, leaf, depth + 1)?;
                        let next = match &acc {
                            Some(a) => intersect(a, &ids),
                            None => ids,
                        };
                        if next.is_empty() {
                            return Ok(next);
                        }
                        acc = Some(next);
                    }
                }
            }
            let mut acc = acc.unwrap_or_else(|| universe.to_vec());
            for inner in negatives {
                if acc.is_empty() {
                    break;
                }
                acc = difference(&acc, &eval(inner, universe, leaf, depth + 1)?);
            }
            Ok(acc)
        }
    }
}

// ----------------------------------------------------------------- phrases

/// Documents that contain every token of the phrase, in any order: the
/// documents that can hold the phrase. A token longer than 64 bytes
/// contributes the documents whose token shares its first 64 bytes. Check each
/// document with [`phrase_matches`].
///
/// The answer is only exact (`exact: true`) for a phrase of a single token of
/// up to 64 bytes; for any other it is candidates, and the caller must run
/// [`phrase_matches`] whatever the flag was for the individual tokens. An
/// empty phrase is `InvalidArgument`.
pub fn phrase_candidates<S: ReadAt, T: AsRef<[u8]>>(
    reader: &IndexReader<S>,
    tokens: &[T],
) -> Result<Matches> {
    if tokens.is_empty() {
        return bad("a phrase needs at least one token");
    }
    let wanted: Vec<Vec<u8>> = tokens
        .iter()
        .map(|t| t.as_ref())
        .map(|t| {
            if t.len() <= MAX_TOKEN_LEN {
                t.to_ascii_lowercase()
            } else {
                t.to_vec()
            }
        })
        .collect();
    let mut m = reader.intersect(&wanted)?;
    m.exact &= tokens.len() == 1;
    Ok(m)
}

/// True if `token` occurs as a whole token of `text`, with ASCII case
/// folded. This confirms a candidate that [`IndexReader::term`] returned for
/// a token longer than 64 bytes.
pub fn contains_token(text: &[u8], token: &[u8]) -> bool {
    tokens(text).any(|t| t.eq_folded(token))
}

/// True if `tokens` occur one after another, in order, in the token stream
/// of `text`. Case is folded for ASCII. A token of `text` longer than 64 bytes
/// still counts as a token, so it separates its neighbours; it can equal a
/// phrase token that is itself that long. An empty phrase matches nothing.
pub fn phrase_matches<T: AsRef<[u8]>>(text: &[u8], phrase: &[T]) -> bool {
    let m = phrase.len();
    if m == 0 {
        return false;
    }
    // Knuth-Morris-Pratt, so a phrase that repeats a token ("a a b") is
    // found inside "a a a b" without scanning back.
    let eq = |a: &[u8], b: &[u8]| a.eq_ignore_ascii_case(b);
    let mut fail = vec![0usize; m];
    let mut k = 0;
    for i in 1..m {
        while k > 0 && !eq(phrase[i].as_ref(), phrase[k].as_ref()) {
            k = fail[k - 1];
        }
        if eq(phrase[i].as_ref(), phrase[k].as_ref()) {
            k += 1;
        }
        fail[i] = k;
    }
    let mut k = 0;
    for tok in tokens(text) {
        while k > 0 && !tok.eq_folded(phrase[k].as_ref()) {
            k = fail[k - 1];
        }
        if tok.eq_folded(phrase[k].as_ref()) {
            k += 1;
            if k == m {
                return true;
            }
        }
    }
    false
}
