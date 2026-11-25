//! A small scanner that finds the elements of a JSON array in a byte string.
//!
//! The text of a model call is often a JSON array of messages. Storing each
//! message as its own piece lets the next call, which sends the same messages
//! again plus a new one, share all but the new one. The scanner must not be
//! fooled by text inside strings, so it understands strings, escapes and
//! nesting; it does not check that numbers or literals are well formed,
//! because the caller only needs to know where an element starts and ends.

use std::ops::Range;

/// Nesting deeper than this is refused, so the scanner's stack is bounded.
pub const MAX_JSON_DEPTH: usize = 256;

fn is_ws(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r')
}

fn skip_ws(bytes: &[u8], mut i: usize) -> usize {
    while bytes.get(i).is_some_and(|&b| is_ws(b)) {
        i += 1;
    }
    i
}

/// The end of the string whose opening quote is at `i`.
fn string_end(bytes: &[u8], i: usize) -> Option<usize> {
    let mut j = i + 1;
    loop {
        match *bytes.get(j)? {
            b'\\' => j += 2,
            b'"' => return Some(j + 1),
            _ => j += 1,
        }
    }
}

/// The end of the array or object that starts at `i`, with strings skipped
/// and brackets matched by kind.
fn container_end(bytes: &[u8], i: usize) -> Option<usize> {
    let mut stack: Vec<u8> = Vec::new();
    let mut j = i;
    while let Some(&b) = bytes.get(j) {
        match b {
            b'"' => {
                j = string_end(bytes, j)?;
                continue;
            }
            b'[' | b'{' => {
                if stack.len() >= MAX_JSON_DEPTH {
                    return None;
                }
                stack.push(if b == b'[' { b']' } else { b'}' });
            }
            b']' | b'}' => {
                if stack.pop() != Some(b) {
                    return None;
                }
                if stack.is_empty() {
                    return Some(j + 1);
                }
            }
            _ => {}
        }
        j += 1;
    }
    None
}

/// The end of the value (string, array, object or bare token) at `i`.
fn value_end(bytes: &[u8], i: usize) -> Option<usize> {
    match *bytes.get(i)? {
        b'"' => string_end(bytes, i),
        b'[' | b'{' => container_end(bytes, i),
        _ => {
            let mut j = i;
            while let Some(&b) = bytes.get(j) {
                match b {
                    b',' | b']' => break,
                    _ if is_ws(b) => break,
                    b'"' | b'[' | b'{' | b'}' | b':' => return None,
                    _ => j += 1,
                }
            }
            (j > i).then_some(j)
        }
    }
}

/// If `bytes` is one complete JSON array, optionally surrounded by
/// whitespace, appends the byte range of each top-level element to `out` (the
/// element without the whitespace around it) and returns true. Otherwise
/// returns false and leaves `out` as it was.
///
/// What lies between the ranges is only whitespace, commas and the outer
/// brackets, so the ranges and the gaps between them tile the array. An empty
/// array adds no range. Anything after the closing bracket other than
/// whitespace, a missing bracket, an empty element (`[1,,2]`, `[1,]`), text
/// that is not an array, and nesting deeper than [`MAX_JSON_DEPTH`] all make
/// it return false. Never panics, on any bytes.
pub fn json_array_elements(bytes: &[u8], out: &mut Vec<Range<usize>>) -> bool {
    let base = out.len();
    let ok = scan(bytes, out);
    if !ok {
        out.truncate(base);
    }
    ok
}

fn scan(bytes: &[u8], out: &mut Vec<Range<usize>>) -> bool {
    let mut i = skip_ws(bytes, 0);
    if bytes.get(i) != Some(&b'[') {
        return false;
    }
    i = skip_ws(bytes, i + 1);
    if bytes.get(i) == Some(&b']') {
        return skip_ws(bytes, i + 1) == bytes.len();
    }
    loop {
        let start = skip_ws(bytes, i);
        let Some(end) = value_end(bytes, start) else {
            return false;
        };
        out.push(start..end);
        i = skip_ws(bytes, end);
        match bytes.get(i) {
            Some(b',') => i += 1,
            Some(b']') => return skip_ws(bytes, i + 1) == bytes.len(),
            _ => return false,
        }
    }
}

#[cfg(test)]
#[allow(clippy::single_range_in_vec_init)] // the tests list one-range cut sets on purpose
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    fn elements(s: &[u8]) -> Option<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        json_array_elements(s, &mut out)
            .then(|| out.iter().map(|r| s[r.clone()].to_vec()).collect())
    }

    fn strs(s: &str) -> Option<Vec<String>> {
        elements(s.as_bytes()).map(|v| {
            v.into_iter()
                .map(|e| String::from_utf8(e).unwrap())
                .collect()
        })
    }

    /// The ranges and the gaps between them tile the array: gaps hold only
    /// whitespace, commas and the outer brackets.
    fn assert_tiles(s: &[u8], cuts: &[Range<usize>]) {
        let mut at = 0;
        for c in cuts {
            assert!(
                c.start >= at && c.end > c.start && c.end <= s.len(),
                "{c:?}"
            );
            for &b in &s[at..c.start] {
                assert!(is_ws(b) || b == b',' || b == b'[', "gap byte {b:#x}");
            }
            at = c.end;
        }
        for &b in &s[at..] {
            assert!(
                is_ws(b) || b == b']' || (cuts.is_empty() && b == b'['),
                "tail byte {b:#x}"
            );
        }
    }

    #[test]
    fn plain_arrays() {
        assert_eq!(strs("[]"), Some(vec![]));
        assert_eq!(strs("  [ \n ]\t"), Some(vec![]));
        assert_eq!(strs("[1]"), Some(vec!["1".into()]));
        assert_eq!(
            strs(" [ 1 ,\n2\t, true,null , -3.5e+2 ] "),
            Some(vec![
                "1".into(),
                "2".into(),
                "true".into(),
                "null".into(),
                "-3.5e+2".into()
            ])
        );
    }

    #[test]
    fn strings_hide_structure() {
        let s = r#"["a,b", "]", "[", "q\"uote,", "back\\", "\\\"]", "}"]"#;
        assert_eq!(
            strs(s),
            Some(vec![
                r#""a,b""#.into(),
                r#""]""#.into(),
                r#""[""#.into(),
                r#""q\"uote,""#.into(),
                r#""back\\""#.into(),
                r#""\\\"]""#.into(),
                r#""}""#.into(),
            ])
        );
    }

    #[test]
    fn nested_values_are_one_element() {
        let s = r#"[{"role":"user","content":[1,[2,"]"],{"a":{"b":[]}}]}, [[[]]], {}]"#;
        assert_eq!(
            strs(s),
            Some(vec![
                r#"{"role":"user","content":[1,[2,"]"],{"a":{"b":[]}}]}"#.into(),
                "[[[]]]".into(),
                "{}".into()
            ])
        );
    }

    #[test]
    fn not_a_complete_array() {
        for s in [
            "",
            " ",
            "x",
            "{}",
            "\"[1]\"",
            "1",
            "[",
            "[1",
            "[1,",
            "[1,]",
            "[,]",
            "[1,,2]",
            "[1 2]",
            "[1]x",
            "[1] [2]",
            "[1]]",
            "[[1]",
            "[{]",
            "[\"a]",
            "[\"a\\\"]",
            "[{\"a\":1]",
            "[1:2]",
            "[a\"b\"]",
            "[}]",
        ] {
            assert_eq!(strs(s), None, "{s:?}");
        }
        let mut out = vec![5..6];
        assert!(!json_array_elements(b"[1,", &mut out));
        assert_eq!(out, [5..6]);
    }

    #[test]
    fn invalid_utf8_is_just_bytes() {
        let s = b"[\"\xff\xfe\", \xc3, 1]";
        let e = elements(s).unwrap();
        assert_eq!(
            e,
            [b"\"\xff\xfe\"".to_vec(), b"\xc3".to_vec(), b"1".to_vec()]
        );
        assert_eq!(elements(b"[\xff"), None);
    }

    #[test]
    fn very_deep_nesting_is_refused_not_overflowed() {
        let deep = |n: usize| format!("[{}{}]", "[".repeat(n), "]".repeat(n));
        assert!(elements(deep(MAX_JSON_DEPTH - 1).as_bytes()).is_some());
        assert_eq!(elements(deep(MAX_JSON_DEPTH + 10).as_bytes()), None);
        assert_eq!(elements("[".repeat(1_000_000).as_bytes()), None);
        let objs = format!("[{}]", "{\"a\":".repeat(10_000));
        assert_eq!(elements(objs.as_bytes()), None);
    }

    fn random_json(rng: &mut StdRng, depth: usize, out: &mut String) {
        match rng.random_range(0..if depth > 3 { 4 } else { 6 }) {
            0 => out.push_str(&rng.random_range(-1000..1000i32).to_string()),
            1 => out.push_str(["true", "false", "null"][rng.random_range(0..3usize)]),
            2 | 3 => {
                out.push('"');
                for _ in 0..rng.random_range(0..12) {
                    out.push_str(
                        ["a", " ", ",", "[", "]", "{", "}", "\\\"", "\\\\", "é", ":"]
                            [rng.random_range(0..11usize)],
                    );
                }
                out.push('"');
            }
            4 => {
                out.push('[');
                for i in 0..rng.random_range(0..4) {
                    if i > 0 {
                        out.push(',');
                    }
                    random_json(rng, depth + 1, out);
                }
                out.push(']');
            }
            _ => {
                out.push('{');
                for i in 0..rng.random_range(0..4) {
                    if i > 0 {
                        out.push(',');
                    }
                    out.push_str(&format!("\"k{i}\" : "));
                    random_json(rng, depth + 1, out);
                }
                out.push('}');
            }
        }
    }

    #[test]
    fn generated_arrays_give_back_their_elements() {
        let mut rng = StdRng::seed_from_u64(3);
        let ws = |rng: &mut StdRng| ["", " ", "\n", "\t ", "\r\n"][rng.random_range(0..5usize)];
        for _ in 0..2_000 {
            let n = rng.random_range(0..6);
            let mut want = Vec::new();
            let mut text = String::from(ws(&mut rng));
            text.push('[');
            for i in 0..n {
                if i > 0 {
                    text.push(',');
                }
                text.push_str(ws(&mut rng));
                let mut e = String::new();
                random_json(&mut rng, 0, &mut e);
                text.push_str(&e);
                want.push(e);
                text.push_str(ws(&mut rng));
            }
            text.push(']');
            text.push_str(ws(&mut rng));
            assert_eq!(strs(&text), Some(want), "{text}");
        }
    }

    #[test]
    fn mutated_input_never_panics_and_always_tiles() {
        let mut rng = StdRng::seed_from_u64(11);
        for _ in 0..5_000 {
            let mut text = String::from("[");
            for i in 0..rng.random_range(0..5) {
                if i > 0 {
                    text.push(',');
                }
                random_json(&mut rng, 0, &mut text);
            }
            text.push(']');
            let mut bytes = text.into_bytes();
            for _ in 0..rng.random_range(0..4) {
                if bytes.is_empty() {
                    break;
                }
                let at = rng.random_range(0..bytes.len());
                match rng.random_range(0..3) {
                    0 => bytes[at] = rng.random(),
                    1 => {
                        bytes.remove(at);
                    }
                    _ => bytes.truncate(at),
                }
            }
            let mut cuts = Vec::new();
            if json_array_elements(&bytes, &mut cuts) {
                assert_tiles(&bytes, &cuts);
            } else {
                assert!(cuts.is_empty());
            }
        }
        for _ in 0..2_000 {
            let n = rng.random_range(0..64usize);
            let bytes: Vec<u8> = (0..n).map(|_| rng.random()).collect();
            let mut cuts = Vec::new();
            if json_array_elements(&bytes, &mut cuts) {
                assert_tiles(&bytes, &cuts);
            }
        }
    }
}
