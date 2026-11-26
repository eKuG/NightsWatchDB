//! A small scanner that finds the elements of a JSON array in a byte string.
//!
//! The text of a model call is often a JSON array of messages. Storing each
//! message as its own piece lets the next call, which sends the same messages
//! again plus a new one, share all but the new one. The scanner must not be
//! fooled by text inside strings, so it understands strings, escapes and
//! nesting; it does not check that numbers or literals are well formed,
//! because the caller only needs to know where an element starts and ends.

use std::borrow::Cow;
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

// ------------------------------------------------------------ JSON as text

/// The end of the string whose opening quote is at `i`, or `None` unless the
/// string is valid: no raw control byte, and every escape is one of the
/// eleven JSON allows, `\u` followed by four hex digits.
fn valid_string_end(bytes: &[u8], i: usize) -> Option<usize> {
    let mut j = i + 1;
    loop {
        match *bytes.get(j)? {
            b'"' => return Some(j + 1),
            b'\\' => match *bytes.get(j + 1)? {
                b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => j += 2,
                b'u' => {
                    let hex = bytes.get(j + 2..j + 6)?;
                    if !hex.iter().all(u8::is_ascii_hexdigit) {
                        return None;
                    }
                    j += 6;
                }
                _ => return None,
            },
            0..=0x1f => return None,
            _ => j += 1,
        }
    }
}

/// The end of the number, `true`, `false` or `null` at `i`.
fn scalar_end(bytes: &[u8], i: usize) -> Option<usize> {
    for lit in [&b"true"[..], b"false", b"null"] {
        if bytes[i..].starts_with(lit) {
            return Some(i + lit.len());
        }
    }
    let digits = |mut j: usize| {
        let from = j;
        while bytes.get(j).is_some_and(u8::is_ascii_digit) {
            j += 1;
        }
        (j > from).then_some(j)
    };
    let mut j = i + usize::from(bytes[i] == b'-');
    j = if bytes.get(j) == Some(&b'0') {
        j + 1
    } else {
        digits(j)?
    };
    if bytes.get(j) == Some(&b'.') {
        j = digits(j + 1)?;
    }
    if matches!(bytes.get(j), Some(b'e' | b'E')) {
        j += 1;
        if matches!(bytes.get(j), Some(b'+' | b'-')) {
            j += 1;
        }
        j = digits(j)?;
    }
    Some(j)
}

/// What the validator expects next.
#[derive(Clone, Copy, PartialEq)]
enum Expect {
    /// A value (after a colon, or a comma in an array).
    Value,
    /// A value, or the `]` of an empty array.
    ValueOrEnd,
    /// A key (after a comma in an object).
    Key,
    /// A key, or the `}` of an empty object.
    KeyOrEnd,
    Colon,
    /// A comma or the closing bracket of the innermost container.
    After,
}

/// True if `bytes` are exactly one valid JSON value that is an object, an
/// array or a string, optionally surrounded by whitespace. Bytes at or above
/// `0x80` inside a string are not checked to be UTF-8.
///
/// Takes time linear in the length and no heap memory: the nesting is a bit
/// per level in a fixed array, and nesting deeper than [`MAX_JSON_DEPTH`] is
/// "no". Never panics, on any bytes.
pub fn is_json_text(bytes: &[u8]) -> bool {
    let mut is_object = [0u64; MAX_JSON_DEPTH / 64];
    let mut depth = 0usize;
    let mut i = skip_ws(bytes, 0);
    match bytes.get(i) {
        Some(b'"') => {
            return valid_string_end(bytes, i).is_some_and(|e| skip_ws(bytes, e) == bytes.len());
        }
        Some(b'[' | b'{') => {}
        _ => return false,
    }
    let mut expect = Expect::Value;
    loop {
        i = skip_ws(bytes, i);
        let Some(&b) = bytes.get(i) else {
            return false;
        };
        let in_object = depth > 0 && (is_object[(depth - 1) / 64] >> ((depth - 1) % 64)) & 1 == 1;
        let mut close = false;
        match (expect, b) {
            (Expect::ValueOrEnd, b']') | (Expect::KeyOrEnd, b'}') => close = true,
            (Expect::After, b']') if !in_object => close = true,
            (Expect::After, b'}') if in_object => close = true,
            (Expect::After, b',') => {
                i += 1;
                expect = if in_object {
                    Expect::Key
                } else {
                    Expect::Value
                };
            }
            (Expect::Key | Expect::KeyOrEnd, b'"') => {
                let Some(e) = valid_string_end(bytes, i) else {
                    return false;
                };
                i = e;
                expect = Expect::Colon;
            }
            (Expect::Colon, b':') => {
                i += 1;
                expect = Expect::Value;
            }
            (Expect::Value | Expect::ValueOrEnd, b'[' | b'{') => {
                if depth == MAX_JSON_DEPTH {
                    return false;
                }
                let (word, bit) = (depth / 64, depth % 64);
                is_object[word] = (is_object[word] & !(1 << bit)) | (u64::from(b == b'{') << bit);
                depth += 1;
                i += 1;
                expect = if b == b'[' {
                    Expect::ValueOrEnd
                } else {
                    Expect::KeyOrEnd
                };
            }
            (Expect::Value | Expect::ValueOrEnd, b'"') => {
                let Some(e) = valid_string_end(bytes, i) else {
                    return false;
                };
                i = e;
                expect = Expect::After;
            }
            (Expect::Value | Expect::ValueOrEnd, _) => {
                let Some(e) = scalar_end(bytes, i) else {
                    return false;
                };
                i = e;
                expect = Expect::After;
            }
            _ => return false,
        }
        if close {
            depth -= 1;
            i += 1;
            if depth == 0 {
                return skip_ws(bytes, i) == bytes.len();
            }
            expect = Expect::After;
        }
    }
}

/// The value of the four hex digits at the start of `hex`.
fn hex4(hex: &[u8]) -> Option<u32> {
    let mut v = 0u32;
    for &h in hex.get(..4)? {
        v = (v << 4) | char::from(h).to_digit(16)?;
    }
    Some(v)
}

/// The text of a piece as it is searched.
///
/// A piece that is [`is_json_text`] is searched with the escapes of its strings
/// decoded: `\n`, `\t`, `\r`, `\b` and `\f` become a space, `\"`, `\\` and `\/`
/// the character itself, `\uXXXX` the UTF-8 of the code point, and a pair of
/// `\u` escapes that is a surrogate pair the one code point it stands for.
/// The six bytes of a lone surrogate are left as they are. Any other piece,
/// and any piece with no backslash, is returned as it is, without a copy.
///
/// It is a function of the bytes alone, so the text that is indexed and the
/// text a phrase is confirmed against are the same. The stored bytes are not
/// changed.
pub fn json_search_text(bytes: &[u8]) -> Cow<'_, [u8]> {
    if !bytes.contains(&b'\\') || !is_json_text(bytes) {
        return Cow::Borrowed(bytes);
    }
    // Valid JSON has a backslash only inside a string, and no escape decodes
    // to more bytes than it takes, so the output fits in the input's length.
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b != b'\\' {
            out.push(b);
            i += 1;
            continue;
        }
        // the validator has checked that every escape is complete and valid
        match bytes[i + 1] {
            b'n' | b't' | b'r' | b'b' | b'f' => out.push(b' '),
            c @ (b'"' | b'\\' | b'/') => out.push(c),
            _ => {
                let unit = hex4(&bytes[i + 2..]).unwrap_or(0xfffd);
                let mut len = 6;
                let ch = match unit {
                    0xd800..=0xdbff => bytes[i + 6..]
                        .strip_prefix(b"\\u")
                        .and_then(hex4)
                        .filter(|l| (0xdc00..=0xdfff).contains(l))
                        .map(|l| {
                            len = 12;
                            0x10000 + ((unit - 0xd800) << 10) + (l - 0xdc00)
                        }),
                    0xdc00..=0xdfff => None,
                    _ => Some(unit),
                };
                match ch.and_then(char::from_u32) {
                    Some(c) => out.extend_from_slice(c.encode_utf8(&mut [0; 4]).as_bytes()),
                    None => out.extend_from_slice(&bytes[i..i + 6]),
                }
                i += len;
                continue;
            }
        }
        i += 2;
    }
    Cow::Owned(out)
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

    fn decoded(s: &str) -> String {
        String::from_utf8(json_search_text(s.as_bytes()).into_owned()).unwrap()
    }

    #[test]
    fn json_text_is_an_object_an_array_or_a_string() {
        for s in [
            "{}",
            " [] ",
            "\"x\"",
            "\n\t{\"a\": [1, -2.5e+3, true, false, null, \"\\u00e9\"], \"b\": {\"c\": []}}\r\n",
            "[[], {}, [[{}]]]",
            "[0, 0.5, 10, 1E5]",
            "\"caf\u{e9}\"",
        ] {
            assert!(is_json_text(s.as_bytes()), "{s:?}");
        }
        for s in [
            "",
            " ",
            "1",
            "true",
            "null",
            "-",
            "[",
            "]",
            "{",
            "}",
            "[1,]",
            "[,1]",
            "[1 2]",
            "{\"a\"}",
            "{\"a\":}",
            "{\"a\":1,}",
            "{a:1}",
            "{\"a\" 1}",
            "{1:1}",
            "[01]",
            "[1.]",
            "[.5]",
            "[1e]",
            "[+1]",
            "[tru]",
            "[truex]",
            "[nul]",
            "\"a",
            "\"a\" \"b\"",
            "[1] x",
            "[1]]",
            "[[1]",
            "{\"a\":1]",
            "[1}",
            "\"\\q\"",
            "\"\\u12\"",
            "\"\\u12g4\"",
            "\"a\nb\"",
            "\"a\u{1}b\"",
            "'a'",
            "{\"a\":1}{}",
            "[\"a\"\"b\"]",
        ] {
            assert!(!is_json_text(s.as_bytes()), "{s:?}");
        }
    }

    #[test]
    fn json_text_nesting_is_bounded_and_does_not_recurse() {
        let deep = |n: usize| format!("{}{}", "[".repeat(n), "]".repeat(n));
        assert!(is_json_text(deep(MAX_JSON_DEPTH).as_bytes()));
        assert!(!is_json_text(deep(MAX_JSON_DEPTH + 1).as_bytes()));
        assert!(!is_json_text("[".repeat(5_000_000).as_bytes()));
        let objs = format!(
            "{}1{}",
            "{\"a\":".repeat(MAX_JSON_DEPTH + 5),
            "}".repeat(10)
        );
        assert!(!is_json_text(objs.as_bytes()));
        // mixed kinds at full depth are matched by kind
        let mixed = "[{\"a\":".repeat(MAX_JSON_DEPTH / 2) + "1" + &"}]".repeat(MAX_JSON_DEPTH / 2);
        assert!(is_json_text(mixed.as_bytes()));
        let wrong = "[{\"a\":".repeat(3) + "1" + &"]}".repeat(3);
        assert!(!is_json_text(wrong.as_bytes()));
    }

    #[test]
    fn escapes_are_decoded_in_json_text() {
        assert_eq!(decoded(r#""a\nb\tc\rd\be\ff""#), "\"a b c d e f\"");
        assert_eq!(
            decoded(r#"["q\"q", "b\\b", "s\/s"]"#),
            r#"["q"q", "b\b", "s/s"]"#
        );
        assert_eq!(
            decoded(r#"["caf\u00e9", "\u65e5\u672c", "\u0041"]"#),
            "[\"caf\u{e9}\", \"\u{65e5}\u{672c}\", \"A\"]"
        );
        // a surrogate pair is one code point
        assert_eq!(decoded(r#"["\ud83d\ude00 x"]"#), "[\"\u{1f600} x\"]");
        assert_eq!(decoded(r#"["\uD83D\uDE00"]"#), "[\"\u{1f600}\"]");
    }

    #[test]
    fn lone_surrogates_stay_as_they_were_written() {
        let s = r#"["\ud83d", "\ude00", "\ud83dx", "\ud83d\u0041", "\ude00\ud83d"]"#;
        assert_eq!(decoded(s), s.replace("\\u0041", "A"));
    }

    #[test]
    fn a_piece_that_is_not_json_text_or_has_no_backslash_is_not_copied() {
        for s in [
            &b"C:\\new\\table"[..],
            b"{\"a\": \"x\\ny\"",
            b"[\"\\q\"]",
            b"12\\n",
            b"plain text",
            b"{\"a\": 1}",
            b"\xff\\n\xfe",
        ] {
            assert!(
                matches!(json_search_text(s), Cow::Borrowed(b) if b == s),
                "{s:?}"
            );
        }
        assert!(matches!(json_search_text(b"[\"a\\nb\"]"), Cow::Owned(_)));
    }

    #[test]
    fn random_bytes_never_panic_and_the_text_is_never_longer() {
        let mut rng = StdRng::seed_from_u64(5);
        let alphabet: &[u8] = b"[]{}\":,\\unrtbf/ 0123456789eE.-+aAdD\x00\xff\xc3\xa9";
        for _ in 0..20_000 {
            let n = rng.random_range(0..40usize);
            let b: Vec<u8> = (0..n)
                .map(|_| alphabet[rng.random_range(0..alphabet.len())])
                .collect();
            let t = json_search_text(&b);
            assert!(t.len() <= b.len());
            // the same bytes always give the same text
            assert_eq!(t, json_search_text(&b));
            if !is_json_text(&b) {
                assert_eq!(&*t, &b[..]);
            }
        }
        for _ in 0..5_000 {
            let n = rng.random_range(0..200usize);
            let b: Vec<u8> = (0..n).map(|_| rng.random()).collect();
            let _ = json_search_text(&b);
        }
    }

    #[test]
    fn generated_json_is_json_text_and_decoding_keeps_it_one_piece() {
        let mut rng = StdRng::seed_from_u64(9);
        for _ in 0..2_000 {
            let mut s = String::new();
            random_json(&mut rng, 0, &mut s);
            let is_container_or_string =
                !matches!(s.as_bytes()[0], b'-' | b't' | b'f' | b'n' | b'0'..=b'9');
            assert_eq!(is_json_text(s.as_bytes()), is_container_or_string, "{s}");
        }
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
