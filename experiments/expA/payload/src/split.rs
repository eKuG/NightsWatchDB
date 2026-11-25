//! Finds the top-level element boundaries of a JSON array by scanning bytes.
//! Nothing is parsed or re-serialised: an element is the exact byte range between
//! the separators, so whitespace stays inside the element it touches.

/// Element ranges of `v` if it is an array starting with `[` and ending with the matching `]`
/// and the elements joined by `,` rebuild it exactly; otherwise None.
pub fn split_array(v: &[u8]) -> Option<Vec<(usize, usize)>> {
    if v.len() < 2 || v[0] != b'[' || v[v.len() - 1] != b']' {
        return None;
    }
    let mut out: Vec<(usize, usize)> = vec![];
    let (mut depth, mut start, mut in_str, mut esc) = (1i32, 1usize, false, false);
    let mut closed = false;
    for (i, &b) in v.iter().enumerate().skip(1) {
        if closed {
            return None; // bytes after the closing bracket
        }
        if in_str {
            if esc {
                esc = false;
            } else if b == b'\\' {
                esc = true;
            } else if b == b'"' {
                in_str = false;
            }
            continue;
        }
        match b {
            b'"' => in_str = true,
            b'[' | b'{' => depth += 1,
            b']' | b'}' => {
                depth -= 1;
                if depth == 0 {
                    if i != v.len() - 1 || b != b']' {
                        return None;
                    }
                    if !(start == i && out.is_empty()) {
                        out.push((start, i));
                    }
                    closed = true;
                }
            }
            b',' if depth == 1 => {
                out.push((start, i));
                start = i + 1;
            }
            _ => {}
        }
    }
    if !closed || !rebuilds(v, &out) {
        return None;
    }
    Some(out)
}

/// `[` + elements joined by `,` + `]` equals `v` byte for byte.
pub fn rebuilds(v: &[u8], parts: &[(usize, usize)]) -> bool {
    let mut at = 0usize;
    let eq = |s: &[u8], at: &mut usize| {
        let ok = v.get(*at..*at + s.len()) == Some(s);
        *at += s.len();
        ok
    };
    if !eq(b"[", &mut at) {
        return false;
    }
    for (i, &(s, e)) in parts.iter().enumerate() {
        if i > 0 && !eq(b",", &mut at) {
            return false;
        }
        if s > e || e > v.len() || !eq(&v[s..e], &mut at) {
            return false;
        }
    }
    eq(b"]", &mut at) && at == v.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parts(v: &str) -> Option<Vec<String>> {
        split_array(v.as_bytes()).map(|p| p.iter().map(|&(s, e)| v[s..e].to_string()).collect())
    }

    #[test]
    fn simple_and_nested() {
        assert_eq!(parts(r#"[{"a":1},{"b":[1,2,{"c":3}]},"x"]"#).unwrap(), [r#"{"a":1}"#, r#"{"b":[1,2,{"c":3}]}"#, r#""x""#]);
        assert_eq!(parts("[[1,2],[3,[4,5]]]").unwrap(), ["[1,2]", "[3,[4,5]]"]);
    }

    #[test]
    fn strings_with_separators_and_escapes() {
        let v = r#"[{"t":"a,b]c[d}"},"q\"uo,te","back\\",{"k":"\\\",]"}]"#;
        let p = parts(v).unwrap();
        assert_eq!(p.len(), 4);
        assert_eq!(p[1], r#""q\"uo,te""#);
        assert_eq!(p[2], r#""back\\""#);
    }

    #[test]
    fn whitespace_stays_in_elements() {
        let v = "[ {\"a\":1} , {\"b\":2}\n,\t3 ]";
        let p = parts(v).unwrap();
        assert_eq!(p, [" {\"a\":1} ", " {\"b\":2}\n", "\t3 "]);
        assert!(rebuilds(v.as_bytes(), &split_array(v.as_bytes()).unwrap()));
    }

    #[test]
    fn empty_and_degenerate() {
        assert_eq!(parts("[]").unwrap(), Vec::<String>::new());
        assert_eq!(parts("[ ]").unwrap(), [" "]);
        assert_eq!(parts("[,]").unwrap(), ["", ""]);
        assert_eq!(parts("[1,]").unwrap(), ["1", ""]);
    }

    #[test]
    fn not_arrays_fall_back() {
        for v in ["", "[", "]", "{}", "\"[1,2]\"", "[1,2", "1,2]", " [1]", "[1] ", "[1]]", "[1}", "[{]", "[\"unterminated]", "plain text"] {
            assert!(split_array(v.as_bytes()).is_none(), "{v:?}");
        }
    }

    #[test]
    fn non_utf8_bytes_are_fine() {
        let v = [b'[', 0xff, b',', 0xfe, 0x00, b']'];
        assert_eq!(split_array(&v).unwrap(), [(1, 2), (3, 5)]);
    }
}
