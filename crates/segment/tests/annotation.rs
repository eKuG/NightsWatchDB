//! The annotation record and the log record body, through the public API.

use std::collections::BTreeSet;

use nwdb_core::{Error, SpanId, TraceId};
use nwdb_segment::{
    ANNOTATION_FORMAT_VERSION, Annotation, MAX_BATCH_ITEMS, MAX_COMMENT_BYTES, MAX_LABEL_BYTES,
    MAX_NAME_BYTES, MAX_SOURCE_BYTES, check_annotation_format_version, decode_batch, encode_batch,
};
use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};

fn base() -> Annotation {
    Annotation {
        trace_id: TraceId([1; 16]),
        span_id: None,
        name: "quality".into(),
        source: "judge".into(),
        time_ns: 1_700_000_000_000_000_000,
        value: Some(0.5),
        label: None,
        comment: None,
        delete: false,
        arrival: 0,
    }
}

fn delete() -> Annotation {
    Annotation {
        value: None,
        delete: true,
        ..base()
    }
}

fn bytes(a: &Annotation) -> Vec<u8> {
    let mut out = Vec::new();
    encode_batch(std::slice::from_ref(a), &mut out).unwrap();
    out
}

fn is_corrupt<T>(r: Result<T, Error>) -> bool {
    matches!(r, Err(Error::Corrupt(_)))
}

fn refused(a: &Annotation) -> String {
    a.validate().expect_err("should be refused")
}

#[test]
fn a_plain_annotation_is_valid() {
    assert_eq!(base().validate(), Ok(()));
    assert_eq!(delete().validate(), Ok(()));
}

#[test]
fn ids_are_not_all_zeros() {
    let mut a = base();
    a.trace_id = TraceId([0; 16]);
    assert!(refused(&a).contains("trace id"));
    let mut a = base();
    a.span_id = Some(SpanId([0; 8]));
    assert!(refused(&a).contains("span id"));
    a.span_id = Some(SpanId([0, 0, 0, 0, 0, 0, 0, 1]));
    assert_eq!(a.validate(), Ok(()));
}

#[test]
fn name_edges() {
    let mut a = base();
    a.name = String::new();
    assert!(refused(&a).contains("name"));
    a.name = "n".into();
    assert_eq!(a.validate(), Ok(()));
    a.name = "n".repeat(MAX_NAME_BYTES);
    assert_eq!(a.validate(), Ok(()));
    a.name = "n".repeat(MAX_NAME_BYTES + 1);
    assert!(refused(&a).contains("name"));
    // Bytes, not characters: 64 two-byte characters fit, 65 do not.
    a.name = "é".repeat(64);
    assert_eq!(a.validate(), Ok(()));
    a.name = "é".repeat(65);
    assert!(refused(&a).contains("name"));
    for bad in ["a\nb", "a\tb", "a\u{0}b", "a\u{7f}b", "a\u{85}b"] {
        a.name = bad.into();
        assert!(refused(&a).contains("control"), "{bad:?}");
    }
}

#[test]
fn source_edges() {
    let mut a = base();
    a.source = String::new();
    assert_eq!(a.validate(), Ok(()));
    a.source = "s".repeat(MAX_SOURCE_BYTES);
    assert_eq!(a.validate(), Ok(()));
    a.source = "s".repeat(MAX_SOURCE_BYTES + 1);
    assert!(refused(&a).contains("source"));
    a.source = "a\tb".into();
    assert!(refused(&a).contains("control"));
}

#[test]
fn label_edges() {
    let mut a = base();
    a.label = Some(String::new());
    assert!(refused(&a).contains("label"));
    a.label = Some("l".into());
    assert_eq!(a.validate(), Ok(()));
    a.label = Some("l".repeat(MAX_LABEL_BYTES));
    assert_eq!(a.validate(), Ok(()));
    a.label = Some("l".repeat(MAX_LABEL_BYTES + 1));
    assert!(refused(&a).contains("label"));
    a.label = Some("a\nb".into());
    assert!(refused(&a).contains("control"));
}

#[test]
fn comment_edges() {
    let mut a = base();
    a.comment = Some(String::new());
    assert!(refused(&a).contains("comment"));
    a.comment = Some("c".into());
    assert_eq!(a.validate(), Ok(()));
    a.comment = Some("c".repeat(MAX_COMMENT_BYTES));
    assert_eq!(a.validate(), Ok(()));
    a.comment = Some("c".repeat(MAX_COMMENT_BYTES + 1));
    assert!(refused(&a).contains("comment"));
    a.comment = Some("line one\n\tline two".into());
    assert_eq!(a.validate(), Ok(()));
    for bad in ["a\rb", "a\u{0}b", "a\u{1b}b", "a\u{7f}b"] {
        a.comment = Some(bad.into());
        assert!(refused(&a).contains("control"), "{bad:?}");
    }
}

#[test]
fn value_must_be_finite() {
    let mut a = base();
    for ok in [0.0, -0.0, 1e308, f64::MIN_POSITIVE, f64::MAX, f64::MIN] {
        a.value = Some(ok);
        assert_eq!(a.validate(), Ok(()), "{ok}");
    }
    for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        a.value = Some(bad);
        assert!(refused(&a).contains("finite"), "{bad}");
    }
}

#[test]
fn a_non_delete_needs_a_value_or_a_label_and_a_comment_alone_is_not_enough() {
    let mut a = base();
    a.value = None;
    assert!(refused(&a).contains("value or a label"));
    a.comment = Some("only a comment".into());
    assert!(refused(&a).contains("value or a label"));
    a.label = Some("good".into());
    assert_eq!(a.validate(), Ok(()));
    a.value = Some(1.0);
    assert_eq!(a.validate(), Ok(()));
}

#[test]
fn a_delete_carries_nothing() {
    assert_eq!(delete().validate(), Ok(()));
    let mut d = delete();
    d.value = Some(1.0);
    assert!(refused(&d).contains("delete"));
    let mut d = delete();
    d.label = Some("x".into());
    assert!(refused(&d).contains("delete"));
    let mut d = delete();
    d.comment = Some("x".into());
    assert!(refused(&d).contains("delete"));
}

fn random_text(
    rng: &mut StdRng,
    lens: std::ops::RangeInclusive<usize>,
    allow_newline: bool,
) -> String {
    let len = rng.random_range(lens);
    const POOL: [char; 8] = ['a', 'Z', '0', ' ', 'é', '日', '😀', '-'];
    let mut s = String::new();
    while s.len() < len {
        let c = if allow_newline && rng.random_range(0..10) == 0 {
            if rng.random() { '\n' } else { '\t' }
        } else {
            POOL[rng.random_range(0..POOL.len())]
        };
        if s.len() + c.len_utf8() > len {
            s.push('x');
        } else {
            s.push(c);
        }
    }
    s
}

fn random_annotation(rng: &mut StdRng) -> Annotation {
    let mut trace = [0u8; 16];
    // Few distinct traces and spans, so identities do collide.
    trace[15] = rng.random_range(1..4);
    let span_id = if rng.random() {
        let mut s = [0u8; 8];
        s[7] = rng.random_range(1..4);
        Some(SpanId(s))
    } else {
        None
    };
    let delete = rng.random_range(0..4) == 0;
    let mut a = Annotation {
        trace_id: TraceId(trace),
        span_id,
        name: random_text(rng, 1..=3, false),
        source: random_text(rng, 0..=2, false),
        time_ns: rng.random(),
        value: None,
        label: None,
        comment: None,
        delete,
        arrival: 0,
    };
    if !delete {
        let (has_value, has_label) = match rng.random_range(0..3) {
            0 => (true, false),
            1 => (false, true),
            _ => (true, true),
        };
        if has_value {
            a.value = Some(match rng.random_range(0..4) {
                0 => 0.0,
                1 => f64::MAX,
                2 => -f64::MIN_POSITIVE,
                _ => f64::from_bits(rng.random::<u64>() & !(0x7ff << 52)),
            });
        }
        if has_label {
            a.label = Some(random_text(rng, 1..=40, false));
        }
        if rng.random() {
            a.comment = Some(random_text(rng, 1..=300, true));
        }
    }
    a
}

fn random_batch(rng: &mut StdRng) -> Vec<Annotation> {
    let want = rng.random_range(1..=12);
    let mut seen = BTreeSet::new();
    let mut items = Vec::new();
    for _ in 0..want * 3 {
        let a = random_annotation(rng);
        if seen.insert((a.trace_id, a.span_id, a.name.clone(), a.source.clone())) {
            items.push(a);
        }
        if items.len() == want {
            break;
        }
    }
    items
}

#[test]
fn random_valid_batches_round_trip() {
    let mut rng = StdRng::seed_from_u64(7);
    let mut flag_sets = BTreeSet::new();
    for round in 0..500u64 {
        let items = random_batch(&mut rng);
        for a in &items {
            a.validate().unwrap();
            flag_sets.insert((
                a.span_id.is_some(),
                a.delete,
                a.value.is_some(),
                a.label.is_some(),
                a.comment.is_some(),
            ));
        }
        let mut body = Vec::new();
        encode_batch(&items, &mut body).unwrap();
        let back = decode_batch(&body, round + 1).unwrap();
        assert_eq!(back.len(), items.len());
        for (b, a) in back.iter().zip(&items) {
            assert_eq!(b.arrival, round + 1);
            assert_eq!(
                *b,
                Annotation {
                    arrival: round + 1,
                    ..a.clone()
                }
            );
        }
    }
    // 2 span flags x (a delete; not a delete: 3 value/label choices x 2
    // comment choices) = 2 x 7.
    assert_eq!(flag_sets.len(), 14, "{flag_sets:?}");
}

#[test]
fn the_largest_record_round_trips() {
    let a = Annotation {
        trace_id: TraceId([0xff; 16]),
        span_id: Some(SpanId([0xff; 8])),
        name: "n".repeat(MAX_NAME_BYTES),
        source: "s".repeat(MAX_SOURCE_BYTES),
        time_ns: i64::MIN,
        value: Some(f64::MIN),
        label: Some("l".repeat(MAX_LABEL_BYTES)),
        comment: Some("c".repeat(MAX_COMMENT_BYTES)),
        delete: false,
        arrival: 0,
    };
    let body = bytes(&a);
    assert_eq!(
        body.len(),
        2 + 16 + 1 + 8 + 8 + 1 + 128 + 1 + 128 + 8 + 2 + 256 + 2 + 4096
    );
    assert_eq!(
        decode_batch(&body, 9).unwrap(),
        vec![Annotation { arrival: 9, ..a }]
    );
}

#[test]
fn the_smallest_record_and_known_bytes() {
    let a = Annotation {
        trace_id: TraceId([1; 16]),
        span_id: None,
        name: "n".into(),
        source: String::new(),
        time_ns: 1,
        value: Some(1.0),
        label: None,
        comment: None,
        delete: false,
        arrival: 0,
    };
    let mut want = vec![1, 0];
    want.extend([1u8; 16]);
    want.push(4);
    want.extend(1i64.to_le_bytes());
    want.extend([1, b'n', 0]);
    want.extend(1.0f64.to_bits().to_le_bytes());
    assert_eq!(bytes(&a), want);

    let d = Annotation {
        trace_id: TraceId([1; 16]),
        span_id: Some(SpanId([2; 8])),
        name: "n".into(),
        source: "s".into(),
        time_ns: -1,
        value: None,
        label: None,
        comment: None,
        delete: true,
        arrival: 0,
    };
    let mut want = vec![1, 0];
    want.extend([1u8; 16]);
    want.push(1 | 2);
    want.extend([2u8; 8]);
    want.extend((-1i64).to_le_bytes());
    want.extend([1, b'n', 1, b's']);
    assert_eq!(bytes(&d), want);
}

#[test]
fn encode_refuses_what_validate_refuses_a_bad_count_and_repeats() {
    let mut out = vec![9];
    let mut bad = base();
    bad.name = String::new();
    assert!(matches!(
        encode_batch(&[bad], &mut out),
        Err(Error::InvalidArgument(_))
    ));
    assert!(matches!(
        encode_batch(&[], &mut out),
        Err(Error::InvalidArgument(_))
    ));
    assert_eq!(out, [9], "nothing is appended on an error");

    let many: Vec<Annotation> = (0..=MAX_BATCH_ITEMS as u16)
        .map(|i| {
            let mut a = base();
            a.name = format!("n{i}");
            a
        })
        .collect();
    assert!(encode_batch(&many[..MAX_BATCH_ITEMS + 1], &mut out).is_err());
    out.clear();
    encode_batch(&many[..MAX_BATCH_ITEMS], &mut out).unwrap();
    assert_eq!(decode_batch(&out, 1).unwrap().len(), MAX_BATCH_ITEMS);

    // Same identity twice, even with different content.
    let mut other = base();
    other.value = Some(2.0);
    assert!(encode_batch(&[base(), other], &mut Vec::new()).is_err());
    // Differing in any one part of the identity is fine.
    let mut a = base();
    a.span_id = Some(SpanId([1; 8]));
    let mut b = base();
    b.source = "other".into();
    let mut c = base();
    c.name = "other".into();
    let mut d = base();
    d.trace_id = TraceId([2; 16]);
    encode_batch(&[base(), a, b, c, d], &mut Vec::new()).unwrap();
}

#[test]
fn a_repeated_identity_in_a_body_is_corrupt() {
    let one = bytes(&base());
    let mut body = vec![2, 0];
    body.extend_from_slice(&one[2..]);
    body.extend_from_slice(&one[2..]);
    assert!(is_corrupt(decode_batch(&body, 1)));
    // Not next to each other either.
    let mut other = base();
    other.name = "other".into();
    let two = bytes(&other);
    let mut body = vec![3, 0];
    body.extend_from_slice(&one[2..]);
    body.extend_from_slice(&two[2..]);
    body.extend_from_slice(&one[2..]);
    assert!(is_corrupt(decode_batch(&body, 1)));
}

#[test]
fn a_count_of_zero_or_over_the_limit_is_corrupt() {
    let one = bytes(&base());
    let mut zero = one.clone();
    zero[0] = 0;
    assert!(is_corrupt(decode_batch(&zero, 1)));
    assert!(is_corrupt(decode_batch(&[0, 0], 1)));
    let mut over = one.clone();
    over[..2].copy_from_slice(&((MAX_BATCH_ITEMS as u16) + 1).to_le_bytes());
    assert!(is_corrupt(decode_batch(&over, 1)));
    // A huge count with a tiny body fails without reserving for it.
    assert!(is_corrupt(decode_batch(&[0xff, 0xff], 1)));
    let mut huge = one;
    huge[..2].copy_from_slice(&1000u16.to_le_bytes());
    assert!(is_corrupt(decode_batch(&huge, 1)));
}

#[test]
fn trailing_bytes_are_corrupt() {
    let mut body = bytes(&base());
    body.push(0);
    assert!(is_corrupt(decode_batch(&body, 1)));
}

#[test]
fn every_truncation_is_corrupt() {
    let mut a = base();
    a.span_id = Some(SpanId([3; 8]));
    a.label = Some("good".into());
    a.comment = Some("because\nof this".into());
    let mut b = base();
    b.name = "other".into();
    b.value = None;
    b.delete = true;
    let mut body = Vec::new();
    encode_batch(&[a, b], &mut body).unwrap();
    assert!(decode_batch(&body, 1).is_ok());
    for len in 0..body.len() {
        assert!(is_corrupt(decode_batch(&body[..len], 1)), "length {len}");
    }
}

#[test]
fn every_single_bit_flip_is_corrupt_or_a_valid_batch_that_encodes_back() {
    let mut a = base();
    a.span_id = Some(SpanId([3; 8]));
    a.label = Some("good".into());
    a.comment = Some("why".into());
    let mut b = base();
    b.name = "x".into();
    b.source = String::new();
    b.value = None;
    b.delete = true;
    let mut body = Vec::new();
    encode_batch(&[a, b], &mut body).unwrap();
    let (mut corrupt, mut valid) = (0, 0);
    for bit in 0..body.len() * 8 {
        let mut flipped = body.clone();
        flipped[bit / 8] ^= 1 << (bit % 8);
        match decode_batch(&flipped, 5) {
            Err(Error::Corrupt(_)) => corrupt += 1,
            Err(e) => panic!("bit {bit}: unexpected {e}"),
            Ok(items) => {
                valid += 1;
                for i in &items {
                    assert_eq!(i.validate(), Ok(()), "bit {bit}");
                }
                let mut again = Vec::new();
                encode_batch(&items, &mut again).unwrap();
                assert_eq!(again, flipped, "bit {bit}");
            }
        }
    }
    // Flips in ids, times, values and the text decode fine.
    assert!(corrupt > 0 && valid > 0, "{corrupt} {valid}");
}

#[test]
fn random_bytes_never_panic() {
    let mut rng = StdRng::seed_from_u64(99);
    for i in 0..10_000 {
        let len = rng.random_range(0..200);
        let mut body = vec![0u8; len];
        rng.fill_bytes(&mut body);
        if i % 2 == 0 && len >= 2 {
            // A plausible count, so decoding goes further than the header.
            body[..2].copy_from_slice(&1u16.to_le_bytes());
        }
        if i % 4 == 0 && len >= 19 {
            body[18] &= 0x1f;
        }
        let _ = decode_batch(&body, 1);
    }
}

/// Offset of the value in the body of `base()` with an empty source and the
/// name `n`: count, trace id, flags, time, two length bytes and the name.
const VALUE_AT: usize = 2 + 16 + 1 + 8 + 1 + 1 + 1;

#[test]
fn nan_and_infinities_are_refused_by_the_decoder() {
    let mut a = base();
    a.name = "n".into();
    a.source = String::new();
    let body = bytes(&a);
    assert_eq!(body[VALUE_AT..], 0.5f64.to_bits().to_le_bytes());
    for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let mut b = body.clone();
        b[VALUE_AT..].copy_from_slice(&bad.to_bits().to_le_bytes());
        assert!(is_corrupt(decode_batch(&b, 1)), "{bad}");
    }
    let mut b = body.clone();
    b[VALUE_AT..].copy_from_slice(&f64::MAX.to_bits().to_le_bytes());
    assert!(decode_batch(&b, 1).is_ok());
}

#[test]
fn a_delete_with_a_value_is_refused_by_the_decoder() {
    let mut body = bytes(&base());
    assert_eq!(body[18], 4);
    body[18] = 4 | 2;
    assert!(is_corrupt(decode_batch(&body, 1)));
    // The delete without the value flag, and without its bytes, is fine.
    let d = bytes(&delete());
    assert_eq!(d[18], 2);
    assert!(decode_batch(&d, 1).unwrap()[0].delete);
}

#[test]
fn a_reserved_flag_bit_is_corrupt() {
    let body = bytes(&base());
    for bit in 5..8 {
        let mut b = body.clone();
        b[18] |= 1 << bit;
        assert!(is_corrupt(decode_batch(&b, 1)), "bit {bit}");
    }
}

#[test]
fn the_decoder_rechecks_validation_rules() {
    // Zero trace id.
    let mut b = bytes(&base());
    b[2..18].fill(0);
    assert!(is_corrupt(decode_batch(&b, 1)));
    // Zero span id.
    let mut a = base();
    a.span_id = Some(SpanId([1; 8]));
    let mut b = bytes(&a);
    b[19..27].fill(0);
    assert!(is_corrupt(decode_batch(&b, 1)));
    // Empty name.
    let mut a = base();
    a.name = "n".into();
    a.source = String::new();
    let mut b = bytes(&a);
    b.splice(27..29, [0u8]);
    assert!(is_corrupt(decode_batch(&b, 1)));
    // Not UTF-8 in the name.
    let mut b = bytes(&a);
    b[28] = 0xff;
    assert!(is_corrupt(decode_batch(&b, 1)));
    // Control character in the name.
    let mut b = bytes(&a);
    b[28] = b'\n';
    assert!(is_corrupt(decode_batch(&b, 1)));
    // Neither value nor label.
    let mut b = bytes(&a);
    b[18] = 0;
    b.truncate(b.len() - 8);
    assert!(is_corrupt(decode_batch(&b, 1)));
    // A name longer than the limit: length byte 129.
    let mut long = a.clone();
    long.name = "n".repeat(MAX_NAME_BYTES);
    let mut b = bytes(&long);
    b[27] = 129;
    b.insert(28, b'n');
    assert!(is_corrupt(decode_batch(&b, 1)));
}

#[test]
fn identity_cmp_agrees_with_a_tuple_comparison() {
    let mut rng = StdRng::seed_from_u64(5);
    let key = |a: &Annotation| {
        (
            *a.trace_id.as_bytes(),
            a.span_id.map(|s| *s.as_bytes()),
            a.name.clone().into_bytes(),
            a.source.clone().into_bytes(),
        )
    };
    let mut equal = 0;
    for _ in 0..5000 {
        let (a, b) = (random_annotation(&mut rng), random_annotation(&mut rng));
        assert_eq!(a.identity_cmp(&b), key(&a).cmp(&key(&b)));
        assert_eq!(a.identity_cmp(&b), b.identity_cmp(&a).reverse());
        equal += usize::from(a.identity_cmp(&b).is_eq());
    }
    assert!(equal > 0, "the generator never produced equal identities");

    // None before some, even for the smallest span id.
    let mut with = base();
    with.span_id = Some(SpanId([0, 0, 0, 0, 0, 0, 0, 1]));
    let without = base();
    assert!(without.identity_cmp(&with).is_lt());
    // Only the identity counts.
    let mut changed = base();
    changed.value = Some(9.0);
    changed.time_ns = 5;
    changed.arrival = 8;
    assert!(base().identity_cmp(&changed).is_eq());
    // Bytes, not characters.
    let mut x = base();
    x.name = "a".into();
    let mut y = base();
    y.name = "é".into();
    assert!(x.identity_cmp(&y).is_lt());
}

#[test]
fn the_format_version_check_names_both_versions() {
    check_annotation_format_version(ANNOTATION_FORMAT_VERSION).unwrap();
    let msg = check_annotation_format_version(ANNOTATION_FORMAT_VERSION + 6)
        .unwrap_err()
        .to_string();
    assert!(msg.contains('7') && msg.contains("version 1"), "{msg}");
    assert!(check_annotation_format_version(0).is_err());
}
