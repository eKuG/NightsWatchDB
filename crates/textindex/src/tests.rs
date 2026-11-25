//! Unit tests of the parts that are not public: the codec, the tokeniser's
//! edges and the skip-data behaviour of intersection. Searches, damage, the
//! query model and the id-list store are tested through the public API in
//! `tests/`.

use nwdb_core::Error;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::codec::*;
use crate::*;

fn toks(b: &[u8]) -> Vec<Vec<u8>> {
    let mut buf = [0u8; MAX_TOKEN_LEN];
    tokens(b)
        .filter_map(|t| t.lowercase(&mut buf).map(<[u8]>::to_vec))
        .collect()
}

fn build(docs: &[(u32, Vec<u8>)], budget: usize) -> Vec<u8> {
    let mut out = Vec::new();
    let mut b = IndexBuilder::new(
        IndexOptions {
            segment_budget_bytes: budget,
        },
        &mut out,
    );
    for (id, text) in docs {
        b.add(*id, text).unwrap();
    }
    b.finish().unwrap();
    out
}

#[test]
fn tokeniser_rules() {
    assert_eq!(
        toks(b"Hello, World! foo_bar 123 A1b2"),
        [&b"hello"[..], b"world", b"foo", b"bar", b"123", b"a1b2"]
    );
    assert!(toks(b"").is_empty());
    assert!(toks(b" \t\n.,;-_/()[]{}\"'\x00\x01\x7f").is_empty());
    // Bytes at or above 0x80 are token bytes, valid UTF-8 or not, and are
    // never case folded.
    assert_eq!(
        toks("Caf\u{e9} na\u{ef}ve-\u{4e16}\u{754c}".as_bytes()),
        [
            "caf\u{e9}".as_bytes(),
            "na\u{ef}ve".as_bytes(),
            "\u{4e16}\u{754c}".as_bytes()
        ]
    );
    assert_eq!(toks(b"x\xff\xfey z"), [&b"x\xff\xfey"[..], b"z"]);
    assert_eq!(toks(b"\x80"), [&b"\x80"[..]]);
}

#[test]
fn tokeniser_length_limit() {
    let t64 = "a".repeat(64);
    let t65 = "b".repeat(65);
    let text = format!("{t64} {t65} {t64}");
    let all: Vec<Token<'_>> = tokens(text.as_bytes()).collect();
    assert_eq!(all.len(), 3);
    assert!(all[0].is_indexable() && !all[1].is_indexable() && all[2].is_indexable());
    // The long token is not indexed but is still a token for adjacency.
    assert_eq!(toks(text.as_bytes()), [t64.as_bytes(), t64.as_bytes()]);
    assert!(phrase_matches(text.as_bytes(), &[&t64, &t65]));
    assert!(!phrase_matches(text.as_bytes(), &[&t64, &t64]));
    let mut buf = [0u8; MAX_TOKEN_LEN];
    assert!(all[1].lowercase(&mut buf).is_none());
    // 64 bytes of multi-byte text, and one more byte, on the same limit.
    assert_eq!(toks("\u{e9}".repeat(32).as_bytes()).len(), 1);
    assert!(toks("\u{e9}".repeat(33).as_bytes()).is_empty());
}

#[test]
fn dictionary_keys_of_short_and_long_tokens() {
    use crate::tokenizer::{LONG_MARK, MAX_KEY_LEN, long_aware_key};
    let mut buf = [0u8; MAX_KEY_LEN];
    // a token of up to 64 bytes is its own key, case folded
    assert_eq!(long_aware_key(b"Hello", &mut buf), b"hello");
    let t64 = [b'Q'; 64];
    assert_eq!(long_aware_key(&t64, &mut buf), [b'q'; 64]);
    // a longer one is a zero byte and its first 64 bytes, case folded
    let mut t90 = vec![b'A'; 70];
    t90.extend_from_slice(&[b'z'; 20]);
    let key = long_aware_key(&t90, &mut buf).to_vec();
    assert_eq!(key.len(), MAX_KEY_LEN);
    assert_eq!(key[0], LONG_MARK);
    assert_eq!(&key[1..], [b'a'; 64]);
    let all: Vec<Token<'_>> = tokens(&t90).collect();
    assert_eq!(all[0].key(&mut buf), key);
    // 65 bytes is already long; bytes above 0x7f are kept as they are
    let t65 = [0xc3u8; 65];
    let key = long_aware_key(&t65, &mut buf);
    assert_eq!((key[0], key.len()), (LONG_MARK, 65));
    assert!(key[1..].iter().all(|&b| b == 0xc3));
}

#[test]
fn varints_round_trip_and_reject_damage() {
    let vals = [
        0u64,
        1,
        127,
        128,
        255,
        16383,
        16384,
        u64::from(u32::MAX),
        u64::MAX,
    ];
    let mut b = Vec::new();
    for &v in &vals {
        put_varint(&mut b, v);
    }
    let mut p = 0;
    for &v in &vals {
        assert_eq!(get_varint(&b, &mut p).unwrap(), v);
    }
    assert_eq!(p, b.len());
    let corrupt = |bytes: &[u8]| matches!(get_varint(bytes, &mut 0), Err(Error::Corrupt(_)));
    assert!(corrupt(&[]));
    assert!(corrupt(&[0x80]));
    assert!(corrupt(&[0x80; 11]));
    // a tenth byte that would pass 64 bits
    assert!(corrupt(&[
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02
    ]));
    assert!(matches!(
        get_varint_u32(&[0x80, 0x80, 0x80, 0x80, 0x10], &mut 0),
        Err(Error::Corrupt(_))
    ));
}

/// Ids whose gaps need exactly `w` bits somewhere in every block.
fn ids_of_width(w: u8, n: usize, rng: &mut StdRng) -> Vec<u32> {
    let mut out = Vec::with_capacity(n);
    let mut next = 0u64;
    for i in 0..n {
        let gap = if w == 0 {
            0
        } else if i % BLOCK == 0 {
            // top bit set so the block's width is w
            if w == 32 { 1u64 << 31 } else { 1u64 << (w - 1) }
        } else {
            rng.random_range(0..(1u64 << w.min(31)))
        };
        let id = next + gap;
        if id > u64::from(u32::MAX) {
            break;
        }
        out.push(id as u32);
        next = id + 1;
    }
    out
}

#[test]
fn bitpacked_blocks_for_all_widths_and_lengths() {
    let mut rng = StdRng::seed_from_u64(7);
    for w in [0u8, 1, 7, 8, 17, 32] {
        for n in [1usize, 8, 9, 128, 129, 1000] {
            // width 32 cannot repeat many times in u32 space; the cap above
            // shortens the list when it runs out of ids.
            let ids = ids_of_width(w, n, &mut rng);
            if ids.is_empty() {
                continue;
            }
            let mut skips = Vec::new();
            let mut bytes = Vec::new();
            encode_blocks(&ids, &mut bytes, |last, start| skips.push((last, start)));
            assert_eq!(skips.len(), ids.len().div_ceil(BLOCK));
            let (mut p, mut out, mut next_min) = (0, Vec::new(), 0u64);
            for (bi, chunk) in ids.chunks(BLOCK).enumerate() {
                assert_eq!(skips[bi].1, p, "w={w} n={n}");
                p += decode_block(&bytes[p..], chunk.len(), next_min, &mut out).unwrap();
                next_min = u64::from(skips[bi].0) + 1;
                assert_eq!(*out.last().unwrap(), skips[bi].0);
            }
            assert_eq!(p, bytes.len());
            assert_eq!(out, ids, "w={w} n={n}");
            if w == 0 {
                assert_eq!(
                    bytes.len(),
                    ids.len().div_ceil(BLOCK),
                    "runs pack to width 0"
                );
            }
        }
    }
}

#[test]
fn pack_unpack_all_widths() {
    let mut rng = StdRng::seed_from_u64(11);
    for w in 0u8..=32 {
        let max = if w == 32 { u32::MAX } else { (1u32 << w) - 1 };
        for n in [1usize, 2, 7, 8, 9, 31, 128] {
            let vals: Vec<u32> = (0..n)
                .map(|_| if w == 0 { 0 } else { rng.random_range(0..=max) })
                .collect();
            let mut b = Vec::new();
            pack(&vals, w, &mut b);
            assert_eq!(b.len(), packed_len(n, w));
            let mut out = vec![0u32; n];
            unpack(&b, w, &mut out);
            assert_eq!(out, vals, "w={w} n={n}");
        }
    }
}

#[test]
fn damaged_blocks_are_corrupt() {
    let ids: Vec<u32> = (0..200).map(|i| i * 3).collect();
    let mut bytes = Vec::new();
    encode_blocks(&ids, &mut bytes, |_, _| {});
    let mut out = Vec::new();
    // width above 32
    let mut b = bytes.clone();
    b[0] = 33;
    assert!(matches!(
        decode_block(&b, 128, 0, &mut out),
        Err(Error::Corrupt(_))
    ));
    // cut short
    assert!(matches!(
        decode_block(&bytes[..5], 128, 0, &mut out),
        Err(Error::Corrupt(_))
    ));
    assert!(matches!(
        decode_block(&[], 1, 0, &mut out),
        Err(Error::Corrupt(_))
    ));
    // ids that would pass u32::MAX
    assert!(matches!(
        decode_block(&bytes, 128, u64::from(u32::MAX) - 5, &mut out),
        Err(Error::Corrupt(_))
    ));
    // impossible lengths
    assert!(matches!(
        decode_block(&bytes, 0, 0, &mut out),
        Err(Error::Corrupt(_))
    ));
    assert!(matches!(
        decode_block(&bytes, 129, 0, &mut out),
        Err(Error::Corrupt(_))
    ));
}

#[test]
fn front_coding_round_trip() {
    // Terms chosen so that neighbours share nothing, share a prefix, and
    // share the whole previous term; more than one dictionary block.
    let mut vocab: Vec<String> = vec!["a", "aa", "aaa", "aaab", "ab", "b", "ba", "bab", "babe"]
        .into_iter()
        .map(String::from)
        .collect();
    for i in 0..100 {
        vocab.push(format!("k{i:03}"));
        vocab.push(format!("k{i:03}x"));
        vocab.push(format!("z{i}"));
    }
    vocab.sort();
    vocab.dedup();
    assert!(vocab.len() > 3 * TERMS_PER_BLOCK_FOR_TEST);
    let docs: Vec<(u32, Vec<u8>)> = vocab
        .iter()
        .enumerate()
        .map(|(i, t)| (i as u32, t.clone().into_bytes()))
        .collect();
    let idx = build(&docs, usize::MAX);
    let r = IndexReader::open(idx.as_slice()).unwrap();
    assert_eq!(r.meta().terms as usize, vocab.len());
    for (i, t) in vocab.iter().enumerate() {
        assert_eq!(r.term(t).unwrap().ids.as_slice(), &[i as u32][..], "{t}");
    }
    for missing in ["", "0", "aaaa", "aab", "bb", "k000y", "zz", "k99"] {
        assert!(r.term(missing).unwrap().ids.is_empty(), "{missing}");
    }
    let want: Vec<u32> = vocab
        .iter()
        .enumerate()
        .filter(|(_, t)| t.starts_with("k00"))
        .map(|(i, _)| i as u32)
        .collect();
    assert_eq!(r.prefix("k00", 1000).unwrap().as_slice(), want.as_slice());
}

const TERMS_PER_BLOCK_FOR_TEST: usize = 32;

#[test]
fn skip_data_is_used_by_intersection() {
    // "common" is in every document, "rare" in five of them.
    let rare = [3u32, 50_000, 120_000, 199_000, 199_999];
    let docs: Vec<(u32, Vec<u8>)> = (0..200_000u32)
        .map(|i| {
            let t: &[u8] = if rare.contains(&i) {
                b"common rare"
            } else {
                b"common"
            };
            (i, t.to_vec())
        })
        .collect();
    let idx = build(&docs, usize::MAX);
    let r = IndexReader::open(idx.as_slice()).unwrap();
    let (found, blocks) = r.intersect_counting(&["common", "rare"]).unwrap();
    assert!(found.exact);
    assert_eq!(found.ids.as_slice(), &rare);
    // The common list has 1,563 blocks; one block per candidate is enough.
    assert!(blocks <= 2 * rare.len() as u64 + 2, "read {blocks} blocks");
    assert_eq!(r.term("common").unwrap().ids.len(), 200_000);
}

#[test]
fn set_operations_match_naive() {
    let mut rng = StdRng::seed_from_u64(3);
    for _ in 0..300 {
        let mk = |rng: &mut StdRng| {
            let n = if rng.random_bool(0.3) {
                rng.random_range(0..6)
            } else {
                rng.random_range(0..400)
            };
            let mut v: Vec<u32> = (0..n).map(|_| rng.random_range(0..600)).collect();
            v.sort_unstable();
            v.dedup();
            v
        };
        let (a, b) = (mk(&mut rng), mk(&mut rng));
        let and: Vec<u32> = a.iter().copied().filter(|x| b.contains(x)).collect();
        let or: Vec<u32> = {
            let mut v = [a.clone(), b.clone()].concat();
            v.sort_unstable();
            v.dedup();
            v
        };
        let minus: Vec<u32> = a.iter().copied().filter(|x| !b.contains(x)).collect();
        assert_eq!(ids::intersect(&a, &b), and);
        assert_eq!(ids::intersect(&b, &a), and);
        assert_eq!(ids::union(&a, &b), or);
        assert_eq!(ids::difference(&a, &b), minus);
    }
    // a long list against a short one takes the searching path
    let long: Vec<u32> = (0..10_000).collect();
    assert_eq!(ids::intersect(&[5, 9_999, 20_000], &long), [5, 9_999]);
    assert!(DocIds::new(vec![1, 1]).is_none() && DocIds::new(vec![1, 2]).is_some());
}

#[test]
fn readers_are_send_and_sync() {
    fn check<T: Send + Sync>() {}
    check::<IndexReader<std::fs::File>>();
    check::<IndexReader<&[u8]>>();
    check::<IndexReader<Vec<u8>>>();
    check::<IdListReader<std::fs::File>>();
    check::<IdListReader<Vec<u8>>>();
}
