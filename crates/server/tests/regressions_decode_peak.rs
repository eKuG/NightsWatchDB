//! The door charges `DECODE_FACTOR` times the size of a request plus
//! `DECODE_FIXED` bytes for decoding it (the README, "What the budget
//! bounds"). This test counts every heap byte the decoder holds at its peak,
//! which includes the interning tables and the buffers that live only while a
//! batch is built, and checks that the charge covers it for the shapes that
//! were worst. For a compressed message the decompressed request is in the
//! same pool, so it is counted too.
//!
//! It is the only test in this file because the allocator counts the whole
//! process.

use nwdb_core::BatchLimits;
use nwdb_otlp::TraceDecoder;
use nwdb_server::{DECODE_FACTOR, DECODE_FIXED};
use nwdb_testkit::alloc::{Counting, live, peak, reset_peak};
use nwdb_testkit::proto::ld;

#[global_allocator]
static ALLOC: Counting = Counting;

fn concat(parts: &[&[u8]]) -> Vec<u8> {
    parts.concat()
}

/// A span with only its two ids; `n` makes the span id unique.
fn minimal_span(n: u32) -> Vec<u8> {
    concat(&[
        &ld(1, &[1; 16]),
        &ld(2, &[n.to_be_bytes(), [9; 4]].concat()),
    ])
}

/// A short printable name that is different for every `n` below 95 * 95.
fn short_name(n: u32) -> Vec<u8> {
    vec![b' ' + (n % 95) as u8, b' ' + (n / 95 % 95) as u8]
}

/// `n` minimal spans in one `ScopeSpans`.
fn one_scope(n: u32) -> Vec<u8> {
    let spans: Vec<u8> = (0..n).flat_map(|i| ld(2, &minimal_span(i))).collect();
    ld(1, &ld(2, &spans))
}

/// `n` `ScopeSpans` under one `ResourceSpans`, each with a scope of its own
/// (name and version), a schema URL of its own and one minimal span.
fn many_scopes(n: u32) -> Vec<u8> {
    let scopes: Vec<u8> = (0..n)
        .flat_map(|i| {
            let name = short_name(i);
            let scope = concat(&[&ld(1, &name), &ld(2, &name)]);
            ld(
                2,
                &concat(&[&ld(1, &scope), &ld(2, &minimal_span(i)), &ld(3, &name)]),
            )
        })
        .collect();
    ld(1, &scopes)
}

/// `n` `ResourceSpans`, each with a schema URL of its own, a scope of its own
/// and one minimal span.
fn many_resources(n: u32) -> Vec<u8> {
    (0..n)
        .flat_map(|i| {
            let name = short_name(i);
            let scope_spans = concat(&[&ld(1, &ld(1, &name)), &ld(2, &minimal_span(i))]);
            ld(1, &concat(&[&ld(2, &scope_spans), &ld(3, &name)]))
        })
        .collect()
}

/// Peak heap growth while decoding `req`, with the result still held.
fn peak_of(req: &[u8]) -> isize {
    let decoder = TraceDecoder::new(BatchLimits::DEFAULT);
    let base = live();
    reset_peak();
    let (batches, stats) = decoder.decode_with_stats(req).unwrap();
    let peak = peak() - base;
    assert!(stats.spans > 0 && stats.rejected_spans == 0);
    drop(batches);
    peak
}

#[test]
fn decoding_never_needs_more_than_the_door_charges() {
    let mut worst = (0.0f64, String::new());
    for n in [1u32, 100, 1_025, 2_049, 4_097, 8_193, 20_000] {
        for (shape, req) in [
            ("minimal spans", one_scope(n)),
            ("a scope per span", many_scopes(n)),
            ("a resource per span", many_resources(n)),
        ] {
            // The request itself is in the pool too when it was compressed.
            let peak = peak_of(&req) + req.len() as isize;
            let charge = (DECODE_FACTOR * req.len() + DECODE_FIXED) as isize;
            let factor = peak as f64 / req.len() as f64;
            eprintln!(
                "{shape}, {n} spans: request {} bytes, peak with the request {peak} bytes \
                 ({factor:.2}x), charge {charge}",
                req.len()
            );
            assert!(
                peak <= charge,
                "decoding {shape}, {n} spans needs {peak} bytes at its peak (the request \
                 included); the door charges {charge}"
            );
            if factor > worst.0 {
                worst = (factor, format!("{shape}, {n} spans"));
            }
        }
    }
    eprintln!("worst: {:.2}x ({})", worst.0, worst.1);
}
