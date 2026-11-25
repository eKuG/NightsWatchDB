mod build;
mod codec;
mod qry;
mod scan;
mod search;
mod seg;
mod store;
#[cfg(test)]
mod tests;
mod tok;
mod util;

use std::path::Path;
use std::time::Instant;
use store::Store;
use util::*;

const USAGE: &str = "usage: textindex build|queries|search|scan|check [--flag value]...";
const CACHE_MB: usize = 256;

fn variant(a: &Args) -> R<bool> {
    match a.get("variant")? {
        "piece" => Ok(true),
        "span" => Ok(false),
        v => Err(format!("--variant must be piece or span, not {v}").into()),
    }
}

fn cache(a: &Args) -> R<usize> {
    Ok(a.num("cache-mb", CACHE_MB)? << 20)
}

fn cmd_build(a: &Args) -> R<()> {
    let t0 = Instant::now();
    let piece = variant(a)?;
    let o = build::Opts {
        store: a.path("store")?,
        out: a.path("out")?,
        piece,
        docs_per_segment: a.num("docs-per-segment", if piece { 32_768 } else { 8_192 })?,
        budget: a.num::<u64>("budget-mb", 512)? << 20,
        cache: cache(a)?,
    };
    let r = build::run(&o)?;
    let mut st = Store::open(&o.store, 0)?;
    let (piece_text, span_text) = (st.piece_bytes_total(), build::span_text_bytes(&mut st)?);
    let mut f = build::report_fields(&r, piece_text, span_text);
    f.push(("docs_per_segment", o.docs_per_segment.to_string()));
    f.push(("budget_bytes", o.budget.to_string()));
    f.push(("distinct_piece_text_bytes", piece_text.to_string()));
    f.push(("full_span_text_bytes", span_text.to_string()));
    finish("build", if piece { "piece" } else { "span" }, t0, f);
    Ok(())
}

fn cmd_queries(a: &Args) -> R<()> {
    let t0 = Instant::now();
    let segs = search::open_segments(&a.path("index")?)?;
    let mut store = Store::open(&a.path("store")?, cache(a)?)?;
    if !a.path("index")?.join("piece_rows.bin").exists() || segs.last().map_or(0, |s| s.doc_hi) != store.pieces() {
        return Err("--index must be a piece index of this store".into());
    }
    let qs = qry::derive(&segs, &mut store, a.num("seed", 1)?)?;
    qry::write_queries(&a.path("out")?, &qs)?;
    let classes: Vec<(&str, String)> = qry::CLASSES.iter().map(|c| (*c, qs.iter().filter(|q| q.class == *c).count().to_string())).collect();
    finish("queries", "piece", t0, vec![("queries", qs.len().to_string()), ("per_class", obj(&classes)), ("pieces", store.pieces().to_string())]);
    Ok(())
}

fn cmd_check(a: &Args) -> R<()> {
    let t0 = Instant::now();
    let (store_dir, qf, cache) = (a.path("store")?, a.path("queries")?, cache(a)?);
    let qs = qry::read_queries(&qf)?;
    let mut st = Store::open(&store_dir, cache)?;
    let key = scan::scan(&mut st, &qs)?;
    let sha: Vec<String> = key.rows.iter().map(|r| rows_sha(r)).collect();
    let mut bad = [0usize; 2];
    let mut s: Vec<search::Searcher> = vec![];
    for (i, (flag, piece)) in [("piece-index", true), ("span-index", false)].into_iter().enumerate() {
        let _ = i;
        s.push(search::Searcher::open(Path::new(a.get(flag)?), Store::open(&store_dir, cache)?, piece)?);
    }
    for (qi, qy) in qs.iter().enumerate() {
        let mut verdict = vec![];
        for (k, se) in s.iter_mut().enumerate() {
            let o = se.run(qy)?;
            let ok = rows_sha(&o.rows) == sha[qi] && o.rows == key.rows[qi];
            if !ok {
                bad[k] += 1;
            }
            verdict.push(if ok { "ok".to_string() } else { format!("MISMATCH rows {} vs scan {}", o.rows.len(), key.rows[qi].len()) });
        }
        println!("{}", obj(&[("id", q(&qy.id)), ("rows", key.rows[qi].len().to_string()), ("piece", q(&verdict[0])), ("span", q(&verdict[1]))]));
    }
    let n = qs.len();
    finish(
        "check",
        "both",
        t0,
        vec![("queries", n.to_string()), ("piece_ok", (n - bad[0]).to_string()), ("span_ok", (n - bad[1]).to_string()), ("all_ok", (bad == [0, 0]).to_string()), ("summary", q(&format!("piece {}/{n} ok, span {}/{n} ok", n - bad[0], n - bad[1])))],
    );
    if bad != [0, 0] {
        return Err("some queries differ from the scan".into());
    }
    Ok(())
}

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let res = (|| -> R<()> {
        let task = argv.first().ok_or(USAGE)?;
        let a = Args::parse(&argv[1..])?;
        match task.as_str() {
            "build" => cmd_build(&a),
            "queries" => cmd_queries(&a),
            "search" => search::search_cmd(&a.path("index")?, &a.path("store")?, variant(&a)?, &a.path("queries")?, a.num("runs", 5)?, cache(&a)?),
            "scan" => scan::scan_cmd(&a.path("store")?, &a.path("queries")?, cache(&a)?),
            "check" => cmd_check(&a),
            _ => Err(USAGE.into()),
        }
    })();
    if let Err(e) = res {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}
