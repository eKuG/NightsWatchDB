//! Prints where the bytes of parts go.
//!
//!     cargo run --release -p nwdb-segment --example part_stats -- PART_DIR...
//!
//! Each argument is one part directory, or a directory of parts (a directory
//! with no `spans.parquet` is taken to hold parts). A directory written by the
//! layout experiment (`seg-*.parquet`) is read as one part. For `spans.parquet` and
//! `summary.parquet` the tool prints, per column and summed over all the
//! parts: compressed and uncompressed bytes, bytes per row, the encodings used,
//! and the number of data pages and dictionary pages. It then prints what the
//! Parquet files hold besides column data (footer and page index) and the size
//! of every other file of a part.
#![allow(missing_docs)]

use parquet::column::page::Page;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};

#[derive(Default)]
struct Col {
    compressed: i64,
    uncompressed: i64,
    data_pages: i64,
    dict_pages: i64,
    encodings: Vec<String>,
}

#[derive(Default)]
struct FileTotals {
    rows: i64,
    file_bytes: u64,
    row_groups: usize,
    // In schema order, so the table reads like the schema.
    order: Vec<String>,
    cols: BTreeMap<String, Col>,
}

fn add_file(path: &Path, totals: &mut FileTotals) -> Result<(), String> {
    let file = File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
    totals.file_bytes += file.metadata().map_err(|e| e.to_string())?.len();
    let reader = SerializedFileReader::new(file).map_err(|e| format!("{}: {e}", path.display()))?;
    let meta = reader.metadata();
    for (g, rg) in meta.row_groups().iter().enumerate() {
        let group = reader.get_row_group(g).map_err(|e| e.to_string())?;
        totals.rows += rg.num_rows();
        totals.row_groups += 1;
        for (i, c) in rg.columns().iter().enumerate() {
            let name = c.column_path().string();
            if !totals.cols.contains_key(&name) {
                totals.order.push(name.clone());
            }
            let col = totals.cols.entry(name).or_default();
            col.compressed += c.compressed_size();
            col.uncompressed += c.uncompressed_size();
            // Walk the pages: the metadata does not say how many there are.
            let mut pages = group.get_column_page_reader(i).map_err(|e| e.to_string())?;
            while let Some(page) = pages.get_next_page().map_err(|e| e.to_string())? {
                match page {
                    Page::DictionaryPage { .. } => col.dict_pages += 1,
                    _ => col.data_pages += 1,
                }
            }
            for e in c.encodings() {
                let e = format!("{e:?}");
                if !col.encodings.contains(&e) {
                    col.encodings.push(e);
                }
            }
        }
    }
    Ok(())
}

fn print_table(title: &str, t: &FileTotals) {
    println!(
        "\n{title}: {} rows, {} row groups, {} bytes ({:.2} per row)",
        t.rows,
        t.row_groups,
        t.file_bytes,
        t.file_bytes as f64 / t.rows.max(1) as f64
    );
    println!(
        "{:<26} {:>12} {:>12} {:>7} {:>7} {:>6} {:>5}  encodings",
        "column", "compressed", "uncompressed", "B/row", "ratio", "pages", "dict"
    );
    let mut sum = 0;
    let mut sum_un = 0;
    for name in &t.order {
        let c = &t.cols[name];
        sum += c.compressed;
        sum_un += c.uncompressed;
        println!(
            "{:<26} {:>12} {:>12} {:>7.3} {:>7.2} {:>6} {:>5}  {}",
            name,
            c.compressed,
            c.uncompressed,
            c.compressed as f64 / t.rows.max(1) as f64,
            c.uncompressed as f64 / c.compressed.max(1) as f64,
            c.data_pages,
            c.dict_pages,
            c.encodings.join(",")
        );
    }
    println!(
        "{:<26} {:>12} {:>12} {:>7.3}",
        "column data total",
        sum,
        sum_un,
        sum as f64 / t.rows.max(1) as f64
    );
    let rest = t.file_bytes as i64 - sum;
    println!(
        "{:<26} {:>12} {:>12} {:>7.3}",
        "footer and page index",
        rest,
        "",
        rest as f64 / t.rows.max(1) as f64
    );
}

/// The segment files of the layout experiment (`seg-*.parquet` with a
/// `summary.parquet` beside them), summed as one part.
fn segments_in(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = std::fs::read_dir(dir)
        .map(|d| d.flatten().map(|e| e.path()).collect())
        .unwrap_or_default();
    v.retain(|p| {
        p.file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with("seg-") && n.ends_with(".parquet"))
    });
    v.sort();
    v
}

fn parts_in(arg: &Path) -> Vec<PathBuf> {
    if arg.join("spans.parquet").exists() || !segments_in(arg).is_empty() {
        return vec![arg.to_path_buf()];
    }
    let mut parts: Vec<PathBuf> = std::fs::read_dir(arg)
        .map(|d| d.flatten().map(|e| e.path()).collect())
        .unwrap_or_default();
    parts.retain(|p| p.join("spans.parquet").exists() || !segments_in(p).is_empty());
    parts.sort();
    parts
}

fn main() -> Result<(), String> {
    let mut parts = Vec::new();
    for a in std::env::args().skip(1) {
        parts.extend(parts_in(Path::new(&a)));
    }
    if parts.is_empty() {
        return Err("usage: part_stats PART_DIR_OR_PARTS_DIR...".into());
    }
    let mut spans = FileTotals::default();
    let mut summary = FileTotals::default();
    let mut other: BTreeMap<String, u64> = BTreeMap::new();
    for p in &parts {
        let segments = segments_in(p);
        if segments.is_empty() {
            add_file(&p.join("spans.parquet"), &mut spans)?;
        }
        for s in &segments {
            add_file(s, &mut spans)?;
        }
        add_file(&p.join("summary.parquet"), &mut summary)?;
        for e in std::fs::read_dir(p).map_err(|e| e.to_string())?.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if !name.ends_with(".parquet") {
                *other.entry(name).or_default() += e.metadata().map_err(|e| e.to_string())?.len();
            }
        }
    }
    println!("{} parts", parts.len());
    print_table("spans.parquet", &spans);
    print_table("summary.parquet", &summary);
    println!("\nother files:");
    let mut total = spans.file_bytes + summary.file_bytes;
    for (name, bytes) in &other {
        total += bytes;
        println!(
            "{:<26} {:>12} {:>7.3} B/span",
            name,
            bytes,
            *bytes as f64 / spans.rows.max(1) as f64
        );
    }
    println!(
        "\nall files: {total} bytes, {:.3} per span",
        total as f64 / spans.rows.max(1) as f64
    );
    Ok(())
}
