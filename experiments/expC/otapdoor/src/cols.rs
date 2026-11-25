//! Column access that tolerates the encodings OTAP producers use: any string, binary or integer column may be
//! plain or dictionary-encoded (with any key width), and optional columns may be absent.
use crate::R;
use arrow::array::*;
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

enum Vals {
    S(StringArray),
    LS(LargeStringArray),
    B(BinaryArray),
    LB(LargeBinaryArray),
    F(FixedSizeBinaryArray),
    Other,
}

/// Row-indexed view over a plain or dictionary-encoded array of text or bytes.
pub struct View {
    vals: Vals,
    keys: Option<Vec<usize>>,
    outer: ArrayRef,
    nvals: usize,
}

impl View {
    pub fn new(a: &ArrayRef) -> View {
        let (inner, keys) = match a.as_any_dictionary_opt() {
            Some(d) => (d.values().clone(), Some(d.normalized_keys())),
            None => (a.clone(), None),
        };
        let vals = match inner.data_type() {
            DataType::Utf8 => Vals::S(inner.as_string::<i32>().clone()),
            DataType::LargeUtf8 => Vals::LS(inner.as_string::<i64>().clone()),
            DataType::Binary => Vals::B(inner.as_binary::<i32>().clone()),
            DataType::LargeBinary => Vals::LB(inner.as_binary::<i64>().clone()),
            DataType::FixedSizeBinary(_) => Vals::F(inner.as_fixed_size_binary().clone()),
            _ => Vals::Other,
        };
        View { vals, keys, outer: a.clone(), nvals: inner.len() }
    }
    pub fn len(&self) -> usize {
        self.outer.len()
    }
    /// Number of entries in the underlying values array (the dictionary size for dictionary columns).
    pub fn nvals(&self) -> usize {
        self.nvals
    }
    /// Index into the underlying values array (the dictionary, when there is one); None for null.
    pub fn pos(&self, i: usize) -> Option<usize> {
        if self.outer.is_null(i) {
            return None;
        }
        Some(self.keys.as_ref().map_or(i, |k| k[i]))
    }
    pub fn bytes(&self, i: usize) -> Option<&[u8]> {
        let p = self.pos(i)?;
        let b = |a: &dyn Array, p: usize| (!a.is_null(p)).then_some(());
        match &self.vals {
            Vals::S(a) => b(a, p).map(|_| a.value(p).as_bytes()),
            Vals::LS(a) => b(a, p).map(|_| a.value(p).as_bytes()),
            Vals::B(a) => b(a, p).map(|_| a.value(p)),
            Vals::LB(a) => b(a, p).map(|_| a.value(p)),
            Vals::F(a) => b(a, p).map(|_| a.value(p)),
            Vals::Other => None,
        }
    }
    /// Text value. Producers write valid UTF-8; invalid bytes are reported as absent rather than trusted.
    pub fn str(&self, i: usize) -> Option<&str> {
        let p = self.pos(i)?;
        match &self.vals {
            Vals::S(a) => (!a.is_null(p)).then(|| a.value(p)),
            Vals::LS(a) => (!a.is_null(p)).then(|| a.value(p)),
            _ => self.bytes(i).and_then(|b| std::str::from_utf8(b).ok()),
        }
    }
    /// Length of the text/bytes value at row i, for bounding memory without touching the data.
    pub fn text_len(&self, i: usize) -> usize {
        self.bytes(i).map_or(0, |b| b.len())
    }
}

/// Integer column of any width or dictionary encoding as u32 (ids, delta-coded or not).
pub fn u32s(a: &ArrayRef) -> R<UInt32Array> {
    Ok(cast(a, &DataType::UInt32).map_err(|e| format!("id column {:?} is not a u32-compatible integer: {e}", a.data_type()))?.as_primitive::<arrow::datatypes::UInt32Type>().clone())
}

pub fn i64s(a: &ArrayRef) -> R<Int64Array> {
    Ok(cast(a, &DataType::Int64).map_err(|e| format!("column {:?} is not an i64-compatible integer: {e}", a.data_type()))?.as_primitive::<arrow::datatypes::Int64Type>().clone())
}

pub fn col<'a>(b: &'a RecordBatch, name: &str) -> Option<&'a ArrayRef> {
    b.schema().index_of(name).ok().map(|i| b.column(i))
}

/// Child column of a struct column, with the struct's own validity applied (null struct -> null child).
pub fn child(parent: &ArrayRef, name: &str) -> Option<ArrayRef> {
    let s = parent.as_struct_opt()?;
    let c = s.column_by_name(name)?.clone();
    match s.nulls() {
        None => Some(c),
        Some(n) => {
            // a null struct row reads as null in the child: rebuild the child's validity as the intersection
            let data = c.to_data();
            let merged = arrow::buffer::NullBuffer::union(data.nulls(), Some(n));
            let d = data.into_builder().nulls(merged).build().ok()?;
            Some(make_array(d))
        }
    }
}

/// Running sum over the non-null entries (delta-coded id columns); nulls stay null and do not move the sum.
pub fn prefix_sum(a: &UInt32Array) -> Vec<Option<u32>> {
    let mut acc = 0u32;
    a.iter()
        .map(|v| {
            v.map(|d| {
                acc = acc.wrapping_add(d);
                acc
            })
        })
        .collect()
}
