//! Attribute tables (SPAN_ATTRS, RESOURCE_ATTRS, ...): typed value columns plus a `parent_id` that is
//! quasi-delta coded. Mirrors the Go consumer (`AttrsParentIDDecoder`) and the Go producer's sorting.
use crate::cols::{col, i64s, u32s, View};
use crate::R;
use arrow::array::*;
use arrow::record_batch::RecordBatch;

// attribute value type codes as written by the Go producer (pdata ValueType), NOT the order of the draft spec tables
pub const T_EMPTY: u8 = 0;
pub const T_STR: u8 = 1;
pub const T_INT: u8 = 2;
pub const T_DOUBLE: u8 = 3;
pub const T_BOOL: u8 = 4;
pub const T_MAP: u8 = 5;
pub const T_SLICE: u8 = 6;
pub const T_BYTES: u8 = 7;

pub struct Attrs {
    pub parent: Vec<u32>,
    pub key: View,
    pub ty: Vec<u8>,
    pub str_: Option<View>,
    pub int: Option<Int64Array>,
    pub double: Option<Float64Array>,
    pub boolean: Option<BooleanArray>,
    pub bytes: Option<View>,
}

impl Attrs {
    pub fn rows(&self) -> usize {
        self.parent.len()
    }
    pub fn key(&self, i: usize) -> &str {
        self.key.str(i).unwrap_or("")
    }
    pub fn str(&self, i: usize) -> Option<&str> {
        if self.ty[i] != T_STR {
            return None;
        }
        self.str_.as_ref()?.str(i)
    }
    pub fn int(&self, i: usize) -> Option<i64> {
        if self.ty[i] != T_INT {
            return None;
        }
        let a = self.int.as_ref()?;
        a.is_valid(i).then(|| a.value(i))
    }

    /// Is row i's (type, key, value) identical to row j's? Map, slice and empty values are never equal, as in the producer.
    fn same(&self, i: usize, j: usize) -> bool {
        if self.ty[i] != self.ty[j] || self.key(i) != self.key(j) {
            return false;
        }
        let same_bytes = |v: &Option<View>, get: &dyn Fn(&View, usize) -> Option<&[u8]>| match v {
            Some(v) => match (get(v, i), get(v, j)) {
                (Some(a), Some(b)) => (a.as_ptr() == b.as_ptr() && a.len() == b.len()) || a == b,
                _ => false,
            },
            None => false,
        };
        match self.ty[i] {
            T_STR => same_bytes(&self.str_, &|v, r| v.bytes(r)),
            T_BYTES => same_bytes(&self.bytes, &|v, r| v.bytes(r)),
            T_INT => matches!((self.int(i), self.int(j)), (Some(a), Some(b)) if a == b),
            T_DOUBLE => match &self.double {
                Some(d) => d.is_valid(i) && d.is_valid(j) && d.value(i) == d.value(j),
                None => false,
            },
            T_BOOL => match &self.boolean {
                Some(d) => d.is_valid(i) && d.is_valid(j) && d.value(i) == d.value(j),
                None => false,
            },
            _ => false,
        }
    }

    pub fn from_batch(b: &RecordBatch) -> R<Attrs> {
        let get = |n: &str| col(b, n);
        let raw = u32s(get("parent_id").ok_or("attribute batch without parent_id")?)?;
        let key = View::new(get("key").ok_or("attribute batch without key")?);
        let ty: Vec<u8> = match get("type") {
            Some(t) => {
                let t = arrow::compute::cast(t, &arrow::datatypes::DataType::UInt8)?;
                t.as_primitive::<arrow::datatypes::UInt8Type>().values().to_vec()
            }
            None => return Err("attribute batch without type".into()),
        };
        let mut a = Attrs {
            parent: vec![],
            key,
            ty,
            str_: get("str").map(View::new),
            int: get("int").map(i64s).transpose()?,
            double: match get("double") {
                Some(d) => Some(arrow::compute::cast(d, &arrow::datatypes::DataType::Float64)?.as_primitive::<arrow::datatypes::Float64Type>().clone()),
                None => None,
            },
            boolean: get("bool").map(|c| c.as_boolean().clone()),
            bytes: get("bytes").map(View::new),
        };
        if a.ty.len() != raw.len() || a.key.len() != raw.len() {
            return Err("attribute batch columns differ in length".into());
        }
        // quasi-delta: a run of rows with the same (type, key, value) stores the difference from the previous row's
        // parent id; any other row stores its parent id as is
        let mut parent = Vec::with_capacity(raw.len());
        let mut prev = 0u32;
        for i in 0..raw.len() {
            let d = raw.value(i);
            let p = if i > 0 && a.same(i, i - 1) { prev.wrapping_add(d) } else { d };
            parent.push(p);
            prev = p;
        }
        a.parent = parent;
        Ok(a)
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use arrow::datatypes::*;
    use std::sync::Arc;

    /// rows: (parent field as stored, key, string value)
    pub fn str_attrs(rows: &[(u16, &str, &str)], dict_str: bool) -> RecordBatch {
        let parent = UInt16Array::from_iter_values(rows.iter().map(|r| r.0));
        let key = StringArray::from_iter_values(rows.iter().map(|r| r.1));
        let ty = UInt8Array::from_iter_values(rows.iter().map(|_| T_STR));
        let sv: ArrayRef = if dict_str {
            let mut b = StringDictionaryBuilder::<UInt16Type>::new();
            rows.iter().for_each(|r| {
                b.append_value(r.2);
            });
            Arc::new(b.finish())
        } else {
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.2)))
        };
        let schema = Schema::new(vec![
            Field::new("parent_id", DataType::UInt16, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("type", DataType::UInt8, false),
            Field::new("str", sv.data_type().clone(), true),
        ]);
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(parent), Arc::new(key), Arc::new(ty), sv]).unwrap()
    }

    #[test]
    fn type_codes_follow_the_implementations() {
        // 1 str, 2 int, 3 double, 4 bool, 5 map, 6 slice, 7 bytes (the draft spec tables list 1 str, 2 bool, 3 int ...)
        assert_eq!([T_STR, T_INT, T_DOUBLE, T_BOOL, T_MAP, T_SLICE, T_BYTES], [1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn quasi_delta_runs_and_resets() {
        // sorted by (key, value, parent): k=a v=x for parents 3,4,7 ; k=a v=y for parent 2 ; k=b v=x for parents 3,5
        let stored = [(3, "a", "x"), (1, "a", "x"), (3, "a", "x"), (2, "a", "y"), (3, "b", "x"), (2, "b", "x")];
        for dict in [false, true] {
            let a = Attrs::from_batch(&str_attrs(&stored, dict)).unwrap();
            assert_eq!(a.parent, [3, 4, 7, 2, 3, 5], "dict={dict}");
        }
    }

    #[test]
    fn plain_ids_when_no_two_rows_match() {
        let stored = [(5, "a", "x"), (1, "a", "y"), (9, "b", "y")];
        let a = Attrs::from_batch(&str_attrs(&stored, false)).unwrap();
        assert_eq!(a.parent, [5, 1, 9]);
    }
}
