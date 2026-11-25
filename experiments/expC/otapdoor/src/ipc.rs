//! Per-gRPC-stream Arrow IPC state. Each payload type has its own IPC stream whose schema message arrives once
//! (with the first payload of a new `schema_id`) and whose dictionaries accumulate across later payloads, so one
//! push-style decoder is kept per payload type and fed each payload's bytes in arrival order.
use crate::proto::BatchArrowRecords;
use crate::R;
use arrow::buffer::Buffer;
use arrow::ipc::reader::StreamDecoder;
use arrow::record_batch::RecordBatch;
use std::collections::{HashMap, HashSet};

struct TypeState {
    schema_id: String,
    dec: StreamDecoder,
    dict_ids: HashSet<i64>,
    /// bytes held by the dictionaries of the last batch decoded for this payload type (what the decoder keeps alive)
    dict_bytes: usize,
}

#[derive(Default)]
pub struct StreamState {
    by_type: HashMap<i32, TypeState>,
}

/// What one message did to the IPC state; merged into the process-wide statistics by the caller.
#[derive(Default, Debug)]
pub struct Ctr {
    /// a payload type started a new IPC stream (first one, or a new schema id)
    pub ipc_streams: u64,
    /// ... and an earlier IPC stream of that type was thrown away to do so
    pub schema_resets: u64,
    pub dict_batches: u64,
    pub dict_deltas: u64,
    /// dictionary batches that replaced an existing dictionary instead of extending it
    pub dict_replacements: u64,
    /// bytes of dictionary values this stream holds after the message (all payload types)
    pub dict_bytes: u64,
    /// (payload type, schema description) for every new IPC stream
    pub schemas: Vec<(i32, String)>,
}

fn describe(b: &RecordBatch) -> String {
    use arrow::datatypes::DataType::*;
    fn short(t: &arrow::datatypes::DataType) -> String {
        match t {
            Dictionary(k, v) => format!("dict<{}>({})", short(k), short(v)),
            Struct(f) => format!("struct{{{}}}", f.iter().map(|f| format!("{}:{}", f.name(), short(f.data_type()))).collect::<Vec<_>>().join(",")),
            t => format!("{t}"),
        }
    }
    b.schema().fields().iter().map(|f| format!("{}:{}", f.name(), short(f.data_type()))).collect::<Vec<_>>().join(",")
}

/// Bytes of dictionary values referenced by a column (dictionaries live as long as the IPC stream does).
fn dict_bytes(a: &arrow::array::ArrayRef) -> usize {
    use arrow::array::AsArray;
    match a.data_type() {
        arrow::datatypes::DataType::Dictionary(..) => a.as_any_dictionary().values().get_array_memory_size(),
        arrow::datatypes::DataType::Struct(_) => a.as_struct().columns().iter().map(dict_bytes).sum(),
        _ => 0,
    }
}

/// Walks the IPC messages of one payload to count dictionary batches (the decoder does not expose them).
fn peek_dictionaries(mut b: &[u8], ids: &mut HashSet<i64>, c: &mut Ctr) {
    use arrow::ipc::MessageHeader;
    while b.len() >= 8 {
        let mut off = 0;
        if b[..4] == [0xff; 4] {
            off = 4;
        }
        let n = u32::from_le_bytes(b[off..off + 4].try_into().unwrap()) as usize;
        off += 4;
        if n == 0 || b.len() < off + n {
            return;
        }
        let Ok(m) = arrow::ipc::root_as_message(&b[off..off + n]) else { return };
        if m.header_type() == MessageHeader::DictionaryBatch {
            if let Some(d) = m.header_as_dictionary_batch() {
                c.dict_batches += 1;
                if d.isDelta() {
                    c.dict_deltas += 1;
                } else if !ids.insert(d.id()) {
                    c.dict_replacements += 1;
                }
                ids.insert(d.id());
            }
        }
        let end = off + n + m.bodyLength() as usize;
        if b.len() < end {
            return;
        }
        b = &b[end..];
    }
}

impl StreamState {
    /// Decodes every payload of one message, in order, returning (payload type, record batch) for each.
    pub fn consume(&mut self, bar: &BatchArrowRecords, c: &mut Ctr) -> R<Vec<(i32, RecordBatch)>> {
        let mut out = Vec::with_capacity(bar.arrow_payloads.len());
        for p in &bar.arrow_payloads {
            let fresh = match self.by_type.get(&p.r#type) {
                Some(s) => s.schema_id != p.schema_id,
                None => true,
            };
            if fresh {
                c.ipc_streams += 1;
                if self.by_type.insert(p.r#type, TypeState { schema_id: p.schema_id.clone(), dec: StreamDecoder::new(), dict_ids: HashSet::new(), dict_bytes: 0 }).is_some() {
                    c.schema_resets += 1;
                }
            }
            let st = self.by_type.get_mut(&p.r#type).unwrap();
            peek_dictionaries(&p.record, &mut st.dict_ids, c);
            let mut buf = Buffer::from(p.record.clone());
            let mut got: Option<RecordBatch> = None;
            while !buf.is_empty() {
                if let Some(b) = st.dec.decode(&mut buf).map_err(|e| format!("payload type {} schema {:?}: {e}", p.r#type, p.schema_id))? {
                    if got.replace(b).is_some() {
                        return Err(format!("payload type {} carries more than one record batch", p.r#type).into());
                    }
                }
            }
            let b = got.ok_or_else(|| format!("payload type {} schema {:?} carries no record batch", p.r#type, p.schema_id))?;
            if fresh {
                c.schemas.push((p.r#type, describe(&b)));
            }
            st.dict_bytes = b.columns().iter().map(dict_bytes).sum();
            out.push((p.r#type, b));
        }
        c.dict_bytes = self.by_type.values().map(|s| s.dict_bytes as u64).sum();
        Ok(out)
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::proto::ArrowPayload;
    use arrow::array::*;
    use arrow::datatypes::*;
    use arrow::ipc::writer::{DictionaryHandling, IpcWriteOptions, StreamWriter};
    use std::sync::Arc;

    /// Producer side of one IPC stream, emitting one payload (schema if first, dictionaries, batch) per `write`.
    pub struct Producer {
        w: StreamWriter<Vec<u8>>,
        taken: usize,
    }
    impl Producer {
        pub fn new(schema: &Schema, delta: bool, zstd: bool) -> Producer {
            let mut o = IpcWriteOptions::default();
            if delta {
                o = o.with_dictionary_handling(DictionaryHandling::Delta);
            }
            if zstd {
                o = o.try_with_compression(Some(arrow::ipc::CompressionType::ZSTD)).unwrap();
            }
            Producer { w: StreamWriter::try_new_with_options(Vec::new(), schema, o).unwrap(), taken: 0 }
        }
        pub fn write(&mut self, b: &RecordBatch) -> Vec<u8> {
            self.w.write(b).unwrap();
            let all = self.w.get_ref();
            let out = all[self.taken..].to_vec();
            self.taken = all.len();
            out
        }
    }

    pub fn payload(schema_id: &str, ty: i32, record: Vec<u8>) -> ArrowPayload {
        ArrowPayload { schema_id: schema_id.into(), r#type: ty, record: record.into() }
    }
    pub fn bar(id: i64, payloads: Vec<ArrowPayload>) -> BatchArrowRecords {
        BatchArrowRecords { batch_id: id, arrow_payloads: payloads, headers: Default::default() }
    }

    fn dict_batch(dict: &[&str], keys: &[u8]) -> RecordBatch {
        let a: ArrayRef = Arc::new(DictionaryArray::<UInt8Type>::try_new(UInt8Array::from(keys.to_vec()), Arc::new(StringArray::from(dict.to_vec()))).unwrap());
        RecordBatch::try_from_iter([("s", a)]).unwrap()
    }

    fn values(b: &RecordBatch) -> Vec<String> {
        let d = b.column(0).as_dictionary::<UInt8Type>();
        let v = d.values().as_string::<i32>();
        d.keys().iter().map(|k| v.value(k.unwrap() as usize).to_string()).collect()
    }

    #[test]
    fn dictionary_deltas_accumulate_across_messages() {
        for zstd in [false, true] {
            let b1 = dict_batch(&["a", "b"], &[0, 1, 0]);
            let b2 = dict_batch(&["a", "b", "c"], &[0, 2, 1, 2]);
            let mut p = Producer::new(&b1.schema(), true, zstd);
            let (r1, r2) = (p.write(&b1), p.write(&b2));
            let mut st = StreamState::default();
            let mut c = Ctr::default();
            let o1 = st.consume(&bar(0, vec![payload("0", 41, r1)]), &mut c).unwrap();
            let o2 = st.consume(&bar(1, vec![payload("0", 41, r2)]), &mut c).unwrap();
            assert_eq!(values(&o1[0].1), ["a", "b", "a"]);
            assert_eq!(values(&o2[0].1), ["a", "c", "b", "c"], "zstd={zstd}");
            assert_eq!((c.ipc_streams, c.schema_resets, c.dict_batches, c.dict_deltas), (1, 0, 2, 1));
        }
    }

    #[test]
    fn schema_change_starts_a_new_ipc_stream() {
        let b1 = dict_batch(&["a", "b"], &[0, 1]);
        let mut p1 = Producer::new(&b1.schema(), true, false);
        let plain = RecordBatch::try_from_iter([("s", Arc::new(StringArray::from(vec!["x", "y"])) as ArrayRef)]).unwrap();
        let mut p2 = Producer::new(&plain.schema(), true, false);
        let mut st = StreamState::default();
        let mut c = Ctr::default();
        st.consume(&bar(0, vec![payload("0", 41, p1.write(&b1))]), &mut c).unwrap();
        st.consume(&bar(1, vec![payload("0", 41, p1.write(&b1))]), &mut c).unwrap();
        // new schema id for the same payload type: the old dictionaries must be gone and the new schema message expected
        let o = st.consume(&bar(2, vec![payload("1", 41, p2.write(&plain))]), &mut c).unwrap();
        assert_eq!(o[0].1.column(0).data_type(), &DataType::Utf8);
        assert_eq!((c.ipc_streams, c.schema_resets), (2, 1));
        // a different payload type with its own schema id is independent and is not a reset
        let mut p3 = Producer::new(&b1.schema(), true, false);
        st.consume(&bar(3, vec![payload("2", 40, p3.write(&b1))]), &mut c).unwrap();
        assert_eq!((c.ipc_streams, c.schema_resets), (3, 1));
        // a payload without the schema message for a schema id never seen is an error, not a silent skip
        let late = p1.write(&b1);
        assert!(st.consume(&bar(4, vec![payload("9", 41, late)]), &mut c).is_err());
    }

    #[test]
    fn replaced_dictionary_is_followed() {
        // without delta handling the writer re-sends the whole dictionary: a replacement
        let b1 = dict_batch(&["a", "b"], &[0, 1]);
        let b2 = dict_batch(&["c", "a"], &[0, 1]);
        let mut p = Producer::new(&b1.schema(), false, false);
        let (r1, r2) = (p.write(&b1), p.write(&b2));
        let mut st = StreamState::default();
        let mut c = Ctr::default();
        st.consume(&bar(0, vec![payload("0", 41, r1)]), &mut c).unwrap();
        let o = st.consume(&bar(1, vec![payload("0", 41, r2)]), &mut c).unwrap();
        assert_eq!(values(&o[0].1), ["c", "a"]);
        assert_eq!((c.dict_deltas, c.dict_replacements), (0, 1));
    }
}
