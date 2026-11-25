//! `traces.idx`: one fixed-size little-endian record per trace, sorted by trace id.
//! Record: trace_id[16] segment u32 first_row u32 row_count u32 start_ns i64 end_ns i64 (44 bytes).
use crate::R;

pub const REC: usize = 44;

#[derive(Clone, Debug, PartialEq)]
pub struct Rec {
    pub trace_id: [u8; 16],
    pub segment: u32,
    pub first_row: u32,
    pub row_count: u32,
    pub start_ns: i64,
    pub end_ns: i64,
}

impl Rec {
    fn put(&self, o: &mut Vec<u8>) {
        o.extend_from_slice(&self.trace_id);
        o.extend_from_slice(&self.segment.to_le_bytes());
        o.extend_from_slice(&self.first_row.to_le_bytes());
        o.extend_from_slice(&self.row_count.to_le_bytes());
        o.extend_from_slice(&self.start_ns.to_le_bytes());
        o.extend_from_slice(&self.end_ns.to_le_bytes());
    }
    fn get(b: &[u8]) -> Self {
        let u = |i: usize| u32::from_le_bytes(b[i..i + 4].try_into().unwrap());
        let l = |i: usize| i64::from_le_bytes(b[i..i + 8].try_into().unwrap());
        Rec { trace_id: b[..16].try_into().unwrap(), segment: u(16), first_row: u(20), row_count: u(24), start_ns: l(28), end_ns: l(36) }
    }
}

/// Sorts by trace id and encodes.
pub fn encode(mut recs: Vec<Rec>) -> Vec<u8> {
    recs.sort_by(|a, b| a.trace_id.cmp(&b.trace_id));
    let mut o = Vec::with_capacity(recs.len() * REC);
    recs.iter().for_each(|r| r.put(&mut o));
    o
}

/// The whole index held in memory (about 44 bytes per trace), loaded once.
pub struct Index(Vec<u8>);

impl Index {
    pub fn load(path: &std::path::Path) -> R<Self> {
        Self::from_bytes(std::fs::read(path)?)
    }
    pub fn from_bytes(b: Vec<u8>) -> R<Self> {
        if b.len() % REC != 0 {
            return Err("traces.idx length is not a multiple of the record size".into());
        }
        Ok(Self(b))
    }
    pub fn len(&self) -> usize {
        self.0.len() / REC
    }
    /// Binary search by trace id.
    pub fn find(&self, id: &[u8; 16]) -> Option<Rec> {
        let (mut lo, mut hi) = (0, self.len());
        while lo < hi {
            let mid = (lo + hi) / 2;
            let r = &self.0[mid * REC..(mid + 1) * REC];
            match r[..16].cmp(&id[..]) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(Rec::get(r)),
            }
        }
        None
    }
    #[cfg(test)]
    pub fn recs(&self) -> impl Iterator<Item = Rec> + '_ {
        self.0.chunks_exact(REC).map(Rec::get)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(n: u8) -> Rec {
        let mut id = [0u8; 16];
        id[0] = n;
        id[15] = n.wrapping_mul(7);
        Rec { trace_id: id, segment: n as u32 * 3, first_row: n as u32 * 11, row_count: n as u32 + 1, start_ns: -(n as i64) * 1_000_000_007, end_ns: i64::MAX - n as i64 }
    }

    #[test]
    fn round_trip_and_search() {
        // shuffled input, 200 traces
        let all: Vec<Rec> = (0..200u32).map(|i| rec(((i * 37) % 200) as u8)).collect();
        let bytes = encode(all.clone());
        assert_eq!(bytes.len(), 200 * REC);
        let idx = Index::from_bytes(bytes).unwrap();
        assert_eq!(idx.len(), 200);
        for r in &all {
            assert_eq!(idx.find(&r.trace_id).as_ref(), Some(r));
        }
        // sorted order on disk
        let ids: Vec<_> = idx.recs().map(|r| r.trace_id).collect();
        assert!(ids.windows(2).all(|w| w[0] < w[1]));
        // absent ids: below, between and above the stored ones
        assert!(idx.find(&[0xff; 16]).is_none());
        let mut miss = rec(5).trace_id;
        miss[15] ^= 1;
        assert!(idx.find(&miss).is_none());
        assert!(Index::from_bytes(vec![]).unwrap().find(&[0; 16]).is_none());
        assert!(Index::from_bytes(vec![0; 5]).is_err());
    }
}
