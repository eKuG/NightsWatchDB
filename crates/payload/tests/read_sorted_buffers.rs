//! One `read_sorted` call reuses its buffers and its zstd context for every
//! block it reads. A buffer and a context per block made the phrase search of
//! the server slower: the decompression itself took longer, not only the
//! allocation. The context is allocated by zstd's C code, which the counting
//! allocator does not see; it is created together with the read buffer, which
//! it does see. The file holds one test, so nothing else allocates meanwhile.

use nwdb_payload::{PieceReader, PieceWriter, PieceWriterOptions};
use nwdb_testkit::alloc::{Counting, allocations};

#[global_allocator]
static ALLOC: Counting = Counting;

#[test]
fn read_sorted_allocates_per_call_not_per_block() {
    let dir = tempfile::tempdir().unwrap();
    let mut w = PieceWriter::create(
        dir.path(),
        PieceWriterOptions {
            block_bytes: 64 << 10,
            dedup_window_bytes: 1 << 20,
        },
    )
    .unwrap();
    // Distinct pieces of 32 KiB, in blocks of at most 64 KiB.
    let ids: Vec<u32> = (0..64u32)
        .map(|i| w.add(format!("{i:08} ").repeat(3641).as_bytes()).unwrap())
        .collect();
    w.finish().unwrap();
    let r = PieceReader::open(dir.path()).unwrap();
    assert!(r.block_count() >= 30, "{} blocks", r.block_count());

    let allocated = |ids: &[u32]| {
        let mut bytes = 0;
        let before = allocations();
        r.read_sorted(ids.iter().copied(), |_, b| bytes += b.len())
            .unwrap();
        (allocations() - before, bytes)
    };
    let (one, _) = allocated(&ids[..1]);
    let loads = r.blocks_loaded();
    let (all, bytes) = allocated(&ids);
    assert_eq!(bytes, 64 * 3641 * 9);
    assert!(r.blocks_loaded() - loads >= 30);
    assert!(
        all <= one + 2,
        "reading {} blocks made {all} allocations, one block {one}",
        r.blocks_loaded() - loads
    );
}
