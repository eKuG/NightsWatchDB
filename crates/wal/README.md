# nwdb-wal

The write-ahead log of NightsWatchDB. An ingest request is appended and
acknowledged to the sender only after the log is synced to disk. After a crash
the engine replays the log from the last record it had already written into
data files. The log is trimmed once data is safely in data files.

One thread owns the active file and does group commit: it writes everything
queued, syncs once, then runs the callbacks. See the crate docs
(`cargo doc -p nwdb-wal --open`) for the API, the failure rule, and
`WalOptions`.

`SyncMode::None` never syncs. It exists for tests and benchmarks only; records
are acknowledged without being durable.

## Sequence numbers

The first record ever written is 1. Numbers grow by exactly 1 per record
across files and restarts and are never reused. A record's number is not
stored; it is the file's first number plus the record's index in the file.

## Files

A log is a directory of files named `wal-<first sequence number>.log`, the
number written as 20 decimal digits, for example
`wal-00000000000000000001.log`. Other names in the directory are ignored.

All integers are little-endian. Checksums are CRC32C (Castagnoli).

```text
file   = header record*

header = magic        4 bytes   "NWAL"
         version      u32       1
         first_lsn    u64       sequence number of the first record in this file
         crc          u32       CRC32C of the preceding 16 bytes          (20 bytes)

record = length       u32       payload length in bytes, at most 256 MiB
         crc          u32       CRC32C over the 4 length bytes then the payload
         payload      length bytes
```


The header's first number must be at least 1 and below `u64::MAX`; anything
else is `Error::Corrupt`, as is a sequence number that would overflow.

A new file is started when the active one would grow past `max_file_bytes`; a
single record larger than the limit gets a file to itself. The old file is
synced before the new one is used.

### File creation

A file never has its final name until its header is durable. A new file is
created as `wal-<n>.log.tmp`, its header is written and synced, it is renamed
to `wal-<n>.log`, and the directory is synced. The directory is synced after
every removal too. A crash can therefore leave a `.tmp` file, which `open`
removes, but never a log file with a missing or unsynced header (on
filesystems that persist a file's size before its data, such a file would be
a zero-filled header). A newly created log directory is synced into its parent
(best effort: skipped on platforms where a directory cannot be synced).

## Recovery

`Wal::open` lists the files and checks them in order.

- Leftover `.tmp` files are removed.
- With `min_next_lsn` set, files wholly below it are removed first (see
  "Keeping numbering above the engine's position"); the checks below apply to
  the files that remain.
- File numbers must be contiguous: each file's first number is the previous
  file's last plus one. A gap or overlap is `Error::Corrupt`.
- In every file except the last, an invalid header, a short record or a
  checksum mismatch is `Error::Corrupt`. A header whose number differs from
  its file name is `Error::Corrupt` too.
- A last file shorter than a full header, or exactly a header long with an
  invalid header, is removed: it was being created when the process died and
  never held a record. Its predecessor must then be intact, like any non-last
  file. A last file longer than a header with an invalid header is
  `Error::Corrupt`.
- Appending continues in the last file if it has room.
- With `SyncMode::Data`, the last file and the directory are synced before
  `open` returns, so every record `Recovery` reports is durable. A record that
  a dead process wrote but never synced is therefore either made durable by
  recovery or cut as a torn tail, never replayed and then lost.

### Damage versus torn tail

When a record in the last file is incomplete or fails its checksum, `open`
scans the rest of the file for any later position at which a complete record
with a valid checksum can be parsed.

- If none exists, the bad record is a torn tail. The file is truncated to the
  end of the last good record and synced, and `Recovery::torn_bytes_discarded`
  reports how much was cut.
- If one exists, the file is damaged, not torn: later records were fully
  written, so they may have been acknowledged. `open` returns `Error::Corrupt`
  naming the file, the offset of the bad record and the offset of the valid
  one, and does not modify the file.

The scan costs one pass over the tail in practice: a candidate whose length
field exceeds the remaining bytes is skipped without hashing. An adversarial
tail where every length field just fits would cost quadratic time in the tail
size. (A torn record whose partial payload happens to contain a complete valid
record is reported as damage; the override below resolves it.)

`WalOptions::discard_after_damage` is an explicit operator override. When
true, `open` cuts the log at the first bad record as for a torn tail.
**This throws away acknowledged data**, and its sequence numbers are reused.

### Keeping numbering above the engine's position

`WalOptions::min_next_lsn` is the lowest number the next record may get. The
engine sets it to one past the highest position it has written into data
files. When it is set, `open` does this, in order:

1. Removes every file all of whose records are numbered below it (the
   following file starts at or below it), oldest first, syncing the directory
   after each, and never the newest file. It is the removal a trim would do, and
   it happens before any file is validated: after a crash between a data-file
   commit and the trim such files remain, and damage or gaps in them are not
   errors.
2. Validates the rest as usual.
3. Compares the log's next number with `min_next_lsn`. In healthy operation the
   log's next number is never below it: records reach data files only after
   they are durable in the log, and `truncate_before` never removes the newest
   file. A log that ends below it therefore proves that log files were lost,
   and the records acknowledged after the last write into data files are gone.
   By default `open` returns `Error::Corrupt`, names the missing range and
   `WalOptions::allow_lost_log`, and changes nothing more (a missing log
   directory is not even created). With `allow_lost_log` set, `open` removes
   all log files (oldest first, syncing the directory after each), starts a new
   file whose first number is `min_next_lsn`, and reports
   `Recovery::reset_to` and `Recovery::records_discarded` (the records still on
   disk that the reset removed; files removed in step 1 are not counted).

If `min_next_lsn` is at or below the next number it has no further effect. A
brand-new log with `min_next_lsn` of 1 is never refused.

### Replay

`replay(from)` is `Error::Corrupt` when the log is not empty and `from` is
below the first number on disk (those records were trimmed), and when `from`
is above the last number plus one (the log is behind the caller). `from` equal
to last plus one replays nothing. `from` of 0 is treated as 1.

`Wal::durable_lsn()` is the highest number covered by a completed sync; right
after `open` it equals `Recovery::last_lsn`.

## Failure

If a write or a sync fails, or the log thread panics, the log is poisoned:
every unacknowledged record, including the batch being written, gets
`Error::Fatal`, later appends fail at once with `Error::Fatal`, and nothing is
retried. `Fatal` means the record's outcome is unknown: it may or may not be on
disk and may reappear on replay after the restart. After a failed `fsync` the
file's contents are unknown; the process must restart and let recovery decide.
A record rejected for being too large is `Error::InvalidArgument`, the
caller's mistake.

## Calling rules

- `append` callbacks run on the log thread; an immediate rejection (too large,
  or poisoned) runs the callback on the calling thread, inside the call.
- A callback must not block, must not call `append_blocking` (it would wait
  for the thread it runs on), and must not drop the last handle to the log
  (it would join its own thread). The caller must not hold a lock that the
  callback takes while calling `append`.
- Every record accepted by `append` has its callback run exactly once.
- The queue is unbounded. Callers bound memory with `stats().pending_bytes`.
- `append` copies the record once; `append_owned` takes it without copying.
  The writer gathers small records into a 64 KiB buffer and writes larger
  payloads directly, so no concatenated copy of the batch exists.
