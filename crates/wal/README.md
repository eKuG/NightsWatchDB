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

A new file is started when the active one would grow past `max_file_bytes`; a
single record larger than the limit gets a file to itself. The old file is
synced before the new one is used, the new file's header is synced before any
record follows it, and the directory is synced after a file is created or
removed.

## Recovery

`Wal::open` lists the files and checks them in order.

- File numbers must be contiguous: each file's first number is the previous
  file's last plus one. A gap or overlap is `Error::Corrupt`.
- In every file except the last, an invalid header, a short record or a
  checksum mismatch is `Error::Corrupt`. A header whose number differs from
  its file name is `Error::Corrupt` too.
- In the last file, the first record that is incomplete or fails its checksum
  marks the end of the log. The file is truncated to the end of the last good
  record and synced, and `Recovery::torn_bytes_discarded` reports how much was
  cut.
- A last file shorter than a full header is removed: it was being created when
  the process died and never held a record. Its predecessor must then be
  intact, like any non-last file. A last file with a complete but invalid
  header is `Error::Corrupt`.
- Appending continues in the last file if it has room.

**Damage in the middle of the last file hides the records after it.** A torn
write and a flipped byte look the same to the scanner, so valid records
following the first bad one in the last file are cut too. They were not yet
acknowledged only if the damage was a torn write; for other damage they may
have been. The size of the cut is reported, and a caller that wants to treat
a large cut as an error can do so from `Recovery`.

## Failure

If a write or a sync fails the log is poisoned: every unacknowledged record
gets the error, later appends fail at once, and nothing is retried. After a
failed `fsync` the file's contents are unknown; the process must restart and
let recovery decide.
