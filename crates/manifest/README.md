# nwdb-manifest

The manifest lists the parts that are live and records how far the
write-ahead log has been written into parts. Parts are immutable, so this is
the one piece of mutable metadata in the database.

A reader calls `Manifest::current()` and gets an `Arc<ManifestState>` that
never changes; it sees a consistent set of part directories no matter what
writers do afterwards. A writer changes the set of parts (a flush adds parts, a
compaction adds one and removes several) with `Manifest::commit(Edit)`. The
change becomes visible to every reader at once, as one new numbered file.

## Files

The manifest directory holds one file per version, named by the version as 20
decimal digits plus `.json`, for example `00000000000000000007.json`. Files are
never modified once published. Version 0 is the empty state that exists before
the first commit; it is not a file.

A file is:

```
<one line of JSON>\n
crc32c:<8 lowercase hex digits>\n
```

The checksum is CRC-32C over the bytes of the JSON line, without its newline.
Unknown JSON fields are ignored on read, so a newer writer that only adds
fields does not break an older reader. A `format_version` other than 1 is
refused, because an older reader cannot know what a changed layout means.

Anything else in the directory is ignored, apart from temporary files (names
starting `.tmp-`), which `open` removes. `open` cannot tell a crashed
commit's temporary file from one a live writer is using; if it removes the
live one, that writer's link step fails with `NotFound` before anything is
published, and `commit` simply writes the file again.

## Publishing protocol (local disk)

1. Apply the edit to the current state and serialise the result as above.
2. Write it to a uniquely named temporary file in the manifest directory and
   sync the file.
3. Publish with `hard_link(temporary, "<version>.json")`. The link is atomic
   and fails with `AlreadyExists` if the version is taken; that is the
   conflict signal and becomes `Error::Conflict`. A reader can never see a
   partly written manifest under a final name, because the content was
   complete and synced before the name existed.
4. Remove the temporary file and sync the directory.
5. Delete manifest files older than the newest `keep_versions` (default 10).
   This is best effort; a failure to delete is not an error.

The only primitive is "create this name if it does not exist". There is no
rename over an existing file and no in-place update, so the same protocol maps
onto an object store's conditional put.

Before step 3 the writer also lists the directory and gives up with a
conflict if it finds its own version or a higher one. Without that, a handle
that fell far behind could publish an old version number after cleanup had
removed it, and the commit would be silently lost.

## Loading and corruption

The highest-numbered file wins. If it does not parse, fails its checksum,
holds a different version than its name says, or breaks an invariant (parts not
sorted and unique, a part id not below `next_part_id`), `open` and `reload`
return `Error::Corrupt` naming the file.

They deliberately do not fall back to the next older file. An older version
could list parts that a compaction already removed, whose directories may be
deleted, or could lack parts and a log position that were acknowledged to
clients. Stopping and asking for repair is safer than serving a wrong but
plausible state.

## Conflicts and retries

Several handles, in one process or many, can share a directory. The one that
publishes version N+1 first wins. The others get `Error::Conflict`, keep their
old in-memory state, and should call `reload()`, re-decide their edit against
the new state (for example, a compaction input may have been removed
meanwhile) and commit again.

If `commit` returns an I/O error after the file was published (the final
directory sync), the new version is already current. Call `reload()` to see
what happened rather than repeating the edit.

## Part ids and crashes

`reserve_part_ids(n)` hands out ids for parts a writer is about to write. It
is in memory only: nothing on disk records a reservation until a commit adds
the parts, which raises `next_part_id` above them.

So after a crash there can be part directories (final, or `.tmp` while being
written) that the manifest never learned about, and a restarted process will
hand out the same ids again. At startup, before any id is reserved and before
any flush runs, the engine must delete every directory returned by
`unreferenced_part_dirs(parts_dir, &state)`. Run it only then: while a flush is
in flight, its directory is unreferenced too.

Two processes must not write to one data directory, since both would reserve
the same ids; if they do, the second to commit gets `InvalidArgument` because
the id is already live.

## Platform notes

Directory sync is done on Unix only. The create-if-absent behaviour of hard
links is relied on from the local filesystem; network filesystems with weak
link semantics are not supported.
