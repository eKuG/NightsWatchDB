# nwdb-manifest

The manifest lists the parts that are live and records how far the
write-ahead log has been written into parts. Parts are immutable, so this is
the one piece of mutable metadata in the database.

A reader calls `Manifest::current()` and gets an `Arc<ManifestState>` that
never changes; it sees a consistent set of part directories no matter what
writers do afterwards. A writer changes the set of parts (a flush adds parts, a
compaction adds one and removes several) with `Manifest::commit(Edit)`. The
change becomes visible to every reader at once, as one new numbered file.
`current()` takes only a brief read lock on a pointer, so readers never wait
for a commit in progress.

## One writer per data directory

The engine takes an exclusive lock on the data directory, so there is exactly
one process and it holds exactly one `Manifest`. Concurrent writers are not a
supported mode. Conflicts between handles are still detected (see below) as a
second line of defence, but nothing else in the engine (for example part id
reservation) is designed for several writers.

Each commit rewrites the whole part list, so it costs O(parts). That is fine
for now and will need an incremental form when part counts grow.

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
starting `.tmp-`), which `open` removes. With one writer these are always
leftovers of a crash. As defence against a second handle, `commit` writes its
file again if the link step reports `NotFound` (another handle's `open` removed
its temporary file) before anything was published.

## Opening

`Manifest::open(dir, mode, options)` takes an explicit `OpenMode`:

* `OpenMode::Create`: for the first start of a new database. The directory is
  created if missing (and its parent synced). An existing directory with no
  manifest file is version 0.
* `OpenMode::MustExist`: for every restart. The directory must exist and hold
  at least one manifest file, otherwise `Error::NotFound`. A wrong path, an
  unmounted volume or an operator mistake is an error, never an empty
  database.

The engine uses `Create` only when it is initialising a data directory (no
parts or log exist yet) and `MustExist` on every later start.

Durability at open: a previous process may have linked version N and died
before syncing the directory, so N could vanish in a power loss after the new
process acted on it (for example trimmed its log). `open` therefore syncs the
manifest directory after loading and before returning, so the version it
reports is durable. `reload()` does the same.

## Publishing protocol (local disk)

1. Apply the edit to the current state and serialise the result as above.
2. Write it to a uniquely named temporary file in the manifest directory and
   sync the file.
3. Publish with `hard_link(temporary, "<version>.json")`. The link is atomic
   and fails with `AlreadyExists` if the version is taken; that is the
   conflict signal and becomes `Error::Conflict`. A reader can never see a
   partly written manifest under a final name, because the content was
   complete and synced before the name existed.
4. Remove the temporary file and sync the directory. Only now does the new
   version become `current()`, so `current()` is always durable.
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
sorted and unique, a part id not below `next_part_id`, a `next_part_id` above
`MAX_NEXT_PART_ID`), `open` and `reload` return `Error::Corrupt` naming the
file.

They deliberately do not fall back to the next older file. An older version
could list parts that a compaction already removed, whose directories may be
deleted, or could lack parts and a log position that were acknowledged to
clients. Stopping and asking for repair is safer than serving a wrong but
plausible state.

## Errors

* `InvalidArgument`: the edit is invalid against the state this handle holds
  (removing a part that is not live, adding an id that is live, a position or
  id going backwards, a `next_part_id` above `MAX_NEXT_PART_ID`), or
  `reserve_part_ids` would pass that limit. This is the caller's bug. After a
  `Conflict` the caller must `reload()` and re-decide its edit against the new
  state; if the edit is then invalid (the part it wanted to remove is already
  gone) it was not re-decided correctly. A handle that has not reloaded learns
  of another writer's changes only through `Conflict`, never through
  `InvalidArgument`.
* `Conflict`: strictly "someone else published this version (or a newer one)
  first". Nothing changed. Reload, re-decide, retry.
* `Io` and other errors before the link: nothing changed; the handle is still
  usable.
* `Corrupt`: a damaged manifest, an implausible value on load, or
  `unreferenced_part_dirs` finding version 0 next to part directories.
* `NotFound`: `MustExist` on a missing or empty manifest directory.
* `Fatal`: see the next section.

## Failure after publishing: the handle is poisoned

If the link succeeded but the directory sync then fails, the new version is
visible on disk, whether it survives a crash is unknown, and the handle cannot
tell which. `commit` returns `Error::Fatal` and the handle is poisoned: every
later `commit` and `reload` returns `Fatal`. `current()` keeps returning the
last state known to be durable and is not advanced.

After `Fatal` the process must restart. Do not retry the edit and do not call
`reload()`. On restart `open` syncs the directory and reports whichever version
survived; the engine then recovers from that.

## Part ids and crashes

`reserve_part_ids(n)` hands out ids for parts a writer is about to write. It
is in memory only: nothing on disk records a reservation until a commit adds
the parts, which raises `next_part_id` above them. It returns
`InvalidArgument` instead of overflowing.

So after a crash there can be part directories (final, or `.tmp` while being
written) that the manifest never learned about, and a restarted process would
hand out the same ids again.

## Startup procedure for the engine

First start (a data directory being initialised): `open(.., OpenMode::Create, ..)`.

Every restart:

1. `open(.., OpenMode::MustExist, ..)`. `NotFound` or `Corrupt` stops the
   engine; nothing is deleted.
2. `highest_part_dir_id(parts_dir, next_part_id)` gives the largest id among
   final and temporary part directories, ignoring ids at or above
   `next_part_id + PART_ID_LEFTOVER_WINDOW` (2^32) and any id from
   `MAX_NEXT_PART_ID` up. Ids are handed out in order and only flushes that
   did not commit leave ids above `next_part_id`, so a leftover of this
   database is close above it; a directory further away is a stray, and
   raising `next_part_id` for it would use up the id space. Step 3 deletes
   strays like any other unreferenced directory. If it is `Some(h)` and
   `h >= next_part_id`,
   first commit `Edit { next_part_id: Some(h + 1), .. }`, so ids are never
   reused even if a later step fails.
3. Only then delete every directory returned by
   `unreferenced_part_dirs(parts_dir, &manifest.current())`. It returns
   `Corrupt`, and the engine must delete nothing, when the state is version 0
   and `parts_dir` holds any final part directory: an empty manifest next to
   existing parts is never a reason to delete them.
4. Start flushes and compactions and reserve ids.

Run steps 2 and 3 only at startup, before any writer runs: while a flush is
in flight its directory is unreferenced too.

## Platform notes

Directory sync is done on Unix only. The create-if-absent behaviour of hard
links is relied on from the local filesystem; network filesystems with weak
link semantics are not supported.
