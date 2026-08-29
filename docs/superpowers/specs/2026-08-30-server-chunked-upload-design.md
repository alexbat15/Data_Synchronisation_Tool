# Server Chunked Upload Design

## Goal

Add a resumable, low-bandwidth server upload protocol that receives only changed fixed-size chunks, validates all received data, atomically publishes complete files, and updates the server manifest throughout the transfer. Client implementation is out of scope, but the API contract must be ready for it.

## Scope and assumptions

- The client splits each file into ordered, fixed-size chunks and uses SHA-256 for the file and every chunk.
- Only one target version may be uploading to a relative path at a time. Re-initializing the same path and file hash resumes it; initializing a different hash replaces its incomplete session.
- Existing whole-file `POST /upload` clients remain supported.
- Authentication, deletion synchronization, cross-file deduplication, and automatic age-based cleanup of abandoned sessions are out of scope.

## Current implementation findings

The current chunking prototype cannot run or persist correct state:

- `server/storage.py` imports the misspelled `haashlib` module.
- The `server_files` DDL is invalid: it is missing a comma, references a nonexistent `chunk_num`, and points its foreign key at a nonexistent table.
- The chunk table uses `path` as its sole primary key, preventing more than one chunk per file.
- Lookup methods return cursors rather than rows, and `lookup_file` returns nothing.
- The `/files/init` chunk lookup loop never terminates because a missing SQLite row does not raise an exception. It also repeatedly overwrites the literal `"chunk_num"` key.
- A dictionary cannot be reliably supplied by `Form(...)` as currently declared, and API booleans are returned as strings.
- New files, missing manifest rows, invalid hashes, interrupted uploads, path traversal, concurrent temporary files, and final assembly are not handled.
- `/upload` reads the whole file into memory and clears one global temporary directory, which makes concurrent uploads unsafe.

The implementation will replace the incomplete server storage code rather than preserve these broken internal interfaces. The existing public `/health` and `/upload` routes remain available.

## API contract

Paths use `/` as their separator. The server accepts `\` from Windows clients, normalizes it to `/`, rejects absolute paths, drive-qualified paths, empty paths, and `..`, and verifies that every destination remains below the configured storage directory.

All hashes are lowercase, 64-character SHA-256 hexadecimal strings.

### Initialize or resume

`POST /files/init` accepts JSON:

```json
{
  "rel_file_path": "reports/summary.dat",
  "file_hash": "<sha256>",
  "file_size": 2500000,
  "chunk_size": 1048576,
  "chunk_hashes": ["<sha256>", "<sha256>", "<sha256>"]
}
```

The number of chunk hashes must equal `ceil(file_size / chunk_size)`; an empty file has no chunk hashes. The server reconciles the committed manifest only when cheap file size/mtime checks show it is missing or stale. It then creates or resumes the pending manifest and returns only chunks that are neither already staged nor reusable at the same index from the committed file.

```json
{
  "status": "success",
  "up_to_date": false,
  "file_hash": "<sha256>",
  "missing_chunks": [1, 2]
}
```

An already-current file returns `up_to_date: true` and an empty list without creating a pending session.

### Upload one chunk

`POST /files/chunks` accepts multipart form data:

- `rel_file_path`
- `file_hash`, identifying the active target version
- `chunk_num`, a zero-based integer
- `chunk_hash`
- `chunk`, the binary upload

The server verifies the session, index, expected hash, and expected byte count. It streams the upload once to a uniquely named temporary file while calculating SHA-256, atomically moves the validated chunk into its staging location, and then records it as received. A retry of an already-recorded chunk whose staging file still exists succeeds idempotently.

```json
{
  "status": "success",
  "chunk_num": 1,
  "already_received": false
}
```

### Complete the file

`POST /files/complete` accepts JSON:

```json
{
  "rel_file_path": "reports/summary.dat",
  "file_hash": "<sha256>"
}
```

Before reusing committed ranges, the server checks the destination's size/mtime against the manifest again. If it changed after initialization, the server reconciles it once and returns HTTP 409 for any newly required chunks. If required chunks are otherwise missing, the server also returns HTTP 409 with their indices. Once all sources are valid, it streams staged chunks and unchanged ranges from the current committed file into an assembled file. It calculates the final SHA-256 and byte count during that single write. A mismatch rejects the completion and leaves the valid staged chunks resumable.

After validation, the server flushes the assembled file, atomically replaces the destination, and commits the new file/chunk manifest. It then removes the successful session's staging files. Repeating completion for an already-current file succeeds idempotently.

```json
{
  "status": "success",
  "rel_file_path": "reports/summary.dat",
  "file_hash": "<sha256>",
  "bytes_written": 2500000
}
```

### Errors

- HTTP 400: malformed path/hash metadata, invalid chunk count, chunk size mismatch, or content hash mismatch.
- HTTP 404: no active upload exists for the path.
- HTTP 409: the supplied target hash is not the active version, or completion is missing chunks.
- HTTP 422: FastAPI request-shape validation errors.
- HTTP 500: unexpected storage or database failures; temporary data remains available when safe to support retry.

## Storage layout and manifest

Committed files retain their relative layout below `server_storage`. Pending data lives below `server_storage/tmp/uploads/<path-digest>/<file-hash>/`, so client-controlled path text is never used as a staging path.

SQLite contains four active tables:

- `server_files`: normalized `path` primary key, `size`, `mtime_ns`, `chunk_size`, `current_hash`, and `last_synched_at`.
- `server_file_chunks`: `path` plus `chunk_num` primary key, actual `chunk_size`, and `current_chunk_hash`; it cascades when its file row is removed.
- `pending_uploads`: normalized `path` primary key, `target_hash`, `size`, `chunk_size`, and created/updated timestamps.
- `pending_upload_chunks`: one row for every expected chunk, keyed by path and index, containing expected size/hash and nullable received size/hash/time fields.

`POST /files/init` writes the pending upload and expected chunk rows. Each successful `/files/chunks` request updates its received fields. `/files/complete` replaces committed file/chunk rows and removes pending rows in one SQLite transaction after the destination has been atomically replaced.

The connection enables foreign keys and uses a transaction for each multi-row state transition. The schema is versioned with `PRAGMA user_version`. Known incompatible prototype tables are preserved under legacy names when they conflict, rather than silently discarding their data; the currently unused `file_chunks` prototype table does not participate in the new manifest.

## Avoiding unnecessary computation

- A committed SHA-256 and its chunk hashes are trusted when the destination exists and its cheap size/mtime metadata still match the manifest.
- A legacy, externally changed, or crash-recovered file is scanned once. Whole-file and chunk hashes are calculated together during that pass and written back to the manifest.
- An uploaded chunk is hashed only while it is being streamed to staging. Subsequent init retries use its database record and file existence rather than recalculating it.
- Final assembly performs the unavoidable whole-file verification while writing, not as a second pass.
- The final manifest uses hashes already validated during upload/assembly and the destination's resulting stat metadata.

## Reliability and recovery

Chunk writes and final publication use temporary files followed by `os.replace`, preventing partial files from appearing committed. Temporary files are on the same filesystem as their targets so replacement remains atomic.

The filesystem replacement necessarily occurs immediately before the SQLite commit. If the process stops between them, the next initialization detects that the destination's size/mtime no longer match the committed row, scans it once, and repairs the manifest. If the database commit succeeds but staging cleanup fails, the committed result remains valid and later initialization can remove the obsolete staging directory.

The existing `/upload` route will also stream to a unique temporary file, validate before atomic replacement, and record a committed manifest using the server's default chunk size of 1,048,576 bytes. This preserves the current client while avoiding whole-file buffering and the shared-temp-directory race.

## Code organization

- `shared/models.py`: Pydantic request models for initialization and completion.
- `server/storage.py`: SQLite schema, migrations, row lookups, pending-session updates, and transactional commit operations. It has no pandas or file-hashing responsibility.
- `server/chunking.py`: path validation, streaming/hash helpers, manifest reconciliation, chunk staging, and final assembly.
- `server/main.py`: FastAPI application factory and thin HTTP routes, while retaining module-level `app` for `uvicorn server.main:app`.
- `tests/`: storage and API behavior tests using isolated temporary storage/database paths.

## Testing

Tests will be written before production changes and will prove:

- schema creation and multiple chunks per file;
- initialization of new and empty files;
- unchanged-file short-circuit without rehashing;
- changed-chunk reuse from a committed file;
- successful chunk validation and persistence;
- interrupted-session resume and idempotent chunk retry;
- rejection of wrong hashes, sizes, indices, target versions, and unsafe paths;
- 409 completion responses listing missing chunks;
- verified assembly and committed manifest replacement;
- manifest repair after destination metadata changes;
- continued atomic, manifest-aware behavior of the legacy whole-file route.
