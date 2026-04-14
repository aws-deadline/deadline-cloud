# Hash Cache

## Hash Cache (Local)

SQLite database at `~/.deadline/job_attachments/hash_cache.db`. Table `hashesV5`
(not compatible with legacy `hashesV4` — first run after migration starts cold).

### Schema

Table `hashesV5`. Primary key: `(file_path, hash_algorithm, range_start, range_end)`.

| Column | Type | Description |
|--------|------|-------------|
| `file_path` | text | Absolute file path |
| `hash_algorithm` | text | e.g., `"xxh128"` |
| `file_hash` | text | Hex-encoded hash value |
| `last_modified_time` | integer | Nanoseconds since epoch |
| `range_start` | integer | 0 for whole-file hashes |
| `range_end` | integer | -1 for whole-file hashes |

Entry types:
- Whole-file: `range_start=0, range_end=-1`
- Byte-range: `range_start >= 0, range_end > range_start` (for chunked files)

Cache hit requires exact nanosecond mtime match — any change triggers a rehash.
No eviction policy — entries persist indefinitely.

**Version history:** Python CLI uses `hashesV4` with string timestamps and
`surrogatepass` blob encoding for paths. Rust CLI uses `hashesV5` with integer
nanosecond timestamps and text paths. The two are incompatible — switching
between CLIs causes a one-time re-hash of all files.

### Database Configuration

- WAL journal mode (set on every open)
- Lock contention: 3 retry attempts with 0.5-1.5s jittered delay
- Jitter derived from system clock nanoseconds (no external RNG dependency)

## S3 Check Cache

SQLite database at `~/.deadline/job_attachments/s3_check_cache.db`.
Table `s3checkV1`. Primary key: `(s3_key)`.

| Column | Type | Description |
|--------|------|-------------|
| `s3_key` | text | Full S3 object key |
| `last_seen_time` | timestamp | Unix timestamp (float string) when object was last confirmed |

Entries expire after 30 days (evaluated at lookup time — expired entries
are ignored, not proactively deleted). Invalid timestamps treated as cache
misses with a warning log.

## Integrity Verification

`verify_hash_cache_integrity` samples up to 30 random entries from the hash
cache. For each sampled entry, calls HeadObject on the corresponding S3 key.
If any sampled file is missing from S3, the entire cache is reset.

This is a heuristic — won't catch every stale entry but catches bulk
invalidation (e.g., bucket was cleared).

When `settings.force_s3_check` is `true`, integrity verification is skipped
entirely because every file is verified via HeadObject before skipping upload.
This is the most reliable mode but slower.
