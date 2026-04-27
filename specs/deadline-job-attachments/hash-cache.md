# Hash Cache

## Hash Cache (Local)

SQLite database at `~/.deadline/job_attachments/hash_cache.db`. Table `hashesV4`
— shared with the Python CLI for zero-cost switching between tools.

### Schema

Table `hashesV4`. Primary key: `(file_path, hash_algorithm, range_start, range_end)`.

| Column | Type | Description |
|--------|------|-------------|
| `file_path` | blob | Absolute file path, UTF-8 encoded (Python uses `surrogatepass`) |
| `hash_algorithm` | text | e.g., `"xxh128"` |
| `file_hash` | text | Hex-encoded hash value |
| `last_modified_time` | timestamp | String in Python's `str(datetime.fromtimestamp(st_mtime))` format |
| `range_start` | integer | 0 for whole-file hashes |
| `range_end` | integer | -1 for whole-file hashes |

Entry types:
- Whole-file: `range_start=0, range_end=-1`
- Byte-range: `range_start >= 0, range_end > range_start` (for chunked files)

Cache hit requires exact string mtime match — any file modification triggers
a rehash. No eviction policy — entries persist indefinitely.

### Timestamp format (`last_modified_time`)

Matches Python's `str(datetime.fromtimestamp(os.stat().st_mtime))`:
- Local time, no timezone suffix
- `"2025-04-21 12:08:54"` when microseconds == 0
- `"2025-04-21 12:08:54.123456"` (6-digit) when microseconds != 0

Rust replicates Python's float-precision path: `(secs, nsec)` → `f64` →
extract microseconds from the float → format. This matches the precision
loss inherent in Python's `os.stat().st_mtime` (a C `double`).

Edge case: when nanoseconds are close to 1 second (e.g., 999999500ns),
float rounding can produce microseconds=1000000, which carries into the
seconds field.

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
