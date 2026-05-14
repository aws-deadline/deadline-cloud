# Manifest Format

## Version

`2023-03-03` — the only supported version. Handled internally by
`openjd_snapshots::decode_v2023` / `encode_snapshot_v2023`.

## JSON Structure

```json
{
  "manifestVersion": "2023-03-03",
  "hashAlg": "xxh128",
  "totalSize": 1234567,
  "paths": [
    {
      "path": "scenes/render.blend",
      "hash": "a1b2c3d4e5f6...",
      "size": 1234567,
      "mtime": 1705312200000000
    }
  ]
}
```

## Rust Types

Manifests are represented directly as `openjd_snapshots::Snapshot`
(type alias for `Manifest<Rel, Full>`). Individual file entries are
`openjd_snapshots::FileEntry`.

| Field | Type | Notes |
|-------|------|-------|
| `path` | `String` | Relative POSIX-style. Never starts with `/`. |
| `hash` | `Option<String>` | 32-char hex xxh128. `None` before hashing. |
| `size` | `Option<u64>` | File size in bytes. |
| `mtime` | `Option<u64>` | Modification time in microseconds since epoch. |

## FileEntry Fields

- `path` — relative POSIX-style path. Windows paths converted at creation time
  (backslashes → forward slashes). Never starts with `/`.
- `hash` — hex-encoded content hash (xxh128, 32 hex characters)
- `size` — file size in bytes
- `mtime` — modification time in microseconds since epoch, truncated (not
  rounded) from nanoseconds: `trunc(mtime_ns / 1000)`. The download side
  restores mtime from this value.

## Canonical Encoding

Deterministic serialization for byte-identical output across platforms:
- Paths sorted by UTF-16 BE byte ordering (matches Python's `sort(key=...)`)
- Object keys sorted lexicographically
- Compact format (no whitespace between tokens)
- Non-ASCII characters escaped to `\uXXXX`

Sort applied at manifest construction time, not deferred to encoding. This
eliminates a class of bugs where the manifest's path order depends on whether
`encode()` has been called.

## Hash Algorithm

`xxh128` — fast non-cryptographic hash via the `xxhash-rust` crate.
`HashAlgorithm` enum currently has one variant. The hash cache table
(`hashesV5`) stores the algorithm alongside the hash value, allowing
future algorithm additions without cache invalidation.

## S3 Storage Layout

Manifests and file data are stored under the queue's job attachment settings
root prefix:

| Folder | Purpose |
|--------|---------|
| `Data/` | Content-addressed storage. Files stored as `{hash}.{algorithm}` |
| `Manifests/` | Manifest files organized by resource hierarchy |

Input manifests: `{rootPrefix}/Manifests/{farmId}/{queueId}/Inputs/{guid}/`
where `{guid}` is a random identifier per upload operation.

Output manifests: `{rootPrefix}/Manifests/{farmId}/{queueId}/{jobId}/{stepId}/{taskId}/{timestamp}_{sessionActionId}/`

S3 object metadata headers on each manifest:
- `asset-root` — asset root path from the submitting machine
- `asset-root-json` — JSON-encoded root path (for non-ASCII paths;
  `asset-root` also contains JSON in this case for backward compatibility)
- `file-system-location-name` — storage profile location name, if applicable

## Manifest Merging

Multiple manifests for the same asset root are merged by path — later
entries overwrite earlier ones. Different hash algorithms across manifests
being merged is an error.

## Output Manifest Retrieval

`read_output_manifests_from_s3`:
1. Lists manifest objects under the job's output prefix in S3
2. Selects the latest per task (by alphabetical sort of timestamp folders)
3. Downloads each manifest
4. Extracts asset root from S3 object metadata (`asset-root-json` preferred,
   `asset-root` fallback)
5. Groups manifests by asset root
