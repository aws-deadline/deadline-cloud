---
status: archived
last_verified: e0346ce1aba96dee825d2c650f064d8740fa99aa
release_tag: "0.54.2"
scope: "Every persistent data structure that the tool reads from or writes to disk. Covers the configuration file INI format and section naming conventions, the job bundle directory layout and file discovery rules, the asset manifest JSON structure and versioning, the hash cache and S3 check cache SQLite schemas, the job history directory layout, and the incremental download checkpoint file format. Does not cover the semantics of individual configuration settings (see configuration_schema.md), how the configuration system is used by commands (see configuration.md), or the behavioral flows that produce or consume these structures (see the relevant feature pages)."
topics: "Configuration file INI format · Section naming conventions (profile-scoped, farm-scoped, queue-scoped) · Dependency chain for section names · File location and environment variable override · Atomic write via temporary file and replace · In-memory caching by modification time · Job bundle directory layout · template.yaml and template.json discovery rules · parameter_values.yaml and parameter_values.json schema · asset_references.yaml and asset_references.json schema · Symlink containment validation · Asset manifest JSON format · Manifest version 2023-03-03 · Canonical JSON serialization (RFC 8785 subset) · Manifest fields (hashAlg, manifestVersion, totalSize, paths) · Path entry fields (path, hash, size, mtime) · xxh128 hashing algorithm · Hash cache SQLite schema (hashesV4) · Whole-file and byte-range hash entries · File path blob encoding with surrogatepass · S3 check cache SQLite schema (s3checkV1) · 30-day entry expiry · Cache database location (~/.deadline/job_attachments/) · WAL journal mode · Job history directory layout · Month-based subdirectories · Sequential numbering scheme · Incremental download checkpoint JSON format · Checkpoint file naming convention · PID lock file · Eventual consistency overlap window · Job-level and session-level tracking"
---

# Data Flow

This page documents every persistent data structure that the tool reads from or writes to.
These are the contracts between features — if two features share state through a file or
cache, the format is defined here.

## Configuration File

### File Location

The configuration file is an INI-format file located at `~/.deadline/config`. This path can
be overridden by setting the `DEADLINE_CONFIG_FILE_PATH` environment variable to an
alternative path. When the environment variable is set, the tool uses that path instead of
the default. Tilde expansion (`~`) is applied to both the default path and the environment
variable value.

### INI Format

The file uses standard INI format as parsed by a standard INI configuration parser. It
consists of sections (delimited by `[section name]` headers) containing key-value pairs
(`key = value`). Section names may contain spaces.

### Section Naming Conventions

Settings are organized into sections using a hierarchical naming scheme that scopes values
to the active AWS profile and, where applicable, to the active farm and queue. The section
name for a setting is constructed by walking the setting's dependency chain and formatting
each level's value into a section name component.

The dependency chain works as follows:

1. **Top-level settings** (no dependency): These are stored in a section named after the
   setting's own section prefix. For example, the setting `defaults.aws_profile_name` is
   stored in the `[defaults]` section with key `aws_profile_name`. The setting
   `deadline-cloud-monitor.path` is stored in the `[deadline-cloud-monitor]` section with
   key `path`.

2. **Profile-scoped settings** (depend on `defaults.aws_profile_name`): The profile name is
   formatted using the pattern `profile-{value}`. For example, if the active profile is
   `myprofile`, the setting `defaults.farm_id` (which depends on `defaults.aws_profile_name`
   and has its own section format `{}`) is stored in section `[profile-myprofile defaults]`
   with key `farm_id`.

3. **Farm-scoped settings** (depend on `defaults.farm_id`): The farm ID is formatted using
   the pattern `{}` (the value itself). For example, if the active profile is `myprofile`
   and the active farm is `farm-abc123`, the setting `defaults.queue_id` (which depends on
   `defaults.farm_id` and has its own section format `{}`) is stored in section
   `[profile-myprofile farm-abc123 defaults]` with key `queue_id`.

4. **Queue-scoped settings** (depend on `defaults.queue_id`): The queue ID value is appended
   to the section prefix chain. For example, the setting `defaults.job_id` (which depends on
   `defaults.queue_id`) is stored in section
   `[profile-myprofile farm-abc123 queue-def456 defaults]` with key `job_id`.

The general rule is: the section name is a space-separated concatenation of all formatted
dependency values in the chain, followed by the setting's own section prefix (the part
before the dot in the setting name).

#### Example Configuration File

```ini
[deadline-cloud-monitor]
path = /Applications/DeadlineCloudMonitor.app/Contents/MacOS/DeadlineCloudMonitor

[defaults]
aws_profile_name = myprofile

[profile-myprofile settings]
job_history_dir = ~/.deadline/job_history/myprofile

[profile-myprofile defaults]
farm_id = farm-abc123def456

[profile-myprofile farm-abc123def456 settings]
storage_profile_id = sp-789xyz

[profile-myprofile farm-abc123def456 defaults]
queue_id = queue-111222333
job_attachments_file_system = COPIED

[profile-myprofile farm-abc123def456 queue-111222333 defaults]
job_id = job-aaa111bbb222

[settings]
auto_accept = false
log_level = WARNING
s3_max_pool_connections = 50

[telemetry]
opt_out = false
identifier = abc123-random-id
```

### Setting Defaults and Substitution

Each setting has a default value. When a setting is not present in the configuration file,
the default is returned. Some defaults contain substitution patterns — for example,
`{aws_profile_name}` in the default job history directory path is replaced with the current
value of the `defaults.aws_profile_name` setting.

See `config_file.py` for the reading/caching (mtime-based) and writing (atomic
temp-file-and-replace) implementation.

For the full list of settings, their defaults, and descriptions, see
[Configuration Schema](configuration_schema.md).

### Boolean Values

Boolean settings accept the following string values (case-insensitive):

| True values | False values |
|-------------|--------------|
| `yes`, `on`, `true`, `1` | `no`, `off`, `false`, `0` |

Any other value raises an error.

## Job Bundle Format

A job bundle is a directory containing the files needed to submit a job to Deadline Cloud.
The directory must contain a template file and may optionally contain parameter values and
asset references files.

### File Discovery Rules

When loading a job bundle, the tool looks for files by base name (without extension) in the
bundle directory. For each expected file, it checks for both `.json` and `.yaml` variants:

1. If both `{filename}.json` and `{filename}.yaml` exist, the tool reports an error: only
   one format is permitted per file.
2. If only one exists, that file is loaded.
3. If neither exists and the file is required, the tool reports an error.
4. If neither exists and the file is optional, the tool proceeds without it.

### Symlink Containment Validation

Before processing a job bundle, the tool validates that all files and directories within the
bundle resolve to paths inside the bundle directory. The bundle directory itself may be a
symlink, but every file and subdirectory within it must resolve (after following all
symlinks) to a path under the resolved bundle root. If any path resolves outside the bundle,
the tool reports an error.

### Template File

**Filename:** `template.json` or `template.yaml` (required)

The template file is an Open Job Description (OpenJD) job template. The tool requires the
template to be a top-level object (dictionary) containing at minimum a `specificationVersion`
field. The only supported specification version is `jobtemplate-2023-09`.

The template may contain a `parameterDefinitions` field, which is a list of parameter
definition objects. Each parameter definition includes at minimum a `name` field and a `type`
field. For the full Open Job Description template specification, see the
[OpenJD specification](https://github.com/OpenJobDescription/openjd-specifications).

The tool reads the template to extract parameter definitions and to submit the job to the
Deadline Cloud service. The template content is not modified by the tool.

### Parameter Values File

**Filename:** `parameter_values.json` or `parameter_values.yaml` (optional)

This file provides pre-selected values for the parameters defined in the template. Its
structure is:

```
{
  "parameterValues": [
    {
      "name": "<parameter name>",
      "value": "<parameter value>"
    },
    ...
  ]
}
```

Each entry in the `parameterValues` list is an object with a `name` field (matching a
parameter name from the template's `parameterDefinitions`) and a `value` field (the string
value to use for that parameter).

Parameter values that do not match any template parameter definition are preserved — they
may provide values for queue-level parameters or render-farm-specific parameters (such as
those prefixed with `deadline:`).

For PATH-type parameters with a `default` value but no `allowedValues` constraint and no
value provided in the parameter values file, the tool resolves the default relative to the
job bundle directory and converts it to an absolute path. The default must be a relative
path that resolves within the bundle directory; absolute defaults or defaults that resolve
outside the bundle are rejected with an error.

### Asset References File

**Filename:** `asset_references.json` or `asset_references.yaml` (optional)

This file declares which local file paths are inputs to or outputs from the job. Its
structure is:

```
{
  "assetReferences": {
    "inputs": {
      "directories": ["<path>", ...],
      "filenames": ["<path>", ...]
    },
    "outputs": {
      "directories": ["<path>", ...]
    },
    "referencedPaths": ["<path>", ...]
  }
}
```

| Field | Description |
|-------|-------------|
| `inputs.directories` | Directories whose entire contents are input to the job. All files within these directories (recursively) are uploaded as job attachments. |
| `inputs.filenames` | Individual files that are input to the job. |
| `outputs.directories` | Directories where the job writes output files. After job execution, files in these directories are uploaded as output attachments. |
| `referencedPaths` | Paths that are referenced by the job but are not necessarily input or output. These are used for path mapping but are not uploaded or downloaded. |

All paths are normalized (OS-specific path normalization) when loaded. When serialized back
to disk, each list is sorted alphabetically.

## Asset Manifest Format

Asset manifests describe the set of files associated with a job attachment root. They are
used during upload to record what was sent to content-addressed storage, and during download
to determine what needs to be retrieved.

### Versioning

Each manifest includes a `manifestVersion` field that identifies its schema version. The
tool uses a version registry to select the correct parser and serializer for each version.
The only supported version is `2023-03-03`.

If a manifest's version is missing, the tool reports an error: "Manifest is missing the
required 'manifestVersion' field." If the version is present but unrecognized, the tool
reports an error listing the supported versions.

### Version 2023-03-03 Schema

A version `2023-03-03` manifest is a JSON object with the following top-level fields:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `manifestVersion` | string | Yes | Must be exactly `"2023-03-03"`. |
| `hashAlg` | string | Yes | The hashing algorithm used for all file hashes. Must be `"xxh128"`. |
| `totalSize` | integer | Yes | The sum of all file sizes in bytes. |
| `paths` | array | Yes | A list of path entry objects. Must contain at least one entry. |

Each entry in the `paths` array is an object with the following fields:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `path` | string | Yes | The file path relative to the asset root. |
| `hash` | string | Yes | The hexadecimal hash of the file contents using the manifest's hash algorithm. Must be alphanumeric (letters and digits only). |
| `size` | integer | Yes | The file size in bytes. |
| `mtime` | integer | Yes | The file's last modification time as a Unix timestamp (seconds since epoch). |

### Hashing Algorithm

The only supported hashing algorithm is `xxh128` — the 128-bit variant of the xxHash
algorithm (specifically xxh3_128). Files are hashed by reading them in chunks (using the
system's default I/O buffer size) and feeding each chunk to the hasher. The result is a
hexadecimal string.

### Canonical JSON Serialization

When encoding a manifest to a string, the tool produces canonical JSON following a subset of
[RFC 8785](https://www.rfc-editor.org/rfc/rfc8785.html):

1. No whitespace is emitted between JSON tokens (compact format with separators `,` and `:`).
2. Object keys are sorted lexicographically.
3. The `paths` array is sorted by the `path` field using UTF-16 big-endian byte ordering
   (with the `surrogatepass` error handler for filenames containing surrogate characters).
4. All output uses ASCII encoding (`ensure_ascii=true`).

The current implementation implicitly follows the RFC 8785 specification because all object
keys fall within the ASCII range and the manifest only serializes strings and integers.

### Validation Rules

When decoding a manifest, the tool validates:

1. The `manifestVersion` field is present and equals `"2023-03-03"`.
2. The `hashAlg` field is present and is one of the supported algorithms (`"xxh128"`).
3. The `totalSize` field is present and is an integer.
4. The `paths` field is present, is a list, and contains at least one entry.
5. Each path entry contains all required fields (`path`, `hash`, `size`, `mtime`) with
   correct types (string, string, integer, integer respectively).
6. Each path entry's `hash` value is alphanumeric (matches the pattern `[a-zA-Z0-9]+`).

If any validation fails, the tool raises a manifest decode validation error with a
descriptive message.

### S3 Storage Layout

Manifests and file data are stored in S3 under the queue's job attachment settings root
prefix. The S3 key structure uses two top-level folders:

| Folder | Purpose |
|--------|---------|
| `Data/` | Content-addressed storage for file data. Files are stored by their hash. |
| `Manifests/` | Manifest files organized by resource hierarchy. |

Input manifests are stored under:
`{rootPrefix}/Manifests/{farmId}/{queueId}/Inputs/{guid}/`

where `{guid}` is a randomly generated identifier for each upload operation.

Output manifests are organized hierarchically:
`{rootPrefix}/Manifests/{farmId}/{queueId}/{jobId}/{stepId}/{taskId}/{timestamp}_{sessionActionId}/`

where `{timestamp}` is the session action's completion time formatted as an ISO datetime
string.

Each manifest stored in S3 includes metadata headers:
- `asset-root`: The asset root path from the submitting machine (ASCII paths).
- `asset-root-json`: A JSON-encoded version of the root path (used when the path contains
  non-ASCII characters; in this case `asset-root` also contains the JSON-encoded value for
  backward compatibility).
- `file-system-location-name`: The name of the file system location from the storage
  profile, if applicable.

## Cache Structures

Both caches are SQLite databases stored in the `~/.deadline/job_attachments/` directory. If
the `HOME` environment variable is not set, the tool cannot determine a default cache
location and reports an error.

Both caches use WAL (Write-Ahead Logging) journal mode for concurrent read access. Write
operations acquire a lock to ensure thread safety. If the database cannot be accessed (for
example, due to a file lock held by another process), the tool retries up to 3 times with
jittered delays between 0.5 and 1.5 seconds before reporting an error.

If SQLite is not available in the runtime environment, caching is disabled and all cache
lookups return no result. A warning is logged once.

### Hash Cache

**File:** `~/.deadline/job_attachments/hash_cache.db`

The hash cache stores previously computed file hashes to avoid re-hashing files that have
not changed. It supports two types of entries: whole-file hashes and byte-range hashes.

#### Schema: `hashesV4`

| Column | Type | Description |
|--------|------|-------------|
| `file_path` | blob | The absolute file path, encoded as UTF-8 with the `surrogatepass` error handler. Part of the composite primary key. |
| `hash_algorithm` | text | The hash algorithm identifier (e.g., `"xxh128"`). Part of the composite primary key. |
| `range_start` | integer | The start byte offset of the hashed range. `0` for whole-file hashes. Part of the composite primary key. |
| `range_end` | integer | The end byte offset (exclusive) of the hashed range. `-1` for whole-file hashes. Part of the composite primary key. |
| `file_hash` | text | The hexadecimal hash value. |
| `last_modified_time` | timestamp | The file's last modification time at the time the hash was computed. |

**Primary key:** `(file_path, hash_algorithm, range_start, range_end)`

**Entry types:**
- Whole-file hash: `range_start = 0`, `range_end = -1`. Represents the hash of the entire
  file.
- Byte-range hash: `range_start >= 0`, `range_end > 0` (and `range_end > range_start`).
  Represents the hash of the byte range `[range_start, range_end)`.

**Cache lookup:** An entry is looked up by the combination of file path, hash algorithm,
range start, and range end. The file path is encoded to a blob using UTF-8 with the
`surrogatepass` error handler before querying.

**Cache insertion:** Entries are inserted or replaced (upsert) using the same composite key.

**Eviction policy:** There is no eviction policy. Entries persist indefinitely.

**Version history:** The Python CLI uses table `hashesV4` with string timestamps. The Rust
CLI uses `hashesV5` with integer nanosecond timestamps (more precise, avoids float-to-string
issues). The two are incompatible — switching between CLIs causes a one-time re-hash of all
files. If the table does not exist when the cache is opened, it is created. Previous table
versions are not migrated.

### S3 Check Cache

**File:** `~/.deadline/job_attachments/s3_check_cache.db`

The S3 check cache records which S3 object keys have been confirmed to exist in the
content-addressed storage bucket. This avoids redundant S3 HEAD requests during upload.

#### Schema: `s3checkV1`

| Column | Type | Description |
|--------|------|-------------|
| `s3_key` | text | The full S3 object key. Primary key. |
| `last_seen_time` | timestamp | A Unix timestamp (as a float string) recording when the object was last confirmed to exist. |

**Primary key:** `(s3_key)`

**Cache lookup:** An entry is looked up by S3 key. If found, the `last_seen_time` is parsed
as a float and converted to a datetime. If the entry is older than 30 days, it is treated as
expired and the lookup returns no result. If the timestamp cannot be parsed, a warning is
logged and the entry is ignored.

**Cache insertion:** Entries are inserted or replaced (upsert) by S3 key.

**Eviction policy:** Entries expire after 30 days. Expired entries are not proactively
deleted — they are simply ignored on lookup and will be replaced on the next insertion for
the same key.

## Job History Directory

When a job is submitted (via CLI or GUI), the tool saves a copy of the job bundle to a
history directory for record-keeping. The history directory is configured by the
`settings.job_history_dir` setting, which defaults to
`~/.deadline/job_history/{aws_profile_name}` (where `{aws_profile_name}` is substituted
with the active AWS profile name).

### Directory Structure

```
{job_history_dir}/
└── YYYY-MM/
    ├── YYYY-MM-DD-01-SubmitterName-JobName/
    │   ├── template.json (or template.yaml)
    │   ├── parameter_values.json (or parameter_values.yaml)
    │   └── asset_references.json (or asset_references.yaml)
    ├── YYYY-MM-DD-02-SubmitterName-JobName/
    │   └── ...
    └── ...
```

### Naming Scheme

1. A month subdirectory is created using the format `YYYY-MM` (e.g., `2024-03`).
2. Within the month directory, each submission gets a directory named:
   `{YYYY-MM-DD}-{NN}-{submitter_name}-{job_name}`
   - `YYYY-MM-DD` is the submission date.
   - `NN` is a two-digit sequential number (zero-padded), starting at `01`. The tool scans
     existing directories matching the date prefix and increments past the highest existing
     number.
   - `submitter_name` is the submitter's name with non-alphanumeric characters removed
     (only letters, digits, spaces, hyphens, and underscores are kept).
   - `job_name` is the job name, similarly cleaned and truncated to 128 characters.

### Contents

The saved bundle contains the same files as the original job bundle (template, parameter
values, asset references) in the same format (JSON or YAML) as the original. The parameter
values file may be updated after the initial save if the submission callback returns
additional job parameters.

## Incremental Download Checkpoint

The `deadline queue sync-output` command uses a JSON checkpoint file to track download
progress across invocations. This allows the command to resume from where it left off
rather than re-downloading all output.

### File Location and Naming

Checkpoint files are stored in the directory specified by the `--checkpoint-dir` option,
which defaults to `~/.deadline/incremental_download`. The filename is constructed as:

`{queueId}_{storageProfileId}_download_checkpoint.json`

If the `--ignore-storage-profiles` option is used, the storage profile portion is replaced
with the literal string `ignore-storage-profiles`:

`{queueId}_ignore-storage-profiles_download_checkpoint.json`

A companion PID lock file is created alongside the checkpoint to prevent concurrent
executions:

`{queueId}_{storageProfileId}_download_checkpoint.json.pid`

### JSON Schema

```
{
  "localStorageProfileId": "<storage profile ID or null>",
  "downloadsStartedTimestamp": "<ISO 8601 datetime>",
  "downloadsCompletedTimestamp": "<ISO 8601 datetime>",
  "eventualConsistencyMaxSeconds": <integer>,
  "jobs": [
    {
      "job": { <job object as returned by the SearchJobs API> },
      "sessionEndedTimestamp": "<ISO 8601 datetime or absent>",
      "sessionCompletedIndexes": {
        "<sessionId>": <integer>,
        ...
      }
    },
    ...
  ]
}
```

| Field | Type | Description |
|-------|------|-------------|
| `localStorageProfileId` | string or null | The storage profile ID of the machine running the download, or `null` if `--ignore-storage-profiles` was used. Must match between checkpoint and CLI invocation. |
| `downloadsStartedTimestamp` | ISO 8601 string | The timestamp when the checkpoint was first bootstrapped. |
| `downloadsCompletedTimestamp` | ISO 8601 string | The timestamp up to which downloads are confirmed complete. On bootstrap, this equals `downloadsStartedTimestamp`. |
| `eventualConsistencyMaxSeconds` | integer | The overlap window (in seconds) applied to SearchJobs queries to account for eventual consistency in the service's materialized views. Defaults to 120 seconds. |
| `jobs` | array | The list of jobs being tracked. |

Each job entry contains:

| Field | Type | Description |
|-------|------|-------------|
| `job` | object | The job object as returned by the Deadline Cloud SearchJobs API. All datetime values within this object are serialized to ISO 8601 strings. |
| `sessionEndedTimestamp` | ISO 8601 string (optional) | The largest `endedAt` timestamp among sessions whose output has been downloaded. Absent when the job has no job attachments. Used to detect requeued jobs. |
| `sessionCompletedIndexes` | object (optional) | A mapping from session ID to the index of the latest completed session action download. Session action IDs are sequential (e.g., `sessionaction-abc123-12` for index 12). Absent or empty when no session actions have been downloaded. |

### Lifecycle

1. **Bootstrap:** On first run (or with `--force-bootstrap`), the tool creates a new
   checkpoint with `downloadsStartedTimestamp` set to the current time minus the
   `--bootstrap-lookback-minutes` value (default: 0 minutes).
2. **Resume:** On subsequent runs, the tool loads the existing checkpoint and continues from
   `downloadsCompletedTimestamp`.
3. **Storage profile validation:** The checkpoint's `localStorageProfileId` must match the
   current invocation's storage profile. A mismatch produces an error.
4. **Save:** After each successful (non-dry-run) invocation, the updated checkpoint is
   written atomically using the same temporary-file-and-replace strategy as the
   configuration file.

### Tracking Model

The checkpoint tracks state at three levels to reconstruct a stream of completed session
actions (which the Deadline Cloud APIs do not provide directly):

1. **Job level:** The `jobs` list contains every job that is active and that has had output
   downloaded. When a job becomes inactive, it retains a minimal stub including the
   `sessionEndedTimestamp` to detect requeued jobs.
2. **Session level:** The `sessionCompletedIndexes` within each job tracks every session
   that is either still running or whose `endedAt` timestamp is at or after the
   `downloadsCompletedTimestamp`. When a job is requeued, the stored
   `sessionEndedTimestamp` allows skipping sessions from before the requeue.
3. **Session action level:** Session action IDs are sequential, so for each session the
   checkpoint stores the highest index for which the download is complete.

## Observations

1. **[DESIGN]** The hash cache (hashesV4) has no eviction policy. Entries accumulate
   indefinitely, which means the cache database grows without bound as more files are
   hashed over time. This may be intentional to maximize cache hit rates, but could become
   a concern for workstations that process many unique files over long periods.

2. **[DESIGN]** The S3 check cache uses a 30-day expiry evaluated at lookup time but does
   not proactively delete expired entries. Over time, the database file may contain a
   significant number of expired entries that consume disk space but are never returned.

3. **[DESIGN]** The configuration file's atomic write strategy creates a temporary file with
   the configuration file path as a prefix. If the tool crashes after creating the temporary
   file but before the atomic replace, orphaned temporary files may accumulate in the
   configuration directory.

4. **[ASSUMPTION]** The hash cache stores file paths as blobs using UTF-8 encoding with the
   `surrogatepass` error handler. This suggests the tool may encounter filenames with
   surrogate characters (common on some Linux filesystems). The same encoding is used in the
   manifest's canonical JSON path sorting. This encoding choice appears consistent across
   both systems.

5. **[DESIGN]** The job history directory naming scheme uses a sequential number that is
   determined by scanning existing directories. If directories are deleted or renamed, the
   numbering may produce gaps or reuse numbers, but this does not affect correctness since
   the directories are only used for record-keeping.

6. **[INCONSISTENCY]** The `clear_setting` operation writes the default value back to the
   configuration file rather than removing the key. This means a cleared setting will not
   pick up future changes to the default value — it is frozen at the default that was
   current when the setting was cleared. This is documented in
   [Configuration](configuration.md) but is worth noting as a data format concern: a
   configuration file with all settings explicitly set to their defaults is
   indistinguishable from one where settings were cleared.

7. **[DESIGN]** The incremental download checkpoint's `eventualConsistencyMaxSeconds`
   defaults to 120 seconds, derived from heavy load testing with a generous margin. This
   value is stored in the checkpoint file, so changing the default in a future version does
   not affect existing checkpoints.
