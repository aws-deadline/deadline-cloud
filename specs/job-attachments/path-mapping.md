# Path Mapping

Cross-OS path remapping for job attachments. When a job is submitted from
one OS (e.g. Windows) and downloaded on another (e.g. Linux), file paths
in manifests need to be transformed using storage profile location mappings.

## `generate_path_mapping_rules`

Takes a source and destination `StorageProfile`. Returns a `Vec<PathMappingRule>`
with one rule per file system location name shared between both profiles.

- Returns empty if `storage_profile_id` is the same (no mapping needed).
- Source OS family determines `source_path_format`: Windows → `"windows"`,
  Linux/macOS → `"posix"`.
- Location matching is by name only, regardless of type (SHARED vs LOCAL).

## `PathMappingRuleApplier`

Trie-based longest-prefix path matcher. Constructed from a list of
`PathMappingRule` values that must all share the same `source_path_format`.

### Construction

- Empty rules → `source_path_format` is `None`, empty trie.
- Mixed formats → error ("multiple source path formats").
- Unrecognized format → error ("Unexpected source path format").
- Builds a trie where each level corresponds to a path component.
  Windows trie keys are lowercased for case-insensitive matching.

### Path splitting

Mirrors Python's `PurePosixPath.parts` / `PureWindowsPath.parts`:
- POSIX: `"/mnt/shared"` → `["/", "mnt", "shared"]`
- Windows: `"C:\foo\bar"` → `["C:\", "foo", "bar"]`

### `transform(source_path) -> String`

Returns the transformed path if a rule matches, or the original path
unchanged if no rule matches (or no rules configured).

### `strict_transform(source_path) -> Result<PathBuf>`

Returns the transformed path as `PathBuf`, or error
"No path mapping rule could be applied" if no rule matches.

### Longest-prefix matching

When multiple rules overlap (e.g. `/mnt/Projects` and
`/mnt/Projects/Special`), the most specific (longest) match wins.
The trie traversal records each intermediate match and continues
deeper, using the last match found.

### Case sensitivity

- POSIX: case-sensitive (trie keys stored as-is).
- Windows: case-insensitive matching (trie keys lowercased), but
  remaining path components after the match preserve original case.

## Differences from Python

None — identical observable behavior. The Rust implementation uses the
same trie algorithm with `"."` replaced by a `destination` field on
`TrieNode`.
