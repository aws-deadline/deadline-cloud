# Path Mapping

Cross-OS path remapping for job attachments. When a job is submitted from
one OS (e.g. Windows) and downloaded on another (e.g. Linux), file paths
in manifests need to be transformed using storage profile location mappings.

## How Rules Are Generated

Path mapping rules are generated from a source and destination storage
profile. Each file system location name shared between both profiles
produces one rule. If the storage profile IDs are the same, no rules are
generated (no mapping needed).

The source OS family determines the path format: Windows paths use
backslash separators and case-insensitive matching, POSIX paths use
forward slashes and case-sensitive matching.

## How Matching Works

The path mapper uses a trie (prefix tree) where each level corresponds
to a path component. Given a source path, the mapper walks the trie
component by component, recording each intermediate match. The longest
(most specific) match wins.

For example, if rules exist for both `/mnt/Projects` and
`/mnt/Projects/Special`, a path under `/mnt/Projects/Special/scene.blend`
matches the more specific rule.

### Path splitting

Mirrors Python's `PurePosixPath.parts` / `PureWindowsPath.parts`:
- POSIX: `"/mnt/shared"` → `["/", "mnt", "shared"]`
- Windows: `"C:\foo\bar"` → `["C:\", "foo", "bar"]`

### Case sensitivity

- POSIX: case-sensitive (trie keys stored as-is)
- Windows: case-insensitive matching (trie keys lowercased), but
  remaining path components after the match preserve original case

## Transform Modes

**Standard transform** returns the transformed path if a rule matches,
or the original path unchanged if no rule matches.

**Strict transform** returns the transformed path, or an error if no
rule matches. Used when all paths must be mappable (e.g., download to
a different OS where unmapped paths would be invalid).

## Differences from Python

None — identical observable behavior. The Rust implementation uses the
same trie algorithm.
