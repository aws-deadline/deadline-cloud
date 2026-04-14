# Output Formatting

## YAML Output (default)

`cli_object_repr()` in `common.rs` serializes `serde_json::Value` to YAML.
Two post-processing steps after `serde_yaml::to_string()`:

1. **Multi-line string normalization.** Strings containing `\n` that don't
   end with `\n` get one appended. This forces serde_yaml to use `|-` block
   literal style instead of quoted strings with `\n` escapes. The appended
   newline is a YAML formatting artifact — the actual value is unchanged.

2. **YAML 1.1 boolean quoting.** A regex matches bare values in mapping
   values (`": `) or sequence items (`"- "`) that are YAML 1.1 boolean
   literals: `y`, `Y`, `yes`, `Yes`, `YES`, `n`, `N`, `no`, `No`, `NO`,
   `true`, `True`, `TRUE`, `false`, `False`, `FALSE`, `on`, `On`, `ON`,
   `off`, `Off`, `OFF`. These are single-quoted to prevent misinterpretation
   by YAML 1.1 parsers (PyYAML, which the Deadline service uses).

   serde_yaml follows YAML 1.2 where only `true`/`false` are booleans, so
   it leaves `ON`/`OFF`/`YES`/`NO` unquoted. Without this fix, a job named
   "yes" would be parsed as boolean `true` by downstream consumers.

## JSON Output

Two JSON formatting modes:

- **`--output json`** on most commands: `serde_json::to_string_pretty()` —
  standard pretty-printed JSON with indentation.

- **`json_with_spaces()`** for `config show --output json`: compact single-line
  JSON with spaces after `:` and `,` (matching Python's `json.dumps()` default).
  Implemented by post-processing the compact serde_json output, inserting spaces
  only outside of string literals.

## Markdown Stripping

`strip_markdown_for_terminal()` processes help text for terminal display:

1. Remove reference link definitions (`[label]: url`)
2. Convert inline links `[text](url)` → `text (url)`
3. Convert reference links `[text][ref]` → `text`
4. Remove bold markers `**text**` → `text`
5. Remove underscore bold `__text__` → `text`
6. Remove italic markers `*text*` → `text`
7. Collapse 3+ consecutive newlines to 2

Applied to the top-level `--help` about text. Uses `LazyLock<Regex>` for
compiled patterns.
