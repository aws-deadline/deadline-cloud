# deadline-config

Manages the `~/.deadline/config` INI file — reading, writing, and resolving
settings through a hierarchical section naming scheme.

## Role in the System

The foundational crate that nearly every other crate depends on. Provides
config file I/O and setting resolution. The CLI reads config once per
invocation and threads the `IniConfig` through all calls — there is no
global cache or mutable singleton.

Consumers: `deadline-cli`, `deadline-client`, `deadline-gui-ffi`,
`deadline-job-attachments`, `deadline-common`.

## Key Concepts

**Hierarchical section naming.** Settings are scoped by AWS profile, farm,
and queue. A setting like `defaults.job_id` resolves to the section
`[profile-myprofile farm-abc queue-123 defaults]`. The hierarchy is:

```
aws_profile_name → farm_id → queue_id
```

Each level's value formats into the section name for the next level's
settings. This means changing your active profile changes which farm/queue
defaults you see — they're namespaced, not global.

**Setting definitions are static.** All 18 settings are defined in a
compile-time table. Each has a default value, a dependency chain (which
parent setting scopes it), and a section format string. Adding a new
setting means adding one entry to this table.

**No global cache by design.** The CLI reads the file once and passes the
`IniConfig` struct through the call chain. This avoids the mtime-based
cache invalidation complexity that existed previously. If the GUI FFI
later needs caching for long-lived processes, it can wrap `IniConfig` in
a `CachedConfig` — but the core crate stays stateless.

## Behavior & Contracts

**File location:** `DEADLINE_CONFIG_FILE_PATH` env var, falling back to
`~/.deadline/config`. Tilde expansion applies to both.

**Read behavior:** Missing file → empty config (not an error). All
settings return their defaults when not explicitly set.

**Write behavior:** Atomic write via temp file + rename. Creates parent
directories. Sets 0o600 permissions on POSIX.

**Clear behavior:** Writing the default value back to the file rather than
removing the key. This is intentional — it preserves the key's presence
as a signal that the setting has been touched, and avoids ambiguity
between "never set" and "explicitly cleared."

**Setting validation:**
- Names must contain a dot. No dot → `"The setting name '<name>' is not valid."`
- Valid format but unknown → `"AWS Deadline Cloud configuration has no setting named '<name>'."`

**str2bool:** Accepts case-insensitive `yes/no`, `on/off`, `true/false`,
`1/0`. Everything else is an error.

**Profile resolution:** `get_best_profile_for_farm` finds the best AWS
profile for a given farm/queue combination. Priority: default profile if
its farm matches → any profile matching both farm and queue → any profile
matching farm only → default profile as fallback. The function is pure —
it takes the profile list as a parameter and works on a cloned config.

## Design Decisions

**Two-tier API (config-explicit vs convenience).** Every operation exists
in two forms: one that takes `&IniConfig` / `&mut IniConfig` (no disk I/O),
and a convenience wrapper that reads/writes the file. The config-explicit
versions are what the CLI uses. The convenience wrappers exist for simple
scripts and the GUI FFI where threading config through isn't practical.

**INI output is sorted.** Sections and keys are sorted alphabetically for
deterministic serialization. This means diffs are clean and config files
don't churn on write.

**Default substitution in defaults.** Some default values contain
`{aws_profile_name}` which gets substituted at resolution time. This
allows farm/queue defaults to be profile-aware without hardcoding.

## Gotchas & Constraints

- Section names can contain spaces (required by the hierarchical scheme).
  The INI parser must handle this — standard INI libraries that reject
  spaces in section names won't work.

- `"(default)"`, `"default"`, and `""` all map to the default credential
  chain (no named profile). This normalization happens in profile
  resolution and affects section name construction.

- The config file format is shared with the GUI and DCC plugins. Changes
  to section naming or key semantics are breaking changes across the
  entire Deadline Cloud client ecosystem.

## Status & Gaps

Fully implemented. No known gaps.
