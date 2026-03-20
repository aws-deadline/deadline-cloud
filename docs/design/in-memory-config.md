# In-Memory Configuration for GUI Submit

## Problem

The current configuration system in deadline-cloud always reads settings from the on-disk config file (`~/.deadline/config`). When a user runs `deadline bundle gui-submit`, there is no way to pass CLI arguments like `--farm-id` or `--queue-id` to pre-populate the submission dialog. The non-GUI `bundle submit` command supports these options via `_apply_cli_options_to_config`, which creates an in-memory `ConfigParser` overlay, but this pattern is not wired into the GUI submission path.

Additionally, the settings dialog (`DeadlineConfigDialog`) always writes changes directly to the on-disk config. There is no concept of editing settings that only apply to the current session.

## Goals

1. Allow users to pass `--farm-id`, `--queue-id`, `--profile`, and `--storage-profile-id` into `deadline bundle gui-submit`, applying them as in-memory overrides for the submission session without writing to disk.
2. In the settings dialog opened from the submission window, allow users to choose between modifying the in-memory (session) config or the workstation (on-disk) config.

## Current Architecture

### Config Read/Write Flow

- `config_file.read_config()` reads from `~/.deadline/config` (with caching based on mtime).
- `config_file.get_setting(name, config=None)` reads from the on-disk config when `config=None`, or from a provided `ConfigParser` when one is passed.
- `config_file.set_setting(name, value, config=None)` writes to disk when `config=None`, or mutates the provided `ConfigParser` in-memory when one is passed.
- `config_file.write_config(config)` persists a `ConfigParser` to disk.

### CLI to In-Memory Config (existing pattern)

`_apply_cli_options_to_config(**args)` in `_common.py`:
- Always returns a fresh `ConfigParser` copy — never `None`, never mutates the caller's config or the `read_config()` cache.
- If any CLI args are non-None, applies overrides via `set_setting(name, value, config=config)` on the copy.
- If no CLI args are provided, returns an unmodified copy of the disk config.
- Used by all CLI commands (`bundle submit`, `bundle gui-submit`, `job get`, etc.).

### GUI Submission Flow

`bundle_gui_submit` -> `show_job_bundle_submitter()` -> creates `SubmitJobToDeadlineDialog`.

The dialog's `SharedJobSettingsWidget` calls `get_setting("defaults.farm_id")` and `get_setting("defaults.queue_id")` **without** passing a config object, so it always reads from disk.

The `DeadlineUIController` and `DeadlineAuthenticationStatus` singletons accept a `ConfigParser` via `set_config()`, but the submission dialog never passes one derived from CLI args.

### Settings Dialog

`DeadlineConfigDialog` / `DeadlineWorkstationConfigWidget`:
- Maintains a `self.config` (copy of on-disk config) and a `self.changes` dict.
- On `apply()`, reads fresh config from disk, applies changes, then calls `write_config()` to persist.
- There is no concept of "session-only" changes.

## Design

### 1. CLI Options on `bundle gui-submit`

Add `--profile`, `--farm-id`, `--queue-id`, and `--storage-profile-id` options to the `bundle_gui_submit` click command, matching what `bundle_submit` already accepts.

In the command handler, use `_apply_cli_options_to_config()` to produce an in-memory `ConfigParser` with the overrides applied. Since `_apply_cli_options_to_config` always returns a fresh copy (never `None`, never mutates the cache), the result can be passed directly as the session config. Pass this config object through to `show_job_bundle_submitter()` and into `SubmitJobToDeadlineDialog`.

### 2. Session Config Propagation

`SubmitJobToDeadlineDialog.__init__` accepts an optional `session_config: Optional[ConfigParser]` parameter. When provided:

- Stored as `self._session_config`.
- Passed to `SharedJobSettingsWidget` via its new `config` parameter.
- Used in `_set_submit_button_state` to check farm/queue configuration.
- Used in `on_submit` when starting job submission (falls back to `config_file.read_config()` when `None`).

When `config` is `None` (no CLI overrides), behavior is unchanged — everything reads from disk as before.

### 3. Config Threading Through Widget Hierarchy

During implementation we discovered that the config needs to be threaded deeper than initially expected. The full propagation chain is:

```
bundle_gui_submit (CLI)
  -> _apply_cli_options_to_config(**args) -> ConfigParser (always a fresh copy)
  -> show_job_bundle_submitter(session_config=...)
    -> SubmitJobToDeadlineDialog(session_config=...)  [stores as self._session_config]
      -> SharedJobSettingsWidget(config=...)  [stores as self._config]
        -> DeadlineCloudSettingsWidget(config=...)  [stores as self._config]
          -> DeadlineFarmDisplay(config=...)  [stores as self._config]
          -> DeadlineQueueDisplay(config=...)  [stores as self._config]
        -> All get_setting() calls pass config=self._config
      -> _set_submit_button_state() uses config=self._session_config
      -> on_submit() uses self._session_config or config_file.read_config()
```

The `_DeadlineNamedResourceDisplay` base class and all its subclasses (`DeadlineFarmDisplay`, `DeadlineQueueDisplay`, `DeadlineStorageProfileNameDisplay`) needed to be updated to accept and use the config. These widgets call `get_setting()` both during `__init__` (to get the initial ID) and in `refresh()` / `get_item()` (to fetch current IDs for API calls). All 14 `get_setting()` call sites in `shared_job_settings_tab.py` were updated to pass `config=self._config`.

### 4. Settings Dialog: Config File Menu

The settings dialog has a consistent button bar in both `deadline config gui` and `bundle gui-submit`: Ok, Cancel, and "Config File" (dropdown menu).

- **`deadline config gui`** (no session_config): "Save to Disk" and "Load from Disk" in the Config File menu are enabled when there are pending changes. "Load from Disk" discards pending changes and refreshes from disk. Ok = Save to Disk + close.
- **`bundle gui-submit`** (with session_config): The "Config File" menu contains "Save to Disk" (writes session config to `~/.deadline/config`) and "Load from Disk" (reloads the session config from the on-disk config, discarding in-memory overrides). Ok = Apply pending changes to session + close.

The submission dialog always provides a session config when opening the settings dialog, even if no CLI overrides were passed. If `_session_config` is `None`, `_ensure_session_config()` creates one from the current on-disk config.

Implementation:

- The button bar always contains Ok, Cancel, and "Config File" (dropdown menu with "Save to Disk" and "Load from Disk").
- "Save to Disk" starts disabled and is enabled when the effective config differs from disk (session mode) or there are pending changes (workstation mode).
- The "Config File" button itself is disabled when all its menu actions are disabled.
- `refresh()` in `DeadlineWorkstationConfigWidget` prunes pending changes that match the base config, so changing a value back to its original disables the buttons.
- `DeadlineConfigDialog.configure_settings()` accepts an optional `session_config: Optional[ConfigParser]` parameter.
- `DeadlineWorkstationConfigWidget` determines its mode from whether `session_config` is provided. `refresh()` uses the session config as the base when one is present.
- In **session mode** (when `session_config` is provided), `apply()` mutates the provided `ConfigParser` in-memory. It does **not** call `write_config()`.
- In **workstation mode** (no `session_config`, e.g. `deadline config gui`), `apply()` behaves as before — reads from disk, applies changes, writes to disk.
- `configure_settings()` returns a `ConfigureSettingsResult(changes_applied, session_config)` so the caller can update its state.

### 5. Return Flow

`_apply_settings_result` in `SubmitJobToDeadlineDialog` handles the result from the settings dialog:

```python
def _apply_settings_result(self, result):
    """Apply the result from DeadlineConfigDialog.configure_settings()."""
    if result.session_config is not None:
        self._session_config = result.session_config
        self.deadline_authentication_status.set_config(self._session_config)
        self.shared_job_settings.set_session_config(self._session_config)
        self.refresh_deadline_settings()
```

The submitter always refreshes when a session config is returned, regardless of whether `changes_applied` is True. This ensures the submitter picks up any changes made via Ok in the settings dialog.

Both `on_settings_button_clicked` and `on_switch_profile_clicked` call `_ensure_session_config()` to guarantee a session config exists (creating one from disk if needed), pass it to `configure_settings()`, and delegate to `_apply_settings_result()`.

### 6. Job ID Persistence with `persist_job_id`

After a successful submission, the job ID should be written to the on-disk config so that subsequent CLI commands like `deadline job get` can find it. The config's hierarchical section structure (`profile → farm → queue → job_id`) means the job ID must be written to the section matching the farm/queue the job was submitted to — not necessarily the on-disk defaults.

Previously, `create_job_from_job_bundle` in the API layer auto-persisted the job ID when `config=None`, and `bundle_submit` skipped persistence entirely when CLI overrides were present. The GUI path wrote the job ID using the on-disk defaults, which was incorrect when `--farm-id`/`--queue-id` overrides were used.

The new approach:
- **The API layer (`create_job_from_job_bundle`) still auto-persists the job ID when `config=None`**, preserving backward compatibility for public API callers (e.g. Unreal) that use on-disk defaults. When a `config` is provided, the API layer does not persist — the caller owns that responsibility.
- **A new `config_file.persist_job_id(job_id, profile, farm_id, queue_id)` helper** handles persistence correctly. The caller extracts the profile, farm ID, and queue ID from its config (e.g. a session config with CLI overrides) and passes them explicitly. The helper builds the correct hierarchical section name and writes the job ID to the on-disk config. The on-disk farm/queue defaults are never changed.
- **Both CLI and GUI callers** call `persist_job_id(job_id, profile=..., farm_id=..., queue_id=...)` after a successful submission, extracting the values from their session config so the section resolves correctly regardless of whether overrides were used.

## Implementation Progress

### Done

- [x] `bundle_group.py`: Added `--profile`, `--farm-id`, `--queue-id`, `--storage-profile-id` click options to `bundle_gui_submit`. Added `_apply_cli_options_to_config(**args)` call. Passing `session_config=config` to `show_job_bundle_submitter()`.
- [x] `_common.py`: `_apply_cli_options_to_config` always copies the input config and always returns a `ConfigParser` (never `None`, never mutates the caller's object or the `read_config()` cache).
- [x] `config_file.py`: Added `persist_job_id(job_id, profile, farm_id, queue_id)` helper that writes the job ID to the correct hierarchical section on disk without changing on-disk farm/queue defaults.
- [x] `bundle_group.py` `bundle_submit`: Uses `persist_job_id(job_id, profile=..., farm_id=..., queue_id=...)` to always persist the job ID to the correct section, even when CLI overrides are present.
- [x] `bundle_group.py` `bundle_gui_submit`: Simplified — no longer needs manual pre-copy workaround since `_apply_cli_options_to_config` always copies.
- [x] `_submit_job_bundle.py`: Scoped `set_setting("defaults.job_id", job_id)` side effect to `config is None` only (preserving backward compat for public API callers). When a `config` is provided, callers own their persistence policy.
- [x] `job_bundle_submitter.py`: `show_job_bundle_submitter()` accepts `session_config` param and passes it to `SubmitJobToDeadlineDialog`.
- [x] `submit_job_to_deadline_dialog.py`: `SubmitJobToDeadlineDialog.__init__` accepts `session_config: Optional[ConfigParser]`, stores as `self._session_config`. Passes to `SharedJobSettingsWidget`. `_set_submit_button_state` and `on_submit` use session config. Added `_ensure_session_config()` to lazily create a session config from disk when none was provided via CLI args. Added `_apply_settings_result()` helper to propagate updated session config to singletons and child widgets after settings dialog closes. `_submission_succeeded_signal_receiver` uses `persist_job_id(job_id, profile=..., farm_id=..., queue_id=...)` to write the job ID to the correct farm/queue section on disk.
- [x] `shared_job_settings_tab.py`: `SharedJobSettingsWidget` accepts `config` param. All 14 `get_setting()` calls updated to pass `config=self._config`. `DeadlineCloudSettingsWidget`, `_DeadlineNamedResourceDisplay`, `DeadlineFarmDisplay`, `DeadlineQueueDisplay`, `DeadlineStorageProfileNameDisplay` all accept and thread config. Added `set_session_config()` method for updating the config after settings dialog closes.
- [x] `deadline_config_dialog.py`: Accept `session_config` in `configure_settings()`. "Config File" dropdown menu offers "Save to Disk" and "Load from Disk". Ok applies to session (session mode) or saves to disk (workstation mode). Returns `ConfigureSettingsResult` dataclass instead of `bool`.
- [x] Propagate session config to `DeadlineUIController` and `DeadlineAuthenticationStatus` singletons via their existing `set_config()` methods.
- [x] Tests for the new behavior (`test/unit/deadline_client/ui/test_session_config.py`).

## Affected Components Summary

| Component | Change | Status |
|---|---|---|
| `_common.py` `_apply_cli_options_to_config` | Always copy input config. Always return `ConfigParser` (never `None`). Never mutate caller's object or `read_config()` cache. | Done |
| `config_file.py` `persist_job_id` | New helper. Accepts explicit profile, farm_id, queue_id to build the correct hierarchical section. Writes job ID to on-disk config without changing farm/queue defaults. | Done |
| `config/__init__.py` | Export `persist_job_id`. | Done |
| `bundle_group.py` `bundle_submit` | Use `persist_job_id(job_id, profile=..., farm_id=..., queue_id=...)` instead of conditional `set_setting`. Job ID now always persisted to correct section even with CLI overrides. | Done |
| `bundle_group.py` `bundle_gui_submit` | Add `--profile`, `--farm-id`, `--queue-id`, `--storage-profile-id` options. Call `_apply_cli_options_to_config()` directly (no manual pre-copy needed). Pass `session_config` to `show_job_bundle_submitter()`. | Done |
| `_submit_job_bundle.py` `create_job_from_job_bundle` | Scoped `set_setting("defaults.job_id", job_id)` to `config is None` only. When a `config` is provided, callers own persistence via `persist_job_id`. | Done |
| `job_bundle_submitter.py` `show_job_bundle_submitter()` | Accept optional `session_config` param, pass to `SubmitJobToDeadlineDialog`. | Done |
| `submit_job_to_deadline_dialog.py` `SubmitJobToDeadlineDialog` | Accept optional `session_config` param. Store as `_session_config`. Propagate to child widgets. Use in `_set_submit_button_state` and `on_submit`. Use `persist_job_id` in `_submission_succeeded_signal_receiver`. | Done |
| `shared_job_settings_tab.py` `SharedJobSettingsWidget` | Accept optional `config` param. Use `get_setting(..., config=self._config)` instead of bare `get_setting(...)`. | Done |
| `shared_job_settings_tab.py` display widgets | `DeadlineCloudSettingsWidget`, `_DeadlineNamedResourceDisplay`, `DeadlineFarmDisplay`, `DeadlineQueueDisplay`, `DeadlineStorageProfileNameDisplay` all accept and use config. | Done |
| `deadline_config_dialog.py` `DeadlineConfigDialog` | Accept optional `session_config`. Button bar: Ok, Cancel, "Config File" dropdown ("Save to Disk" / "Load from Disk"). Ok applies to session or saves to disk depending on mode. Return `ConfigureSettingsResult`. | Done |
| `submit_job_to_deadline_dialog.py` settings button | Update `on_settings_button_clicked` to pass/receive session config via `_ensure_session_config()`. Propagate to singletons and child widgets via `_apply_settings_result()`. | Done |
| `_deadline_controller.py` `DeadlineUIController` | Already supports `set_config()` — called with session config at init and after settings dialog. | Done |
| `deadline_authentication_status.py` | Already supports `set_config()` — called with session config at init and after settings dialog. | Done |

## Breaking Changes

### For library consumers (`deadline.client.api`)

**`create_job_from_job_bundle` auto-persist behavior is now scoped to `config=None` only.**

When calling `create_job_from_job_bundle()` without passing `config=`, the function still auto-persists the job ID to `~/.deadline/config` as before — no change needed. This preserves backward compatibility for callers like Unreal that use the public API with on-disk defaults.

When `config=` is provided (a `ConfigParser` object), the function no longer auto-persists the job ID. This was already the behavior before — the `config is None` check meant it only ever auto-persisted in the no-config case. Callers that pass a config and want persistence should use `persist_job_id`:
```python
from deadline.client.config import persist_job_id, get_setting
job_id = create_job_from_job_bundle(..., config=my_config)
if job_id:
    persist_job_id(
        job_id,
        profile=get_setting("defaults.aws_profile_name", config=my_config),
        farm_id=get_setting("defaults.farm_id", config=my_config),
        queue_id=get_setting("defaults.queue_id", config=my_config),
    )
```

### For UI plugin authors (`deadline.client.ui`)

**`DeadlineConfigDialog.configure_settings()` return type changed from `bool` to `ConfigureSettingsResult`.**

- **Who is affected**: Code that captures and inspects the return value of `configure_settings()`, including truthiness checks like `if configure_settings():`.
- **Migration**: The `ConfigureSettingsResult` dataclass instance is always truthy (even when `changes_applied=False`), so `if configure_settings():` will no longer work as before. Replace with `if configure_settings().changes_applied:`. Code that discarded the return value is unaffected.

### Behavioral changes (non-breaking but notable)

**Job ID is now persisted even when CLI overrides are used.**

Previously, `deadline bundle submit --farm-id X --queue-id Y` would submit the job but *not* save the job ID to disk. Now it always persists the job ID to the correct hierarchical section (`profile/farm-X/queue-Y`). This means `deadline job get` will find the job ID when the same farm/queue is configured, even if they were originally specified via CLI flags.

**`deadline bundle gui-submit` always passes a session config to the submit dialog.**

Previously, when no `--farm-id`/`--queue-id`/`--profile`/`--storage-profile-id` flags were provided, `bundle_gui_submit` passed `session_config=None` to the dialog. Now it always passes a `ConfigParser` (a copy of the disk config). The dialog's `Optional[ConfigParser]` parameter still accepts `None` for third-party submitters that don't use the CLI entry point.

## Key Design Decisions

1. **Reuse `_apply_cli_options_to_config`**: Rather than inventing a new mechanism, we reuse the existing pattern from `bundle submit` to create the in-memory config overlay. The function was hardened to always copy and always return a `ConfigParser`, eliminating a class of cache-mutation bugs.
2. **ConfigParser as the session state carrier**: The `ConfigParser` object is already the abstraction used throughout the codebase. Passing it explicitly avoids introducing a new config abstraction.
3. **No global/singleton session config**: The session config is scoped to the dialog instance and passed explicitly. This avoids side effects on other parts of the system and keeps the on-disk config as the single source of truth for non-GUI paths.
4. **Consistent settings dialog button bar**: The button bar is identical across both entry points: Ok, Cancel, and "Config File" (dropdown menu with "Save to Disk" and "Load from Disk"). Without a session config (`deadline config gui`), "Save to Disk" and "Load from Disk" enable when there are pending changes. With a session config (`bundle gui-submit`), "Save to Disk" and "Load from Disk" enable when the effective config differs from disk. Ok saves to disk in workstation mode and applies to the in-memory session in session mode.
5. **Session config does not persist across submissions**: If the user closes the GUI and re-runs `bundle gui-submit` without CLI args, the session config is gone. This is intentional — CLI args are ephemeral by nature.
6. **Workstation changes in settings dialog write to disk on Ok**: This preserves the existing behavior and user expectations for workstation config.
7. **Deep config threading required**: The config must be passed through the entire widget hierarchy, not just to `SharedJobSettingsWidget`. The display widgets (`DeadlineFarmDisplay`, `DeadlineQueueDisplay`, `DeadlineStorageProfileNameDisplay`) also call `get_setting()` and need the config for correct behavior. All 14 `get_setting()` call sites in `shared_job_settings_tab.py` were updated.
8. **`_apply_cli_options_to_config` always returns a `ConfigParser`**: The function never returns `None`. When no CLI overrides are provided, it returns an unmodified copy of the disk config. This eliminates `config or read_config()` fallback patterns in callers and ensures the session config is always available for threading through the widget hierarchy.
9. **`configure_settings()` returns a dataclass instead of `bool`**: The return type changed from `bool` to `ConfigureSettingsResult(changes_applied, session_config)`. This is a breaking change for callers that used the return value in a truthiness check (e.g. `if configure_settings():`), since a dataclass instance is always truthy. Existing callers in this repo (`dev_application.py`, `config_group.py`) discard the return value and are unaffected.
10. **Session config must be re-propagated after settings dialog closes**: When the settings dialog returns an updated session config, it must be pushed to `DeadlineAuthenticationStatus`, `DeadlineUIController`, and `SharedJobSettingsWidget._config` — not just stored on the submit dialog. The `_apply_settings_result()` helper method handles this and always refreshes the submitter when a session config is returned.
11. **Config sections are hierarchical (farm-scoped queues)**: The `ConfigParser` sections are structured like `profile-(default) farm-XXXX defaults`, meaning `queue_id` is scoped under the farm section. Overriding `--farm-id` alone causes `queue_id` to resolve to empty (the new farm section doesn't exist yet). This is correct behavior — a queue belongs to a specific farm — but means `--farm-id` and `--queue-id` should typically be passed together.
12. **Lazy session config creation from the submitter**: The submit dialog's `_ensure_session_config()` creates a session config from the on-disk config on first use if none was provided via CLI args. This guarantees the settings dialog always operates in session mode when opened from the submitter, while `deadline config gui` (which never passes a session config) remains workstation-only.
13. **Load from Disk is a menu action with context-dependent enablement**: In session mode, enabled when the effective config (session + pending changes) differs from disk. In workstation mode, enabled when there are pending changes (acts as a discard-changes operation). "Save to Disk" follows the same logic per mode. The "Config File" button itself is disabled when all its menu actions are disabled.
14. **Change pruning keeps button state accurate**: `refresh()` removes pending changes whose values match the base config. This means changing a value and changing it back correctly disables Save to Disk, rather than leaving stale entries in the changes dict.
15. **Job ID persistence moved out of the API layer for config-aware callers**: `create_job_from_job_bundle` still auto-persists the job ID when `config=None` (preserving backward compatibility for public API callers like Unreal). When a `config` is provided, the caller owns persistence via `persist_job_id`. This keeps the simple public API path working while giving CLI and GUI callers control over which section the job ID is written to.
16. **`persist_job_id` takes explicit profile, farm_id, and queue_id**: Rather than accepting a `ConfigParser` and internally resolving the section via the dependency chain, `persist_job_id(job_id, profile, farm_id, queue_id)` makes its dependencies explicit. The caller extracts the three values from its config (via `get_setting`) and passes them directly. This makes the function easier to test, removes coupling to `_get_section_prefixes` internals, and makes the section construction transparent. For example, `deadline bundle submit --farm-id X --queue-id Y` extracts the profile, farm, and queue from the session config and passes them to `persist_job_id`, which writes the job ID under the `profile-{profile} X Y defaults` section.
17. **`_apply_cli_options_to_config` never mutates its inputs**: The function always creates a fresh `ConfigParser` copy before applying overrides. This eliminates the footgun where the `read_config()` cache could be permanently mutated by CLI overrides, which previously required callers (like `bundle_gui_submit`) to defensively pre-copy the config.
