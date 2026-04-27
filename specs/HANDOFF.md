# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

## No active work item

Last completed: Documentation reorganization (spec layer cleanup,
CONTRIBUTING.md, DEVELOPMENT.md, crate folder renames, consolidation,
deduplication, readability pass).

## Next session: GUI fixes + packaging (#16d3 → #16e)

The goal is to get a fully packaged output of everything needed for the
Deadline CLI: Rust binary + Python GUI + `_native.abi3.so` in a single
distributable artifact.

### Recommended sequence

1. **#16d3 — GUI widget rendering fixes.** The dialogs open but have
   broken widgets. Root cause: `gui/deadline/client/config/config_file.py`
   shim returns types incompatible with what Qt widgets expect.
   `read_config()` returns a string from FFI but `DeadlineConfigDialog`
   and `SharedJobSettingsWidget` expect a `ConfigParser` object.

   **Approach options:**
   - A: Make the shim return a `ConfigParser` populated from the FFI data
   - B: Adapt the widgets to work with dict/string returns from `_native`
   - C: Add a new FFI function that returns structured config data

   Start by cataloging every call site that uses `read_config()` or
   `get_setting_default()` in the `gui/` Python code, then pick the
   approach that touches the fewest files.

2. **#16e — Python packaging.** Configure maturin to include the CLI
   binary in the wheel via `.data/scripts/`. Verify:
   - `pip install deadline` → `deadline` binary on PATH, no PySide6
   - `pip install "deadline[gui]"` → pulls in PySide6 + qtpy
   - `deadline bundle gui-submit` works from pip install

3. **#16f — DCC submitter switchover** (if time permits).

### Key files for #16d3

- `gui/deadline/client/config/config_file.py` — the shim (root cause)
- `gui/deadline/client/ui/dialogs/deadline_config_dialog.py` — config dialog
- `gui/deadline/client/ui/widgets/shared_job_settings_tab.py` — job settings
- `gui/deadline/client/ui/deadline_authentication_status.py` — auth status
- `gui/deadline/client/ui/controllers/_deadline_controller.py` — controller

### Key files for #16e

- `pyproject.toml` — maturin config (add `[tool.maturin] data` for CLI binary)
- `crates/deadline-cli/Cargo.toml` — the CLI binary crate
- `crates/deadline-python-bindings/Cargo.toml` — the PyO3 crate
