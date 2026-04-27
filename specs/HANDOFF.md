# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

## No active work item

Last completed: #16d3 — GUI widget rendering fixes (config shim types).

## Next session: #16e — Python packaging

Configure maturin to include the CLI binary in the wheel via
`.data/scripts/`. Verify:
- `pip install deadline` → `deadline` binary on PATH, no PySide6
- `pip install "deadline[gui]"` → pulls in PySide6 + qtpy
- `deadline bundle gui-submit` works from pip install
