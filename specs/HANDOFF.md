# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

## Completed — PyO3 + maturin migration

Replaced the C ABI FFI layer (`deadline-gui-ffi` + `_ffi.py`) with a
PyO3 extension module (`deadline-python-bindings` → `deadline._native`).
maturin packages the PyO3 module + Python GUI code into a `deadline`
PyPI wheel.

**What was done:**
- Created `crates/deadline-python-bindings/` (PyO3, abi3-py39)
- Created root `pyproject.toml` (maturin build backend)
- Ported all 18 FFI functions to PyO3 with 29 passing tests
- Switched all Python GUI code from `_ffi.py` to `deadline._native`
- Unified `DeadlineOperationError` (single class from PyO3, Python
  subclasses inherit from it)
- Fixed `ProgressReportMetadata` boundary: Rust passes dict, Python
  converts via `from_dict()` at the worker boundary, GUI uses typed
  attribute access
- Deleted `crates/deadline-gui-ffi/` and `gui/deadline/client/_ffi.py`
- Updated all specs to reflect new architecture

## Next Steps

1. **Include CLI binary in wheel** — configure maturin `.data/scripts/`
   so `pip install deadline` puts the `deadline` binary on PATH.
2. **AUDIT-034: `bundle gui-submit` + `--submitter-info`** — add the
   CLI command that spawns Python to launch the Qt submission dialog.
   Study notes from earlier session are in git history.
3. **`[gui]` extra verification** — verify `pip install deadline` works
   without PySide6, `pip install "deadline[gui]"` pulls in Qt deps.
4. **CI wheel building** — GitHub Actions matrix for linux-x64,
   macos-arm64, windows-x64.

## Recently Completed — #21: Python bug-fix parity sweep

All 5 Python bug fixes verified as already correct in Rust.

## Previously Completed — #9 + #11: Parallel/multipart S3 transfer + bundle submit --json

All audit findings resolved (0 remaining).
