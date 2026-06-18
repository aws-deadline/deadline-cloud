# Python Parity Backlog — 2026-06-16

Delta between `deadline-cloud-python` `67ffaa3` (our last-known baseline)
and `848af18` (mainline as of 2026-06-16 pull). This is the work needed to
bring `deadline-cloud-rs` up to date with the Python reference.

**Scope:** 25 commits, 135 files, +10,907 / -1,164. Source-only: 49 files,
+1,721 / -200. Releases crossed: 0.57.3, 0.57.4, 0.58.0 (0.59.0 in flight).

Two `feat!` (breaking) changes are included — multi-region and drop-3.8.

---

## P0 — Large features (cross-cutting, port to deadline-lib + CLI)

### 1. Multi-region farm support — `feat!` (#1202, #1203 follow-up, #848af18 test fix)
The dominant change. Farms can now live across all Deadline Cloud regions;
session/endpoint resolution, config, and every resource-listing path became
region-aware.

Python source touched:
- `api/_list_apis.py` (+247) — region-aware farm/queue/fleet listing
- `api/_session.py` (+117) — per-region client/endpoint resolution
- `config/config_file.py` (+137) — region settings, multi-region compat
- `api/_queue_parameters.py`, `api/_list_jobs_by_filter_expression.py`
- CLI groups: `farm_group.py` (+26), `job_group.py` (+21), `queue_group.py`,
  `fleet_group.py`, `attachment_group.py` (+25), `manifest_group.py`,
  `worker_group.py`, `_incremental_download.py` (+31), `_common.py` (+66)

Rust work: region resolution in `deadline-lib` session/api layer, config
schema additions, `--region`-style per-command handling in `deadline-cli`.
Large; likely its own multi-step work item. **Breaking** — affects output
shape (see #848af18 multi-region farm list output assertions).

New Python tests to mirror (parity oracle):
- `cli/test_cli_region_per_command.py` (+676)
- `config/test_config_multi_region_compat.py` (+411)
- `cli/test_cli_attachment_region.py` (+199)
- `cli/test_incremental_download_region.py` (+77)
- `api/test_queue_parameters_region.py` (+35)

### 2. preGUI submission hook phase (#1178)
New hook phase in the job-bundle submission lifecycle.
- `job_bundle/_hooks/_manager.py` (+65), `_validator.py` (+27), `_models.py`
- Test: `job_bundle/test_hooks.py` (+286)

Rust work: extend `deadline-lib` submission-hooks to support the preGUI phase.

---

## P1 — Smaller features (behavioral parity)

### 3. Auto-select farm/queue when only one available (#1015)
- `ui/widgets/_deadline_list_combo_boxes.py` (+174), controllers
- Tests: `ui/dialogs/test_submit_dialog_auto_select.py` (+399)
- Note: primarily GUI behavior, but confirm whether CLI defaults change too.

### 4. Monitor session_id in telemetry (#1184)
Cross-component telemetry correlation.
- `api/_telemetry.py` (+28), `api/_session.py`
- Tests: `api/test_api_telemetry.py` (+273 delta)
- Rust work: telemetry event detail in `deadline-lib/src/api/telemetry.rs`.

### 5. Apply default client config to remaining boto clients (#1197)
- `api/_session.py` — config applied to all clients, not just deadline.
- Rust work: ensure session/config applies uniformly across SDK clients.

### 6. Host requirements populated from job template in gui-submit (#1198)
- `ui/job_bundle_submitter.py` (+118 total in range)
- Tests: `ui/gui/test_gui_host_requirements.py`

### 7. MCP changes (#1184 area) — `api/_mcp.py` (+39); test `api/test_mcp.py` (+126)

---

## P2 — GUI dataclasses / widgets overhaul (Python-side, affects gui/ + bindings)

Large GUI refactor with extensive new tests. Mostly lives in our `gui/`
Python and the PyO3 boundary, not core Rust logic — but the dataclass
contracts crossing Rust↔Python may need updating.
- `ui/dataclasses/__init__.py` (+120) — test `ui/gui/test_gui_dataclasses.py` (+792)
- `ui/widgets/job_timeouts_widget.py` (+77) — fix(ui): defer timeout
  validation until focus leaves row (#1180); test `ui/gui/test_gui_job_timeouts.py`
- `ui/widgets/shared_job_settings_tab.py` — test `test_shared_job_settings_tab.py` (+100)
- `ui/widgets/_deadline_list_combo_boxes.py` — test `test_deadline_list_combo_boxes.py` (+248)
- `ui/dialogs/submit_job_to_deadline_dialog.py` (+111)
- `ui/controllers/_deadline_controller.py` (+163), `_async_runner.py` (+74), `_async_task.py` (+57)

---

## Test infrastructure (mirror into our pytests/)

### xa11y harness robustness — `test/ui/helpers.py` (+52)
No new xa11y *cases*, but two robustness fixes worth porting to
`pytests/ui_accessibility/helpers.py`:
- `#1192` — relaunch GUI subprocess when its dialog fails to surface
- `#1186` — bump `STARTUP_TIMEOUT` to 45s (CI flakiness)

### CI / tooling (affects our future GitHub Actions)
- `#1200` `feat!`: drop Python 3.8 — our CI matrix should drop 3.8 too.
- `#1203` API change detection now uses `__all__` as the public surface —
  relevant to the planned cross-repo API parity snapshot and #25 shim.
- `#0af48d2` unify GUI + unit test coverage into a single suite.
- dependabot: `xa11y >=0.8.2,<0.9` then `>=0.9.0,<0.10` — bump our pin.

---

## Suggested sequencing

1. **Multi-region (#1202)** — biggest, breaking, gates CLI output parity. Own work item.
2. **preGUI hook phase (#1178)** — self-contained, clear test oracle.
3. **Telemetry session_id (#1184)** + **default client config (#1197)** — small lib changes.
4. **Auto-select (#1015)** + **host requirements gui-submit (#1198)** — GUI behavior.
5. **GUI dataclasses/widgets** — align `gui/` + PyO3 contracts.
6. **xa11y helper robustness** + **CI matrix drop 3.8** — test infra.

Parity verification: each item has a Python test file listed above; port or
diff against them per the rust-port-workflow.
