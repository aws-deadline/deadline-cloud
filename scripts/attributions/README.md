# Attributions & License Compliance

This directory contains scripts and data for generating the `THIRD_PARTY_LICENSES` document
bundled with the Deadline Cloud Client installer, and for complying with open source license
obligations (attribution, source distribution).

## How It Works

The installer bundles third-party dependencies. We must:

1. **Attribute** every bundled dependency with its license text
2. **Distribute source code** for LGPL-licensed dependencies (Qt, PySide6, Shiboken6)

### Attribution Pipeline

```
pip-licenses (auto-discovers pip packages)
    + additional/ (manually maintained license texts)
    → cli.py generates THIRD_PARTY_LICENSES
    → diffed against approved_text/<platform>/THIRD_PARTY_LICENSES (golden files)
```

- `cli.py` — Main generation script. Combines auto-discovered licenses with manual ones.
- `additional/` — License text files for dependencies that pip-licenses can't discover
  (native libs, Qt, PyInstaller, etc.).
- `approved_text/{Darwin,Linux,Windows}/THIRD_PARTY_LICENSES` — Golden files. The generation
  script diffs its output against these and fails if they don't match. This ensures changes
  are reviewed.
- `fetch_qt_licenses.py` — Scrapes Qt's third-party license page to regenerate
  `additional/QT_LICENSE.txt`'s third-party section.

### Safeguards

| Check                                    | What it catches                              | Where it runs               |
| ---------------------------------------- | -------------------------------------------- | --------------------------- |
| `_ATTRIBUTIONS_ALLOW_LIST` SHA256 hashes | License text changes in pip packages         | `cli.py` at generation time |
| Golden file diff                         | Any change to the generated output           | `cli.py` at generation time |
| Bundled dependency check                 | Bundled dependency missing from attributions | `cli.py` at generation time |

### Key Data Structures in cli.py

- **`_ATTRIBUTIONS_ALLOW_LIST`** — Pip-discoverable packages with SHA256 hashes of their
  license (and optionally notice) files. If a package updates its license text, generation
  fails until the hash is updated.
- **`_ADDITIONAL_ATTRIBUTIONS`** — Manually attributed packages. Each entry has a `name`,
  `attribution_path` (file in `additional/`), and optional `platforms`, `spdx`, `url`,
  and `sort_key` fields.
- **`_EXPECTED_MISSING_LICENSE`** — Packages where pip-licenses won't find a license file
  (e.g., pywin32). These must have entries in `_ADDITIONAL_ATTRIBUTIONS`.

### Qt / PySide6 / Shiboken6 Structure

These three components are LGPL-licensed and attributed separately:

| Entry     | License file            | Contains                                             |
| --------- | ----------------------- | ---------------------------------------------------- |
| Qt        | `QT_LICENSE.txt`        | LGPLv3 full text + Qt third-party component licenses |
| PySide6   | `PYSIDE6_LICENSE.txt`   | Reference to Qt license above                        |
| Shiboken6 | `SHIBOKEN6_LICENSE.txt` | Reference to Qt license above                        |

## Commands

```bash
# Generate attributions (local dev) — also validates bundled deps have attributions
hatch run attributions:generate_local

# Generate attributions (CI / pinned Python)
hatch run attributions:generate

# Generate LGPL source tarballs (version read from requirements-installer.txt)
python scripts/attributions/generate_source_tarballs.py --output-dir ./source-tarballs

# Refresh Qt third-party licenses (then update the third-party section in additional/QT_LICENSE.txt)
python scripts/attributions/fetch_qt_licenses.py --output /tmp/qt_third_party.txt
```

Upload source tarballs to the `amazon-source-code-downloads` S3 bucket
under `deadline-cloud/`.

## Updating PySide6 / Qt Version

1. Update the version pin in `requirements-installer.txt`
2. Update `BASE_URL` in `fetch_qt_licenses.py` to match the Qt minor version
   (e.g. `https://doc.qt.io/qt-6.8` for Qt 6.8.x). The version-specific page lists
   the correct third-party component versions for that release.
3. Regenerate Qt third-party licenses and update `additional/QT_LICENSE.txt`:
   ```bash
   python scripts/attributions/fetch_qt_licenses.py --output /tmp/qt_third_party.txt
   ```
   Then replace the third-party section in `additional/QT_LICENSE.txt` (everything after
   the LGPLv3 full text) with the generated output.
4. Regenerate attributions: `hatch run attributions:generate_local`
5. If the golden file diff fails, review the changes and copy the new output to
   `approved_text/<platform>/THIRD_PARTY_LICENSES`
6. Generate new source tarballs:
   ```bash
   python scripts/attributions/generate_source_tarballs.py --output-dir ./source-tarballs
   ```
7. Upload tarballs to the S3 compliance bucket

## Adding a New Dependency

1. Add the package to `pyproject.toml`
2. If it will be bundled in the installer, add it to `scripts/pyinstaller/allowlist.py`'s
   `DEPENDENCIES` list
3. Run `hatch run attributions:generate_local`
   - **If pip-licenses finds it**: Add an entry to `_ATTRIBUTIONS_ALLOW_LIST` in `cli.py`
     with the SHA256 of its license text. Run generation again.
   - **If pip-licenses doesn't find it**: Add the license file to `additional/` and add an
     entry to `_ADDITIONAL_ATTRIBUTIONS` in `cli.py`. Add the package name to
     `_EXPECTED_MISSING_LICENSE` if needed.
4. Copy the new generated output to `approved_text/<platform>/THIRD_PARTY_LICENSES`
5. If the dependency is LGPL/GPL licensed, generate and upload source tarballs

## File Reference

| File                          | Purpose                                             |
| ----------------------------- | --------------------------------------------------- |
| `cli.py`                      | Main attributions generation and validation script  |
| `generate_source_tarballs.py` | Creates LGPL source compliance tarballs             |
| `fetch_qt_licenses.py`        | Scrapes Qt third-party license info                 |
| `merge_versions.py`           | Merges version info for installer metadata          |
| `additional/`                 | Manually maintained license text files              |
| `approved_text/`              | Golden files per platform                           |
| `requirements.txt`            | Dependencies for the attributions hatch environment |
