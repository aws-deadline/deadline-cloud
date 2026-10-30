# -*- mode: python -*-
import os
import sys

from pathlib import Path
from PyInstaller.utils.hooks import collect_all

# Find the deadline job attachments module location and grab its json schemas
import deadline.client
b_module_path = os.path.dirname(os.path.dirname(deadline.client.__file__))

ROOT = Path(b_module_path).absolute().parents[1]
EXE_NAME = 'deadline'
OUTPUT_DIR = 'deadline'

datas, binaries, hiddenimports = collect_all('deadline')

# Filter out optional MCP subpackages
_MCP_PREFIXES = ('deadline.mcp', 'deadline._mcp', 'mcp')
hiddenimports = [h for h in hiddenimports if not any(h == p or h.startswith(p + '.') for p in _MCP_PREFIXES)]
datas = [d for d in datas if not any(p in str(d[0]) for p in ('deadline/mcp', 'deadline/_mcp', 'deadline\\mcp', 'deadline\\_mcp'))]

# The 'datas' parameter adds data files to the bundle.
# Each entry is a pair (local_filename, destination_path).
datas += [
    (b_module_path + '/../../THIRD_PARTY_LICENSES',
     '.')
]

# PySide6/Qt modules to include
hiddenimports += ['PySide6.QtCore', 'PySide6.QtGui', 'PySide6.QtWidgets']

cli_a = Analysis(
    ['../../src/deadline/client/cli/deadline_cli_main.py'],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=['.'],
    runtime_hooks=[],
    excludes=[
        'cmd',
        'code',
        'pdb',
        'readline',
        'setuptools',
        'jaraco',
        'importlib_metadata',
        'zipp',
        'pkg_resources',
        'pyinstaller_hooks_contrib',
        # MCP is optional - exclude the MCP package itself.
        # Its transitive deps (httpx, uvicorn, etc.) are not listed here
        # because some may also be real dependencies of the core package
        # (e.g. certifi, idna, jsonschema). PyInstaller's dependency
        # analysis will only include what's actually imported.
        'deadline.mcp',
        'deadline._mcp',
        'mcp',
    ],
)

# Shiboken uses the stable Python ABI on Windows.
if sys.platform == "win32" and not any(name == "python3.dll" for name, _, _ in cli_a.binaries):
    python3_dll = next(
        (Path(path).with_name("python3.dll") for name, path, _ in cli_a.binaries
         if name.startswith("python3") and name.endswith(".dll")),
        None,
    )
    if python3_dll is None or not python3_dll.is_file():
        raise RuntimeError("python3.dll is required for Shiboken on Windows. Failing build")
    cli_a.binaries.append(('python3.dll', str(python3_dll), 'BINARY'))


cli_a.datas = [item for item in cli_a.datas if not item[0].startswith("Python.framework")]
cli_a.exclude_system_libraries(list_of_exceptions=['libssl*', 'libsqlite3*', 'libcrypto*'])

cli_pyz = PYZ(cli_a.pure)
cli_exe = EXE(
    cli_pyz,
    cli_a.scripts,
    [],
    exclude_binaries=True,
    name=EXE_NAME,
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
)
coll = COLLECT(
    cli_exe,
    cli_a.binaries,
    cli_a.datas,
    strip=False,
    upx=True,
    name=OUTPUT_DIR,
)
