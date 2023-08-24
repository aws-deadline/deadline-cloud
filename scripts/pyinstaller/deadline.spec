# -*- mode: python -*-
import os

EXE_NAME = 'deadline'
OUTPUT_DIR = 'deadline'
BLOCK_CIPHER = None

deadline_cli_dist_path = os.environ.get('PYINSTALLER_DEADLINE_CLI_DIST_PATH')
if not deadline_cli_dist_path:
    raise Exception(f'PYINSTALLER_DEADLINE_CLI_DIST_PATH env var is required but was not specified')

cli_a = Analysis(
    ['deadline.py'],
    datas=[(deadline_cli_dist_path, 'cli')],
    hookspath=['.'],
    runtime_hooks=[],
    excludes=['cmd', 'code', 'pdb'],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=BLOCK_CIPHER,
)

cli_a.exclude_system_libraries()

cli_pyz = PYZ(cli_a.pure, cli_a.zipped_data, cipher=BLOCK_CIPHER)
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
    cli_a.zipfiles,
    cli_a.datas,
    strip=False,
    upx=True,
    name=OUTPUT_DIR,
)
