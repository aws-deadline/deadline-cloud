# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

DEPENDENCIES = [
    "attr",
    "attrs",
    "boto3",
    "botocore",
    "click",
    "colorama",
    "deadline",
    "deadline_job_attachments",
    "jmespath",
    "packaging",
    "psutil",
    "pyrsistent",
    "dateutil",
    "yaml",
    "qtpy",
    "s3transfer",
    "six",
    "typing_extensions",
    "urllib3",
    "xxhash",
]

ALLOWLIST = {
    "files": [
        "deadline",
        "_internal/cli/deadline_cli",
        "deadline.exe",
        "_internal/cli/deadline_cli.exe",
        "_internal/cli/_internal/THIRD_PARTY_LICENSES",
        # Python
        "_internal/python3.dll",
        "_internal/cli/_internal/python3.dll",
        "_internal/base_library.zip",
        "_internal/cli/_internal/base_library.zip",
        # psutil
        "_internal/cli/_internal/pvectorc.cpython-39-x86_64-linux-gnu.so",
        "_internal/cli/_internal/pvectorc.cp39-win_amd64.pyd",
        # openssl
        "_internal/libcrypto-1_1-x64.dll",
        "_internal/libssl-1_1-x64.dll",
        "_internal/cli/_internal/libssl-1_1-x64.dll",
        "_internal/cli/_internal/libcrypto-1_1-x64.dll",
        # Visual Studio Redist
        "_internal/cli/_internal/VCRUNTIME140.dll",
        "_internal/cli/_internal/VCRUNTIME140_1.dll",
        "_internal/VCRUNTIME140.dll",
        "_internal/VCRUNTIME140_1.dll",
        # libffi
        "_internal/libffi-8.dll",
        "_internal/cli/_internal/libffi-8.dll",
        # sqlite
        "_internal/sqlite3.dll",
        "_internal/cli/_internal/sqlite3.dll",
        # pywin32
        "_internal/cli/_internal/win32/win32security.pyd",
        # Windows SDK
        "_internal/cli/_internal/ucrtbase.dll",
        "_internal/ucrtbase.dll",
    ],
    "globs": [
        "_internal/api-ms-win-*.dll",
        "_internal/cli/_internal/api-ms-win-*.dll",
        "_internal/cli/_internal/libpython3.*.so.1.0",
        "_internal/libpython3.*.so.1.0",
        "_internal/python3*.dll",
        "_internal/cli/_internal/python3*.dll",
        "_internal/pywin32_system32/pywintypes3*.dll",
        "_internal/cli/_internal/pywin32_system32/pywintypes3*.dll",
    ],
    "conditions": {
        "_internal/base_library.zip": {
            # Contents are all from Python Standard Library
            # Standard library modules are automatically added to
            # allowlist, but we still need to specify
            # the condition so the contents are checked.
            "archive_contents": {}
        },
        "_internal/cli/_internal/base_library.zip": {"archive_contents": {}},
        "deadline.exe": {
            "archive_contents": {
                "files": [
                    # All of these are from pyinstaller
                    # except for the pyz
                    "struct",
                    "pyimod01_archive",
                    "pyimod02_importers",
                    "pyimod03_ctypes",
                    "pyimod04_pywin32",
                    "pyiboot01_bootstrap",
                    "PYZ.pyz",
                    "pyi_rth_inspect",
                ],
                "conditions": {"PYZ.pyz": {"archive_contents": {}}},
            }
        },
        "_internal/cli/deadline_cli.exe": {
            "archive_contents": {
                "files": [
                    # All of these are from pyinstaller
                    # except for the pyz and deadline_cli_main
                    "struct",
                    "pyimod01_archive",
                    "pyimod02_importers",
                    "pyimod03_ctypes",
                    "pyimod04_pywin32",
                    "pyiboot01_bootstrap",
                    "pyi_rth_pkgutil",
                    "pyi_rth_inspect",
                    "pyi_rth_multiprocessing",
                    "deadline_cli_main",
                    "PYZ.pyz",
                ],
                "conditions": {
                    "PYZ.pyz": {
                        "archive_contents": {
                            "files": [
                                # pywin32
                                "ntsecuritycon"
                            ]
                        }
                    }
                },
            }
        },
        "deadline": {
            "archive_contents": {
                "files": [
                    # All of these are from pyinstaller
                    # except for the pyz and deadline
                    "struct",
                    "pyimod01_archive",
                    "pyimod02_importers",
                    "pyimod03_ctypes",
                    "pyiboot01_bootstrap",
                    "deadline",
                    "PYZ.pyz",
                    "pyi-contents-directory _internal",
                    "pyi_rth_inspect",
                ],
                "conditions": {
                    "PYZ.pyz": {
                        "archive_contents": {
                            "files": [
                                "_opcode_metadata",
                                "_pydatetime",
                                "_ios_support",
                                "_colorize",
                            ],
                        }
                    }
                },
            }
        },
        "_internal/cli/deadline_cli": {
            "archive_contents": {
                "files": [
                    # All of these are from pyinstaller
                    # except for the pyz and deadline_cli_main
                    "struct",
                    "pyimod01_archive",
                    "pyimod02_importers",
                    "pyimod03_ctypes",
                    "pyiboot01_bootstrap",
                    "pyi_rth_pkgutil",
                    "pyi_rth_inspect",
                    "pyi_rth_multiprocessing",
                    "deadline_cli_main",
                    "pyi-contents-directory _internal",
                    "PYZ.pyz",
                ],
                "conditions": {
                    "PYZ.pyz": {
                        "archive_contents": {
                            "files": [
                                "_opcode_metadata",
                                "_pydatetime",
                                "_ios_support",
                                "_colorize",
                            ],
                        }
                    }
                },
            }
        },
    },
}
