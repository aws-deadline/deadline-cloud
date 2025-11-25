# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

DEPENDENCIES = [
    "boto3",
    "botocore",
    "click",
    "colorama",
    "deadline",
    "deadline_job_attachments",
    "jmespath",
    "packaging",
    "psutil",
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
        "deadline.exe",
        "_internal/THIRD_PARTY_LICENSES",
        "_internal/Python.framework/Python",
        "_internal/Python",
        # Python
        "_internal/python3.dll",
        "_internal/base_library.zip",
        # psutil
        "_internal/pvectorc.cpython-3*-x86_64-linux-gnu.so",
        "_internal/pvectorc.cp3*-win_amd64.pyd",
        # Visual Studio Redist
        "_internal/VCRUNTIME140.dll",
        "_internal/VCRUNTIME140_1.dll",
        # sqlite
        "_internal/sqlite3.dll",
        "_internal/libsqlite3.dylib",
        # pywin32
        "_internal/win32/win32security.pyd",
        # Windows SDK
        "_internal/ucrtbase.dll",
        # Ncurses
        "_internal/libncurses.6.dylib",
        # lzma
        "_internal/liblzma.5.dylib",
        # mpdec
        "_internal/libmpdec.4.dylib",
        # openssl
        "_internal/libcrypto-3.dll",
        "_internal/libssl-3.dll",
    ],
    "globs": [
        "_internal/api-ms-win-*.dll",
        "_internal/libpython3.*.so.1.0",
        "_internal/libpython3.*.dylib",
        "_internal/python3*.dll",
        "_internal/pywin32_system32/pywintypes3*.dll",
        "_internal/Python.framework/Versions/3.*/Resources/Info.plist",
        "_internal/Python.framework/Versions/3.*/Python",
        "_internal/libsqlite3.so.*",
        # openssl
        "_internal/libssl.so.*",
        "_internal/libcrypto.so.*",
        "_internal/libssl-*.dll",
        "_internal/libssl.*.dylib",
        "_internal/libcrypto.*.dylib",
        # libffi
        "_internal/libffi-*.dll",
    ],
    "conditions": {
        "_internal/base_library.zip": {
            # Contents are all from Python Standard Library
            # Standard library modules are automatically added to
            # allowlist, but we still need to specify
            # the condition so the contents are checked.
            "archive_contents": {}
        },
        "deadline.exe": {
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
