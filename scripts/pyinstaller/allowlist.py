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
    # PySide6/shiboken6 are NOT listed here because the auto-generated
    # _internal/{dep}/**/* glob would blanket-allow all files under PySide6/,
    # defeating the purpose of the explicit plugin allowlist below.
    # Their dist-info and binding globs are listed explicitly instead.
]

ALLOWLIST = {
    "files": [
        "deadline",
        "deadline.exe",
        "_internal/THIRD_PARTY_LICENSES",
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
        # bz2
        "_internal/libbz2.dylib",
        # expat
        "_internal/libexpat.1.dylib",
    ],
    "globs": [
        "_internal/api-ms-win-*.dll",
        "_internal/libpython3.*.so.1.0",
        "_internal/libpython3.*.so",
        "_internal/libpython3.*.dylib",
        "_internal/python3*.dll",
        "_internal/pywin32_system32/pywintypes3*.dll",
        "_internal/Python.framework/Versions/3.*/Python",
        "_internal/Python.framework/Versions/3.*/Resources/Info.plist",
        "_internal/libsqlite3.so.*",
        "_internal/libsqlite3.*.dylib",
        # zlib
        "_internal/libz.*.dylib",
        # openssl
        "_internal/libssl.so.*",
        "_internal/libcrypto.so.*",
        "_internal/libssl-*.dll",
        "_internal/libcrypto-*.dll",
        "_internal/libssl.*.dylib",
        "_internal/libcrypto.*.dylib",
        # libffi
        "_internal/libffi-*.dll",
        "_internal/libffi.*.dylib",
        # xxsubtype (CPython internal C extension, pulled in by shiboken6/PySide6)
        "_internal/lib-dynload/xxsubtype.cpython-3*-darwin.so",
        "_internal/lib-dynload/xxsubtype.cpython-3*-x86_64-linux-gnu.so",
        # PySide6/shiboken6 dist-info (auto-generated globs excluded, so list explicitly)
        "_internal/PySide6-*.dist-info/**/*",
        "_internal/PySide6-*.dist-info/*",
        "_internal/shiboken6-*.dist-info/**/*",
        "_internal/shiboken6-*.dist-info/*",
        # PySide6/shiboken6 Python bindings
        "_internal/PySide6/Qt*.abi3.so",
        "_internal/PySide6/libpyside6.abi3.*.dylib",
        "_internal/PySide6/libpyside6.abi3.so.*",
        "_internal/shiboken6/Shiboken.abi3.so",
        "_internal/shiboken6/libshiboken6.abi3.*.dylib",
        "_internal/shiboken6/libshiboken6.abi3.so.*",
        "_internal/libpyside6.abi3.*.dylib",
        "_internal/libshiboken6.abi3.*.dylib",
        "_internal/libpyside6.abi3.so.*",
        "_internal/libshiboken6.abi3.so.*",
        # Qt module symlinks (macOS)
        "_internal/QtCore",
        "_internal/QtGui",
        "_internal/QtWidgets",
        "_internal/QtDBus",
        "_internal/QtSvg",
        # Qt/PySide6 core libraries - Linux (deep path + top-level symlink per module)
        "_internal/PySide6/Qt/lib/libQt6Core.so.*",
        "_internal/libQt6Core.so.*",
        "_internal/PySide6/Qt/lib/libQt6Gui.so.*",
        "_internal/libQt6Gui.so.*",
        "_internal/PySide6/Qt/lib/libQt6Widgets.so.*",
        "_internal/libQt6Widgets.so.*",
        "_internal/PySide6/Qt/lib/libQt6DBus.so.*",
        "_internal/libQt6DBus.so.*",
        "_internal/PySide6/Qt/lib/libQt6Svg.so.*",
        "_internal/libQt6Svg.so.*",
        "_internal/PySide6/Qt/lib/libQt6XcbQpa.so.*",
        "_internal/libQt6XcbQpa.so.*",
        "_internal/PySide6/Qt/lib/libQt6WaylandClient.so.*",
        "_internal/libQt6WaylandClient.so.*",
        "_internal/PySide6/Qt/lib/libQt6WaylandEglClientHwIntegration.so.*",
        "_internal/libQt6WaylandEglClientHwIntegration.so.*",
        "_internal/PySide6/Qt/lib/libQt6WlShellIntegration.so.*",
        "_internal/libQt6WlShellIntegration.so.*",
        # Qt/PySide6 transitive dependencies of platform plugins - Linux
        "_internal/PySide6/Qt/lib/libQt6OpenGL.so.*",
        "_internal/libQt6OpenGL.so.*",
        "_internal/PySide6/Qt/lib/libQt6EglFSDeviceIntegration.so.*",
        "_internal/libQt6EglFSDeviceIntegration.so.*",
        "_internal/PySide6/Qt/lib/libQt6EglFsKmsSupport.so.*",
        "_internal/libQt6EglFsKmsSupport.so.*",
        # ICU libraries - required by Qt6Core on Linux
        "_internal/PySide6/Qt/lib/libicui18n.so.*",
        "_internal/PySide6/Qt/lib/libicuuc.so.*",
        "_internal/PySide6/Qt/lib/libicudata.so.*",
        "_internal/libicui18n.so.*",
        "_internal/libicuuc.so.*",
        "_internal/libicudata.so.*",
        # Qt/PySide6 core libraries - Windows
        "_internal/PySide6/Qt6Core.dll",
        "_internal/PySide6/Qt6Gui.dll",
        "_internal/PySide6/Qt6Widgets.dll",
        "_internal/PySide6/Qt6DBus.dll",
        "_internal/PySide6/Qt6Svg.dll",
        # PySide6/shiboken6 Python bindings - Windows
        "_internal/PySide6/QtCore.pyd",
        "_internal/PySide6/QtGui.pyd",
        "_internal/PySide6/QtWidgets.pyd",
        "_internal/PySide6/QtDBus.pyd",
        "_internal/PySide6/QtSvg.pyd",
        "_internal/PySide6/QtOpenGL.pyd",
        "_internal/PySide6/QtOpenGLWidgets.pyd",
        "_internal/PySide6/QtNetwork.pyd",
        "_internal/PySide6/pyside6.abi3.dll",
        "_internal/shiboken6/Shiboken.pyd",
        "_internal/shiboken6/shiboken6.abi3.dll",
        # MSVC runtime bundled with PySide6/shiboken6 - Windows
        "_internal/PySide6/VCRUNTIME140.dll",
        "_internal/PySide6/VCRUNTIME140_1.dll",
        "_internal/PySide6/MSVCP140.dll",
        "_internal/PySide6/MSVCP140_1.dll",
        "_internal/PySide6/MSVCP140_2.dll",
        "_internal/shiboken6/VCRUNTIME140.dll",
        "_internal/shiboken6/VCRUNTIME140_1.dll",
        "_internal/shiboken6/MSVCP140.dll",
        # OpenGL software renderer - Windows
        "_internal/PySide6/opengl32sw.dll",
        # Qt/PySide6 core libraries - macOS
        # fnmatch's ** doesn't do recursive matching, so we need both * and **/* patterns
        "_internal/PySide6/Qt/lib/QtCore.framework/*",
        "_internal/PySide6/Qt/lib/QtCore.framework/**/*",
        "_internal/PySide6/Qt/lib/QtGui.framework/*",
        "_internal/PySide6/Qt/lib/QtGui.framework/**/*",
        "_internal/PySide6/Qt/lib/QtWidgets.framework/*",
        "_internal/PySide6/Qt/lib/QtWidgets.framework/**/*",
        "_internal/PySide6/Qt/lib/QtDBus.framework/*",
        "_internal/PySide6/Qt/lib/QtDBus.framework/**/*",
        "_internal/PySide6/Qt/lib/QtSvg.framework/*",
        "_internal/PySide6/Qt/lib/QtSvg.framework/**/*",
        # Qt plugins - macOS/Linux (PySide6/Qt/plugins/)
        "_internal/PySide6/Qt/plugins/platforms/libqcocoa.dylib",
        "_internal/PySide6/Qt/plugins/platforms/libqoffscreen.*",
        "_internal/PySide6/Qt/plugins/platforms/libqminimal.*",
        "_internal/PySide6/Qt/plugins/platforms/libqminimalegl.so",
        "_internal/PySide6/Qt/plugins/platforms/libqxcb.so",
        "_internal/PySide6/Qt/plugins/platforms/libqeglfs.so",
        "_internal/PySide6/Qt/plugins/platforms/libqlinuxfb.so",
        "_internal/PySide6/Qt/plugins/platforms/libqvkkhrdisplay.so",
        "_internal/PySide6/Qt/plugins/platforms/libqvnc.so",
        "_internal/PySide6/Qt/plugins/platforms/libqwayland*.so",
        # Wayland shell integration plugins (required for wayland platform plugin)
        "_internal/PySide6/Qt/plugins/wayland-shell-integration/lib*.so",
        "_internal/PySide6/Qt/plugins/styles/libqmacstyle.dylib",
        "_internal/PySide6/Qt/plugins/iconengines/libqsvgicon.*",
        "_internal/PySide6/Qt/plugins/imageformats/libqsvg.*",
        # Qt plugins - Windows (PySide6/plugins/ - no Qt subdirectory)
        "_internal/PySide6/plugins/platforms/qwindows.dll",
        "_internal/PySide6/plugins/platforms/qminimal.dll",
        "_internal/PySide6/plugins/platforms/qoffscreen.dll",
        "_internal/PySide6/plugins/platforms/qdirect2d.dll",
        "_internal/PySide6/plugins/styles/qwindowsvistastyle.dll",
        "_internal/PySide6/plugins/styles/qmodernwindowsstyle.dll",
        "_internal/PySide6/plugins/iconengines/qsvgicon.dll",
        "_internal/PySide6/plugins/imageformats/qsvg.dll",
        # Qt translations
        "_internal/PySide6/Qt/translations/*",
        "_internal/PySide6/translations/*",
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
                    "pyi_rth_pyside6",
                    "deadline_cli_main",
                    "PYZ.pyz",
                ],
                "conditions": {
                    "PYZ.pyz": {
                        "archive_contents": {
                            "files": [
                                # pywin32
                                "ntsecuritycon",
                                # pyinstaller runtime utils
                                "_pyi_rth_utils",
                                "_pyi_rth_utils.qt",
                                "shiboken6",
                                "PySide6",
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
                    "pyi_rth_pyside6",
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
                                "_sysconfigdata__linux_x86_64-linux-gnu",
                                "_sysconfigdata__darwin_darwin",
                                "_pyi_rth_utils",
                                "_pyi_rth_utils.qt",
                                "shiboken6",
                                "PySide6",
                            ],
                        }
                    }
                },
            }
        },
    },
}
