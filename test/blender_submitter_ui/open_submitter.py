# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Blender script: creates a cube scene and opens the Deadline Cloud submitter.

Invokes the submitter via its registered Blender operator
(bpy.ops.ops.open_deadline_cloud_dialog), which is the same code path
as clicking Render > Submit to AWS Deadline Cloud in the UI.

The Qt dialog is then driven externally by xa11y from the pytest test.
"""

import sys
import os
import importlib
import pkgutil

# Ensure PYTHONPATH entries are at the FRONT of sys.path so our packages
# (deadline, PySide6, etc.) take priority over Blender's bundled copies.
_pythonpath = os.environ.get("PYTHONPATH", "").split(os.pathsep)
for p in reversed(_pythonpath):
    if p:
        if p in sys.path:
            sys.path.remove(p)
        sys.path.insert(0, p)

# The `deadline` namespace spans multiple packages. Clear any cached
# partial namespace and re-discover all subpackages from updated sys.path.
for key in list(sys.modules.keys()):
    if key.startswith("deadline"):
        del sys.modules[key]
importlib.invalidate_caches()
import deadline  # noqa: E402

deadline.__path__ = list(pkgutil.extend_path(deadline.__path__, deadline.__name__))

# Patch socket.getaddrinfo so management.localhost resolves to 127.0.0.1.
# Botocore prepends "management." host-prefix to Deadline API calls.
# Without this patch, management.localhost may resolve to ::1 (IPv6) or fail.
import socket  # noqa: E402

_orig_getaddrinfo = socket.getaddrinfo


def _patched_getaddrinfo(host, port, *args, **kwargs):
    if isinstance(host, str) and host.startswith("management."):
        host = "127.0.0.1"
    return _orig_getaddrinfo(host, port, *args, **kwargs)


socket.getaddrinfo = _patched_getaddrinfo

import argparse  # noqa: E402
import tempfile  # noqa: E402

import bpy  # noqa: E402


def main():
    argv = sys.argv[sys.argv.index("--") + 1 :] if "--" in sys.argv else []
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default=tempfile.mkdtemp())
    args = parser.parse_args(argv)

    os.makedirs(args.output_dir, exist_ok=True)
    blend_path = os.path.join(args.output_dir, "cube.blend")

    bpy.ops.wm.read_homefile(use_empty=False)

    scene = bpy.context.scene
    scene.render.engine = "CYCLES"
    scene.render.resolution_x = 64
    scene.render.resolution_y = 64
    scene.frame_start = 1
    scene.frame_end = 1
    scene.render.filepath = os.path.join(args.output_dir, "render_####")

    bpy.ops.wm.save_as_mainfile(filepath=blend_path)

    # Register and invoke the submitter addon operator — same code path as
    # clicking Render > Submit to AWS Deadline Cloud in the Blender menu.
    from deadline_cloud_blender_submitter import register

    register()
    bpy.ops.ops.open_deadline_cloud_dialog("EXEC_DEFAULT")


if __name__ == "__main__":
    main()
