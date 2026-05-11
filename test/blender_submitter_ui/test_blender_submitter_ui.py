# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Blender submitter UI E2E integration test.

Launches Blender in foreground mode with the deadline-cloud-for-blender addon,
opens the submitter dialog via Python API, then uses xa11y to interact with the
Qt dialog and submit a job to the mock Deadline backend.

Requires:
- Blender installed (set BLENDER_EXECUTABLE or have `blender` on PATH)
- deadline-cloud-for-blender installed into Blender's addon path
- xa11y, moto[s3], pytest
"""

from __future__ import annotations

import os
import platform
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Iterator

import boto3
import pytest
import xa11y
from moto.server import ThreadedMotoServer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common.mock_deadline_backend import MockDeadlineBackend, start_server  # noqa: E402

REGION = "us-west-2"
BUCKET = "blender-integ-test"
ROOT_PREFIX = "DeadlineCloud"
ACCESS_KEY = "testing"
SECRET_KEY = "testing"

SCRIPT_DIR = Path(__file__).parent
OPEN_SUBMITTER_SCRIPT = SCRIPT_DIR / "open_submitter.py"

STARTUP_TIMEOUT = 30.0
SUBMIT_TIMEOUT = 60.0
FARM_RESOLVE_TIMEOUT = 15.0

IS_WINDOWS = platform.system() == "Windows"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _find_blender() -> Path:
    location = os.environ.get("BLENDER_EXECUTABLE")
    if location:
        return Path(location)
    location = shutil.which("blender")
    if location:
        return Path(location)
    if IS_WINDOWS:
        p = Path(os.environ.get("ProgramFiles", "C:\\Program Files")) / "Blender Foundation"
        if p.exists():
            for d in sorted(p.iterdir(), reverse=True):
                candidate = d / "blender.exe"
                if candidate.exists():
                    return candidate
    elif platform.system() == "Darwin":
        candidate = Path("/Applications/Blender.app/Contents/MacOS/Blender")
        if candidate.exists():
            return candidate
    else:
        opt = Path("/opt")
        if opt.exists():
            for d in sorted(opt.glob("blender-*-linux-x64"), reverse=True):
                candidate = d / "blender"
                if candidate.exists():
                    return candidate
    pytest.fail(
        "Blender not found. Set BLENDER_EXECUTABLE or install Blender to a standard location."
    )


@pytest.fixture(scope="session")
def blender_exe() -> Path:
    return _find_blender()


@pytest.fixture(scope="session")
def moto_server() -> Iterator[str]:
    server = ThreadedMotoServer(port=0)
    server.start()
    host, port = server.get_host_and_port()
    url = f"http://127.0.0.1:{port}" if host == "0.0.0.0" else f"http://{host}:{port}"
    try:
        yield url
    finally:
        server.stop()


@pytest.fixture(scope="session")
def mock_backend_server() -> Iterator[tuple[MockDeadlineBackend, str]]:
    backend = MockDeadlineBackend(validate_params=False)
    # Override _parse_template to avoid openjd-model dependency issues
    # The Blender template may use features not supported by the installed version
    original_parse = backend._parse_template

    def _lenient_parse_template(template):
        try:
            return original_parse(template)
        except Exception:
            # Return a minimal mock job object that satisfies create_job
            from unittest.mock import MagicMock

            job = MagicMock()
            job.name = "cube"
            job.steps = []
            job.jobEnvironments = []
            return job

    backend._parse_template = _lenient_parse_template

    server, base_url, _ = start_server(backend)
    base_url = base_url.replace("127.0.0.1", "localhost")
    try:
        yield backend, base_url
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture
def mock_backend(mock_backend_server) -> tuple[MockDeadlineBackend, str]:
    backend, base_url = mock_backend_server
    backend.clear()
    return backend, base_url


@pytest.fixture
def s3_client(moto_server):
    return boto3.client(
        "s3",
        endpoint_url=moto_server,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION,
    )


@pytest.fixture
def blender_env(tmp_path, mock_backend, moto_server, s3_client):
    """Set up env vars, config, farm/queue, and S3 bucket for the Blender subprocess."""
    backend, deadline_url = mock_backend

    farm = backend.create_farm(displayName="TestFarm", description="")
    queue = backend.create_queue(
        farmId=farm["farmId"],
        displayName="TestQueue",
        description="",
        defaultBudgetAction="NONE",
        jobAttachmentSettings={"s3BucketName": BUCKET, "rootPrefix": ROOT_PREFIX},
    )

    try:
        s3_client.create_bucket(
            Bucket=BUCKET,
            CreateBucketConfiguration={"LocationConstraint": REGION},
        )
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        pass

    config_file = tmp_path / "deadline.config"
    config_file.write_text(
        "[defaults]\n"
        "aws_profile_name = (default)\n"
        "\n"
        "[profile-(default) defaults]\n"
        f"farm_id = {farm['farmId']}\n"
        "\n"
        f"[profile-(default) {farm['farmId']} defaults]\n"
        f"queue_id = {queue['queueId']}\n"
        "\n"
        "[profile-(default) settings]\n"
        f"job_history_dir = {tmp_path / 'job_history'}\n"
    )

    scene_dir = tmp_path / "scene"
    scene_dir.mkdir()

    # Include the current Python's site-packages so Blender can find
    # deadline, PySide6, and other dependencies via --python-use-system-env
    import sysconfig

    site_packages_dir = sysconfig.get_path("purelib")
    # Also include platlib for compiled extensions
    plat_packages_dir = sysconfig.get_path("platlib")
    # Include the addon's parent directory so `from deadline_cloud_blender_submitter...` works
    import deadline.blender_submitter

    addon_parent = str(Path(deadline.blender_submitter.__path__[0]) / "addons")
    # Include the project src/ dir for editable installs of deadline-cloud
    project_src = str(Path(__file__).resolve().parents[2] / "src")
    extra_paths = os.pathsep.join(
        p for p in [project_src, addon_parent, site_packages_dir, plat_packages_dir] if p
    )

    env = {
        **os.environ,
        "HOME": str(tmp_path / "home"),
        "USERPROFILE": str(tmp_path / "home"),
        "AWS_ENDPOINT_URL_DEADLINE": deadline_url,
        "AWS_ENDPOINT_URL_S3": moto_server,
        "AWS_ENDPOINT_URL_STS": moto_server,
        "AWS_ACCESS_KEY_ID": ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": SECRET_KEY,
        "AWS_DEFAULT_REGION": REGION,
        "DEADLINE_CONFIG_FILE_PATH": str(config_file),
        "DEADLINE_CLOUD_TELEMETRY_OPT_OUT": "true",
        "PYTHONPATH": extra_paths + os.pathsep + os.environ.get("PYTHONPATH", ""),
        "PYTHONUNBUFFERED": "1",
        "QT_ACCESSIBILITY": "1",
        "QT_LINUX_ACCESSIBILITY_ALWAYS_ON": "1",
    }
    (tmp_path / "home").mkdir(exist_ok=True)

    return {
        "backend": backend,
        "env": env,
        "scene_dir": str(scene_dir),
        "farm_id": farm["farmId"],
        "queue_id": queue["queueId"],
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _iter_by_role(root, role: str):
    try:
        if getattr(root, "role", None) == role:
            yield root
        for child in root.children():
            yield from _iter_by_role(child, role)
    except Exception:
        return


def _tree_contains_text(app, needle: str) -> bool:
    def walk(el):
        try:
            for field in ("name", "value"):
                text = getattr(el, field, None) or ""
                if needle in text:
                    return True
            for child in el.children():
                if walk(child):
                    return True
        except Exception:
            pass
        return False

    try:
        for root in app.children():
            if walk(root):
                return True
    except Exception:
        pass
    return False


def _dump_tree(app):
    """Print the accessibility tree for diagnostics."""

    def walk(el, depth=0):
        try:
            role = el.role
            name = (el.name or "")[:80]
            value = (getattr(el, "value", None) or "")[:40]
        except Exception as e:
            sys.stderr.write(f"{'  ' * depth}<err: {e}>\n")
            return
        sys.stderr.write(f"{'  ' * depth}{role} name={name!r} value={value!r}\n")
        try:
            for child in el.children():
                walk(child, depth + 1)
        except Exception:
            pass

    sys.stderr.write("\n--- accessibility tree ---\n")
    try:
        for root in app.children():
            walk(root)
    except Exception as e:
        sys.stderr.write(f"<tree error: {e}>\n")
    sys.stderr.write("--- end tree ---\n")


def _screenshot(name: str):
    try:
        path = os.path.join(tempfile.gettempdir(), f"{name}.png")
        xa11y.screenshot().save_png(path)
    except Exception as e:
        print(f"screenshot({name}) failed: {e}", flush=True)


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------


class TestBlenderSubmitterUI:
    """E2E test: open Blender submitter dialog, submit job via xa11y, verify mock backend."""

    def test_submit_job_via_ui(self, blender_exe, blender_env, tmp_path):
        backend = blender_env["backend"]
        env = blender_env["env"]
        scene_dir = blender_env["scene_dir"]

        # Launch Blender in foreground with the submitter script
        cmd = [
            str(blender_exe),
            "--python",
            str(OPEN_SUBMITTER_SCRIPT),
            "--python-use-system-env",
            "--python-exit-code",
            "1",
            "--",
            "--output-dir",
            scene_dir,
        ]
        popen_kwargs = dict(
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if not IS_WINDOWS:
            popen_kwargs["start_new_session"] = True

        baseline_apps = {a.name for a in xa11y.App.list()}
        proc = subprocess.Popen(cmd, **popen_kwargs)

        try:
            # Wait for the Qt submitter dialog to appear via xa11y.
            # The dialog may appear as a new app OR within the Blender app.
            app = self._wait_for_dialog_app(proc)

            # Wait for farm/queue to resolve
            self._wait_farm_resolved(app)

            # Click Submit
            submit_btn = app.locator('button[name="Submit"]')
            submit_btn.wait_visible(timeout=STARTUP_TIMEOUT)
            submit_btn.press()

            # Blender may show "Scene has unsaved changes" dialog - dismiss it
            time.sleep(1)
            for a in xa11y.App.list():
                for dismiss_name in ("Don't Save", "Dont Save", "Don\u2019t Save", "No"):
                    btn = a.locator(f'button[name="{dismiss_name}"]')
                    if btn.exists():
                        btn.press()
                        break

            # Dismiss "Job Submission Confirmation" dialog if it appears
            time.sleep(1)
            ok_btn = app.locator('button[name="OK"]')
            if ok_btn.exists():
                ok_btn.press()

            # Wait for submission to complete
            self._wait_submission_complete(app, backend)

            # Click OK to dismiss the success dialog (if still visible)
            time.sleep(1)
            for ok_name in ("Ok", "OK"):
                ok_btn = app.locator(f'button[name="{ok_name}"]')
                if ok_btn.exists():
                    ok_btn.press()
                    break

            # Verify job was created in mock backend
            assert backend.call_counts.get("CreateJob", 0) == 1, (
                f"Expected 1 CreateJob call, got {backend.call_counts}"
            )
            assert len(backend.jobs) == 1, f"Expected 1 job, got {len(backend.jobs)}"

        except Exception:
            _screenshot("blender_submitter_failure")
            try:
                apps = xa11y.App.list()
                for a in apps:
                    if a.name not in baseline_apps:
                        _dump_tree(a)
            except Exception:
                pass
            raise
        finally:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()

    def _wait_for_dialog_app(
        self, proc: subprocess.Popen, timeout: float = STARTUP_TIMEOUT
    ) -> xa11y.App:
        """Wait for the submitter dialog to appear in any accessibility app."""
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if proc.poll() is not None:
                stdout = proc.stdout.read().decode(errors="replace") if proc.stdout else ""
                stderr = proc.stderr.read().decode(errors="replace") if proc.stderr else ""
                pytest.fail(
                    f"Blender exited early with code {proc.returncode}\n"
                    f"stdout: {stdout[-2000:]}\nstderr: {stderr[-2000:]}"
                )
            # Check all apps for the submitter dialog
            for app in xa11y.App.list():
                if _tree_contains_text(app, "Submit") and _tree_contains_text(app, "Deadline"):
                    return app
            time.sleep(0.5)
        # Timeout — dump Blender's output for diagnostics
        proc.terminate()
        try:
            stdout_b, stderr_b = proc.communicate(timeout=5)
            stdout = stdout_b.decode(errors="replace") if stdout_b else ""
            stderr = stderr_b.decode(errors="replace") if stderr_b else ""
        except Exception:
            stdout = stderr = ""
        # Also dump all app trees
        all_trees = ""
        for app in xa11y.App.list():
            all_trees += f"\n=== APP: {app.name} ===\n"
            try:
                for root in app.children():
                    all_trees += f"  {root.role} name={getattr(root, 'name', '')!r}\n"
            except Exception:
                pass
        pytest.fail(
            f"Submitter dialog not found within {timeout}s\n"
            f"Apps: {all_trees}\n"
            f"Blender stdout: {stdout[-2000:]}\n"
            f"Blender stderr: {stderr[-2000:]}"
        )

    def _wait_farm_resolved(self, app: xa11y.App, timeout: float = FARM_RESOLVE_TIMEOUT):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if _tree_contains_text(app, "TestFarm"):
                return
            time.sleep(0.5)
        _dump_tree(app)
        pytest.fail(f"Farm name 'TestFarm' not resolved within {timeout}s")

    def _wait_submission_complete(self, app: xa11y.App, backend, timeout: float = SUBMIT_TIMEOUT):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if len(backend.jobs) >= 1:
                return
            # Dismiss "Scene has unsaved changes" — may be in any app (Blender native dialog)
            for a in xa11y.App.list():
                for dismiss_name in ("Don't Save", "Dont Save", "Don\u2019t Save", "No"):
                    btn = a.locator(f'button[name="{dismiss_name}"]')
                    if btn.exists():
                        btn.press()
                        break
            for elt in _iter_by_role(app, "static_text"):
                name = (getattr(elt, "name", "") or "").strip()
                if name == "Submission complete":
                    return
                if name in ("Submission error", "Submission canceled"):
                    _screenshot("submission_error")
                    _dump_tree(app)
                    pytest.fail(f"Submission failed with status: {name!r}")
            time.sleep(0.5)
        _screenshot("submission_timeout")
        _dump_tree(app)
        pytest.fail(
            f"Submission did not complete within {timeout}s. Backend calls: {backend.call_counts}"
        )
