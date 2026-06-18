# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Page-object helpers for UI tests.

Wraps a ``deadline`` GUI subprocess + its xa11y ``App`` handle so tests
can drive the real CLI through the accessibility tree.
"""

from __future__ import annotations

import atexit
import json
import subprocess
import sys
import time
import weakref
from typing import Optional, Sequence, TypeVar

import xa11y

# ---------------------------------------------------------------------------
# Timeouts (seconds)
# ---------------------------------------------------------------------------
# Tuned for mock-backend runs over localhost HTTP. Generous ceilings
# because Qt/AT-SPI startup under Xvfb on Linux CI and Windows UIA can
# be substantially slower than a developer workstation.
STARTUP_TIMEOUT = 45.0
# Number of times to (re)spawn the GUI subprocess if it fails to surface a
# visible dialog within STARTUP_TIMEOUT. The accessibility bridge (AT-SPI on
# Linux, UIA on Windows, AX on macOS) occasionally wedges on the hosted CI
# runners so that a dialog never becomes queryable, even though the process
# launched fine. A fresh subprocess resets that bridge state. A real failure
# (e.g. the GUI crashes on startup) reproduces on every attempt, so retrying
# costs a little wall-clock without masking genuine bugs.
STARTUP_ATTEMPTS = 3
CLOSE_TIMEOUT = 10.0
TERMINATE_TIMEOUT = 3.0
SUBMIT_TIMEOUT = 20.0
CANCEL_TIMEOUT = 10.0
FARM_RESOLVE_TIMEOUT = 10.0
EXPORT_TIMEOUT = 10.0

# ---------------------------------------------------------------------------
# Shared test data
# ---------------------------------------------------------------------------
SAMPLE_TEMPLATE = {
    "specificationVersion": "jobtemplate-2023-09",
    "name": "Test Render Job",
    "description": "A test job for UI verification",
    "steps": [
        {
            "name": "RenderStep",
            "script": {
                "actions": {"onRun": {"command": "bash", "args": ["-c", "echo hello"]}},
            },
        }
    ],
}

# ---------------------------------------------------------------------------
# CLI helpers
# ---------------------------------------------------------------------------


def cli_get(env: dict, setting: str) -> str:
    """Read a deadline config setting via the CLI subprocess."""
    return subprocess.check_output(
        ["deadline", "config", "get", setting], env=env, text=True, stderr=subprocess.DEVNULL
    ).strip()


def cli_set(env: dict, setting: str, value: str) -> None:
    """Write a deadline config setting via the CLI subprocess."""
    subprocess.check_call(
        ["deadline", "config", "set", setting, value],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def last_json_object(text: str) -> dict:
    """Return the last JSON object found in *text*.

    ``deadline bundle gui-submit --output json`` prints a single object at
    the end of the run. Other libraries may log to stdout; this helper
    tolerates them by scanning for every ``{...}`` block that
    ``json.JSONDecoder.raw_decode`` can parse and returning the last one.
    """
    decoder = json.JSONDecoder()
    last: dict | None = None
    i = 0
    while i < len(text):
        if text[i] != "{":
            i += 1
            continue
        try:
            obj, end = decoder.raw_decode(text, i)
        except json.JSONDecodeError:
            i += 1
            continue
        if isinstance(obj, dict):
            last = obj
        i = end
    if last is None:
        raise AssertionError(f"No JSON object found in stdout: {text!r}")
    return last


# ---------------------------------------------------------------------------
# Accessibility selector helpers
# ---------------------------------------------------------------------------


def _dialog_selector(name: str) -> str:
    """Return the xa11y selector for a Qt dialog with the given *name*."""
    return f'dialog[name="{name}"]'


# ---------------------------------------------------------------------------
# Process management
# ---------------------------------------------------------------------------
_T = TypeVar("_T", bound="DeadlineApp")
_LIVE_PROCS: "weakref.WeakSet[subprocess.Popen]" = weakref.WeakSet()


def _register(proc: subprocess.Popen) -> None:
    _LIVE_PROCS.add(proc)


def _send_signal_to_proc(proc: subprocess.Popen, sig: int) -> None:
    """Send *sig* to the subprocess's process group (POSIX) or call
    ``kill()``/``terminate()`` (Windows).

    On POSIX the subprocess is started with ``start_new_session=True``, so
    signalling the group also reaches any dbus / AT-SPI helpers Qt spawned.
    """
    if proc.poll() is not None:
        return
    if sys.platform != "win32":
        import os

        try:
            os.killpg(os.getpgid(proc.pid), sig)
        except (ProcessLookupError, PermissionError, OSError):
            try:
                proc.kill()
            except OSError:
                pass
    else:
        try:
            # Windows has no POSIX signals; map to kill()/terminate().
            # signal.SIGKILL (9) doesn't exist on Windows, so compare
            # against the integer directly.
            if sig == 9:  # SIGKILL
                proc.kill()
            else:
                proc.terminate()
        except OSError:
            pass


def _terminate(proc: subprocess.Popen, timeout: float = TERMINATE_TIMEOUT) -> None:
    """SIGKILL a subprocess and reap it."""
    if proc.poll() is not None:
        return
    if sys.platform != "win32":
        import signal

        _send_signal_to_proc(proc, signal.SIGKILL)
    else:
        try:
            proc.kill()
        except OSError:
            pass
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        pass


def reap_all() -> None:
    """Terminate any subprocesses still tracked in ``_LIVE_PROCS``."""
    for proc in list(_LIVE_PROCS):
        _terminate(proc)


atexit.register(reap_all)


def _find_app(pid: int, baseline_names: set, timeout: float) -> xa11y.App:
    """Wait for an ``xa11y.App`` to appear for *pid*.

    ``App.by_pid`` selects elements with role ``application``, which Windows
    UIA never reports (apps are exposed as ``window``). Iterating
    ``App.list()`` works on every platform because list collects both
    ``application`` and ``window`` roles. The name fallback is for AT-SPI on
    Linux, which sometimes reports the wrong PID (typically 1) for child
    processes.
    """
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        apps = xa11y.App.list()
        for a in apps:
            if a.pid == pid:
                return xa11y.App.by_name(a.name)
        for a in apps:
            if a.name not in baseline_names:
                return xa11y.App.by_name(a.name)
        time.sleep(0.25)
    raise TimeoutError(f"No accessibility app found for PID {pid}")


# Time to wait for the accessibility bridge to start responding to queries
# before launching the first GUI. Only paid once per session (see the
# ``_warm_accessibility_bridge`` fixture in conftest.py).
BRIDGE_READY_TIMEOUT = 30.0


def wait_for_accessibility_bridge(timeout: float = BRIDGE_READY_TIMEOUT) -> None:
    """Block until the platform accessibility bridge answers a query.

    ``xa11y.App.list()`` raises (or the bridge wedges) until the AT-SPI registry
    (Linux), UIA (Windows), or AX (macOS) is actually serving requests. The CI
    workflow only ``sleep``s a fixed amount after starting the bridge, which is
    a guess; polling here turns that guess into a real readiness signal so the
    first test isn't the one that discovers the bridge wasn't up yet.
    """
    end = time.monotonic() + timeout
    last_exc: Optional[BaseException] = None
    while time.monotonic() < end:
        try:
            xa11y.App.list()
            return
        except Exception as exc:  # noqa: BLE001 — any bridge error means "not ready"
            last_exc = exc
        time.sleep(0.25)
    raise TimeoutError(
        f"Accessibility bridge did not become queryable within {timeout}s"
        + (f" (last error: {last_exc})" if last_exc is not None else "")
    )


# ---------------------------------------------------------------------------
# Page objects
# ---------------------------------------------------------------------------


class DeadlineApp:
    """Launches a deadline subprocess and exposes its accessibility tree."""

    DIALOG: str = ""  # overridden by subclasses

    def __init__(self, proc: subprocess.Popen, app: xa11y.App):
        self.proc = proc
        self._app = app

    @classmethod
    def launch(
        cls: type[_T],
        args: Sequence[str],
        env: Optional[dict] = None,
        timeout: float = STARTUP_TIMEOUT,
        capture_stdio: bool = False,
        dialog_name: Optional[str] = None,
        attempts: int = STARTUP_ATTEMPTS,
    ) -> _T:
        """Spawn ``deadline <args>`` and attach to its accessibility tree.

        Retries the whole spawn up to *attempts* times if the dialog never
        becomes visible within *timeout*. The accessibility bridge on the
        hosted CI runners intermittently wedges so a dialog never surfaces
        even though the process started fine; a fresh subprocess clears it.
        Each attempt spawns its own process, and only the successful instance
        is returned, so ``capture_stdio`` callers still read the right pipes.
        """
        last_exc: Optional[BaseException] = None
        for attempt in range(1, attempts + 1):
            try:
                return cls._launch_once(
                    args,
                    env=env,
                    timeout=timeout,
                    capture_stdio=capture_stdio,
                    dialog_name=dialog_name,
                )
            except (xa11y.TimeoutError, TimeoutError) as exc:
                # xa11y.TimeoutError: dialog never became visible (wait_visible).
                # builtin TimeoutError: process never surfaced in the a11y tree
                # (_find_app). Both indicate a wedged bridge worth relaunching.
                last_exc = exc
                if attempt < attempts:
                    sys.stderr.write(
                        f"[ui-test] GUI dialog did not become visible within "
                        f"{timeout}s (attempt {attempt}/{attempts}); relaunching.\n"
                    )
        assert last_exc is not None
        raise last_exc

    @classmethod
    def _launch_once(
        cls: type[_T],
        args: Sequence[str],
        env: Optional[dict],
        timeout: float,
        capture_stdio: bool,
        dialog_name: Optional[str],
    ) -> _T:
        """Spawn the GUI subprocess once and wait for its dialog to appear."""
        cmd = [sys.executable, "-m", "deadline", *args]
        baseline = {a.name for a in xa11y.App.list()}
        popen_kwargs: dict = dict(
            env=env,
            stdout=subprocess.PIPE if capture_stdio else subprocess.DEVNULL,
            stderr=subprocess.PIPE if capture_stdio else subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
        )
        if sys.platform != "win32":
            popen_kwargs["start_new_session"] = True
        proc = subprocess.Popen(cmd, **popen_kwargs)
        _register(proc)
        try:
            app = _find_app(proc.pid, baseline, timeout)
            instance = cls(proc, app)
            if dialog_name is not None:
                instance._dialog_name = dialog_name  # type: ignore[attr-defined]
            instance.dialog().wait_visible(timeout=timeout)
            return instance
        except Exception:
            _terminate(proc)
            raise

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def locator(self, selector: str) -> xa11y.Locator:
        return self._app.locator(selector)

    def elements_by_role(self, role: str) -> list:
        """Return every element with the given *role*, depth-first."""
        return self._app.locator(role).elements()

    def tree_contains_text(self, needle: str) -> bool:
        """True if any element in the tree has *needle* in its name or value."""
        return needle in self._app.dump()

    def _tab_locator(self, tab_name: str) -> xa11y.Locator:
        # Qt exposes tabs as ``radio_button`` on macOS and ``page_tab`` or
        # ``tab`` on Linux/Windows. Comma alternation matches all variants.
        return self.locator(
            f'radio_button[name="{tab_name}"], page_tab[name="{tab_name}"], tab[name="{tab_name}"]'
        )

    def tab_exists(self, tab_name: str) -> bool:
        return self._tab_locator(tab_name).exists()

    def activate_tab(self, tab_name: str) -> None:
        loc = self._tab_locator(tab_name)
        if not loc.exists():
            raise AssertionError(f"Tab {tab_name!r} not found via any role")
        loc.press()

    @property
    def dialog_name(self) -> str:
        return getattr(self, "_dialog_name", self.DIALOG)

    def dialog(self) -> xa11y.Locator:
        return self.locator(_dialog_selector(self.dialog_name))

    def button(self, name: str) -> xa11y.Locator:
        return self.locator(f'button[name="{name}"]')

    def dump_tree(self) -> None:
        """Print the accessibility tree to stderr for diagnostics."""
        sys.stderr.write("\n--- accessibility tree ---\n")
        try:
            sys.stderr.write(self._app.dump())
            sys.stderr.write("\n")
        except Exception as e:
            sys.stderr.write(f"<tree error: {e}>\n")
        sys.stderr.write("--- end tree ---\n")

    def close(self, button_name: str = "Cancel") -> None:
        """Dismiss the dialog and reap the subprocess."""
        if self.proc.poll() is not None:
            _terminate(self.proc)
            return

        for candidate in (button_name, "Close", "close button"):
            if self.proc.poll() is not None:
                break
            try:
                btn = self.dialog().descendant(f'button[name="{candidate}"]')
                if btn.exists():
                    btn.press()
                    break
            except xa11y.XA11yError:
                continue
        else:
            self._signal_terminate()

        try:
            self.proc.wait(timeout=CLOSE_TIMEOUT)
        except subprocess.TimeoutExpired:
            self._signal_terminate()
            try:
                self.proc.wait(timeout=CLOSE_TIMEOUT)
            except subprocess.TimeoutExpired:
                pass
        _terminate(self.proc)

    def _signal_terminate(self) -> None:
        """Ask the subprocess to shut down gracefully via SIGTERM."""
        import signal

        _send_signal_to_proc(self.proc, signal.SIGTERM)


class ConfigDialog(DeadlineApp):
    """Page object for ``deadline config gui``."""

    DIALOG = "AWS Deadline Cloud workstation configuration"

    @classmethod
    def open(cls, env: Optional[dict] = None) -> "ConfigDialog":
        return cls.launch(["config", "gui"], env=env)

    @property
    def log_level(self) -> str:
        return self._combo_text(group="General settings", index=1)

    @property
    def conflict_resolution(self) -> str:
        return self._combo_text(group="General settings", index=0)

    def _combo_text(self, *, group: str, index: int) -> str:
        """Return the visible text of the Nth combo box in a group."""
        combo = self.locator(f'group[name="{group}"] combo_box').elements()[index]
        return combo.value or combo.name or ""


def warm_up_gui(env: dict) -> None:
    """Launch and close one real GUI to warm the session before tests run.

    Two cold-start costs dominate the startup-time flakiness on hosted CI
    runners, and both are one-time:

    1. The accessibility bridge often only fully initialises when its first
       client attaches; the very first test otherwise absorbs that latency.
    2. The first GUI subprocess pays uncached Python ``.pyc`` and Qt-plugin
       load costs that later launches don't.

    Launching one ``deadline config gui`` here (after the bridge-readiness
    probe) pays both up front, so steady-state launches are warm and land well
    inside the existing ``STARTUP_TIMEOUT``. A failure here is surfaced as a
    session-setup error with a clear message rather than a confusing first-test
    failure.
    """
    wait_for_accessibility_bridge()
    with ConfigDialog.open(env=env) as app:
        app.dialog().wait_visible(timeout=STARTUP_TIMEOUT)


class SubmitterDialog(DeadlineApp):
    """Page object for ``deadline bundle gui-submit``."""

    DIALOG = "Deadline Cloud JobBundle Submitter"
    _PROGRESS_TITLE = "AWS Deadline Cloud submission"

    @classmethod
    def open(
        cls,
        bundle_dir: str,
        env: Optional[dict] = None,
        extra_args: Sequence[str] = (),
        dialog_name: Optional[str] = None,
        capture_stdio: bool = False,
    ) -> "SubmitterDialog":
        args = ["bundle", "gui-submit", *extra_args, bundle_dir]
        return cls.launch(
            args,
            env=env,
            dialog_name=dialog_name,
            capture_stdio=capture_stdio,
        )

    @property
    def _progress_selector(self) -> str:
        return _dialog_selector(self._PROGRESS_TITLE)

    @property
    def job_name(self) -> str:
        return self.locator('text_field[name="Name"]').element().value or ""

    def wait_farm_resolved(
        self,
        farm_name: str = "TestFarm",
        timeout: float = FARM_RESOLVE_TIMEOUT,
    ) -> None:
        """Block until the async farm/queue name refresh has populated the UI."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.tree_contains_text(farm_name):
                return
            time.sleep(0.25)
        self.dump_tree()
        raise TimeoutError(
            f"Farm name {farm_name!r} did not appear in the submitter's "
            f"accessibility tree within {timeout}s"
        )

    def export_bundle(self) -> None:
        """Click 'Export bundle' and dismiss the confirmation dialog."""
        self.button("Export bundle").press()
        ok = self.button("OK")
        ok.wait_visible(timeout=EXPORT_TIMEOUT)
        ok.press()

    def submit_and_ok(self, timeout: float = SUBMIT_TIMEOUT) -> None:
        """Click Submit, wait for success, click Ok."""
        self.button("Submit").press()
        deadline = time.monotonic() + timeout
        status = ""
        while time.monotonic() < deadline:
            status = self._progress_status_label_text()
            if status and status != "Preparing files...":
                break
            time.sleep(0.2)
        if status == "Submission complete":
            for ok_name in ("Ok", "OK"):
                btn = self._progress_button(ok_name)
                if btn.exists():
                    btn.press()
                    return
            self.dump_tree()
            raise AssertionError("Submission complete but could not find Ok/OK button to press")
        if status in ("Submission error", "Submission canceled"):
            self.dump_tree()
            raise AssertionError(f"Submission failed: progress dialog status is {status!r}")
        self.dump_tree()
        raise TimeoutError(
            f"Progress dialog did not reach a terminal state within {timeout}s. "
            f"Last status_label: {status!r}"
        )

    def _progress_status_label_text(self) -> str:
        """Return the progress dialog's status_label text, or ""."""
        terminal_labels = {
            "Submission complete",
            "Submission error",
            "Submission canceled",
        }
        for elt in self._app.locator("static_text").elements():
            name = (elt.name or "").strip()
            if name == "Preparing files...":
                return name
            if name in terminal_labels:
                return name
        return ""

    def submit_then_cancel(self, cancel_timeout: float = CANCEL_TIMEOUT) -> None:
        """Click Submit then immediately Cancel on the progress dialog."""
        self.button("Submit").press()
        cancel = self._progress_button("Cancel")
        try:
            cancel.wait_visible(timeout=cancel_timeout)
        except Exception:
            self.dump_tree()
            raise
        cancel.press()

    def dismiss_progress_close(self, timeout: float = CANCEL_TIMEOUT) -> None:
        """Wait for the progress dialog to close after cancel."""
        try:
            close = self.locator(f'{self._progress_selector} button[name="Close"]')
            close.wait_visible(timeout=timeout)
            close.press()
        except xa11y.XA11yError:
            pass
        try:
            self.locator(self._progress_selector).wait_detached(timeout=timeout)
        except xa11y.XA11yError:
            pass

    def _progress_button(self, name: str) -> xa11y.Locator:
        """Locate a button inside the submission progress dialog."""
        return self.locator(f'{self._progress_selector} button[name="{name}"]')
