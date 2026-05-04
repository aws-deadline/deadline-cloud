"""E2E: deadline auth login → DCM opens browser → drive IdC sign-in via xa11y.

Cross-platform. Relies on xa11y >= 0.7 for:
  - xa11y.screenshot().save_png(path)  — native screen capture on all platforms
  - Locator.type_text(str)              — splice text via a11y API (no keystrokes)
"""

import os
import platform
import subprocess
import sys
import time

import xa11y

sys.stdout.reconfigure(line_buffering=True)

IS_LINUX = platform.system() == "Linux"

USERNAME = os.environ["MONITOR_USERNAME"]
PASSWORD = os.environ["MONITOR_PASSWORD"]

# Browser xa11y app name and Open-Link button label vary per platform.
BROWSER_NAME = os.environ.get("BROWSER_NAME", "Firefox")
OPEN_LINK_LABELS = os.environ.get("OPEN_LINK_LABELS", "Open Link,Open link,Open").split(",")

SCREENSHOT_DIR = os.environ.get("SCREENSHOT_DIR", "/tmp")


def run(cmd, check=True, timeout=180):
    print(f"$ {' '.join(cmd)}", flush=True)
    r = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)
    print(r.stdout, r.stderr, flush=True)
    if check and r.returncode:
        fail(f"failed: {cmd}")
    return r


def screenshot(name):
    path = os.path.join(SCREENSHOT_DIR, f"{name}.png")
    try:
        xa11y.screenshot().save_png(path)
    except Exception as e:
        print(f"screenshot({name}) failed: {e}", flush=True)


def dump_all_trees():
    for app in xa11y.App.list():
        print(f"=== APP {app.name!r} pid={app.pid} ===", flush=True)

        def walk(el, d=0, maxd=30):
            if d > maxd:
                return
            try:
                print(
                    "  " * d
                    + f"{el.role} name={(el.name or '')[:80]!r} value={(el.value or '')[:40]!r}",
                    flush=True,
                )
            except Exception:
                return
            try:
                for c in el.children():
                    walk(c, d + 1, maxd)
            except Exception:
                pass

        try:
            for c in app.children():
                walk(c)
        except Exception as e:
            print(f"  (children failed: {e})", flush=True)


def on_failure(exc_type, exc, tb):
    ts = int(time.time())
    print(f"\n=== FAILURE {exc_type.__name__}: {exc} ===", flush=True)
    try:
        screenshot(f"failure_{ts}")
        dump_all_trees()
    except Exception as e:
        print(f"diagnostic failed: {e}", flush=True)
    sys.__excepthook__(exc_type, exc, tb)


sys.excepthook = on_failure


def fail(msg):
    raise RuntimeError(msg)


def browser():
    """Find the browser app. BROWSER_NAME is a substring; first matching app wins."""
    end = time.time() + 90
    while time.time() < end:
        for app in xa11y.App.list():
            if BROWSER_NAME.lower() in (app.name or "").lower():
                return app
        time.sleep(1)
    fail(f"browser {BROWSER_NAME!r} not found; apps: {[a.name for a in xa11y.App.list()]}")


def click_open_link(br):
    """Click the browser's 'Open this link in Deadline Cloud monitor?' dialog."""
    for label in OPEN_LINK_LABELS:
        try:
            br.locator(f"button[name='{label}']").press()
            print(f"pressed button[name='{label}']", flush=True)
            return True
        except Exception:
            pass
    # Linux Firefox fallback: auto-redirect page with a 'here' link. Locator.press
    # invokes the link's a11y action, which triggers the browser's scheme-open UI.
    if IS_LINUX:
        try:
            br.locator("link[name='here']").press()
            print("pressed link[name='here']", flush=True)
            return True
        except Exception:
            pass
    return False


def _type_into(locator, text):
    """Type text into a focused field. Locator.type_text() uses the a11y splice
    API, which doesn't work on browser web-content fields on macOS/Linux (only
    on native widgets). InputSim.type_text() synthesises real keystrokes
    through the OS, which works everywhere as long as the field has focus."""
    locator.focus()
    time.sleep(0.3)
    xa11y.input_sim().type_text(text)


def sign_in():
    br = browser()
    # Dismiss any first-run welcome screens (Edge has several layered ones)
    for _ in range(6):
        dismissed = False
        for label in (
            "Start without your data",
            "Confirm and continue",
            "Confirm and start browsing",
            "Skip",
            "Not now",
            "Continue without this data",
            "Continue without Microsoft data",
        ):
            try:
                btn = br.locator(f"button[name='{label}']")
                btn.wait_visible(timeout=1)
                btn.press()
                print(f"dismissed welcome: {label}", flush=True)
                dismissed = True
                time.sleep(1)
                break
            except Exception:
                pass
        if not dismissed:
            break
    user = br.locator("text_field[name='Username']")
    user.wait_visible(timeout=120)
    _type_into(user, USERNAME)
    br.locator("button[name='Next']").press()
    pw = br.locator("text_field[name='Password']")
    pw.wait_visible(timeout=60)
    _type_into(pw, PASSWORD)
    br.locator("button[name='Sign in']").press()
    # Optional IdC 'Allow' consent screen
    try:
        br.locator("button[name='Allow']").wait_visible(timeout=10)
        br.locator("button[name='Allow']").press()
    except xa11y.TimeoutError:
        pass
    # Wait for Open-Link dialog (Linux Firefox). On macOS Safari + some browsers,
    # the deadline-cloud-monitor:// scheme is auto-dispatched without prompt.
    end = time.time() + 60
    while time.time() < end:
        if click_open_link(br):
            return
        time.sleep(1)
    print("No Open Link dialog appeared (browser may have auto-dispatched the scheme)", flush=True)


def main():
    cli = subprocess.Popen(["deadline", "auth", "login"])
    print(f"auth login pid={cli.pid}", flush=True)
    try:
        time.sleep(10)
        sign_in()
        cli.wait(timeout=180)
        if cli.returncode:
            fail(f"auth login failed: {cli.returncode}")
    finally:
        if cli.poll() is None:
            cli.kill()

    run(["deadline", "auth", "status"])
    run(["deadline", "farm", "list"])
    print("SUCCESS")


if __name__ == "__main__":
    main()
