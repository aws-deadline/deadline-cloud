# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""End-to-end tests for `deadline auth` subcommands."""

import json

import pytest


def test_cli_auth_status_verbose(deadline_env, run_cli, configure_cli_defaults):
    _, env = deadline_env
    configure_cli_defaults(env)
    # run_cli executes the CLI in a subprocess (no TTY), where --output now
    # auto-detects to json, so request verbose explicitly to assert its format.
    r = run_cli(env, "auth", "status", "--output", "verbose")
    assert r.returncode == 0, r.stderr or r.stdout
    assert "Profile Name:" in r.stdout
    assert "Source:" in r.stdout
    assert "Status:" in r.stdout
    assert "API Availability:" in r.stdout


def test_cli_auth_status_json(deadline_env, run_cli, configure_cli_defaults):
    _, env = deadline_env
    configure_cli_defaults(env)
    r = run_cli(env, "auth", "status", "--output", "json")
    assert r.returncode == 0, r.stderr or r.stdout
    payload = json.loads(r.stdout)
    assert "profile_name" in payload
    assert "status" in payload
    assert "source" in payload
    assert "api_availability" in payload


def test_cli_auth_status_api_available_when_backend_reachable(seeded_farm_queue, run_cli):
    # seeded_farm_queue configures defaults.farm_id + defaults.queue_id, which
    # makes the auth-status ListFarms probe actually hit the mock backend, so
    # the derived api_availability resolves to True.
    _, _, _, env = seeded_farm_queue
    r = run_cli(env, "auth", "status", "--output", "json")
    assert r.returncode == 0, r.stderr or r.stdout
    payload = json.loads(r.stdout)
    assert payload["api_availability"] is True


def test_cli_auth_logout_without_monitor_profile_reports_error(deadline_env, run_cli):
    _, env = deadline_env
    # Our env uses plain AWS creds -- neither Deadline Cloud monitor nor AWS Console
    # sign-in -- so logout responds with a clear error rather than silently succeeding.
    r = run_cli(env, "auth", "logout")
    assert r.returncode != 0
    combined = (r.stdout + r.stderr).lower()
    assert "monitor" in combined
    # Both supported profile types are named, so the user knows what would work.
    assert "console sign-in" in combined


def test_cli_auth_login_without_monitor_profile_reports_error(deadline_env, run_cli):
    _, env = deadline_env
    # Login requires a Deadline Cloud monitor or AWS Console sign-in profile; a plain
    # credentials profile fails cleanly.
    r = run_cli(env, "auth", "login")
    assert r.returncode != 0
    combined = (r.stdout + r.stderr).lower()
    # Error text comes from api._loginout and names both supported profile types.
    assert "only supported" in combined
    assert "monitor" in combined
    assert "console sign-in" in combined


def test_cli_auth_status_detects_console_login_profile(
    seeded_farm_queue, run_cli, set_cli_console_login_profile
):
    """
    A profile carrying `login_session` is reported as AWS_CONSOLE_LOGIN. Before this
    was recognized it fell through to HOST_PROVIDED, which is what made `auth login`
    refuse to run.
    """
    _, _, _, env = seeded_farm_queue
    set_cli_console_login_profile(env)

    r = run_cli(env, "auth", "status", "--output", "json")

    assert r.returncode == 0, r.stderr or r.stdout
    payload = json.loads(r.stdout)
    assert payload["source"] == "AWS_CONSOLE_LOGIN"
    assert payload["profile_name"] == "console-signin"
    # Static creds still resolve ahead of the login provider, so the probe succeeds.
    assert payload["status"] == "AUTHENTICATED"


def test_cli_auth_status_console_profile_needs_login_when_unreachable(
    deadline_env, run_cli, set_cli_console_login_profile
):
    """
    An unusable console profile reports NEEDS_LOGIN, not CONFIGURATION_ERROR -- only
    the former surfaces a "Log in" affordance to the user.
    """
    _, env = deadline_env
    set_cli_console_login_profile(env)
    # Point Deadline at a closed port so the ListFarms auth probe fails.
    env = {**env, "AWS_ENDPOINT_URL_DEADLINE": "http://127.0.0.1:1"}

    r = run_cli(env, "auth", "status", "--output", "json")

    assert r.returncode == 0, r.stderr or r.stdout
    payload = json.loads(r.stdout)
    assert payload["source"] == "AWS_CONSOLE_LOGIN"
    assert payload["status"] == "NEEDS_LOGIN"
    assert payload["api_availability"] is False


@pytest.mark.skip(
    reason="Console sign-in hands off to Deadline Cloud monitor, which must be installed "
    "and opens a real browser for the sign-in. Neither is available in e2e; the handoff is "
    "covered by unit tests with a mocked subprocess."
)
def test_cli_auth_login_console_profile_opens_deadline_cloud_monitor():
    """`deadline auth login` on a console profile launches Deadline Cloud monitor."""


def test_cli_auth_login_console_profile_without_monitor_reports_both_routes(
    seeded_farm_queue, run_cli, set_cli_console_login_profile
):
    """
    Starting a console session is an interactive browser handshake, so login hands off to
    Deadline Cloud monitor. With no monitor configured -- which is this environment, and
    any workstation where the profile came from `aws login` -- there is nothing to hand off
    to, so it must fail naming both routes rather than launching anything.
    """
    _, _, _, env = seeded_farm_queue
    profile_name = set_cli_console_login_profile(env)

    r = run_cli(env, "auth", "login")

    assert r.returncode != 0
    combined = r.stdout + r.stderr
    assert "Deadline Cloud monitor" in combined
    assert f"aws login --profile {profile_name}" in combined


def test_cli_auth_logout_console_profile_deletes_cached_token(
    seeded_farm_queue, run_cli, set_cli_console_login_profile, seed_cli_login_token_cache
):
    """
    `deadline auth logout` clears a console profile's cached token by deleting the file
    botocore caches it in. No subprocess is involved, so nothing needs to be on PATH.
    """
    _, _, _, env = seeded_farm_queue
    profile_name = set_cli_console_login_profile(env)
    cached_token = seed_cli_login_token_cache(env)
    assert cached_token.exists()

    r = run_cli(env, "auth", "logout")

    assert r.returncode == 0, r.stderr or r.stdout
    assert not cached_token.exists()
    # The confirmation names the profile that was actually signed out. It used to
    # hardcode the monitor wording, misreporting a console logout.
    assert f"AWS Console sign-in profile: {profile_name}" in r.stdout


def test_cli_auth_logout_console_profile_succeeds_when_already_logged_out(
    seeded_farm_queue, run_cli, set_cli_console_login_profile, seed_cli_login_token_cache
):
    """
    Logging out twice is not an error: an absent cached token is the state logout is
    trying to reach.
    """
    _, _, _, env = seeded_farm_queue
    set_cli_console_login_profile(env)
    cached_token = seed_cli_login_token_cache(env)
    cached_token.unlink()

    r = run_cli(env, "auth", "logout")

    assert r.returncode == 0, r.stderr or r.stdout
    assert not cached_token.exists()


def test_cli_auth_help(deadline_env, run_cli):
    _, env = deadline_env
    r = run_cli(env, "auth", "--help")
    assert r.returncode == 0
    for sub in ("login", "logout", "status"):
        assert sub in r.stdout
