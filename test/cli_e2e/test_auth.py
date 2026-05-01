# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""End-to-end tests for `deadline auth` subcommands."""

import json


def test_cli_auth_status_verbose(deadline_env, run_cli, configure_cli_defaults):
    _, env = deadline_env
    configure_cli_defaults(env)
    r = run_cli(env, "auth", "status")
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
    # makes `check_deadline_api_available` actually hit the mock backend.
    _, _, _, env = seeded_farm_queue
    r = run_cli(env, "auth", "status", "--output", "json")
    assert r.returncode == 0, r.stderr or r.stdout
    payload = json.loads(r.stdout)
    assert payload["api_availability"] is True


def test_cli_auth_logout_without_monitor_profile_reports_error(deadline_env, run_cli):
    _, env = deadline_env
    # Our env uses plain AWS creds, not Deadline Cloud monitor, so logout
    # responds with a clear error rather than silently succeeding.
    r = run_cli(env, "auth", "logout")
    assert r.returncode != 0
    assert "monitor" in (r.stdout + r.stderr).lower()


def test_cli_auth_login_without_monitor_profile_reports_error(deadline_env, run_cli):
    _, env = deadline_env
    # Login also requires a Deadline Cloud monitor profile; fails cleanly.
    r = run_cli(env, "auth", "login")
    assert r.returncode != 0


def test_cli_auth_help(deadline_env, run_cli):
    _, env = deadline_env
    r = run_cli(env, "auth", "--help")
    assert r.returncode == 0
    for sub in ("login", "logout", "status"):
        assert sub in r.stdout
