# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""End-to-end tests for `deadline auth` subcommands."""

import json


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


def test_cli_auth_login_console_profile_invokes_aws_login(
    seeded_farm_queue, run_cli, set_cli_console_login_profile, fake_aws_cli
):
    """
    `deadline auth login` on a console profile drives `aws login --profile <name>`.
    Deadline Cloud monitor can't refresh these -- it rejects profiles absent from its
    own settings -- so the AWS CLI owns the flow.
    """
    _, _, _, env = seeded_farm_queue
    profile_name = set_cli_console_login_profile(env)
    argv_log = fake_aws_cli(env)

    r = run_cli(env, "auth", "login")

    assert r.returncode == 0, r.stderr or r.stdout
    assert "AWS Console sign-in page" in r.stdout
    assert f"AWS Console sign-in profile: {profile_name}" in r.stdout
    recorded = argv_log.read_text()
    assert "login" in recorded
    assert f"--profile {profile_name}" in recorded


def test_cli_auth_login_console_profile_reports_aws_cli_failure(
    seeded_farm_queue, run_cli, set_cli_console_login_profile, fake_aws_cli
):
    """A failed `aws login` surfaces the CLI's own output instead of reporting success."""
    _, _, _, env = seeded_farm_queue
    set_cli_console_login_profile(env)
    fake_aws_cli(env, exit_code=1, output="Sign-in was cancelled")

    r = run_cli(env, "auth", "login")

    assert r.returncode != 0
    combined = r.stdout + r.stderr
    assert "Sign-in was cancelled" in combined


def test_cli_auth_logout_console_profile_invokes_aws_logout(
    seeded_farm_queue, run_cli, set_cli_console_login_profile, fake_aws_cli
):
    """`deadline auth logout` clears a console profile's cached token via `aws logout`."""
    _, _, _, env = seeded_farm_queue
    profile_name = set_cli_console_login_profile(env)
    argv_log = fake_aws_cli(env, output="Logged out")

    r = run_cli(env, "auth", "logout")

    assert r.returncode == 0, r.stderr or r.stdout
    recorded = argv_log.read_text()
    assert "logout" in recorded
    assert f"--profile {profile_name}" in recorded


def test_cli_auth_help(deadline_env, run_cli):
    _, env = deadline_env
    r = run_cli(env, "auth", "--help")
    assert r.returncode == 0
    for sub in ("login", "logout", "status"):
        assert sub in r.stdout
