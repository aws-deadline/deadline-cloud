# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""End-to-end tests for `deadline fleet` subcommands."""

import pytest


@pytest.fixture
def fleet_setup(seeded_farm_queue):
    backend, farm_id, queue_id, env = seeded_farm_queue
    fleet = backend.create_fleet(
        farmId=farm_id,
        displayName="Test Fleet",
        roleArn="arn:aws:iam::000000000000:role/mock",
        maxWorkerCount=10,
        configuration=backend._DEFAULT_FLEET_CONFIG,
    )
    # Register a QueueFleetAssociation so `fleet get --queue-id` returns this fleet.
    backend.queue_fleet_associations = getattr(backend, "queue_fleet_associations", {})
    backend.queue_fleet_associations[(farm_id, queue_id, fleet["fleetId"])] = {
        "queueId": queue_id,
        "fleetId": fleet["fleetId"],
        "status": "ACTIVE",
        "createdAt": backend._now(),
        "createdBy": "mock-user",
    }
    return backend, farm_id, queue_id, fleet["fleetId"], env


def test_cli_fleet_list(fleet_setup, run_cli):
    _, _, _, fleet_id, env = fleet_setup
    r = run_cli(env, "fleet", "list")
    assert r.returncode == 0, r.stderr or r.stdout
    assert fleet_id in r.stdout
    assert "Test Fleet" in r.stdout
    # Shape of the yaml-ish output from _cli_object_repr.
    assert f"fleetId: {fleet_id}" in r.stdout
    assert "displayName: Test Fleet" in r.stdout


def test_cli_fleet_get_with_fleet_id(fleet_setup, run_cli):
    _, farm_id, _, fleet_id, env = fleet_setup
    r = run_cli(env, "fleet", "get", "--fleet-id", fleet_id)
    assert r.returncode == 0, r.stderr or r.stdout
    assert fleet_id in r.stdout
    assert f"farmId: {farm_id}" in r.stdout
    assert "status: ACTIVE" in r.stdout


def test_cli_fleet_get_with_queue_id(fleet_setup, run_cli):
    _, _, queue_id, fleet_id, env = fleet_setup
    r = run_cli(env, "fleet", "get", "--queue-id", queue_id)
    assert r.returncode == 0, r.stderr or r.stdout
    assert fleet_id in r.stdout


def test_cli_fleet_get_rejects_both_ids(fleet_setup, run_cli):
    _, _, queue_id, fleet_id, env = fleet_setup
    r = run_cli(env, "fleet", "get", "--queue-id", queue_id, "--fleet-id", fleet_id)
    assert r.returncode != 0


def test_cli_fleet_get_without_any_id_and_no_default_queue(
    deadline_env, run_cli, configure_cli_defaults
):
    backend, env = deadline_env
    farm = backend.create_farm(displayName="F")
    configure_cli_defaults(env, farm_id=farm["farmId"])  # no queue_id
    r = run_cli(env, "fleet", "get")
    assert r.returncode != 0
    combined = r.stdout + r.stderr
    assert "fleet-id" in combined or "queue-id" in combined


def test_cli_fleet_help(deadline_env, run_cli):
    _, env = deadline_env
    r = run_cli(env, "fleet", "--help")
    assert r.returncode == 0
    assert "list" in r.stdout and "get" in r.stdout
