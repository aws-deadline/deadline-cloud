# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""End-to-end tests for `deadline worker` subcommands."""

import pytest


@pytest.fixture
def worker_setup(seeded_farm_queue):
    backend, farm_id, _, env = seeded_farm_queue
    fleet = backend.create_fleet(
        farmId=farm_id,
        displayName="Test Fleet",
        roleArn="arn:aws:iam::000000000000:role/mock",
        maxWorkerCount=10,
        configuration=backend._DEFAULT_FLEET_CONFIG,
    )
    fleet_id = fleet["fleetId"]
    worker = backend.create_worker(farmId=farm_id, fleetId=fleet_id)
    worker_id = worker["workerId"]
    return backend, farm_id, fleet_id, worker_id, env


def test_cli_worker_list(worker_setup, run_cli):
    _, _, fleet_id, worker_id, env = worker_setup
    r = run_cli(env, "worker", "list", "--fleet-id", fleet_id)
    assert r.returncode == 0, r.stderr or r.stdout
    assert worker_id in r.stdout
    assert "Displaying 1 of 1 workers" in r.stdout
    assert f"workerId: {worker_id}" in r.stdout
    assert "status: CREATED" in r.stdout


def test_cli_worker_get(worker_setup, run_cli):
    _, farm_id, fleet_id, worker_id, env = worker_setup
    r = run_cli(env, "worker", "get", "--fleet-id", fleet_id, "--worker-id", worker_id)
    assert r.returncode == 0, r.stderr or r.stdout
    assert f"workerId: {worker_id}" in r.stdout
    assert f"fleetId: {fleet_id}" in r.stdout
    assert f"farmId: {farm_id}" in r.stdout
    assert "status: CREATED" in r.stdout


def test_cli_worker_list_empty(seeded_farm_queue, run_cli):
    backend, farm_id, _, env = seeded_farm_queue
    fleet = backend.create_fleet(
        farmId=farm_id,
        displayName="Empty Fleet",
        roleArn="arn:aws:iam::000000000000:role/mock",
        maxWorkerCount=10,
        configuration=backend._DEFAULT_FLEET_CONFIG,
    )
    r = run_cli(env, "worker", "list", "--fleet-id", fleet["fleetId"])
    assert r.returncode == 0, r.stderr or r.stdout
    assert "0 of 0" in r.stdout


def test_cli_worker_get_unknown_id_fails(worker_setup, run_cli):
    _, _, fleet_id, _, env = worker_setup
    r = run_cli(
        env,
        "worker",
        "get",
        "--fleet-id",
        fleet_id,
        "--worker-id",
        "worker-00000000000000000000000000000999",
    )
    assert r.returncode != 0


def test_cli_worker_help(deadline_env, run_cli):
    _, env = deadline_env
    r = run_cli(env, "worker", "--help")
    assert r.returncode == 0
