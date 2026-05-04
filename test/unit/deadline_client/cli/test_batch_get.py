# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for the private ``_batch_get`` helper."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from deadline.client.cli._groups._batch_get import batch_get


def _task_id_key(item: dict) -> str:
    # Works for both items and identifier dicts in these tests.
    return item["taskId"]


TASK_ID_FIELDS = ("farmId", "queueId", "jobId", "stepId", "taskId")


def _ident(task_id: str, stepId: str = "step-1") -> dict:
    return {
        "farmId": "farm-1",
        "queueId": "queue-1",
        "jobId": "job-1",
        "stepId": stepId,
        "taskId": task_id,
    }


def _item(task_id: str) -> dict:
    # Returned items have all identifier fields plus extra data.
    return {**_ident(task_id), "runStatus": "SUCCEEDED"}


def _err(task_id: str, code: str) -> dict:
    return {**_ident(task_id), "code": code, "message": f"{code} for {task_id}"}


def test_all_succeed_single_batch():
    deadline = MagicMock()
    deadline.batch_get_task.return_value = {
        "tasks": [_item("t1"), _item("t2")],
        "errors": [],
    }

    results, terminal = batch_get(
        deadline=deadline,
        operation="batch_get_task",
        identifiers=[_ident("t1"), _ident("t2")],
        key_fn=_task_id_key,
        items_field="tasks",
        id_fields=TASK_ID_FIELDS,
    )

    assert set(results) == {"t1", "t2"}
    assert terminal == []
    assert deadline.batch_get_task.call_count == 1


def test_chunks_over_100():
    deadline = MagicMock()
    deadline.batch_get_task.side_effect = lambda *, identifiers: {
        "tasks": [_item(i["taskId"]) for i in identifiers],
        "errors": [],
    }

    ids = [_ident(f"t{i}") for i in range(250)]
    results, terminal = batch_get(
        deadline=deadline,
        operation="batch_get_task",
        identifiers=ids,
        key_fn=_task_id_key,
        items_field="tasks",
        id_fields=TASK_ID_FIELDS,
    )

    assert len(results) == 250
    assert terminal == []
    assert deadline.batch_get_task.call_count == 3
    # First two batches are max size, last is the remainder.
    sizes = [len(c.kwargs["identifiers"]) for c in deadline.batch_get_task.call_args_list]
    assert sizes == [100, 100, 50]


def test_terminal_error_not_retried():
    deadline = MagicMock()
    deadline.batch_get_task.return_value = {
        "tasks": [_item("t1")],
        "errors": [_err("t2", "ResourceNotFoundException")],
    }

    results, terminal = batch_get(
        deadline=deadline,
        operation="batch_get_task",
        identifiers=[_ident("t1"), _ident("t2")],
        key_fn=_task_id_key,
        items_field="tasks",
        id_fields=TASK_ID_FIELDS,
    )

    assert set(results) == {"t1"}
    assert len(terminal) == 1
    assert terminal[0]["code"] == "ResourceNotFoundException"
    assert terminal[0]["taskId"] == "t2"
    assert deadline.batch_get_task.call_count == 1


def test_transient_error_retried_and_recovers():
    deadline = MagicMock()
    # First call: t2 throttled. Second call: t2 succeeds.
    deadline.batch_get_task.side_effect = [
        {"tasks": [_item("t1")], "errors": [_err("t2", "ThrottlingException")]},
        {"tasks": [_item("t2")], "errors": []},
    ]

    sleep = MagicMock()
    results, terminal = batch_get(
        deadline=deadline,
        operation="batch_get_task",
        identifiers=[_ident("t1"), _ident("t2")],
        key_fn=_task_id_key,
        items_field="tasks",
        id_fields=TASK_ID_FIELDS,
        sleep=sleep,
    )

    assert set(results) == {"t1", "t2"}
    assert terminal == []
    assert deadline.batch_get_task.call_count == 2
    assert sleep.call_count == 1


def test_transient_error_exhausts_retries():
    deadline = MagicMock()
    deadline.batch_get_task.return_value = {
        "tasks": [],
        "errors": [_err("t1", "InternalServerErrorException")],
    }

    sleep = MagicMock()
    results, terminal = batch_get(
        deadline=deadline,
        operation="batch_get_task",
        identifiers=[_ident("t1")],
        key_fn=_task_id_key,
        items_field="tasks",
        id_fields=TASK_ID_FIELDS,
        max_attempts=3,
        sleep=sleep,
    )

    assert results == {}
    # Each attempt's transient error is discarded in favor of the final
    # ExhaustedRetries entry.
    assert len(terminal) == 1
    assert terminal[-1]["code"] == "ExhaustedRetries"
    assert terminal[-1]["taskId"] == "t1"
    assert deadline.batch_get_task.call_count == 3
    # sleep called between attempts 1-2 and 2-3, not after final attempt.
    assert sleep.call_count == 2


def test_mix_success_transient_terminal():
    deadline = MagicMock()

    def side_effect(*, identifiers):
        # On first call: t1 ok, t2 throttled, t3 not-found.
        # On second call: t2 ok.
        ids = {i["taskId"] for i in identifiers}
        if ids == {"t1", "t2", "t3"}:
            return {
                "tasks": [_item("t1")],
                "errors": [
                    _err("t2", "ThrottlingException"),
                    _err("t3", "ResourceNotFoundException"),
                ],
            }
        if ids == {"t2"}:
            return {"tasks": [_item("t2")], "errors": []}
        raise AssertionError(f"Unexpected call with {ids}")

    deadline.batch_get_task.side_effect = side_effect

    results, terminal = batch_get(
        deadline=deadline,
        operation="batch_get_task",
        identifiers=[_ident("t1"), _ident("t2"), _ident("t3")],
        key_fn=_task_id_key,
        items_field="tasks",
        id_fields=TASK_ID_FIELDS,
        sleep=MagicMock(),
    )

    assert set(results) == {"t1", "t2"}
    assert len(terminal) == 1
    assert terminal[0]["taskId"] == "t3"
    assert terminal[0]["code"] == "ResourceNotFoundException"


def test_empty_identifiers_makes_no_calls():
    deadline = MagicMock()
    results, terminal = batch_get(
        deadline=deadline,
        operation="batch_get_task",
        identifiers=[],
        key_fn=_task_id_key,
        items_field="tasks",
        id_fields=TASK_ID_FIELDS,
    )
    assert results == {}
    assert terminal == []
    deadline.batch_get_task.assert_not_called()


@pytest.mark.parametrize("code", ["ThrottlingException", "InternalServerErrorException"])
def test_each_transient_code_is_retried(code):
    deadline = MagicMock()
    deadline.batch_get_task.side_effect = [
        {"tasks": [], "errors": [_err("t1", code)]},
        {"tasks": [_item("t1")], "errors": []},
    ]
    results, terminal = batch_get(
        deadline=deadline,
        operation="batch_get_task",
        identifiers=[_ident("t1")],
        key_fn=_task_id_key,
        items_field="tasks",
        id_fields=TASK_ID_FIELDS,
        sleep=MagicMock(),
    )
    assert set(results) == {"t1"}
    assert terminal == []
