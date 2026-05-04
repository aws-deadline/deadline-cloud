# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Private helper for invoking Deadline Cloud batch-get APIs with partial-success
handling.

The Deadline Cloud batch-get APIs (e.g. BatchGetTask, BatchGetStep) accept up
to 100 identifiers per call and return {<items>: [...], errors: [...]}.
Individual items can fail with codes like ResourceNotFoundException (terminal)
or ThrottlingException (transient). This helper chunks inputs, collects
successful results, and retries transient per-item errors with exponential
backoff.
"""

from __future__ import annotations

import time
from typing import Any, Callable, Iterable

# Per-item error codes that are transient and worth retrying.
_TRANSIENT_CODES = frozenset({"InternalServerErrorException", "ThrottlingException"})

# Maximum identifiers per batch request (Deadline Cloud API limit).
_MAX_BATCH_SIZE = 100


def _chunks(items: list, size: int) -> Iterable[list]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _backoff_seconds(attempt: int) -> float:
    # 0.5, 1.0, 2.0, ...
    return 0.5 * (2**attempt)


def batch_get(
    *,
    deadline: Any,
    operation: str,
    identifiers: list[dict],
    key_fn: Callable[[dict], Any],
    items_field: str,
    id_fields: tuple[str, ...],
    max_attempts: int = 3,
    sleep: Callable[[float], None] = time.sleep,
) -> tuple[dict, list[dict]]:
    """Call a Deadline Cloud batch-get API with partial-success handling.

    Args:
        deadline: boto3 deadline client.
        operation: Method name on the client (e.g. ``"batch_get_task"``).
        identifiers: List of identifier dicts to fetch.
        key_fn: Maps an identifier dict (or a returned item dict) to a hashable
            key used to index the results.
        items_field: Name of the list of successful items in the response
            (e.g. ``"tasks"``, ``"steps"``).
        id_fields: Tuple of identifier field names, used to extract identifier
            dicts from the ``errors`` entries so transient failures can be
            retried.
        max_attempts: Maximum total attempts per identifier (including the
            first). Exponential backoff between attempts.
        sleep: Sleep function, overridable for tests.

    Returns:
        ``(results, terminal_errors)`` where ``results`` is ``{key: item}`` for
        each successfully fetched item and ``terminal_errors`` is the list of
        error dicts whose code is not transient (or which ran out of attempts).
    """
    remaining: list[dict] = list(identifiers)
    results: dict = {}
    terminal: list[dict] = []
    method = getattr(deadline, operation)

    for attempt in range(max_attempts):
        next_round: list[dict] = []
        for chunk in _chunks(remaining, _MAX_BATCH_SIZE):
            response = method(identifiers=chunk)
            for item in response.get(items_field, []):
                results[key_fn(item)] = item
            for err in response.get("errors", []):
                if err.get("code") in _TRANSIENT_CODES:
                    next_round.append({k: err[k] for k in id_fields if k in err})
                else:
                    terminal.append(err)
        remaining = next_round
        if not remaining:
            break
        if attempt + 1 < max_attempts:
            sleep(_backoff_seconds(attempt))

    # Anything still remaining exhausted its retries.
    for ident in remaining:
        terminal.append({"code": "ExhaustedRetries", **ident})

    return results, terminal
