# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""Tests for the OpenJD <IntRangeExpr> parser used by RANGE_EXPR job parameters."""

from __future__ import annotations

from typing import Any

import pytest

from deadline.client.job_bundle._range_expr import IntRange, parse_int_range_expr


@pytest.mark.parametrize(
    "expr,expected",
    [
        # Single values
        pytest.param("42", [IntRange(42, 42, 1)], id="single-positive"),
        pytest.param("0", [IntRange(0, 0, 1)], id="single-zero"),
        pytest.param("-100", [IntRange(-100, -100, 1)], id="single-negative"),
        pytest.param(" 5 ", [IntRange(5, 5, 1)], id="single-whitespace"),
        # Simple ranges
        pytest.param("1-100", [IntRange(1, 100, 1)], id="simple-range"),
        pytest.param("-10-10", [IntRange(-10, 10, 1)], id="negative-to-positive"),
        pytest.param("-10--5", [IntRange(-10, -5, 1)], id="negative-to-negative"),
        pytest.param("1 - 5", [IntRange(1, 5, 1)], id="range-whitespace"),
        pytest.param("-1 - 1", [IntRange(-1, 1, 1)], id="spec-example-neg-one-to-one"),
        # Stepped ranges; the end is normalized to the last value reached
        pytest.param("0-100:10", [IntRange(0, 100, 10)], id="step-exact-fit"),
        pytest.param("1-9:3", [IntRange(1, 7, 3)], id="step-not-evenly-divide"),
        pytest.param("1-10:4", [IntRange(1, 9, 4)], id="spec-example-1-10-4"),
        pytest.param("1-5:10", [IntRange(1, 1, 10)], id="step-exceeds-range"),
        pytest.param("1-10 : 2", [IntRange(1, 9, 2)], id="whitespace-around-step"),
        # Descending ranges are normalized to ascending
        pytest.param("10-1:-1", [IntRange(1, 10, 1)], id="descending"),
        pytest.param("10-1:-2", [IntRange(2, 10, 2)], id="descending-step-2"),
        pytest.param("-1--10:-1", [IntRange(-10, -1, 1)], id="descending-negative"),
        pytest.param("-5--14:-2", [IntRange(-13, -5, 2)], id="descending-negative-step-2"),
        # Multiple elements, sorted
        pytest.param("1,3,5,7", [IntRange(i, i, 1) for i in (1, 3, 5, 7)], id="comma-values"),
        pytest.param(
            "10-15:2,1-5", [IntRange(1, 5, 1), IntRange(10, 14, 2)], id="spec-example-sorted"
        ),
        # A stepped range's declared end may reach into the next range as long as no
        # value it actually produces does; this matches the service and openjd-model.
        pytest.param(
            "1-10:4,10-15", [IntRange(1, 9, 4), IntRange(10, 15, 1)], id="stepped-adjacent"
        ),
        pytest.param(
            "1-10,20-30:2,42",
            [IntRange(1, 10, 1), IntRange(20, 30, 2), IntRange(42, 42, 1)],
            id="mixed",
        ),
        pytest.param(
            "20-29,0-9,10-19",
            [IntRange(0, 9, 1), IntRange(10, 19, 1), IntRange(20, 29, 1)],
            id="out-of-order",
        ),
        pytest.param(
            " 0 - 1 : 1, 2 - 100 : 1", [IntRange(0, 1, 1), IntRange(2, 100, 1)], id="whitespace"
        ),
        pytest.param(" 1 , 2 , 3 ", [IntRange(i, i, 1) for i in (1, 2, 3)], id="whitespace-values"),
        pytest.param("1\t-\t3", [IntRange(1, 3, 1)], id="tabs"),
        pytest.param("100,-1--2:-1,3-5,9,8,7,12", None, id="complex-conformance"),
        # int64 boundaries
        pytest.param(
            "-9223372036854775808-9223372036854775807",
            [IntRange(-(2**63), 2**63 - 1, 1)],
            id="int64-bounds",
        ),
    ],
)
def test_parse_valid(expr: str, expected: list[IntRange] | None) -> None:
    result = parse_int_range_expr(expr)
    if expected is not None:
        assert result == expected
    # Ranges are always sorted, ascending, with positive step.
    assert all(r.start <= r.end and r.step > 0 for r in result)
    assert result == sorted(result)


@pytest.mark.parametrize(
    "expr,message",
    [
        pytest.param("", "must not be empty", id="empty"),
        pytest.param("   ", "must not be empty", id="whitespace-only"),
        pytest.param("abc", "invalid element", id="non-numeric"),
        pytest.param("1-abc", "invalid element", id="range-non-numeric"),
        pytest.param("not-a-range", "invalid element", id="not-a-range"),
        pytest.param("1.5", "invalid element", id="float"),
        pytest.param("1,,3", "invalid element", id="double-comma"),
        pytest.param("1,2,3,", "invalid element", id="trailing-comma"),
        pytest.param(",1", "invalid element", id="leading-comma"),
        pytest.param("1-", "invalid element", id="dangling-hyphen"),
        pytest.param("1-5:", "invalid element", id="dangling-colon"),
        pytest.param("1:2", "invalid element", id="step-without-range"),
        pytest.param("1-5:2:3", "invalid element", id="double-step"),
        pytest.param("1 2", "invalid element", id="space-separated"),
        pytest.param("1-10:0", "zero step", id="zero-step"),
        pytest.param("5-1", "descending range", id="descending-no-step"),
        pytest.param("1 - -1", "descending range", id="descending-to-negative-no-step"),
        pytest.param("1-5,10-6", "descending range", id="descending-in-list"),
        pytest.param("1-10:-1", "ascending range", id="wrong-step-direction"),
        pytest.param("1-10,5-15", "overlapping", id="overlap"),
        pytest.param("1-10:2,3-10:2", "overlapping", id="overlap-interleaved-steps"),
        pytest.param("1-10:1,10-1:-1", "overlapping", id="overlap-asc-desc"),
        pytest.param("1,2,3,2", "overlapping", id="duplicate-values"),
        pytest.param("1-9223372036854775808", "64-bit", id="int64-overflow"),
        pytest.param("-9223372036854775809", "64-bit", id="int64-underflow"),
    ],
)
def test_parse_invalid(expr: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        parse_int_range_expr(expr)


@pytest.mark.parametrize("value", [None, 5, 1.5, ["1-5"], {"a": 1}])
def test_parse_non_string(value: Any) -> None:
    with pytest.raises(ValueError, match="must be a string"):
        parse_int_range_expr(value)
