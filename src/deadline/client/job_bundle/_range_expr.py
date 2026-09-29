# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
Parsing and validation of Open Job Description <IntRangeExpr> strings, the value
type of RANGE_EXPR job parameters (EXPR extension).

    <IntRangeExpr> ::= <Element> | <Element>,<IntRangeExpr>
    <Element>      ::= <WS>*<Int><WS>* | <WS>*<Range><WS>* | <WS>*<SkipRange><WS>*
    <Range>        ::= <Int><WS>*-<WS>*<Int>
    <SkipRange>    ::= <Range>:<Skip>
    <Int>          ::= Any integer value (positive, negative, or zero)
    <Skip>         ::= base-10 non-zero number
    <WS>           ::= whitespace character: tabs or spaces

The ranges are validated without expanding them, since a range expression may
describe an arbitrarily long list of values.
"""

from __future__ import annotations

import re
from typing import NamedTuple

# Integers in the EXPR expression language are 64-bit signed.
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1

# Whitespace is accepted anywhere between tokens, matching the reference implementations.
_ELEMENT_RE = re.compile(
    r"^[ \t]*(?P<start>-?[0-9]+)[ \t]*"
    r"(?:-[ \t]*(?P<end>-?[0-9]+)[ \t]*"
    r"(?::[ \t]*(?P<step>-?[0-9]+)[ \t]*)?)?$"
)


class IntRange(NamedTuple):
    """A normalized element of a range expression: start <= end and step > 0."""

    start: int
    end: int
    step: int


def parse_int_range_expr(expr: str) -> list[IntRange]:
    """Parses and validates an <IntRangeExpr> string.

    Returns the normalized ranges sorted by their smallest value. Raises ValueError
    describing the first problem found when the expression is invalid.
    """
    if not isinstance(expr, str):
        raise ValueError(f"Range expression must be a string, got {type(expr).__name__}.")
    if not expr.strip(" \t"):
        raise ValueError("Range expression must not be empty.")

    ranges: list[IntRange] = []
    for element in expr.split(","):
        match = _ELEMENT_RE.match(element)
        if not match:
            raise ValueError(
                f"Range expression {expr!r} has invalid element {element.strip()!r}. "
                "Expected an integer, a range like 1-10, or a stepped range like 1-10:2."
            )
        start = _parse_int64(match.group("start"), expr)
        if match.group("end") is None:
            ranges.append(IntRange(start, start, 1))
            continue
        end = _parse_int64(match.group("end"), expr)
        step = 1 if match.group("step") is None else _parse_int64(match.group("step"), expr)

        if step == 0:
            raise ValueError(f"Range expression {expr!r} has a zero step in {element.strip()!r}.")
        if step > 0 and start > end:
            raise ValueError(
                f"Range expression {expr!r} has descending range {element.strip()!r} "
                "with a positive step; a descending range requires a negative step."
            )
        if step < 0 and start < end:
            raise ValueError(
                f"Range expression {expr!r} has ascending range {element.strip()!r} "
                "with a negative step; an ascending range requires a positive step."
            )

        # Normalize to an ascending range whose end is the last value actually reached.
        if step < 0:
            step = -step
            start, end = start - ((start - end) // step) * step, start
        else:
            end = start + ((end - start) // step) * step
        ranges.append(IntRange(start, end, step))

    ranges.sort()
    for previous, current in zip(ranges, ranges[1:]):
        if previous.end >= current.start:
            raise ValueError(
                f"Range expression {expr!r} has overlapping ranges "
                f"{_format_range(previous)} and {_format_range(current)}."
            )
    return ranges


def _parse_int64(text: str, expr: str) -> int:
    value = int(text)
    if not (_INT64_MIN <= value <= _INT64_MAX):
        raise ValueError(
            f"Range expression {expr!r} has integer {text} outside the 64-bit signed range."
        )
    return value


def _format_range(r: IntRange) -> str:
    if r.start == r.end:
        return str(r.start)
    if r.step == 1:
        return f"{r.start}-{r.end}"
    return f"{r.start}-{r.end}:{r.step}"
