# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Payload merging for submission hooks."""

from __future__ import annotations

from typing import Any as _Any, Dict as _Dict, Optional as _Optional


def merge_asset_references(
    original: _Optional[_Dict[str, _Any]], modified: _Optional[_Dict[str, _Any]]
) -> _Dict[str, _Any]:
    """Merge asset references with replacement at the nested key level.

    Top-level keys from modified replace the corresponding keys in original.
    Keys not present in modified are preserved from original.
    """
    original = original or {}
    modified = modified or {}
    result = dict(original)
    result.update(modified)
    return result


def merge_payload(original: _Dict[str, _Any], modified: _Dict[str, _Any]) -> _Dict[str, _Any]:
    """Merge modified payload into original."""
    result = original.copy()

    for key, value in modified.items():
        if key == "attachments" and isinstance(value, dict) and "assetReferences" in value:
            orig_attachments = result.get("attachments", {})
            result["attachments"] = orig_attachments.copy()
            result["attachments"]["assetReferences"] = merge_asset_references(
                orig_attachments.get("assetReferences"), value.get("assetReferences")
            )
            # Merge other attachment fields
            for k, v in value.items():
                if k != "assetReferences":
                    result["attachments"][k] = v
        else:
            result[key] = value

    return result
