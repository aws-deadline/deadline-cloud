# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Compare API snapshots to detect changes.

This script compares the current API against a baseline snapshot
to detect any changes including additions, removals, and modifications.
"""

# If griffe not available, run through: hatch run docs:python scripts/validate_api_snapshot.py
import griffe
import json
import sys
from pathlib import Path
from typing import Dict, Any, List, Set, Tuple

# Import shared functions from generate_api_snapshot
from generate_api_snapshot import normalize_paths_in_snapshot


def load_snapshot(snapshot_path: str) -> Dict[str, Any]:
    """Load API snapshot from JSON file."""
    try:
        with open(snapshot_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: Snapshot file not found: {snapshot_path}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in snapshot file: {e}")
        sys.exit(1)


def generate_current_snapshot() -> Dict[str, Any]:
    """Generate current API snapshot."""
    try:
        deadline_api = griffe.load(
            "deadline",
            search_paths=["src"],
            resolve_aliases=True,
            resolve_external=False,
            allow_inspection=True,
        )

        # Convert to dict and normalize paths - use full=False for consistency
        api_dict = json.loads(deadline_api.as_json(full=False))
        project_root = Path.cwd()
        return normalize_paths_in_snapshot(api_dict, project_root)
    except Exception as e:
        print(f"Error generating current API snapshot: {e}")
        sys.exit(1)


def extract_api_paths(api_data: Dict[str, Any]) -> Set[str]:
    """Extract all API paths from the snapshot data."""
    paths = set()

    def is_version_related(path: str) -> bool:
        """Check if a path is a 'version' and should be ignored, as versions change on each build."""
        version_patterns = ["._version.version", "._version.__version__", ".version", "__version__"]
        return any(pattern in path for pattern in version_patterns)

    def traverse(obj: Dict[str, Any], path: str):
        if isinstance(obj, dict):
            # Add current object path (skip version-related paths)
            if path and obj.get("kind") in ["module", "class", "function", "attribute", "alias"]:
                if not is_version_related(path):
                    paths.add(path)

            # Traverse members
            if "members" in obj and isinstance(obj["members"], dict):
                for name, member in obj["members"].items():
                    member_path = f"{path}.{name}" if path else name
                    traverse(member, member_path)

    # Start traversal from the root
    if "name" in api_data:
        traverse(api_data, api_data["name"])

    return paths


def normalize_decorator_info(decorators):
    """Normalize decorator info by removing line numbers to focus on functional changes."""
    if not decorators or not isinstance(decorators, list):
        return decorators

    normalized = []
    for decorator in decorators:
        if isinstance(decorator, dict):
            # Keep only the decorator name/value, ignore line numbers
            normalized_decorator = {}
            if "value" in decorator:
                normalized_decorator["value"] = decorator["value"]
            normalized.append(normalized_decorator)
    return normalized


def extract_functional_signature(obj: Dict[str, Any]) -> Dict[str, Any]:
    """Extract only the functional signature of an API object, ignoring all noise."""
    kind = obj.get("kind")
    signature = {"kind": kind}

    if kind == "function":
        # Only track parameter names, types, and defaults - ignore everything else
        if "parameters" in obj and isinstance(obj["parameters"], list):
            params = []
            for p in obj["parameters"]:
                if isinstance(p, dict):
                    param_sig = {}
                    if "name" in p:
                        param_sig["name"] = p["name"]
                    if "kind" in p:
                        param_sig["kind"] = p["kind"]
                    if "default" in p and p["default"] is not None:
                        param_sig["default"] = p["default"]
                    if "annotation" in p and p["annotation"] is not None:
                        param_sig["annotation"] = p["annotation"]

                    # Only add if we have meaningful data
                    if param_sig:
                        params.append(param_sig)
            if params:
                signature["parameters"] = params

        # Track return type annotation if present
        if "returns" in obj and obj["returns"] is not None:
            signature["returns"] = obj["returns"]

        # Track decorator names only (ignore line numbers and other metadata)
        if "decorators" in obj and isinstance(obj["decorators"], list):
            decorator_names = []
            for dec in obj["decorators"]:
                if isinstance(dec, dict) and "value" in dec:
                    decorator_names.append(dec["value"])
            if decorator_names:
                signature["decorators"] = decorator_names

    elif kind == "class":
        # Track base classes
        if "bases" in obj and obj["bases"]:
            signature["bases"] = obj["bases"]

        # Track decorator names only
        if "decorators" in obj and isinstance(obj["decorators"], list):
            decorator_names = []
            for dec in obj["decorators"]:
                if isinstance(dec, dict) and "value" in dec:
                    decorator_names.append(dec["value"])
            if decorator_names:
                signature["decorators"] = decorator_names

    elif kind == "attribute":
        # Track type annotation and value if meaningful
        if "annotation" in obj and obj["annotation"] is not None:
            signature["annotation"] = obj["annotation"]
        # Only track simple values, ignore complex ones that might have noise
        if "value" in obj and obj["value"] is not None:
            value = obj["value"]
            # Only track simple string/number values, ignore complex expressions
            if isinstance(value, (str, int, float, bool)) or (
                isinstance(value, str) and len(value) < 100
            ):
                signature["value"] = value

    elif kind == "alias":
        # Track what this alias points to
        if "target" in obj and obj["target"] is not None:
            signature["target"] = obj["target"]

    # Remove empty signature if it only has kind
    if len(signature) == 1 and "kind" in signature:
        return {"kind": kind}

    return signature


def extract_detailed_api_info(api_data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Extract detailed API information for comparison, focusing ONLY on functional changes."""
    api_info = {}

    def is_version_related(path: str) -> bool:
        """Check if a path is version-related and should be ignored."""
        version_patterns = ["._version.version", "._version.__version__", ".version", "__version__"]
        return any(pattern in path for pattern in version_patterns)

    def traverse(obj: Dict[str, Any], path: str):
        if isinstance(obj, dict) and path:
            # Skip version-related paths
            if is_version_related(path):
                return

            # Extract only functional signature
            signature = extract_functional_signature(obj)
            if signature:
                api_info[path] = signature

            # Traverse members
            if "members" in obj and isinstance(obj["members"], dict):
                for name, member in obj["members"].items():
                    member_path = f"{path}.{name}" if path else name
                    traverse(member, member_path)

    # Start traversal from the root
    if "name" in api_data:
        traverse(api_data, api_data["name"])

    return api_info


def compare_snapshots(baseline_path: str) -> Tuple[bool, List[str]]:
    """Compare current API against baseline snapshot."""
    print(f"Loading baseline snapshot from: {baseline_path}")
    baseline_snapshot = load_snapshot(baseline_path)

    print("Generating current API snapshot...")
    current_snapshot = generate_current_snapshot()

    print("Comparing API snapshots...")

    # Extract API paths and detailed info
    baseline_paths = extract_api_paths(baseline_snapshot)
    current_paths = extract_api_paths(current_snapshot)

    baseline_info = extract_detailed_api_info(baseline_snapshot)
    current_info = extract_detailed_api_info(current_snapshot)

    changes = []
    has_changes = False

    # Check for additions
    added_paths = current_paths - baseline_paths
    if added_paths:
        has_changes = True
        changes.append(f"🆕 ADDED APIs ({len(added_paths)}):")
        for path in sorted(added_paths):
            info = current_info.get(path, {})
            kind = info.get("kind", "unknown")
            changes.append(f"  + {path} ({kind})")

    # Check for removals
    removed_paths = baseline_paths - current_paths
    if removed_paths:
        has_changes = True
        changes.append(f"🗑️  REMOVED APIs ({len(removed_paths)}):")
        for path in sorted(removed_paths):
            info = baseline_info.get(path, {})
            kind = info.get("kind", "unknown")
            changes.append(f"  - {path} ({kind})")

    # Check for modifications in common paths
    common_paths = baseline_paths & current_paths
    modified_paths = []

    for path in common_paths:
        baseline_obj = baseline_info.get(path, {})
        current_obj = current_info.get(path, {})

        # Compare relevant fields
        if baseline_obj != current_obj:
            modified_paths.append((path, baseline_obj, current_obj))

    if modified_paths:
        has_changes = True
        changes.append(f"🔄 MODIFIED APIs ({len(modified_paths)}):")
        for path, baseline_obj, current_obj in modified_paths:
            changes.append(f"  ~ {path}")

            # Show specific changes
            for key in set(baseline_obj.keys()) | set(current_obj.keys()):
                baseline_val = baseline_obj.get(key)
                current_val = current_obj.get(key)
                if baseline_val != current_val:
                    changes.append(f"    {key}: {baseline_val} -> {current_val}")

    # Summary
    if not has_changes:
        changes.append("✅ No API changes detected")
    else:
        changes.insert(0, "❌ API changes detected:")
        changes.insert(1, f"  Added: {len(added_paths)}")
        changes.insert(2, f"  Removed: {len(removed_paths)}")
        changes.insert(3, f"  Modified: {len(modified_paths)}")
        changes.insert(4, "")

    return has_changes, changes


def main():
    """Main function."""
    if len(sys.argv) < 2:
        print("Usage: python validate_api_snapshot.py <api_snapshot.json>")
        sys.exit(1)

    baseline_path = sys.argv[1]

    try:
        has_changes, change_report = compare_snapshots(baseline_path)

        # Print the report
        for line in change_report:
            print(line)

        # Exit with appropriate code
        sys.exit(1 if has_changes else 0)

    except Exception as e:
        print(f"Error comparing snapshots: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
