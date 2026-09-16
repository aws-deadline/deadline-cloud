# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Bundle repository abstraction for browsing job bundles from local filesystem or S3.
Supports both directory-based bundles and .ojd archive bundles (zip format).
"""

from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import re
import shutil
import sys
import tempfile
import zipfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from email.header import decode_header
from logging import getLogger
from typing import Callable, Optional, Protocol

import yaml

from botocore.exceptions import ClientError

from .._path_utils import is_path_contained
from ..config import config_file
from ..config.config_file import get_cache_directory
from ..exceptions import DeadlineOperationError

logger = getLogger(__name__)

TEMPLATE_FILENAMES = ("template.yaml", "template.json")
S3_JOB_BUNDLES_PREFIX = "job-bundles"
ARCHIVE_EXTENSION = ".ojd"
CACHE_META_FILENAME = ".bundle_cache_meta.json"

# ── Bundle visibility (local, per-user) ──────────────────────
#
# "Hiding" a shared bundle is a private, per-user view preference stored in a
# local JSON file — it never touches S3 and does not affect other users.
# Show/hide only changes the local user's own listing. Each queue gets its own
# hidden (dot-prefixed) .visibility.json in a per-queue folder inside the bundle
# cache — not intended for manual editing.
VISIBILITY_FILENAME = ".visibility.json"
# On-disk format version for the local ``.visibility.json``. Written so a future
# format change can detect and migrate old files; kept at 1 because the format
# has not changed in any released version.
VISIBILITY_VERSION = 1

# Max concurrent head_object calls issued when warming the preview prefetch cache.
# This is a background optimization (off the listing critical path), not visibility.
PREVIEW_PREFETCH_MAX_WORKERS = 16


# S3 user-defined metadata is limited to 2 KB total (keys + values, UTF-8 encoded).
# Keys include the "x-amz-meta-" prefix (12 bytes) added by S3.
# We reserve 256 bytes for customer-defined metadata on the same object.
# See: https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html#UserMetadata
METADATA_KEY_NAME = "ojd-name"
METADATA_KEY_DESC = "ojd-desc"
METADATA_KEY_STEPS = "ojd-steps"
METADATA_KEY_PARAMS = "ojd-params"
METADATA_KEY_STEP_COUNT = "ojd-step-count"
METADATA_KEY_PARAM_COUNT = "ojd-param-count"
# Hard caps to prevent any single field from consuming the entire budget
METADATA_LIMIT_NAME = 256
METADATA_LIMIT_DESC = 600
S3_METADATA_TOTAL_BUDGET = 2048 - 256  # Reserve 256 bytes for customer metadata

# Preview display caps.
#
# A job bundle is fully readable once it is local, but a maliciously constructed
# template could declare enormous names/descriptions or huge numbers of steps and
# parameters that would freeze the preview UI or exhaust memory. We always cap
# what the preview surfaces, regardless of the source. The caps are set at (and
# never above) the OpenJD 2023-09 spec's documented maxima so any spec-valid
# bundle previews in full:
#   - name / identifier: 512 chars (spec max with the FEATURE_BUNDLE_1 extension)
#   - description: 2048 chars
#   - parameter string value: 1024 chars
#   - parameterDefinitions count: 200 (spec max with FEATURE_BUNDLE_1)
# The OpenJD spec does NOT bound the number of steps, so PREVIEW_MAX_STEPS is our
# own defensive limit for display purposes only.
PREVIEW_MAX_NAME_LEN = 512
PREVIEW_MAX_DESC_LEN = 2048
PREVIEW_MAX_PARAM_NAME_LEN = 512
PREVIEW_MAX_PARAM_VALUE_LEN = 1024
PREVIEW_MAX_PARAMS = 200
PREVIEW_MAX_STEPS = 500

# Archive extraction safety limits (always enforced, independent of any UI
# warning) to defend against maliciously constructed .ojd archives:
#   - too many entries (resource exhaustion),
#   - decompression bombs (tiny compressed size expanding to a huge payload).
# Uncompressed sizes come from the zip central directory via infolist(), which
# does not decompress anything; zipfile also caps actual extraction output at the
# declared file_size, so this is a sound upper bound. A generous absolute floor is
# always allowed; above it, the expansion ratio must be plausible for real data
# (a bomb has a tiny compressed size relative to its uncompressed size). There is
# no absolute size ceiling here — the physical limit is free disk space, checked
# separately — so legitimately large (low-ratio) bundles still extract.
MAX_ARCHIVE_ENTRIES = 100_000
MAX_ARCHIVE_COMPRESSION_RATIO = 200
MAX_ARCHIVE_UNCOMPRESSED_FLOOR = 256 * 1024 * 1024  # 256 MB always permitted
# A template (even with embedded scripts) is small; refuse to read a giant one.
MAX_TEMPLATE_BYTES = 16 * 1024 * 1024  # 16 MB

# POSIX only forbids / and null; Windows also forbids \ : * ? " < > |
# Control characters (0x00-0x1F, 0x7F) are problematic on all platforms
_WINDOWS_UNSAFE_CHARS = re.compile(r'[\\/:*?"<>|\x00-\x1f\x7f]+')
_POSIX_UNSAFE_CHARS = re.compile(r"[/\x00-\x1f\x7f]+")


def sanitize_bundle_name(name: str) -> str:
    """Sanitize a bundle name for use as a local directory name.

    Only replaces characters illegal on the current OS, preserving the
    original name as closely as possible. Rejects path traversal attempts.
    """
    pattern = _WINDOWS_UNSAFE_CHARS if sys.platform == "win32" else _POSIX_UNSAFE_CHARS
    sanitized = pattern.sub("_", name)
    # Reject names that are empty or consist solely of replaced unsafe characters
    # (e.g. "///" -> "___"), as well as path-traversal components. Underscores that
    # result from replacing real content (e.g. a trailing "<") are preserved so the
    # sanitized name stays distinct from other bundles.
    if (
        not sanitized.strip("_")
        or sanitized in (".", "..")
        or ".." in re.split(r"[/\\]", sanitized)
    ):
        raise ValueError("Bundle name is empty or unsafe after sanitization")
    return sanitized


def _is_archive(name: str) -> bool:
    """Check if a filename is an .ojd archive."""
    return name.endswith(ARCHIVE_EXTENSION)


def _strip_archive_ext(name: str) -> str:
    """Remove the .ojd extension from a filename."""
    if name.endswith(ARCHIVE_EXTENSION):
        return name[: -len(ARCHIVE_EXTENSION)]
    return name


def _safe_zip_extract(
    zf: zipfile.ZipFile, dest_dir: str, progress_callback=None, size_callback=None
) -> None:
    """Extract a zip file, rejecting archives with entries that would escape dest_dir."""
    dest = os.path.realpath(dest_dir)

    for member in zf.namelist():
        # A zip entry with a leading separator is rooted/absolute. os.path.isabs()
        # alone is insufficient: on Windows (Python 3.13+) it returns False for a
        # leading-slash path that has no drive letter, so check separators explicitly
        # to classify such entries consistently across platforms.
        if os.path.isabs(member) or member.startswith(("/", "\\")):
            raise ValueError(f"Archive contains absolute path: {member}")
        target = os.path.realpath(os.path.join(dest, member))
        # Unrelated path spaces -- a different drive, a different UNC host -- are not
        # contained, so they are rejected here rather than raising from the comparison.
        # path_module is passed explicitly, and read at call time, so tests can patch it
        # for another platform.
        if not is_path_contained(target, dest, path_module=os.path):
            raise ValueError(f"Archive entry would extract outside target directory: {member}")

    _check_archive_extraction_safety(zf, dest)

    if progress_callback:
        if size_callback:
            size_callback(sum(info.file_size for info in zf.infolist()))
        for info in zf.infolist():
            zf.extract(info, dest_dir)
            progress_callback(info.file_size)
    else:
        zf.extractall(dest_dir)


def _check_archive_extraction_safety(zf: zipfile.ZipFile, dest: str) -> None:
    """Reject archives that would exhaust resources on extraction.

    Uses the zip central directory (infolist) — no decompression — to check the
    entry count and the declared uncompressed size against a bomb-detection ratio,
    then verifies the payload fits on the destination filesystem. Raises ValueError
    if the archive is unsafe to extract.
    """
    infos = zf.infolist()
    if len(infos) > MAX_ARCHIVE_ENTRIES:
        raise ValueError(
            f"Archive has too many entries ({len(infos)} > {MAX_ARCHIVE_ENTRIES}); "
            "refusing to extract"
        )

    total_uncompressed = sum(i.file_size for i in infos)
    total_compressed = sum(i.compress_size for i in infos)
    max_uncompressed = max(
        MAX_ARCHIVE_UNCOMPRESSED_FLOOR,
        total_compressed * MAX_ARCHIVE_COMPRESSION_RATIO,
    )
    if total_uncompressed > max_uncompressed:
        raise ValueError(
            f"Archive expands to {total_uncompressed} bytes from {total_compressed} "
            f"compressed, exceeding the safe limit of {max_uncompressed} bytes "
            "(possible zip bomb); refusing to extract"
        )

    # Refuse if the extracted payload wouldn't fit on the target filesystem.
    try:
        free = shutil.disk_usage(dest if os.path.exists(dest) else os.path.dirname(dest)).free
    except OSError:
        free = None
    if free is not None and total_uncompressed > free:
        raise ValueError(
            f"Not enough free disk space to extract archive: needs {total_uncompressed} "
            f"bytes, {free} available"
        )


def _extract_archive(archive_path: str, dest_dir: str) -> None:
    """Extract an .ojd archive to dest_dir.

    Raises ValueError with a clear message if the file is not a valid zip archive
    (an ``.ojd`` is a zip under the hood), so callers surface a friendly error
    instead of a raw ``zipfile.BadZipFile`` for a corrupt/renamed/non-zip file.
    """
    try:
        with zipfile.ZipFile(archive_path, "r") as zf:
            _safe_zip_extract(zf, dest_dir)
    except zipfile.BadZipFile as e:
        raise ValueError(
            f"{os.path.basename(archive_path)!r} is not a valid .ojd archive (expected a zip file)"
        ) from e


def read_template_from_archive(archive_path: str) -> Optional[tuple[str, str]]:
    """Read a template file from a local .ojd archive. Returns (contents, filename) or None."""
    try:
        with zipfile.ZipFile(archive_path, "r") as zf:
            return _read_template_from_zip(zf)
    except Exception:
        logger.debug("Failed to read template from archive %s", archive_path, exc_info=True)
        return None


def _read_template_from_zip(zf: zipfile.ZipFile) -> Optional[tuple[str, str]]:
    """Read a template file from an open ZipFile. Returns (contents, filename) or None."""
    names = zf.namelist()
    for fname in TEMPLATE_FILENAMES:
        matches = [n for n in names if n == fname or n.endswith("/" + fname)]
        matches.sort(key=lambda n: n.count("/"))
        if matches:
            # Refuse to read an implausibly large template (bomb defense). The
            # declared size bounds the actual read: zipfile caps output at
            # file_size, so this check on the central-directory value is sound.
            info = zf.getinfo(matches[0])
            if info.file_size > MAX_TEMPLATE_BYTES:
                raise ValueError(
                    f"Template '{matches[0]}' is too large to read "
                    f"({info.file_size} > {MAX_TEMPLATE_BYTES} bytes)"
                )
            return zf.read(matches[0]).decode("utf-8"), fname
    return None


def _extract_archive_from_fileobj(
    fileobj, dest_dir: str, progress_callback=None, size_callback=None
) -> None:
    """Extract an .ojd archive from a seekable binary file object to dest_dir.

    Operating on a file object (rather than a bytes blob) lets callers stream a
    large download into a temp file and extract from it without ever holding the
    whole compressed archive in memory.

    Raises ValueError with a clear message if the object is not a valid zip
    archive, so a corrupt or non-`.ojd` download surfaces a friendly error rather
    than a raw ``zipfile.BadZipFile``.
    """
    fileobj.seek(0)
    try:
        with zipfile.ZipFile(fileobj, "r") as zf:
            _safe_zip_extract(
                zf, dest_dir, progress_callback=progress_callback, size_callback=size_callback
            )
    except zipfile.BadZipFile as e:
        raise ValueError(
            "Downloaded object is not a valid .ojd archive (expected a zip file)"
        ) from e


# Downloads at or below this size are buffered in memory; larger archives spill
# to a temp file so we never hold a multi-GB archive (in several copies) in RAM.
_DOWNLOAD_SPOOL_THRESHOLD = 64 * 1024 * 1024  # 64 MB


def _open_download_sink(size_hint: int):
    """Return a seekable binary sink for a download of approximately size_hint bytes.

    Small downloads stay in memory (fast); larger ones spill to a temporary file
    created inside the bundle cache directory. Keeping the temp file in the cache
    dir — rather than the shared system temp dir — co-locates the spill with the
    extraction target's filesystem and keeps it out of world-writable /tmp. The
    temp file is unlinked on close (immediately, on POSIX), so no other process
    can reference or tamper with it.

    ``SpooledTemporaryFile`` is intentionally avoided: before Python 3.11 it does
    not implement ``seekable()``, which ``zipfile`` requires. ``io.BytesIO`` and
    ``tempfile.TemporaryFile`` are fully seekable on all supported versions.
    """
    if size_hint and size_hint > _DOWNLOAD_SPOOL_THRESHOLD:
        cache_root = get_bundle_cache_dir()
        os.makedirs(cache_root, exist_ok=True)
        return tempfile.TemporaryFile(dir=cache_root)
    return io.BytesIO()


_LARGE_FILE_THRESHOLD = 8 * 1024 * 1024  # 8MB
_CHUNK_SIZE = 4 * 1024 * 1024  # 4MB


def archive_bundle_dir(source_dir: str, progress_callback=None) -> io.BytesIO:
    """Archive a job bundle directory into an in-memory .ojd (zip) buffer.

    Args:
        source_dir: Path to the bundle directory to archive.
        progress_callback: Optional callable(bytes_archived) called after each file/chunk.

    Returns:
        A BytesIO buffer positioned at the start, containing the zip archive.
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
        for root, dirs, files in os.walk(source_dir, followlinks=False):
            dirs[:] = [d for d in dirs if not os.path.islink(os.path.join(root, d))]
            for fname in files:
                local_path = os.path.join(root, fname)
                if os.path.islink(local_path):
                    logger.warning("Skipping symlink: %s", local_path)
                    continue
                arcname = os.path.relpath(local_path, source_dir)
                fsize = os.path.getsize(local_path)
                if fsize <= _LARGE_FILE_THRESHOLD:
                    zf.write(local_path, arcname)
                    if progress_callback:
                        progress_callback(fsize)
                else:
                    with (
                        zf.open(arcname.replace(os.sep, "/"), "w", force_zip64=True) as dest,
                        open(local_path, "rb") as src,
                    ):
                        while True:
                            chunk = src.read(_CHUNK_SIZE)
                            if not chunk:
                                break
                            dest.write(chunk)
                            if progress_callback:
                                progress_callback(len(chunk))
    buf.seek(0)
    return buf


def get_bundle_dir_size(source_dir: str) -> int:
    """Calculate total size of archivable files in a bundle directory."""
    total = 0
    for root, dirs, files in os.walk(source_dir, followlinks=False):
        dirs[:] = [d for d in dirs if not os.path.islink(os.path.join(root, d))]
        for fname in files:
            fpath = os.path.join(root, fname)
            if not os.path.islink(fpath):
                total += os.path.getsize(fpath)
    return total


def _collapse_ws(s: str) -> str:
    """Collapse runs of whitespace — including control characters like CR/LF/TAB
    — into single spaces so a value is safe to place in an S3 ``x-amz-meta-*``
    HTTP header.

    A template field can legally contain a newline (valid YAML: ``name: "a\\nb"``).
    Left raw it becomes a bare LF in the header value, which urllib3 rejects with
    an opaque ``ValueError`` from inside ``upload_fileobj`` — and is the
    header-injection shape of the same bug on older urllib3 that didn't reject it.
    """
    return " ".join(str(s).split())


def build_bundle_metadata(
    source_dir: Optional[str] = None,
    bundle_name: Optional[str] = None,
    bundle_info: Optional["BundleInfo"] = None,
) -> dict[str, str]:
    """Build S3 user metadata dict for a bundle.

    Extracts name, description, steps, and parameters from the template,
    fitting them within the S3 2KB metadata budget. Returns an empty dict
    if no template is found.

    Provide either source_dir (to auto-extract info) or bundle_info (pre-extracted).

    Args:
        source_dir: Path to the bundle directory.
        bundle_name: Override for the metadata name field (defaults to template name).
        bundle_info: Pre-extracted BundleInfo (skips template parsing if provided).
    """
    metadata: dict[str, str] = {}

    if bundle_info:
        info = bundle_info
    elif source_dir:
        template = None
        for tname in TEMPLATE_FILENAMES:
            tpath = os.path.join(source_dir, tname)
            if os.path.isfile(tpath):
                try:
                    with open(tpath, encoding="utf-8") as f:
                        template = _parse_template(f.read(), tname)
                    if template:
                        break
                except OSError:
                    pass  # Unreadable template file — try next candidate
        if not template:
            return metadata
        pv = LocalBundleRepository.read_parameter_values(source_dir)
        info = extract_bundle_info(template, source_dir, pv)
    else:
        return metadata

    name_value = _collapse_ws(bundle_name or info.name)
    metadata[METADATA_KEY_NAME] = _truncate_s3_value(
        name_value, METADATA_LIMIT_NAME, METADATA_KEY_NAME
    )
    if info.description:
        desc = _collapse_ws(info.description)
        metadata[METADATA_KEY_DESC] = _truncate_s3_value(
            desc, METADATA_LIMIT_DESC, METADATA_KEY_DESC
        )
    if info.step_names:
        metadata[METADATA_KEY_STEP_COUNT] = str(len(info.step_names))
    if info.parameters:
        metadata[METADATA_KEY_PARAM_COUNT] = str(len(info.parameters))

    # Dynamically allocate remaining budget to steps and params. All values are
    # whitespace-collapsed so a control character in a template name/step/param
    # can't produce an invalid (or injectable) HTTP header value.
    steps_str = _collapse_ws(",".join(info.step_names)) if info.step_names else ""
    param_strs = (
        _collapse_ws(
            ",".join(f"{p.get('name', '?')}:{p.get('type', '?')}" for p in info.parameters)
        )
        if info.parameters
        else ""
    )

    used = sum(12 + len(k.encode("utf-8")) + len(v.encode("utf-8")) for k, v in metadata.items())
    remaining = S3_METADATA_TOTAL_BUDGET - used
    keys_needed = 0
    if steps_str:
        keys_needed += 12 + len(METADATA_KEY_STEPS)
    if param_strs:
        keys_needed += 12 + len(METADATA_KEY_PARAMS)
    remaining -= keys_needed

    if remaining > 0:
        if steps_str and param_strs:
            steps_budget = remaining // 2
            params_budget = remaining - steps_budget
            metadata[METADATA_KEY_STEPS] = _truncate_s3_value(
                steps_str, steps_budget, METADATA_KEY_STEPS
            )
            metadata[METADATA_KEY_PARAMS] = _truncate_s3_value(
                param_strs, params_budget, METADATA_KEY_PARAMS
            )
        elif steps_str:
            metadata[METADATA_KEY_STEPS] = _truncate_s3_value(
                steps_str, remaining, METADATA_KEY_STEPS
            )
        elif param_strs:
            metadata[METADATA_KEY_PARAMS] = _truncate_s3_value(
                param_strs, remaining, METADATA_KEY_PARAMS
            )

    return metadata


# RFC 2047 base64 encoded-word for UTF-8: "=?utf-8?b?" + base64 + "?=".
# The wrapper is a constant 12 characters and base64 expands N bytes to exactly
# 4*ceil(N/3) characters, so the encoded length is fully predictable — see
# _truncate_s3_value.
_S3_ENCODED_WORD_PREFIX = "=?utf-8?b?"
_S3_ENCODED_WORD_SUFFIX = "?="
_S3_ENCODED_WORD_OVERHEAD = len(_S3_ENCODED_WORD_PREFIX) + len(_S3_ENCODED_WORD_SUFFIX)  # 12


def _encode_s3_value(value: str) -> str:
    """Encode a metadata value so it is safe to send as S3 user metadata.

    S3 user metadata is transmitted as ``x-amz-meta-*`` HTTP headers, and botocore
    rejects any non-US-ASCII value with a ``ParamValidationError`` ("S3 metadata
    can only contain ASCII characters"). ASCII values are returned unchanged, so
    existing uploads and readers are unaffected. Values containing non-ASCII
    characters (e.g. Japanese, accented Latin, emoji) are encoded as an RFC 2047
    base64 encoded-word so the original text can be recovered on read via
    ``_decode_s3_value``.

    The encoded-word is built explicitly (rather than via ``email.header``) so the
    encoded length is exactly ``12 + 4*ceil(N/3)`` for ``N`` UTF-8 bytes — this
    guarantee lets ``_truncate_s3_value`` size a truncation in one shot without a
    retry loop, and produces a single, unfolded word.

    The authoritative values still live inside the bundle template; this metadata
    is only a zero-download preview hint.
    """
    if value.isascii():
        return value
    body = base64.b64encode(value.encode("utf-8")).decode("ascii")
    return f"{_S3_ENCODED_WORD_PREFIX}{body}{_S3_ENCODED_WORD_SUFFIX}"


def _decode_s3_value(value: str) -> str:
    """Inverse of ``_encode_s3_value``: decode RFC 2047 encoded-words if present.

    Decoding is intentionally permissive and uses the stdlib ``decode_header`` so
    it handles *any* valid encoded-word, not just this module's exact output:
    upper/lowercase charset (``UTF-8``/``utf-8``), both Base64 (``B``) and
    quoted-printable (``Q``) encodings, and values folded into multiple
    space-separated encoded-words. This makes previews robust to bundles uploaded
    by other clients or earlier builds. Plain ASCII values (no encoded-word
    marker) are returned unchanged, and a trailing truncation marker ("...") is
    preserved.
    """
    if "=?" not in value:
        return value
    try:
        return "".join(
            fragment.decode(charset or "utf-8") if isinstance(fragment, bytes) else fragment
            for fragment, charset in decode_header(value)
        )
    except Exception:
        # Never let a malformed value break preview — fall back to the raw text.
        return value


def _truncate_to_utf8_bytes(value: str, max_bytes: int) -> str:
    """Truncate a string to at most ``max_bytes`` UTF-8 bytes, on a char boundary."""
    encoded = value.encode("utf-8")
    if len(encoded) <= max_bytes:
        return value
    return encoded[:max_bytes].decode("utf-8", errors="ignore")


def _truncate_s3_value(value: str, limit: int, field: str = "") -> str:
    """Encode to an S3-safe (US-ASCII) form and fit it within a byte ``limit``.

    The byte limit applies to the *encoded* value (that is what is sent to S3 and
    counts against the metadata budget). Because a base64 encoded-word cannot be
    sliced without corrupting it, we instead size how much *raw* text to keep:

    * ASCII values are stored verbatim, so we slice to ``limit - 3`` (reserving 3
      bytes for the "..." marker).
    * Non-ASCII values become a base64 encoded-word of length
      ``12 + 4*ceil(N/3)`` for ``N`` UTF-8 bytes. Solving ``12 + 4*ceil(N/3) + 3
      <= limit`` gives ``N <= 3 * ((limit - 15) // 4)`` — an exact bound, so we
      truncate the raw value to that many UTF-8 bytes and encode once.
    """
    encoded = _encode_s3_value(value)
    if len(encoded) <= limit:  # encoded form is ASCII: one byte per character
        return encoded
    if limit <= 3:
        return ""

    if value.isascii():
        truncated_raw = value[: limit - 3]
    else:
        # 12 bytes wrapper + 3 bytes for "..." = 15 bytes of fixed overhead.
        max_groups = (limit - _S3_ENCODED_WORD_OVERHEAD - 3) // 4
        truncated_raw = _truncate_to_utf8_bytes(value, max_groups * 3) if max_groups > 0 else ""

    if field:
        logger.warning(
            "Bundle metadata '%s' truncated from %d to %d bytes",
            field,
            len(encoded),
            limit,
        )
    return _encode_s3_value(truncated_raw) + "..."


@dataclass
class BundleInfo:
    """Metadata extracted from a job bundle's template."""

    path: str
    name: str
    description: str = ""
    step_names: list[str] = field(default_factory=list)
    parameters: list[dict] = field(default_factory=list)
    total_steps: Optional[int] = None  # Actual count (when metadata was truncated)
    total_parameters: Optional[int] = None  # Actual count (when metadata was truncated)
    size_bytes: Optional[int] = None  # Archive/download size in bytes, when known

    def to_dict(self) -> dict:
        """Serialize to a dict suitable for JSON output."""
        return {
            "name": self.name,
            "description": self.description,
            "steps": self.step_names,
            "parameters": self.parameters,
            "sizeBytes": self.size_bytes,
        }

    def format_text(self) -> str:
        """Format as human-readable text."""
        lines = [f"Name: {self.name}"]
        if self.description:
            lines.append(f"Description: {self.description}")
        if self.step_names:
            lines.append("Steps:")
            for step in self.step_names:
                lines.append(f"  \u2022 {step}")
        if self.parameters:
            lines.append("Parameters:")
            for p in self.parameters:
                name = p.get("name", "?")
                ptype = p.get("type", "?")
                value = p.get("_display_value", "")
                line = f"  {name} ({ptype})"
                if value:
                    line += f" = {value}"
                lines.append(line)
        return "\n".join(lines)


@dataclass
class BrowseEntry:
    """A single item in the browser listing."""

    name: str
    path: str
    is_bundle: bool
    is_archive: bool = False


class BundleRepository(Protocol):
    def list_entries(self, path: str) -> list[BrowseEntry]:
        """List immediate children of `path`. Returns folders and bundles."""
        ...

    def get_bundle_info(self, path: str) -> Optional[BundleInfo]:
        """Load and return metadata for the bundle at `path`, or None if invalid."""
        ...

    def root_path(self) -> str:
        """The starting path for browsing."""
        ...


def _parse_template(raw: str, filename: str) -> Optional[dict]:
    """Parse a template file's contents, returning the dict or None on failure."""
    try:
        if filename.endswith(".json"):
            return json.loads(raw)
        else:
            return yaml.safe_load(raw)
    except Exception:
        logger.debug("Failed to parse template %s", filename, exc_info=True)
        return None


def extract_bundle_info(
    template: dict, path: str, parameter_values: Optional[dict] = None
) -> BundleInfo:
    """Extract BundleInfo from a parsed template dict.

    If parameter_values is provided, merges values into the parameter definitions.

    All fields are capped to the preview limits (see PREVIEW_MAX_* constants) and
    every access is defensive: the template comes from a job bundle that may have
    been authored by another user (e.g. a shared queue bundle), so it could be
    malformed or maliciously oversized. We never let it crash or DoS the preview.
    """
    raw_params = template.get("parameterDefinitions", [])
    if not isinstance(raw_params, list):
        raw_params = []

    # Build a lookup from parameter_values file
    pv_map: dict[str, str] = {}
    if isinstance(parameter_values, dict):
        for pv in parameter_values.get("parameterValues", []) or []:
            # Only string names are usable as dict keys; a hostile file could
            # carry a dict/list name that would raise TypeError on insert.
            if isinstance(pv, dict) and isinstance(pv.get("name"), str) and "value" in pv:
                pv_map[pv["name"]] = pv["value"]

    # Cap the number of parameters; record the true total so the preview can show
    # "… N more" via the existing truncation UI.
    total_parameters = len(raw_params) if len(raw_params) > PREVIEW_MAX_PARAMS else None
    params: list[dict] = []
    for p in raw_params[:PREVIEW_MAX_PARAMS]:
        if not isinstance(p, dict):
            continue
        capped = dict(p)  # copy so we never mutate the caller's template
        # Coerce the name to a str up front: a hostile template can make it a
        # dict/list, and using the raw value as a dict key (``name in pv_map``)
        # would raise ``TypeError: unhashable type``. extract_bundle_info is
        # documented to never crash on a malformed template.
        name = str(capped.get("name", "") or "")
        capped["name"] = name[:PREVIEW_MAX_PARAM_NAME_LEN]
        if capped.get("type"):
            capped["type"] = str(capped["type"])[:PREVIEW_MAX_PARAM_NAME_LEN]
        # Attach resolved value: parameter_values > default > (unset)
        if name in pv_map:
            capped["_display_value"] = str(pv_map[name])[:PREVIEW_MAX_PARAM_VALUE_LEN]
        elif "default" in capped:
            capped["_display_value"] = str(capped["default"])[:PREVIEW_MAX_PARAM_VALUE_LEN]
        params.append(capped)

    raw_steps = template.get("steps", [])
    if not isinstance(raw_steps, list):
        raw_steps = []
    total_steps = len(raw_steps) if len(raw_steps) > PREVIEW_MAX_STEPS else None
    step_names = [
        str(s.get("name", ""))[:PREVIEW_MAX_NAME_LEN]
        for s in raw_steps[:PREVIEW_MAX_STEPS]
        if isinstance(s, dict)
    ]

    raw_name = template.get("name")
    if not isinstance(raw_name, str) or not raw_name:
        raw_name = os.path.basename(path.rstrip("/"))
    name = raw_name[:PREVIEW_MAX_NAME_LEN]

    raw_desc = template.get("description", "")
    description = raw_desc[:PREVIEW_MAX_DESC_LEN] if isinstance(raw_desc, str) else ""

    return BundleInfo(
        path=path,
        name=name,
        description=description,
        step_names=step_names,
        parameters=params,
        total_steps=total_steps,
        total_parameters=total_parameters,
    )


class LocalBundleRepository:
    """Browse job bundles on the local filesystem. Supports directories and archives."""

    def __init__(self, root: str = "", include_archives: bool = True):
        self._root = root or os.path.expanduser("~")
        self._include_archives = include_archives

    def root_path(self) -> str:
        return self._root

    def list_entries(self, path: str) -> list[BrowseEntry]:
        entries: list[BrowseEntry] = []
        try:
            with os.scandir(path) as it:
                children = sorted(it, key=lambda e: e.name)
        except OSError:
            return entries
        for entry in children:
            if entry.is_dir(follow_symlinks=False):
                is_bundle = self._is_dir_bundle(entry.path)
                entries.append(BrowseEntry(name=entry.name, path=entry.path, is_bundle=is_bundle))
            elif (
                self._include_archives
                and entry.is_file(follow_symlinks=False)
                and _is_archive(entry.name)
            ):
                # Trust the .ojd extension rather than opening and decompressing
                # every archive here — this runs on the Qt main thread and the
                # default root is the user's home directory, so parsing each file
                # (worse on a network home) would freeze the UI. A file that is
                # not a valid bundle simply shows an empty/error preview when
                # selected (the preview path tolerates a missing template).
                entries.append(
                    BrowseEntry(
                        name=_strip_archive_ext(entry.name),
                        path=entry.path,
                        is_bundle=True,
                        is_archive=True,
                    )
                )
        return entries

    def get_bundle_info(self, path: str) -> Optional[BundleInfo]:
        if os.path.isfile(path) and _is_archive(path):
            return self._get_archive_bundle_info(path)
        return self._get_dir_bundle_info(path)

    def extract_bundle(self, path: str) -> str:
        """Extract an archive bundle into the local cache, using an mtime-based
        cache to avoid redundant extraction.

        Returns the path to the extracted bundle directory (inside the cache)."""
        cache_dir = os.path.join(get_bundle_cache_dir(), _local_cache_key(path))
        meta = _read_cache_meta(cache_dir)
        current_mtime = os.path.getmtime(path)

        # Cache hit — mtime unchanged
        if meta and meta.get("mtime") == current_mtime and os.path.isdir(cache_dir):
            bundle_dir = self._find_bundle_root(cache_dir)
            if bundle_dir:
                logger.info("Using cached bundle: %s", cache_dir)
                return bundle_dir

        # Cache miss or stale — extract
        if os.path.exists(cache_dir):
            shutil.rmtree(cache_dir)
        os.makedirs(cache_dir, exist_ok=True)
        _extract_archive(path, cache_dir)

        # Write mtime to cache meta
        meta_path = os.path.join(cache_dir, CACHE_META_FILENAME)
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump({"mtime": current_mtime}, f)

        return self._find_bundle_root(cache_dir) or cache_dir

    @staticmethod
    def _find_bundle_root(extract_dir: str) -> Optional[str]:
        """If the archive has a single top-level wrapper dir, return it; otherwise extract_dir."""
        contents = [c for c in os.listdir(extract_dir) if c != CACHE_META_FILENAME]
        if len(contents) == 1 and os.path.isdir(os.path.join(extract_dir, contents[0])):
            return os.path.join(extract_dir, contents[0])
        return extract_dir

    def _get_dir_bundle_info(self, path: str) -> Optional[BundleInfo]:
        for fname in TEMPLATE_FILENAMES:
            fpath = os.path.join(path, fname)
            if os.path.isfile(fpath):
                try:
                    with open(fpath, encoding="utf-8") as f:
                        raw = f.read()
                except OSError:
                    return None
                template = _parse_template(raw, fname)
                if template:
                    pv = self.read_parameter_values(path)
                    return extract_bundle_info(template, path, pv)
        return None

    @staticmethod
    def read_parameter_values(path: str) -> Optional[dict]:
        """Read parameter_values.yaml or .json from a bundle directory."""
        for pvname in ("parameter_values.yaml", "parameter_values.json"):
            pvpath = os.path.join(path, pvname)
            if os.path.isfile(pvpath):
                try:
                    with open(pvpath, encoding="utf-8") as f:
                        return _parse_template(f.read(), pvname)
                except OSError:
                    pass
        return None

    def _get_archive_bundle_info(self, path: str) -> Optional[BundleInfo]:
        result = read_template_from_archive(path)
        if result:
            raw, fname = result
            template = _parse_template(raw, fname)
            if template:
                return extract_bundle_info(template, path)
        return None

    @staticmethod
    def _is_dir_bundle(path: str) -> bool:
        for fname in TEMPLATE_FILENAMES:
            if os.path.isfile(os.path.join(path, fname)):
                return True
        return False


# ── S3 Cache ─────────────────────────────────────────────────


def get_bundle_cache_dir() -> str:
    """Get the root cache directory for S3 bundle archives."""
    return os.path.join(get_cache_directory(), "job-bundles")


def _safe_cache_suffix(name: str) -> str:
    """Return a traversal-free, filesystem-safe cache subdir suffix, or "".

    The cache dir is always uniquely keyed by a hash; this readable suffix is
    only for human inspection. A crafted bundle name (e.g. ``"...ojd"`` ->
    ``".."``) must never be able to escape the per-bundle cache dir, so run it
    through ``sanitize_bundle_name`` and drop it entirely if it is unsafe.
    """
    try:
        return sanitize_bundle_name(_strip_archive_ext(name))
    except ValueError:
        return ""


def _cache_key(bucket: str, s3_key: str) -> str:
    """Deterministic cache subdirectory from bucket + key."""
    h = hashlib.sha256(f"{bucket}/{s3_key}".encode()).hexdigest()[:16]
    name = _safe_cache_suffix(s3_key.rstrip("/").rsplit("/", 1)[-1])
    return os.path.join(h, name) if name else h


def _local_cache_key(path: str) -> str:
    """Deterministic cache subdirectory from a local file path."""
    h = hashlib.sha256(os.path.abspath(path).encode()).hexdigest()[:16]
    name = _safe_cache_suffix(os.path.basename(path))
    return os.path.join(h, name) if name else h


def _read_cache_meta(cache_dir: str) -> Optional[dict]:
    meta_path = os.path.join(cache_dir, CACHE_META_FILENAME)
    if os.path.isfile(meta_path):
        try:
            with open(meta_path, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass  # Corrupt or unreadable cache meta — treat as cache miss
    return None


def _normalize_etag(etag: Optional[str]) -> str:
    """Strip surrounding quotes from an ETag for consistent comparison."""
    if not etag:
        return ""
    return etag.strip('"')


def _write_cache_meta(cache_dir: str, etag: str, last_modified: str) -> None:
    meta_path = os.path.join(cache_dir, CACHE_META_FILENAME)
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump({"etag": etag, "last_modified": last_modified}, f)


# ── S3 Repository ────────────────────────────────────────────


def _safe_int(value: Optional[str]) -> Optional[int]:
    """Parse a string as int, returning None on failure or empty/None input."""
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _bundle_info_from_s3_metadata(metadata: dict, path: str) -> Optional[BundleInfo]:
    """Try to construct BundleInfo from S3 user metadata set during upload.
    Returns None if the required 'ojd-name' key is missing.

    For bundles uploaded outside this tool the metadata is attacker-influenced,
    so every value is capped to the same PREVIEW_MAX_* limits used for templates.
    S3 already bounds total user metadata to ~2 KB, but we defend in depth and
    stay consistent with the template preview path so a crafted object can't
    bloat the preview.
    """
    name = metadata.get(METADATA_KEY_NAME)
    if not name:
        return None
    name = _decode_s3_value(name)[:PREVIEW_MAX_NAME_LEN]
    params = []
    params_str = _decode_s3_value(metadata.get(METADATA_KEY_PARAMS, ""))
    if params_str:
        for p in params_str.split(",")[:PREVIEW_MAX_PARAMS]:
            parts = p.split(":", 1)
            if len(parts) == 2:
                params.append(
                    {
                        "name": parts[0][:PREVIEW_MAX_PARAM_NAME_LEN],
                        "type": parts[1][:PREVIEW_MAX_PARAM_NAME_LEN],
                        "_from_metadata": True,
                    }
                )

    step_count_str = metadata.get(METADATA_KEY_STEP_COUNT)
    param_count_str = metadata.get(METADATA_KEY_PARAM_COUNT)

    step_names = [
        s[:PREVIEW_MAX_NAME_LEN]
        for s in _decode_s3_value(metadata.get(METADATA_KEY_STEPS, "")).split(",")
        if s
    ][:PREVIEW_MAX_STEPS]

    return BundleInfo(
        path=path,
        name=name,
        description=_decode_s3_value(metadata.get(METADATA_KEY_DESC, ""))[:PREVIEW_MAX_DESC_LEN],
        step_names=step_names,
        parameters=params,
        total_steps=_safe_int(step_count_str),
        total_parameters=_safe_int(param_count_str),
    )


def get_bundle_queue_cache_dir(bucket: str, prefix: str) -> str:
    """Per-queue folder inside the bundle cache (keyed by bucket + prefix).

    Holds this queue's local, per-user data such as ``.visibility.json``.
    """
    key = hashlib.sha256(f"{bucket}/{prefix.rstrip('/')}".encode()).hexdigest()[:16]
    return os.path.join(get_bundle_cache_dir(), key)


class _LocalBundleVisibility:
    """Per-user "hidden" view for a queue's shared bundles, stored locally.

    Hiding a bundle is a private view preference written to a ``.visibility.json``
    in this queue's cache folder. It never touches S3, so showing or hiding a
    bundle only changes this user's own listing, not anyone else's.
    """

    def __init__(self, bucket: str, prefix: str):
        self._bucket = bucket
        self._prefix = prefix

    def _view_path(self) -> str:
        return os.path.join(
            get_bundle_queue_cache_dir(self._bucket, self._prefix), VISIBILITY_FILENAME
        )

    def get_hidden_set(self) -> set[str]:
        """Read this user's hidden bundle keys for the queue (empty if none).

        Entries are keyed by the bundle's path relative to the queue's
        job-bundles prefix (``.ojd`` stripped). The file lives in the disposable
        bundle cache dir, so a stale/unreadable one is simply treated as nothing
        hidden.
        """
        try:
            with open(self._view_path(), encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, ValueError):
            # No file yet, unreadable, or malformed — treat as nothing hidden.
            return set()
        return set(data.get("hidden", []))

    def set_bundle_visibility(self, bundle_key: str, *, hidden: bool) -> None:
        """Hide or unhide a bundle (by prefix-relative key) in this user's local
        view (no S3 calls)."""
        hidden_set = self.get_hidden_set()
        if hidden:
            if bundle_key in hidden_set:
                return  # Already hidden
            hidden_set.add(bundle_key)
        else:
            if bundle_key not in hidden_set:
                return  # Already visible
            hidden_set.discard(bundle_key)

        path = self._view_path()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        body = json.dumps({"version": VISIBILITY_VERSION, "hidden": sorted(hidden_set)}, indent=2)
        # Atomic write so a crash can't corrupt the view file.
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(body)
        os.replace(tmp, path)


def _make_s3_client(session):
    """Create an S3 client for a bundle repository.

    Sizes the connection pool to cover the parallel ``head_object`` calls the
    background preview prefetch issues (and the managed upload/download
    transfers) so urllib3 does not log "Connection pool is full".
    """
    from ..api._session import get_default_client_config

    try:
        configured = int(config_file.get_setting("settings.s3_max_pool_connections"))
    except (ValueError, TypeError):
        configured = 0
    max_pool = max(configured, PREVIEW_PREFETCH_MAX_WORKERS)
    return session.client("s3", config=get_default_client_config(max_pool_connections=max_pool))


class S3BundleRepository:
    """Browse .ojd job bundles in an S3 bucket under {rootPrefix}/job-bundles/.
    Only .ojd archives are supported. Subfolders are shown for navigation only.
    Archive bundles are cached locally with ETag validation."""

    def __init__(self, bucket_name: str, root_prefix: str, session=None):
        import boto3 as _boto3

        self._bucket = bucket_name
        base = root_prefix.rstrip("/")
        self._prefix = f"{base}/{S3_JOB_BUNDLES_PREFIX}/"
        self._session = session or _boto3.Session()
        self._s3 = _make_s3_client(self._session)
        self._last_head: Optional[tuple[str, dict]] = None
        # Preview prefetch cache of head_object responses keyed by S3 key. Warmed
        # in the background by prefetch_previews() after the listing is shown, and
        # consulted by the preview/size/download paths to avoid a second HEAD.
        self._head_cache: dict[str, dict] = {}

    @classmethod
    def from_config(cls, config=None) -> "S3BundleRepository":
        """Create an S3BundleRepository from the user's Deadline Cloud configuration.

        Handles session creation, queue lookup, and attachment settings extraction.
        Uses queue role credentials for S3 access (required for DCM profiles).
        Raises DeadlineOperationError if farm/queue is not configured or has no attachments.
        """
        from concurrent.futures import ThreadPoolExecutor, Future

        from ..api import get_boto3_client, get_boto3_session, get_queue_user_boto3_session

        farm_id = config_file.get_setting("defaults.farm_id", config=config)
        queue_id = config_file.get_setting("defaults.queue_id", config=config)
        if not farm_id or not queue_id:
            raise DeadlineOperationError("A default farm and queue must be configured.")

        # Ensure session is cached (used internally by get_boto3_client and get_queue_user_boto3_session)
        get_boto3_session(config=config)

        # Create one Deadline client — reused for both get_queue and AssumeQueueRole
        deadline_client = get_boto3_client("deadline", config=config)

        # Set up queue role session (just wires up credential provider, no API call)
        s3_session = get_queue_user_boto3_session(
            deadline=deadline_client,
            config=config,
            farm_id=farm_id,
            queue_id=queue_id,
            queue_display_name=None,
        )

        # Run get_queue API call and S3 client creation in parallel.
        # get_queue gives us the bucket name; S3 client creation triggers AssumeRole.
        def _call_get_queue():
            return deadline_client.get_queue(farmId=farm_id, queueId=queue_id)

        with ThreadPoolExecutor(max_workers=2) as executor:
            queue_future: Future = executor.submit(_call_get_queue)
            s3_client_future: Future = executor.submit(_make_s3_client, s3_session)

            s3_client = s3_client_future.result()
            queue_response = queue_future.result()

        # Extract job attachment settings
        ja_settings = queue_response.get("jobAttachmentSettings")
        if not ja_settings or not ja_settings.get("s3BucketName"):
            raise DeadlineOperationError(
                f"Queue {queue_id} does not have job attachment settings configured."
            )

        repo = cls.__new__(cls)
        base = ja_settings["rootPrefix"].rstrip("/")
        repo._bucket = ja_settings["s3BucketName"]
        repo._prefix = f"{base}/{S3_JOB_BUNDLES_PREFIX}/"
        repo._session = s3_session
        repo._s3 = s3_client
        repo._last_head = None
        repo._head_cache = {}
        return repo

    def root_path(self) -> str:
        return f"s3://{self._bucket}/{self._prefix}"

    def list_entries(self, path: str) -> list[BrowseEntry]:
        prefix = self._to_s3_prefix(path)
        entries: list[BrowseEntry] = []
        child_prefixes: list[tuple[str, str, str]] = []  # (name, child_prefix, child_path)
        try:
            paginator = self._s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix, Delimiter="/"):
                # Subfolders (for navigation only, not bundles)
                for cp in page.get("CommonPrefixes", []):
                    child_prefix = cp["Prefix"]
                    name = child_prefix.rstrip("/").rsplit("/", 1)[-1]
                    child_path = f"s3://{self._bucket}/{child_prefix}"
                    child_prefixes.append((name, child_prefix, child_path))
                # .ojd archive bundles
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    name = key.rsplit("/", 1)[-1] if "/" in key else key
                    if _is_archive(name):
                        s3_path = f"s3://{self._bucket}/{key}"
                        entries.append(
                            BrowseEntry(
                                name=_strip_archive_ext(name),
                                path=s3_path,
                                is_bundle=True,
                                is_archive=True,
                            )
                        )
        except Exception:
            logger.warning("Failed to list S3 prefix %s", prefix, exc_info=True)
            raise

        # Subfolders are shown for navigation but never as bundles
        for name, child_prefix, child_path in child_prefixes:
            entries.append(BrowseEntry(name=name, path=child_path, is_bundle=False))

        entries.sort(key=lambda e: e.name.lower())
        return entries

    def get_bundle_info(self, path: str) -> Optional[BundleInfo]:
        return self._get_archive_bundle_info(path)

    def download_full_bundle(
        self,
        path: str,
        progress_callback=None,
        extract_callback=None,
        extract_size_callback=None,
    ) -> str:
        """Download and extract a complete S3 .ojd bundle into the local cache,
        returning the local directory path. Uses the ETag cache for repeated
        access. Callers that want the bundle at a specific location copy it from
        the returned cache path."""
        return self._resolve_archive_bundle(
            path,
            progress_callback=progress_callback,
            extract_callback=extract_callback,
            extract_size_callback=extract_size_callback,
        )

    def prefetch_previews(self, should_cancel: Optional[Callable[[], bool]] = None) -> None:
        """Warm the preview cache by issuing ``head_object`` for every ``.ojd``
        object under the prefix, in parallel.

        Each response (ETag + user metadata + ContentLength) is stashed in
        ``_head_cache`` so the preview/size/download paths can reuse it instead of
        issuing their own HEAD — making previews effectively instant once warmed.

        This is a background optimization and is safe to call off the UI thread
        after the listing is displayed; it never blocks the initial listing.
        Individual HEAD failures are ignored. The cache is rebuilt from scratch,
        dropping entries for bundles that no longer exist.

        ``should_cancel`` is an optional predicate polled per key so a caller
        tearing down (e.g. the browser dialog closing) doesn't have to block on
        every outstanding HEAD.
        """
        keys = self._list_all_bundle_keys()
        self._head_cache.clear()
        if not keys:
            return

        def _head(key: str) -> None:
            if should_cancel is not None and should_cancel():
                return
            try:
                # dict setitem is atomic under the GIL, so this is safe to do
                # from the worker threads while the UI reads the cache.
                self._head_cache[key] = self._s3.head_object(Bucket=self._bucket, Key=key)
            except Exception:
                logger.debug("prefetch head_object failed for %s", key, exc_info=True)

        max_workers = min(PREVIEW_PREFETCH_MAX_WORKERS, len(keys))
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            list(ex.map(_head, keys))

    def _list_all_bundle_keys(self) -> list[str]:
        """List every ``.ojd`` object key under the prefix, recursively."""
        keys: list[str] = []
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=self._prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(ARCHIVE_EXTENSION):
                    keys.append(key)
        return keys

    def _head_object(self, key: str, use_cache: bool = True) -> dict:
        """Return head metadata for ``key``.

        ``use_cache`` controls whether the background prefetch cache
        (``_head_cache``) may satisfy the request. The cache is warmed once when
        the listing appears and is never invalidated, so it must only be used for
        *preview* (where a slightly stale ETag/size is harmless). Correctness
        paths — download resolution and size — pass ``use_cache=False`` to force a
        live ``head_object``; otherwise a bundle overwritten on the queue while
        the dialog is open would still match the local cache's ETag and serve the
        old, no-longer-existing contents. Raises on a genuine HEAD failure —
        callers that tolerate failure wrap this in try/except.
        """
        if use_cache:
            cached = self._head_cache.get(key)
            if cached is not None:
                return cached
        return self._s3.head_object(Bucket=self._bucket, Key=key)

    def get_bundle_size(self, path: str) -> int:
        """Get the size in bytes of a bundle archive on S3.
        Caches the result so a subsequent download_full_bundle doesn't repeat the call."""
        key = self._to_s3_key(path)
        head = self._head_object(key, use_cache=False)
        self._last_head = (key, head)
        return head.get("ContentLength", 0)

    def bundle_exists(self, bundle_name: str) -> bool:
        """Check if a bundle with the given name exists on S3.

        Only a 404 means "does not exist". Any other HEAD failure (throttling,
        network error, expired credentials, or an AccessDenied on a
        least-privilege queue role) is re-raised rather than reported as
        "absent" — otherwise the overwrite guard in the GUI would fail open and
        silently clobber another user's shared bundle.
        """
        key = f"{self._prefix}{bundle_name}.ojd"
        try:
            self._s3.head_object(Bucket=self._bucket, Key=key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise

    def upload_archive(
        self,
        buf: io.BytesIO,
        bundle_name: str,
        metadata: Optional[dict[str, str]] = None,
        progress_callback=None,
    ) -> str:
        """Upload an in-memory archive buffer to S3 as an .ojd bundle.

        Returns the S3 URI of the uploaded bundle.
        """
        key = f"{self._prefix}{bundle_name}.ojd"
        # Advertise the archive's true type. This is a courtesy hint for other
        # consumers/tools; the download path deliberately does NOT trust it (an
        # object's ContentType is set by whoever uploaded it), and validates by
        # actually parsing the zip. See _extract_archive_from_fileobj.
        extra_args: dict = {"ContentType": "application/zip"}
        if metadata:
            extra_args["Metadata"] = metadata
        kwargs: dict = {
            "Bucket": self._bucket,
            "Key": key,
            "ExtraArgs": extra_args,
        }
        if progress_callback:
            kwargs["Callback"] = progress_callback
        self._s3.upload_fileobj(buf, **kwargs)
        return f"s3://{self._bucket}/{key}"

    def clear_cache_for(self, path: str) -> None:
        """Remove cached data for a specific bundle path."""
        key = self._to_s3_key(path)
        cache_dir = os.path.join(get_bundle_cache_dir(), _cache_key(self._bucket, key))
        if os.path.exists(cache_dir):
            shutil.rmtree(cache_dir, ignore_errors=True)

    # ── Archive bundles ──────────────────────────────────────

    def _get_archive_bundle_info(self, path: str) -> Optional[BundleInfo]:
        key = self._to_s3_key(path)
        cache_dir = os.path.join(get_bundle_cache_dir(), _cache_key(self._bucket, key))
        meta = _read_cache_meta(cache_dir)

        # Always do a head_object first — it's cheap and gives us both
        # ETag (for cache validation) and user metadata (for preview without download).
        # Reuse the background prefetch cache if it already warmed this object.
        head = None
        try:
            head = self._head_object(key)
        except Exception:
            pass  # head_object failure is non-fatal; we fall through to download

        # The archive/download size, reused for the preview (shown on the Download
        # button) without an extra call. May be refined from the GET below if the
        # HEAD failed.
        content_length = head.get("ContentLength") if head else None

        def _stamp(info: Optional[BundleInfo]) -> Optional[BundleInfo]:
            if info is not None and content_length is not None:
                info.size_bytes = content_length
            return info

        if head:
            # Check local cache validity
            cache_valid = meta and _normalize_etag(head.get("ETag")) == _normalize_etag(
                meta.get("etag")
            )

            # Prefer cached template (source of truth) over S3 metadata (fast hint)
            if cache_valid:
                cached_info = self._read_info_from_cache(cache_dir, path)
                if cached_info:
                    return _stamp(cached_info)

            # No valid cache — use S3 user metadata as a fast preview hint
            # (avoids downloading the archive just for preview)
            s3_metadata = head.get("Metadata", {})
            info = _bundle_info_from_s3_metadata(s3_metadata, path)
            if info:
                return _stamp(info)

        # Cache miss or stale — download, cache, and parse. Stream into a
        # memory/temp-file sink so a large archive is never held whole in RAM.
        buf = _open_download_sink(head.get("ContentLength", 0) if head else 0)
        try:
            try:
                resp = self._s3.get_object(Bucket=self._bucket, Key=key)
                shutil.copyfileobj(resp["Body"], buf)
                etag = resp.get("ETag", "")
                last_modified = str(resp.get("LastModified", ""))
                # Refine the size if the earlier HEAD was unavailable.
                if content_length is None:
                    content_length = resp.get("ContentLength")
            except Exception:
                logger.debug("Failed to download S3 archive %s", key, exc_info=True)
                return None

            # Extract to cache so resolve_bundle can reuse it
            if os.path.exists(cache_dir):
                shutil.rmtree(cache_dir)
            os.makedirs(cache_dir, exist_ok=True)
            try:
                _extract_archive_from_fileobj(buf, cache_dir)
                _write_cache_meta(cache_dir, etag, last_modified)
                return _stamp(self._read_info_from_cache(cache_dir, path))
            except Exception:
                logger.debug("Failed to cache S3 archive %s", key, exc_info=True)

            # Caching failed — still try to read the template for a preview.
            try:
                buf.seek(0)
                with zipfile.ZipFile(buf, "r") as zf:
                    result = _read_template_from_zip(zf)
            except Exception:
                return None
            if result:
                raw, fname = result
                template = _parse_template(raw, fname)
                if template:
                    return _stamp(extract_bundle_info(template, path))
            return None
        finally:
            buf.close()

    def _resolve_archive_bundle(
        self, path: str, progress_callback=None, extract_callback=None, extract_size_callback=None
    ) -> str:
        key = self._to_s3_key(path)
        cache_dir = os.path.join(get_bundle_cache_dir(), _cache_key(self._bucket, key))

        # Single head_object for both cache validation and metadata. Reuse a prior
        # head from the get_bundle_size hand-off (_last_head) or the prefetch cache.
        head = None
        if self._last_head is not None and self._last_head[0] == key:
            head = self._last_head[1]
            self._last_head = None
        else:
            try:
                head = self._head_object(key, use_cache=False)
            except Exception:
                pass  # head_object failure is non-fatal; proceeds without cache validation

        # Check if cache is valid
        meta = _read_cache_meta(cache_dir)
        if meta and head:
            if _normalize_etag(head.get("ETag")) == _normalize_etag(meta.get("etag")):
                bundle_path = self._find_bundle_in_cache(cache_dir)
                if bundle_path:
                    logger.info("Using cached bundle: %s", bundle_path)
                    return bundle_path

        # Download, extract, and cache
        etag = head.get("ETag", "") if head else ""
        last_modified = str(head.get("LastModified", "")) if head else ""

        # Stream small archives through memory and large ones through a temp file
        # in the cache dir, so a large bundle is never held whole (in several
        # copies) in RAM.
        buf = _open_download_sink(head.get("ContentLength", 0) if head else 0)
        try:
            download_kwargs: dict = {"Bucket": self._bucket, "Key": key}
            if progress_callback:
                download_kwargs["Callback"] = progress_callback
            self._s3.download_fileobj(Fileobj=buf, **download_kwargs)

            # Clear old cache and extract
            if os.path.exists(cache_dir):
                shutil.rmtree(cache_dir)
            os.makedirs(cache_dir, exist_ok=True)

            _extract_archive_from_fileobj(
                buf,
                cache_dir,
                progress_callback=extract_callback,
                size_callback=extract_size_callback,
            )
            _write_cache_meta(cache_dir, etag, last_modified)
        finally:
            buf.close()

        bundle_path = self._find_bundle_in_cache(cache_dir)
        if bundle_path:
            return bundle_path
        return cache_dir

    def _read_info_from_cache(self, cache_dir: str, original_path: str) -> Optional[BundleInfo]:
        """Read bundle info from an already-extracted cache directory."""
        bundle_dir = self._find_bundle_in_cache(cache_dir)
        if not bundle_dir:
            return None
        for fname in TEMPLATE_FILENAMES:
            fpath = os.path.join(bundle_dir, fname)
            if os.path.isfile(fpath):
                try:
                    with open(fpath, encoding="utf-8") as f:
                        raw = f.read()
                except OSError:
                    return None
                template = _parse_template(raw, fname)
                if template:
                    pv = LocalBundleRepository.read_parameter_values(bundle_dir)
                    return extract_bundle_info(template, original_path, pv)
        return None

    @staticmethod
    def _find_bundle_in_cache(cache_dir: str) -> Optional[str]:
        """Find the actual bundle directory within a cache dir.
        Handles both flat extraction and single-directory-wrapped archives."""
        # Check if template is directly in cache_dir
        for fname in TEMPLATE_FILENAMES:
            if os.path.isfile(os.path.join(cache_dir, fname)):
                return cache_dir
        # Check one level deep (single wrapper directory)
        try:
            contents = [
                d
                for d in os.listdir(cache_dir)
                if os.path.isdir(os.path.join(cache_dir, d)) and d != CACHE_META_FILENAME
            ]
        except OSError:
            return None
        for d in contents:
            subdir = os.path.join(cache_dir, d)
            for fname in TEMPLATE_FILENAMES:
                if os.path.isfile(os.path.join(subdir, fname)):
                    return subdir
        return None

    # ── Helpers ──────────────────────────────────────────────

    def _to_s3_key(self, path: str) -> str:
        """Convert an s3:// URI to a raw S3 key."""
        if path.startswith("s3://"):
            _, _, key = path.partition(f"s3://{self._bucket}/")
            return key
        return path

    def _to_s3_prefix(self, path: str) -> str:
        """Convert an s3:// URI or prefix to a raw S3 prefix ending with /."""
        key = self._to_s3_key(path)
        return key if key.endswith("/") else key + "/"

    # ── Visibility (local, per-user) ─────────────────────────

    def _visibility(self) -> "_LocalBundleVisibility":
        """Per-user local view for this queue's bundles (keyed by bucket + prefix)."""
        return _LocalBundleVisibility(self._bucket, self._prefix)

    def visibility_key(self, path: str) -> str:
        """Canonical key for the hidden-view file: the bundle's path relative to
        the queue's job-bundles prefix, with the ``.ojd`` extension stripped.

        Keying by the relative path (e.g. ``maya/render``) rather than the bare
        leaf name (``render``) keeps same-named bundles in different subfolders
        distinct, so hiding one doesn't collaterally hide the others.
        """
        key = self._to_s3_key(path)
        if key.startswith(self._prefix):
            key = key[len(self._prefix) :]
        return _strip_archive_ext(key)

    def get_hidden_set(self) -> set[str]:
        """Fetch this user's locally hidden bundle keys for the queue."""
        return self._visibility().get_hidden_set()

    def set_bundle_visibility(self, bundle_key: str, *, hidden: bool) -> None:
        """Hide or unhide a bundle (by prefix-relative key from ``visibility_key``)
        in this user's local view (no S3 calls)."""
        self._visibility().set_bundle_visibility(bundle_key, hidden=hidden)
