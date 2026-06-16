# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Best-effort detection of an AI agent / harness invoking the CLI.

Kept in its own dependency-free module (imports only the standard library) so
that both ``_session`` and ``_telemetry`` can import it at module level without
creating an import cycle between them.

Detection follows the convention implemented by unjs/std-env (the de-facto
reference) and the AGENT proposal tracked in agentsmd/agents.md#136. No
personally-identifiable information is read - only the presence/value of
well-known agent marker variables.
"""

import os
import re
from typing import Optional

# Agents that set a marker env var whose mere presence identifies them. Each
# entry maps a detection env var to the canonical agent name we report. Checked
# in order; the first match wins.
_AGENT_ENV_MARKERS: tuple[tuple[str, str], ...] = (
    ("CLAUDECODE", "claude-code"),
    ("CLAUDE_CODE", "claude-code"),
    ("CODEX_SANDBOX", "codex"),
    ("CODEX_THREAD_ID", "codex"),
    ("CURSOR_AGENT", "cursor"),
    ("REPL_ID", "replit"),
    ("GEMINI_CLI", "gemini"),
    ("OPENCODE", "opencode"),
    ("AUGMENT_AGENT", "auggie"),
    ("GOOSE_PROVIDER", "goose"),
)

# Agents detected by matching a substring within the value of an env var
# (rather than presence alone) - typically IDE/editor integrations. Checked
# after the presence markers above so a more specific agent running inside the
# IDE is detected first.
_AGENT_ENV_VALUE_MARKERS: tuple[tuple[str, str, str], ...] = (
    ("EDITOR", "devin", "devin"),
    ("TERM_PROGRAM", "kiro", "kiro"),
)

# Maximum length of a sanitized agent name. The value flows into the HTTP
# User-Agent header and telemetry payloads, so cap it to keep those bounded.
_MAX_AGENT_NAME_LENGTH = 64

# Characters permitted in an agent name. Anything else is stripped so a
# malformed env value (spaces, slashes, control characters, newlines) can't
# corrupt the User-Agent header token boundary or the telemetry payload.
_DISALLOWED_AGENT_NAME_CHARS = re.compile(r"[^A-Za-z0-9._-]")


def _sanitize_agent_name(value: str) -> Optional[str]:
    """Constrains a free-form agent name to a safe charset and length.

    Returns the cleaned name, or ``None`` if nothing usable remains. Used only
    for the user-controlled ``AI_AGENT``/``AGENT`` override values; the built-in
    marker tables already return fixed, known-safe names.
    """
    cleaned = _DISALLOWED_AGENT_NAME_CHARS.sub("", value.strip())
    cleaned = cleaned[:_MAX_AGENT_NAME_LENGTH]
    return cleaned.lower() or None


def detect_invoking_agent() -> Optional[str]:
    """Best-effort detection of an AI agent / harness invoking the CLI.

    Returns the canonical agent name (e.g. ``"claude-code"``) if a known agent
    environment is detected, otherwise ``None`` (treated as a human / direct
    invocation). Detection is based purely on environment variables that agents
    set; no personally-identifiable information is collected.
    """
    # ``AGENT`` (proposed standard, agents.md#136) and ``AI_AGENT`` (Vercel
    # convention) carry the agent name directly and take priority. claude-code
    # sets a "<name>_<version>_agent" form, so take the leading token to avoid
    # recording a version-specific value. These values are user-controlled, so
    # sanitize before returning.
    for override_var in ("AI_AGENT", "AGENT"):
        value = os.environ.get(override_var)
        if value and value not in ("1", "true"):
            sanitized = _sanitize_agent_name(value.split("_", 1)[0])
            if sanitized:
                return sanitized

    for env_var, agent_name in _AGENT_ENV_MARKERS:
        if os.environ.get(env_var):
            return agent_name

    for env_var, needle, agent_name in _AGENT_ENV_VALUE_MARKERS:
        value = os.environ.get(env_var)
        if value and needle in value.lower():
            return agent_name

    # Generic AGENT=1/true (proposed standard) with no parseable name.
    if os.environ.get("AGENT") in ("1", "true"):
        return "unknown"

    return None
