# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Strips markdown syntax from click help text for clean terminal rendering.

mkdocs-click reads the raw docstrings as markdown for the docs site,
so we only transform text at display time in the terminal.
"""

import re


def strip_markdown_for_terminal(text: str) -> str:
    """
    Transform markdown syntax into plain text suitable for terminal display.

    Handles:
    - Inline links: [text](url) -> text (url)
    - Reference links: [text][ref] or [text] -> text
    - Reference definitions: [ref]: url -> (removed)
    - Bold: **text** -> text
    - Italic: *text* -> text (but not list markers)
    """
    # Remove reference-style link definitions (lines like "[text]: url")
    text = re.sub(r"^\s*\[([^\]]+)\]:\s*\S+.*$", "", text, flags=re.MULTILINE)

    # Convert inline links [text](url) -> text (url)
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1 (\2)", text)

    # Convert reference links [text][ref] -> text
    text = re.sub(r"\[([^\]]+)\]\[[^\]]*\]", r"\1", text)

    # Convert shorthand reference links [text] -> text
    # (where [text] matches a removed definition above)
    # Must come after inline/reference link handling to avoid false matches
    text = re.sub(r"\[([^\]]+)\](?!\()", r"\1", text)

    # Strip bold **text** or __text__
    text = re.sub(r"\*\*([^*]+)\*\*", r"\1", text)
    text = re.sub(r"__([^_]+)__", r"\1", text)

    # Strip italic *text* or _text_ but not list markers (line-start *)
    text = re.sub(r"(?<!\w)\*([^*\n]+)\*(?!\w)", r"\1", text)

    # Clean up any blank lines left from removed reference definitions
    text = re.sub(r"\n{3,}", "\n\n", text)

    return text.strip()
