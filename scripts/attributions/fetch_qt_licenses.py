#!/usr/bin/env python3
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
Fetch Qt third-party license texts from doc.qt.io.

Usage:
    python fetch_qt_licenses.py --output qt_licenses.txt
    python fetch_qt_licenses.py --list-modules  # Show available modules
"""

import argparse
import re
import sys
import time
from html.parser import HTMLParser
from pathlib import Path
from typing import Optional
from urllib.request import urlopen
from urllib.error import HTTPError

BASE_URL = "https://doc.qt.io/qt-6.8"

# Qt modules bundled with PySide6 that we use in this project.
# Qt Widgets has no third-party components (pure Qt code).
MODULES = ["Qt Core", "Qt D-Bus", "Qt GUI", "Qt Network", "Qt SVG", "Qt Wayland Compositor"]


class QtLicensePageParser(HTMLParser):
    """Parse the main licenses-used-in-qt.html page to extract module sections and links."""

    def __init__(self):
        super().__init__()
        self.modules: dict[str, list[tuple[str, str]]] = {}  # module -> [(name, url), ...]
        self._current_module: Optional[str] = None
        self._in_h2 = False
        self._in_link = False
        self._current_link_href: Optional[str] = None
        self._current_link_text = ""

    def handle_starttag(self, tag, attrs):
        if tag == "h2":
            self._in_h2 = True
        elif tag == "a":
            attrs_dict = dict(attrs)
            href = attrs_dict.get("href", "")
            if href.endswith(".html") and "attribution" in href:
                self._in_link = True
                self._current_link_href = href
                self._current_link_text = ""

    def handle_endtag(self, tag):
        if tag == "h2":
            self._in_h2 = False
        elif tag == "a" and self._in_link:
            self._in_link = False
            if self._current_module and self._current_link_href:
                if self._current_module not in self.modules:
                    self.modules[self._current_module] = []
                self.modules[self._current_module].append(
                    (self._current_link_text.strip(), self._current_link_href)
                )
            self._current_link_href = None
            self._current_link_text = ""

    def handle_data(self, data):
        if self._in_h2:
            self._current_module = data.strip()
        elif self._in_link:
            self._current_link_text += data


class LicenseTextParser(HTMLParser):
    """Parse individual license pages to extract the license text."""

    def __init__(self):
        super().__init__()
        self.title = ""
        self.description = ""
        self.license_texts: list[str] = []
        self._in_h1 = False
        self._in_pre = False
        self._in_descr = False
        self._in_p = False
        self._descr_parts: list[str] = []
        self._current_pre = ""
        self._descr_depth = 0

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        if tag == "h1":
            self._in_h1 = True
        elif tag == "div":
            if "descr" in attrs_dict.get("class", ""):
                self._in_descr = True
                self._descr_depth = 1
            elif self._in_descr:
                self._descr_depth += 1
        elif tag == "pre" and self._in_descr:
            self._in_pre = True
            self._current_pre = ""
        elif tag == "p" and self._in_descr:
            self._in_p = True

    def handle_endtag(self, tag):
        if tag == "h1":
            self._in_h1 = False
        elif tag == "div" and self._in_descr:
            self._descr_depth -= 1
            if self._descr_depth == 0:
                self._in_descr = False
        elif tag == "pre":
            if self._in_pre and self._current_pre.strip():
                self.license_texts.append(self._current_pre.strip())
            self._in_pre = False
            self._current_pre = ""
        elif tag == "p":
            if self._in_p and self._descr_parts:
                self.description += " ".join(self._descr_parts) + "\n"
                self._descr_parts = []
            self._in_p = False

    def handle_data(self, data):
        if self._in_h1 and not self.title:
            self.title = data.strip()
        elif self._in_pre:
            self._current_pre += data
        elif self._in_p and self._in_descr:
            self._descr_parts.append(data.strip())

    def get_license_text(self) -> str:
        """Return the longest pre block as the license text."""
        if not self.license_texts:
            return ""
        # The actual license is usually the longest block
        return max(self.license_texts, key=len)


def fetch_page(url: str) -> str:
    """Fetch a page with basic rate limiting."""
    time.sleep(0.2)  # Be nice to Qt's servers
    with urlopen(url, timeout=30) as response:
        return response.read().decode("utf-8")


def get_available_modules() -> dict[str, list[tuple[str, str]]]:
    """Fetch and parse the main licenses page to get all modules and their components."""
    url = f"{BASE_URL}/licenses-used-in-qt.html"
    html = fetch_page(url)
    parser = QtLicensePageParser()
    parser.feed(html)
    return parser.modules


def fetch_license_text(relative_url: str) -> tuple[str, str, str]:
    """Fetch the license text from an individual component page."""
    url = f"{BASE_URL}/{relative_url}"
    try:
        html = fetch_page(url)
    except HTTPError as e:
        return "", "", f"Failed to fetch {url}: {e}"

    parser = LicenseTextParser()
    parser.feed(html)

    # Clean up the text
    text = parser.get_license_text()
    # Remove excessive blank lines
    text = re.sub(r"\n{3,}", "\n\n", text)
    desc = parser.description.strip()

    return parser.title, desc, text


def format_license_entry(name: str, description: str, text: str) -> str:
    """Format a single license entry for the attributions file."""
    separator = "-" * 74
    parts = [name, ""]
    if description:
        parts.append(description)
        parts.append("")
    parts.append(text)
    parts.append("")
    parts.append(separator)
    parts.append("")
    return "\n".join(parts)


def main():
    parser = argparse.ArgumentParser(description="Fetch Qt third-party licenses")
    parser.add_argument(
        "--list-modules",
        action="store_true",
        help="List available Qt modules and exit",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Output file path",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be fetched without fetching",
    )
    args = parser.parse_args()

    print("Fetching Qt module list...", file=sys.stderr)
    modules = get_available_modules()

    if args.list_modules:
        print("Available Qt modules:")
        for module, components in sorted(modules.items()):
            print(f"  {module} ({len(components)} components)")
        return

    # Validate hardcoded modules
    for module in MODULES:
        if module not in modules:
            print(f"Error: Unknown module '{module}'", file=sys.stderr)
            print(f"Available: {', '.join(sorted(modules.keys()))}", file=sys.stderr)
            sys.exit(1)

    if args.dry_run:
        print("Would fetch licenses for:")
        for module in MODULES:
            print(f"\n{module}:")
            for name, url in modules[module]:
                print(f"  - {name}")
        return

    # Fetch all licenses
    output_parts = []
    output_parts.append("Qt Third-Party Licenses\n")
    output_parts.append("=" * 74 + "\n\n")
    output_parts.append(
        "The following third-party components are included in Qt modules used by this software.\n"
    )
    output_parts.append(f"Source: {BASE_URL}/licenses-used-in-qt.html\n\n")

    for module in MODULES:
        print(f"Processing {module}...", file=sys.stderr)
        output_parts.append(f"\n{'=' * 74}\n")
        output_parts.append(f"{module}\n")
        output_parts.append(f"{'=' * 74}\n\n")

        for name, url in modules[module]:
            print(f"  Fetching {name}...", file=sys.stderr)
            title, desc, text = fetch_license_text(url)
            if text:
                output_parts.append(format_license_entry(title or name, desc, text))
            else:
                print(f"    Warning: No license text found for {name}", file=sys.stderr)

    result = "".join(output_parts)

    if args.output:
        args.output.write_text(result, encoding="utf-8")
        print(f"Written to {args.output}", file=sys.stderr)
    else:
        print(result)


if __name__ == "__main__":
    main()
