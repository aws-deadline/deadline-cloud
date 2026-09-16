# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import builtins
import json
import os
import pytest
from pathlib import Path

import deadline.client.ui._utils as utils


@pytest.fixture(autouse=True)
def clear_translation_cache():
    utils._get_translations.cache_clear()
    yield


@pytest.mark.skip(reason="Skip until we've added translations")
def test_japanese_translation_loading():
    # Force Japanese locale
    os.environ["LANG"] = "ja_JP.UTF-8"

    # Test a known translation
    result = utils.tr("Submit to AWS Deadline Cloud")
    assert result == "AWS Deadline Cloudに送信", f"Expected Japanese translation, got: {result}"


def test_all_locales_have_same_keys():
    translations_dir = (
        Path(__file__).parent.parent.parent.parent.parent
        / "src"
        / "deadline"
        / "client"
        / "ui"
        / "translations"
        / "locales"
    )

    # Load English as reference
    with open(translations_dir / "en_US.json", encoding="utf-8") as f:
        en_keys = set(json.load(f).keys())

    # Check all other locale files
    for locale_file in translations_dir.glob("*.json"):
        if locale_file.name == "en_US.json":
            continue

        with open(locale_file, encoding="utf-8") as f:
            locale_keys = set(json.load(f).keys())

        missing = en_keys - locale_keys
        extra = locale_keys - en_keys

        assert not missing, f"{locale_file.name} missing keys: {missing}"
        assert not extra, f"{locale_file.name} has extra keys: {extra}"


@pytest.mark.parametrize("locale_name", ["en_US", "ja_JP", "zh_CN"])
def test_translations_load_under_a_non_utf8_platform_encoding(monkeypatch, locale_name):
    """
    The catalogs are UTF-8, but open() without an explicit encoding uses the platform default,
    which is cp1252 on Windows. Reading them that way either raises UnicodeDecodeError (CJK) or
    silently yields mojibake (the en_US U+2022 bullets), so every lookup misses and tr() falls
    back to the raw key. Force cp1252 to reproduce a Windows console without a UTF-8 codepage.
    """
    real_open = builtins.open

    def cp1252_open(file, *args, **kwargs):
        if "encoding" not in kwargs and "b" not in kwargs.get("mode", args[0] if args else "r"):
            kwargs["encoding"] = "cp1252"
        return real_open(file, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", cp1252_open)
    monkeypatch.setattr(utils.config_file, "get_setting", lambda _setting: locale_name)

    translations = utils._get_translations()

    assert translations, f"{locale_name}.json failed to load under a cp1252 default encoding"
    # cp1252 decodes the UTF-8 bullet bytes without raising, so en_US loads with mangled
    # keys rather than coming back empty. Assert the bullet survived, not just that keys exist.
    assert any("•" in key for key in translations), (
        f"{locale_name}.json decoded to mojibake instead of UTF-8"
    )


def test_type_hints_generated():
    """Ensure type hints file is generated from translations."""
    type_file = (
        Path(__file__).parent.parent.parent.parent.parent
        / "src"
        / "deadline"
        / "client"
        / "ui"
        / "_translation_keys.py"
    )

    assert type_file.exists(), "Type hints file not generated"

    content = type_file.read_text()
    assert "TranslationKey" in content
    assert "Literal[" in content
