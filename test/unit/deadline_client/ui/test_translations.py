# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

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
    with open(translations_dir / "en_US.json") as f:
        en_keys = set(json.load(f).keys())

    # Check all other locale files
    for locale_file in translations_dir.glob("*.json"):
        if locale_file.name == "en_US.json":
            continue

        with open(locale_file) as f:
            locale_keys = set(json.load(f).keys())

        missing = en_keys - locale_keys
        extra = locale_keys - en_keys

        assert not missing, f"{locale_file.name} missing keys: {missing}"
        assert not extra, f"{locale_file.name} has extra keys: {extra}"


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
