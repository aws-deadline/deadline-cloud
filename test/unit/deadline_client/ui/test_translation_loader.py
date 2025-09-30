# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the translation loader functionality.
"""

import os
import pytest


class TestTranslationLoader:
    """Tests for translation loading."""

    def test_load_translations_with_qt_available(self):
        """Test translation loading when Qt is available."""
        pytest.importorskip("qtpy")
        
        # Set up Qt environment
        os.environ["QT_API"] = "pyside6"
        
        from qtpy.QtWidgets import QApplication, QLabel
        from deadline.client.ui.translation_loader import load_translations
        
        app = QApplication([])
        
        # Should not raise an exception
        load_translations(app, "en")
        
        # Test that tr() works
        label = QLabel()
        text = label.tr("Submit to AWS Deadline Cloud")
        assert text == "Submit to AWS Deadline Cloud"

    def test_load_translations_fallback_to_english(self):
        """Test that non-existent locales fall back to English."""
        pytest.importorskip("qtpy")
        
        os.environ["QT_API"] = "pyside6"
        
        from qtpy.QtWidgets import QApplication
        from deadline.client.ui.translation_loader import load_translations
        
        app = QApplication([])
        
        # Should not raise an exception even with non-existent locale
        load_translations(app, "nonexistent_locale")
