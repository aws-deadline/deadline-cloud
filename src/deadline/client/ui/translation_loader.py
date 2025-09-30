"""Translation loader for deadline GUI."""

import os
from pathlib import Path
from qtpy.QtCore import QTranslator, QLocale
from qtpy.QtWidgets import QApplication


def load_translations(app: QApplication, locale: str = None) -> None:
    """Load translation files for the application.
    
    Args:
        app: The QApplication instance
        locale: Locale string (e.g., 'en_US'). If None, uses system locale.
    """
    if locale is None:
        locale = QLocale.system().name()
    
    translations_dir = Path(__file__).parent / "translations"
    
    translator = QTranslator()
    translation_file = translations_dir / f"deadline_{locale}.qm"
    
    if translation_file.exists():
        if translator.load(str(translation_file)):
            app.installTranslator(translator)
    else:
        # Fallback to English
        en_file = translations_dir / "deadline_en.qm"
        if en_file.exists() and translator.load(str(en_file)):
            app.installTranslator(translator)
