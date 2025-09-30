# Deadline Cloud GUI Translations

This directory contains translation files for the AWS Deadline Cloud GUI.

## Files

- `deadline_en.ts` - English translation source file
- `deadline_en.qm` - Compiled English translation file (auto-generated)

## Adding New Languages

1. Copy `deadline_en.ts` to `deadline_<locale>.ts` (e.g., `deadline_es.ts` for Spanish)
2. Translate the `<translation>` elements in the new file
3. Compile the .ts file to .qm using `lrelease deadline_<locale>.ts`
4. The translation will be automatically loaded based on system locale

## Updating Translations

When new translatable strings are added to the GUI code:

1. Run `python scripts/generate_translations.py` to update the .ts files
2. Update translations in the .ts files
3. Compile to .qm files using `lrelease`

## Translation Guidelines

- Keep translations concise to fit in UI elements
- Maintain consistent terminology across the application
- Consider cultural context for button labels and messages
- Test translations with the actual GUI to ensure proper fit

## Technical Notes

- All GUI strings should be wrapped with `self.tr("text")` in the Python code
- The translation system uses Qt's internationalization framework
- Fallback to English occurs if a translation is not found
- Translations are loaded automatically at application startup
