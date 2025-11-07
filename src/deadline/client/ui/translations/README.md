# UI Translations

Translation files for displaying the Deadline Cloud submitter UI in multiple langauges. During development, type checkers (mypy, pyright) validate translation keys at development time using auto-generated `_translation_keys.py`. At runtime, the `tr()` function automatically loads the appropriate locale file based on system locale, falling back to `en_US.json`.

Do translate:
- Buttons, labels, UI elements, and help text
- Common error messages such as "Please login to continue"

Do not translate:
- CLI text
- Unexpected errors or stack traces

## Structure

```
translations/
├── locales/
│   ├── en_US.json    # English translations
│   └── ja_JP.json    # Japanese translations (example)
└── README.md
```

## Usage

### Adding a New String

1. **Add to `locales/en_US.json`:**
```json
{
  "Submit to AWS Deadline Cloud": "Submit to AWS Deadline Cloud",
  "New string here": "New string here"
}
```

2. **Use in Python code:**
```python
from .._utils import tr

label = tr("New string here")
```

3. **Build to generate type hints:**
```bash
hatch build  # Generates _translation_keys.py with type checking
```

### Using Placeholders

For dynamic values, use Python named placeholders:

```python
# Single placeholder
message = tr("Profile '{name}' has an error.").format(name=profile_name)

# Multiple placeholders
message = tr("Uploaded {count} files to {destination}").format(
    count=file_count,
    destination=bucket_name
)
```

In JSON, keep placeholders in the translation:
```json
{
  "Profile '{name}' has an error.": "Profile '{name}' has an error.",
  "Uploaded {count} files to {destination}": "Uploaded {count} files to {destination}"
}
```

### Adding Translations for Other Locales

1. Copy `locales/en_US.json` to `locales/<locale>.json`
2. Translate the **values** (keep keys in English)
3. Keep placeholders unchanged

Example `locales/ja_JP.json`:
```json
{
  "Submit to AWS Deadline Cloud": "AWS Deadline Cloudに送信",
  "Profile '{name}' has an error.": "プロファイル'{name}'にエラーがあります。"
}
```
