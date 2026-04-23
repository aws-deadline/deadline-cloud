# UI Translations

Translation files for displaying the Deadline Cloud submitter UI in multiple languages. During development, type checkers (mypy, pyright) validate translation keys at development time using auto-generated `_translation_keys.py`. At runtime, the `tr()` function automatically loads the appropriate locale file based on system locale, falling back to `en_US.json`.

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

### Generating translations with agents

We'll generate translations of UI strings with an AI agent. We'll use the existing AWS documentation for Deadline and its officials translations as context for the agent to improve its quality. These instructions were tested with Q CLI using Claude Sonnet 4.5. Adapt the prompt as needed. Install the [AWS docs MCP tool](https://awslabs.github.io/mcp/servers/aws-documentation-mcp-server) to give the agent access to the docs and translations.

Sample prompt:

```
Generate translations for UI strings. See the list of translations we support by listing files in `./src/deadline/client/ui/translations/locales`. We'll use Deadline's official documentation and its translations as both background context and a translation guide.

To translate the strings:
1. Read the official AWS Deadline Cloud documentation on [concepts and terminology](https://docs.aws.amazon.com/deadline-cloud/latest/userguide/concepts-terminology.html). Also discover other docs pages that relate to the strings that are being translated. These pages will give you background context for the service, the feature, and the official language used with it.
2. For each language we're translating in to:
  a. Read the same AWS docs pages you found to be relevant in step 1 but in the language we're relating in to. For example, see [this Spanish translation of the concepts and terminology page](https://docs.aws.amazon.com/es_es/deadline-cloud/latest/userguide/concepts-terminology.html)
  b. For strings where you need additional context for a high quality translation, ask me for input. I will not know the target language, but I can provide additional context to inform how you translate it. STOP HERE AND WAIT FOR MY INPUT.
  c. Translate the English strings I've provided into the target language. The strings will be used in an interface for submitting jobs to Deadline.
    a. The language should be precise, technical, and professional.
    b. Mirror the language from the AWS docs where possible.
    c. ALWAYS use the same language as the AWS docs translation for specific concepts, resource types, or features such as "farm", "fleet", "queue", "worker", "job attachments", or "required capabilities". 

<note: adapt these instructions as needed to match your workflow>
  Run git diff on `./src/deadline/client/ui/translations/locales/en_US.json` to see the strings to be translated.
</note>
```