# Submission Hooks

Submission hooks allow you to run custom scripts during the job submission workflow. You can use hooks to validate job configurations, discover additional assets, modify submission parameters, or integrate with external systems.

## Quick Start

There are two ways to configure hooks:

### 1. Bundle Hooks

Place a `hooks.yaml` (or `hooks.json`) in your job bundle directory alongside `template.yaml`. This works well for CLI workflows where the bundle already exists before submission:

```yaml
version: "1.0"
preSubmission:
  - command: python3
    args: [validate.py]
    timeout: 30

postSubmission:
  - command: python3
    args: [notify.py]
```

Requires `settings.allow_bundle_hooks` to be enabled.

### 2. Environment Hooks

For DCC submitters (Maya, Blender, etc.) where the bundle is created at submission time, or for studios that want to enforce hooks across all submissions, point `DEADLINE_HOOKS_DIR` to a directory containing `hooks.yaml`:

```bash
# Studio IT sets this per-application
export DEADLINE_HOOKS_DIR=/studio/pipeline/hooks/maya
```

Requires `settings.allow_environment_hooks` to be enabled. Both sources can be active simultaneously — environment hooks run first, then bundle hooks.

## Hook Types

### Pre-Submission Hooks

Run **before** job attachments are hashed and uploaded. Use these to:
- Validate job configuration
- Discover and add additional input files
- Modify job parameters (priority, etc.)
- Enforce studio policies

Pre-submission hooks **block submission** if they fail (non-zero exit code or timeout).

### Post-Submission Hooks

Run **after** the CreateJob API call returns successfully (i.e., the job has been accepted by AWS Deadline Cloud). Use these to:
- Send notifications (Slack, email, etc.)
- Update tracking systems
- Log submission details

Post-submission hooks **do not block** - failures are logged as warnings.

## Configuration

### hooks.yaml or hooks.json

Place either `hooks.yaml` or `hooks.json` in your job bundle directory alongside `template.yaml`, or in the directory specified by `DEADLINE_HOOKS_DIR`. If both formats exist in the same directory, an error is raised.

The `version` field is required and must be `"1.0"`.

**YAML format:**
```yaml
version: "1.0"
preSubmission:
  - command: python3
    args: [scripts/validate_assets.py]
    timeout: 60
    env:
      VALIDATION_LEVEL: strict

  - command: python3
    args: [scripts/discover_textures.py]

postSubmission:
  - command: python3
    args: [scripts/notify_slack.py]
    timeout: 15
    env:
      SLACK_WEBHOOK: https://hooks.slack.com/...
```

**JSON format:**
```json
{
  "preSubmission": [
    {
      "command": "python3",
      "args": ["scripts/validate_assets.py"],
      "timeout": 60,
      "env": {
        "VALIDATION_LEVEL": "strict"
      }
    }
  ],
  "postSubmission": [
    {
      "command": "python3",
      "args": ["scripts/notify_slack.py"],
      "timeout": 15
    }
  ]
}
```

### Hook Definition Fields

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `command` | Yes | - | Executable or interpreter (e.g., `python3`, `bash`) |
| `args` | No | `[]` | Command-line arguments |
| `timeout` | No | `60` | Maximum execution time in seconds |
| `env` | No | `{}` | Additional environment variables |

> **Note:** Hooks inherit the submitter's full environment. The `DEADLINE_*` variables and any hook-specific `env` values are layered on top (overriding on conflict).

### Path Resolution

- **Absolute paths**: Used as-is
- **Relative paths**: Resolved relative to the job bundle directory
- **Command names**: Searched in system PATH

## Hook Input

Hooks receive job metadata via JSON on stdin and through convenience environment variables.

### Environment Variables

Environment variables provide a convenient shorthand for simple scripts (e.g., `echo $DEADLINE_JOB_NAME` in bash). For structured or nested data, use the JSON stdin payload instead.

| Variable | Description |
|----------|-------------|
| `DEADLINE_JOB_NAME` | Job name |
| `DEADLINE_PRIORITY` | Job priority |
| `DEADLINE_FARM_ID` | Farm ID |
| `DEADLINE_QUEUE_ID` | Queue ID |
| `DEADLINE_JOB_BUNDLE_DIR` | Path to job bundle directory |
| `DEADLINE_STORAGE_PROFILE_ID` | Storage profile ID (if set) |
| `DEADLINE_JOB_ID` | Job ID (post-submission only) |

### JSON via stdin

Complete metadata is provided as JSON on stdin:

```json
{
  "jobName": "My Render Job",
  "priority": 50,
  "farmId": "farm-abc123",
  "queueId": "queue-def456",
  "jobBundleDir": "/path/to/bundle",
  "parameters": {"SceneFile": "/path/to/scene.ma"},
  "submitterName": "Maya",
  "assetReferences": {
    "inputFilenames": ["/path/to/texture.exr"],
    "inputDirectories": [],
    "outputDirectories": ["/path/to/output"],
    "referencedPaths": []
  },
  "submissionPayload": {}
}
```

## Hook Output

### Pre-Submission Hooks

Output JSON to stdout to modify the submission:

```python
import json

# Add files to upload
output = {
    "attachments": {
        "assetReferences": {
            "inputFilenames": ["/path/to/discovered/texture.exr"]
        }
    }
}
print(json.dumps(output))
```

Asset references are **replaced** at the nested key level - if your hook outputs `inputFilenames`, it replaces the entire `inputFilenames` list. Keys not included in your output are preserved from the original. This allows hooks to both add and remove files.

For example, to remove a file from the input list, output the full list without it:

```python
import json, sys

metadata = json.load(sys.stdin)
current_files = metadata["assetReferences"].get("inputFilenames", [])
# Remove unwanted files
filtered = [f for f in current_files if not f.endswith(".bak")]
print(json.dumps({"attachments": {"assetReferences": {"inputFilenames": filtered}}}))
```

You can also modify other submission parameters:

```python
print(json.dumps({"priority": 100}))
```

### Post-Submission Hooks

Output is logged but does not modify anything.

## Example Scripts

### Asset Validator (Pre-Submission)

```python
#!/usr/bin/env python3
import json
import os
import sys

metadata = json.load(sys.stdin)
asset_refs = metadata.get("assetReferences", {})

missing = []
for f in asset_refs.get("inputFilenames", []):
    if not os.path.exists(f):
        missing.append(f)

if missing:
    print("Missing files:", file=sys.stderr)
    for f in missing:
        print(f"  {f}", file=sys.stderr)
    sys.exit(1)

print("All assets validated", file=sys.stderr)
sys.exit(0)
```

### Texture Discovery (Pre-Submission)

```python
#!/usr/bin/env python3
import json
import os
import sys

metadata = json.load(sys.stdin)
bundle_dir = metadata["jobBundleDir"]

# Find all texture files in bundle
textures = []
for root, _, files in os.walk(bundle_dir):
    for f in files:
        if f.endswith(('.exr', '.png', '.jpg', '.tx')):
            textures.append(os.path.join(root, f))

if textures:
    print(json.dumps({
        "attachments": {
            "assetReferences": {
                "inputFilenames": textures
            }
        }
    }))
```

### Slack Notification (Post-Submission)

```python
#!/usr/bin/env python3
import json
import os
import sys
import urllib.request

metadata = json.load(sys.stdin)
webhook = os.environ.get("SLACK_WEBHOOK")

if webhook:
    message = {
        "text": f"Job submitted: {metadata['jobName']} (ID: {metadata['jobId']})"
    }
    req = urllib.request.Request(
        webhook,
        data=json.dumps(message).encode(),
        headers={"Content-Type": "application/json"}
    )
    urllib.request.urlopen(req)
```

## Security

Hooks are disabled by default. There are two sources of hooks, each with its own setting:

### Bundle Hooks

Hooks defined in `hooks.yaml` within job bundles. Enable with:

```bash
deadline config set settings.allow_bundle_hooks true
```

### Environment Hooks

Hooks from a directory specified by the `DEADLINE_HOOKS_DIR` environment variable. Enable with:

```bash
deadline config set settings.allow_environment_hooks true
```

Then set the environment variable (typically in application launcher scripts):

```bash
export DEADLINE_HOOKS_DIR=/studio/pipeline/hooks/blender
```

This is useful for studios that want to enforce hooks across all submissions without modifying job bundles.

### Confirmation Prompt

When hooks are enabled, you'll be prompted to confirm before they run:

```
This job bundle contains submission hooks that will execute on your machine:

  Pre-submission hooks:
    [1] python3 validate_assets.py

  Post-submission hooks:
    [1] python3 notify.py

  Bundle: /path/to/bundle

Do you want to run these hooks? [Y/n]
```

This prompt shows exactly which commands will execute, allowing you to review before proceeding.

### Configuration Summary

| Setting | Default | Description |
|---------|---------|-------------|
| `settings.allow_bundle_hooks` | `false` | Allow hooks from job bundle hooks.yaml |
| `settings.allow_environment_hooks` | `false` | Allow hooks from DEADLINE_HOOKS_DIR |
| `settings.auto_accept` | `false` | Skip confirmation prompts (for CI/automation) |

To skip confirmation prompts (use with caution, for CI/automation):
```bash
deadline config set settings.auto_accept true
```

### Studio Deployment

For pipeline TDs who want hooks to run automatically for all artists:

1. Configure workstations:
   ```bash
   deadline config set settings.allow_environment_hooks true
   ```

2. Set `DEADLINE_HOOKS_DIR` in each application's launcher script:
   ```bash
   # blender_launcher.sh
   export DEADLINE_HOOKS_DIR=/studio/pipeline/hooks/blender
   exec blender "$@"
   ```

3. Create hooks at the specified location:
   ```
   /studio/pipeline/hooks/blender/
   ├── hooks.yaml
   └── validate_scene.py
   ```

### Best Practices

1. **Use environment hooks for studio-wide policies** - More secure than bundle hooks
2. **Review bundle hooks before enabling** - Inspect hooks.yaml in bundles from untrusted sources
3. **Keep bundle hooks disabled by default** - Only enable if your workflow requires it

## Error Handling

### Pre-Submission Hook Failures

When a pre-submission hook fails, you'll see:
- Which hook failed
- Exit code
- stdout and stderr output
- Timeout duration (if timed out)

Submission is blocked until the issue is resolved.

### Post-Submission Hook Failures

Failures are logged as warnings but don't affect the submitted job.

## CLI vs GUI

Hooks work with both submission methods:
- `deadline bundle submit` (CLI)
- `deadline bundle gui-submit` (GUI)

The GUI copies `hooks.yaml` to the job history bundle and resolves script paths back to your original bundle directory.

## Best Practices

1. **Keep hooks fast** - Use appropriate timeouts, avoid long-running operations
2. **Log to stderr** - stdout is reserved for JSON output (pre-submission)
3. **Handle errors gracefully** - Provide clear error messages
4. **Test with CLI first** - Easier to debug than GUI
5. **Use absolute paths in output** - When adding files to asset references
