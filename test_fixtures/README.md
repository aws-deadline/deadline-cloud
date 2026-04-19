# Test Fixtures

Job bundles for manual CLI comparison testing between the Python
(`deadline`) and Rust (`./target/debug/deadline`) CLIs.

## Job Bundles

| Bundle | Description | Use Case |
|--------|-------------|----------|
| `simple_job` | Minimal echo job, no attachments | `bundle submit --dry-run`, parameter validation |
| `cli_job` | Bash script with INOUT PATH parameter | Attachment upload/download, `bundle submit` with files |
| `job_attachments_devguide_output` | Script with OUTPUT directory | Output download, `job download-output`, `queue sync-output` |

## Usage

Compare Python and Rust CLI output for the same bundle:

```bash
# Dry-run submission (no actual job created)
diff <(deadline bundle submit test_fixtures/job_bundles/simple_job --dry-run --yes 2>&1) \
     <(./target/debug/deadline bundle submit test_fixtures/job_bundles/simple_job --dry-run --yes 2>&1)

# With attachments
diff <(deadline bundle submit test_fixtures/job_bundles/cli_job -p DataDir=. --dry-run --yes 2>&1) \
     <(./target/debug/deadline bundle submit test_fixtures/job_bundles/cli_job -p DataDir=. --dry-run --yes 2>&1)
```

These bundles are copied from
[deadline-cloud-samples](https://github.com/aws-deadline/deadline-cloud-samples/tree/mainline/job_bundles).
