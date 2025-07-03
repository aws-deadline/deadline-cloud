# AWS Deadline Cloud Client

[![pypi](https://img.shields.io/pypi/v/deadline.svg?style=flat)](https://pypi.python.org/pypi/deadline)
[![python](https://img.shields.io/pypi/pyversions/deadline.svg?style=flat)](https://pypi.python.org/pypi/deadline)
[![license](https://img.shields.io/pypi/l/deadline.svg?style=flat)](https://github.com/aws-deadline/deadline/blob/mainline/LICENSE)

AWS Deadline Cloud client is a multi-purpose python library and command line tool for interacting with and submitting
[Open Job Description (OpenJD)][openjd] jobs to [AWS Deadline Cloud][deadline-cloud].

To support building workflows on top of AWS Deadline Cloud, it implements its own user interaction, job creation, file upload/download, and other useful
helpers around the service's API. It can function as a pipeline tool, a standalone GUI application, or even be embedded within other applications' runtimes.

Notable features include:
* A command-line interface with subcommands for querying your AWS Deadline Cloud resources, and submitting jobs to your AWS Deadline Cloud Farm.
* A library of functions that implement AWS Deadline Cloud's Job Attachments functionality.
* A library of functions for creating a job submission UI within any content creation tool that supports Python 3.8+ based plugins and
  the Qt GUI framework.

[cas]: https://en.wikipedia.org/wiki/Content-addressable_storage
[deadline-cloud]: https://docs.aws.amazon.com/deadline-cloud/latest/userguide/what-is-deadline-cloud.html
[deadline-cloud-monitor]: https://docs.aws.amazon.com/deadline-cloud/latest/userguide/working-with-deadline-monitor.html
[deadline-cloud-samples]: https://github.com/aws-deadline/deadline-cloud-samples
[deadline-jobs]: https://docs.aws.amazon.com/deadline-cloud/latest/userguide/deadline-cloud-jobs.html
[job-attachments]: https://docs.aws.amazon.com/deadline-cloud/latest/developerguide/build-job-attachments.html
[shared-storage]: https://docs.aws.amazon.com/deadline-cloud/latest/userguide/storage-shared.html
[job-bundles]: https://docs.aws.amazon.com/deadline-cloud/latest/developerguide/build-job-bundle.html
[openjd]: https://github.com/OpenJobDescription/openjd-specifications/wiki

## Compatibility

This library requires:

1. Python 3.8 through 3.13; and
2. Linux, Windows, or macOS operating system.

## Versioning

This package's version follows [Semantic Versioning 2.0](https://semver.org/), but is still considered to be in its
initial development, thus backwards incompatible versions are denoted by minor version bumps. To help illustrate how
versions will increment during this initial development stage, they are described below:

1. The MAJOR version is currently 0, indicating initial development.
2. The MINOR version is currently incremented when backwards incompatible changes are introduced to the public API.
3. The PATCH version is currently incremented when bug fixes or backwards compatible changes are introduced to the public API.

## Contributing

We welcome all contributions. Please see [CONTRIBUTING.md](https://github.com/aws-deadline/deadline-cloud/blob/mainline/CONTRIBUTING.md)
for guidance on how to contribute. Please report issues such as bugs, inaccurate or confusing information, and so on,
by making feature requests in the [issue tracker](https://github.com/aws-deadline/deadline-cloud/issues). We encourage
code contributions in the form of [pull requests](https://github.com/aws-deadline/deadline-cloud/pulls).

## Getting Started

The AWS Deadline Cloud client can be installed by the standard python packaging mechanisms:
```sh
$ pip install deadline
```

or if you want the optional gui dependencies:
```sh
$ pip install "deadline[gui]"
```

## Usage

After installation it can then be used as a command line tool:
```sh
$ deadline farm list
- farmId: farm-1234567890abcdefg
  displayName: my-first-farm
```

or as a python library:
```python
from deadline.client import api
api.list_farms()
# {'farms': [{'farmId': 'farm-1234567890abcdefg', 'displayName': 'my-first-farm', ...},]}
```

## Job-related Files
For job-related files and data, AWS Deadline Cloud supports either transferring files to AWS using job attachments or reading files from network storage that is shared between both your local workstation and your farm.

### Job attachments

Job attachments enable you to transfer files between your workstations and AWS Deadline Cloud using Amazon S3 buckets as
[content-addressed storage][cas] in your AWS account. The use of a content-addressed storage means that a file will never need
to be uploaded again once it has been uploaded once.

See [job attachments][job-attachments] for a more in-depth look at how files are uploaded, stored, and retrieved.

### Shared storage and storage profiles
Jobs can reference files that are stored on shared network storage. The Deadline Client uses a storage profile to determine which paths on the workstation are part of the network storage and do not need to be transferred using job attachments.

To use an existing storage profile with the Deadline Client, you can configure your default storage profile via CLI:

```sh
deadline config set settings.storage_profile_id sp-10b2e48ad6ac4fc88595dfcbef6271f2
```

Or with the configuration GUI:
```sh
deadline config gui
```


Shared storage is possible with customer-managed fleets (CMF) but not service-managed fleets (SMF). See [shared storage][shared-storage] for more information.

## Job Bundles

A job bundle is one of the tools that you can use to define jobs for AWS Deadline Cloud. They group an [Open Job Description (OpenJD)][openjd] template with
additional information such as files and directories that your jobs use with job attachments. You can use this package's command-line interface and/or
its Python interface to use a job bundle to submit jobs for a queue to run. Please see the [Job Bundles][job-bundles]
section of the AWS Deadline Cloud Developer Guide for detailed information on job bundles.

At a minimum, a job bundle is a folder that contains an [OpenJD][openjd] template as a file named `template.json` or `template.yaml`. However, it can optionally include:
1. An `asset_references.yaml` file - lists file inputs and outputs.
2. A `parameter_values.yaml` file - contains the selected values for the job template's parameters.
3. Any number of additional files required for the job.

For example job bundles, visit the [samples repository][deadline-cloud-samples].

To submit a job bundle, you can run
```sh
$ deadline bundle submit <path/to/bundle>
```

or if you have the optional GUI components installed, you can load up a job bundle for submission by running:
```sh
$ deadline bundle gui-submit --browse
```

On submission, a job bundle will be created in the job history directory (default: `~/.deadline/job_history`).

## Configuration

You can see the current configuration by running:
```sh
$ deadline config show
```
and change the settings by running the associated `get`, `set` and `clear` commands.

If you need to parse the settings as json, you can specify the output by running:
```sh
$ deadline config show --output json
```
Which will output:
```sh
{"settings.config_file_path": "~/.deadline/config", "deadline-cloud-monitor.path": "", "defaults.aws_profile_name": "(default)", "settings.job_history_dir": "~/.deadline/job_history/(default)", "defaults.farm_id": "", "settings.storage_profile_id": "", "defaults.queue_id": "", "defaults.job_id": "", "settings.auto_accept": "false", "settings.conflict_resolution": "NOT_SELECTED", "settings.log_level": "WARNING", "telemetry.opt_out": "false", "telemetry.identifier": "00000000-0000-0000-0000-000000000000", "defaults.job_attachments_file_system": "COPIED", "settings.s3_max_pool_connections": "50", "settings.small_file_threshold_multiplier": "20"}
```

To see a list of settings that can be configured, run:
```sh
$ deadline config --help
```

Or you can manage settings by a graphical user-interface if you have the optional GUI dependencies:
```sh
$ deadline config gui
```

By default, configuration of AWS Deadline Cloud is provided at `~/.deadline/config`, however this can be overridden by the `DEADLINE_CONFIG_FILE_PATH` environment variable.

## Authentication

In addition to the standard AWS credential mechanisms (AWS Profiles, instance profiles, and environment variables), AWS Deadline Cloud monitor credentials are also supported.

To view the currently configured credentials authentication status, run:

```sh
$ deadline auth status
    Profile Name: (default)
          Source: HOST_PROVIDED
          Status: AUTHENTICATED
API Availability: True
```

If the currently selected AWS Profile is set-up to use [AWS Deadline Cloud monitor][deadline-cloud-monitor] credentials, you can authenticate by logging in:

```sh
$ deadline auth login
```

and removing them by logging out:
```sh
$ deadline auth logout
```

## Job Monitoring and Logs

### Waiting for Job Completion

After submitting a job, you can wait for it to complete using the `wait` command:

```sh
# Wait for a job to complete with default settings
$ deadline job wait --job-id job-12345

# Customize the maximum polling interval (default is 120 seconds)
# The polling interval starts at 0.5 seconds and doubles until reaching this maximum
$ deadline job wait --job-id job-12345 --max-poll-interval 30

# Set a timeout (default is 0, meaning no timeout)
$ deadline job wait --job-id job-12345 --timeout 3600

# Get the result in JSON format
$ deadline job wait --job-id job-12345 --output json
```

The command blocks until the job reaches a terminal state (SUCCEEDED, FAILED, CANCELED, SUSPENDED, NOT_COMPATIBLE), then returns information about the job's status and any failed tasks. It uses exponential backoff for polling, starting at 0.5 seconds and doubling the interval after each check until it reaches the maximum polling interval.

**Exit Codes:**
- `0` - Job succeeded
- `1` - Timeout waiting for job completion
- `2` - Job failed or has failed tasks
- `3` - Job was canceled
- `4` - Job was archived
- `5` - Job is not compatible 

### Retrieving Job Logs

You can monitor job status and retrieve logs using the CLI. The logs lines are returned starting from the most recent log event with timestamps in ISO 8601 format:

```sh
# Get logs for a specific session
$ deadline job logs --session-id session-12345

# Get logs for a job (automatically selects session: ongoing sessions preferred, then most recently started/ended)
$ deadline job logs --job-id job-12345

# Limit the number of log lines returned to the 50 most recent.
$ deadline job logs --session-id session-12345 --limit 50

# Filter logs by time range
$ deadline job logs --session-id session-12345 --start-time 2023-01-01T12:00:00Z --end-time 2023-01-01T13:00:00Z

# Get logs in JSON format
$ deadline job logs --session-id session-12345 --output json

# Get logs with timestamps in local timezone (default is UTC)
$ deadline job logs --session-id session-12345 --timezone local

# Get logs with explicit UTC timestamps (default behavior)
$ deadline job logs --session-id session-12345 --timezone utc

# Combine timezone option with JSON output
$ deadline job logs --session-id session-12345 --timezone local --output json

# Paginate through logs
$ deadline job logs --session-id session-12345 --next-token next-token-value
```

**Timestamp Format**: All timestamps are displayed in ISO 8601 format with full microsecond precision and timezone information:
- UTC format: `2025-07-03T10:49:33.821306+00:00`
- Local format: `2025-07-03T03:49:33.821306-07:00` (example for PST)

**Timezone Options**:
- `--timezone utc` (default): Display timestamps in UTC with `+00:00` offset
- `--timezone local`: Display timestamps converted to your local system timezone

When using a Deadline Cloud monitor profile, the `job logs` command will use the Queue role credentials to read logs. Otherwise, the chosen profile credentials are used for all API invocations. This allows you to access logs with the appropriate permissions based on your authentication method.


## AWS Credentials Integration

You can use the Deadline Cloud client to obtain temporary AWS credentials for a queue and use them with the AWS CLI or SDK. This enables you to create AWS profiles that have queue-specific permissions for use in programmatic workflows.

To export credentials for a queue in a format compatible with the AWS SDK credentials_process interface:

```sh
$ deadline queue export-credentials --farm-id farm-1234567890abcdefg --queue-id q-12345abcdef --mode USER
{
  "Version": 1,
  "AccessKeyId": "ASIA...",
  "SecretAccessKey": "wJalr...",
  "SessionToken": "AQoD...",
  "Expiration": "2025-04-08T20:00:46Z"
}
```

You can then reference this command in your AWS config file to create a profile that uses these credentials. Often this will be a profile from a Deadline Cloud monitor session:

```
[profile deadline-queue]
credential_process = deadline queue export-credentials --farm-id farm-1234567890abcdefg --queue-id q-12345abcdef --mode USER --profile myfarm-us-west-2
region = us-west-2
```

Then use this profile with AWS CLI commands:

```sh
$ aws s3 ls --profile deadline-queue
```

Available modes:
- `USER`: Credentials with full queue-role permissions.
- `READ`: Credentials with read-only permissions for queue logs


## Code of Conduct

This project has adopted the [Amazon Open Source Code of Conduct](https://aws.github.io/code-of-conduct).
For more information see the [Code of Conduct FAQ](https://aws.github.io/code-of-conduct-faq) or contact
opensource-codeofconduct@amazon.com with any additional questions or comments.

## Security Issue Notifications

We take all security reports seriously. When we receive such reports, we will
investigate and subsequently address any potential vulnerabilities as quickly
as possible. If you discover a potential security issue in this project, please
notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/)
or directly via email to [AWS Security](mailto:aws-security@amazon.com). Please do not
create a public GitHub issue in this project.

## Telemetry

See [telemetry](https://github.com/aws-deadline/deadline-cloud/blob/release/docs/telemetry.md) for more information.

## License

This project is licensed under the Apache-2.0 License.

### Optional third party dependencies - GUI

N.B.: Although this repository is released under the Apache-2.0 license, its optional GUI feature
uses the third party Qt and PySide projects. The Qt and PySide projects' licensing includes the LGPL-3.0 license.
