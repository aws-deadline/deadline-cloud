# AWS Deadline Cloud client

[![pypi](https://img.shields.io/pypi/v/deadline.svg?style=flat)](https://pypi.python.org/pypi/deadline)
[![python](https://img.shields.io/pypi/pyversions/deadline.svg?style=flat)](https://pypi.python.org/pypi/deadline)
[![license](https://img.shields.io/pypi/l/deadline.svg?style=flat)](https://github.com/aws-deadline/deadline/blob/mainline/LICENSE)

AWS Deadline Cloud client is a multi-purpose python library and command line tool for interacting with and submitting
[Open Job Description (OpenJD)][openjd] jobs to [AWS Deadline Cloud][deadline-cloud].

To support building workflows on top of AWS Deadline Cloud it implements its own user interaction, job creation, file upload/download, and other useful
helpers around the service's api. It can function as a pipeline tool, a standalone gui application, or even be embedded within other applications' runtimes.

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
[job-bundles]: https://docs.aws.amazon.com/deadline-cloud/latest/developerguide/build-job-bundle.html
[openjd]: https://github.com/OpenJobDescription/openjd-specifications/wiki

## Compatibility

This library requires:

1. Python 3.8 or higher; and
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
for guidance on how to contribute. Please report issues like bugs, inaccurate or confusing information, and so on
and make feature requests in the [issue tracker](https://github.com/aws-deadline/deadline-cloud/issues). We encourage
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

## Job attachments

Job attachments enable you to transfer files between your workstations and AWS Deadline Cloud, by using Amazon S3 buckets as
[content-addressed storage][cas] in your AWS account. The use of a content-addressed storage means that a file will never need
to be uploaded again once it has been uploaded once.

See [job attachments][job-attachments] for a more in-depth look at how files are uploaded, stored, and retrieved.

## Job bundles

A job bundle is one of the tools that you can use to define jobs for AWS Deadline Cloud. They group an [Open Job Description](openjd) template with
additional information such as files and directories that your jobs use with job attachments. You can use this package's command-line interface and/or
its Python interface to use a job bundle to submit jobs for a queue to run. Please see the [Job Bundles](job-bundles)
section of the AWS Deadline Cloud Developer Guide for detailed information on job bundles.

At minimum a job bundle is a folder that contains an [OpenJD][openjd] template as a file named `template.json` or `template.yaml`, however it can optionally include:
1. an `asset_references.yaml` - lists file inputs and outputs,
2. a `parameter_values.yaml` - contains the selected values for the job template's parameters,
3. and any number of additional files required for the job.

For example job bundles, visit the [samples repository][deadline-cloud-samples].

To submit a job bundle, you can run
```sh
$ deadline bundle submit <path/to/bundle>
```

or if you have the optional GUI components installed, you can load up a job bundle for submission by running:
```sh
$ deadline bundle gui-submit --browse
```

On submission a job bundle will be created in the job history directory (default: `~/.deadline/job_history`).

## Configuration

You can see the current configuration by running:
```sh
$ deadline config show
```
and change the settings by running the associated `get` and `set` commands.

To see a list of settings that can be configured, run:
```sh
$ deadline config --help
```

Or you can manage settings by a graphical interface if you have the optional gui dependencies:
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

## Code of Conduct

This project has adopted the [Amazon Open Source Code of Conduct](https://aws.github.io/code-of-conduct).
For more information see the [Code of Conduct FAQ](https://aws.github.io/code-of-conduct-faq) or contact
opensource-codeofconduct@amazon.com with any additional questions or comments.

## Security issue notifications

We take all security reports seriously. When we receive such reports, we will 
investigate and subsequently address any potential vulnerabilities as quickly 
as possible. If you discover a potential security issue in this project, please 
notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/)
or directly via email to [AWS Security](aws-security@amazon.com). Please do not 
create a public GitHub issue in this project.

## Telemetry

See [telemetry](https://github.com/aws-deadline/deadline-cloud/blob/release/docs/telemetry.md) for more information.

## License 

This project is licensed under the Apache-2.0 License.

### Optional third party dependencies - GUI

N.B.: Although this repository is released under the Apache-2.0 license, its optional GUI feature
uses the third party Qt and PySide projects. The Qt and PySide projects' licensing includes the LGPL-3.0 license.
