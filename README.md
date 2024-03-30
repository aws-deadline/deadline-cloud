# The AWS Deadline Cloud Client Library (`deadline.client`)

## Overview

This is a shared Python library that implements functionality to support
client applications using AWS Deadline Cloud.

It is divided into the following submodules:

### api

This submodule contains utilities to call boto3 in a standardized way
using an aws profile configured for AWS Deadline Cloud, helpers for working with
Deadline Cloud Monitor Desktop login/logout, and objects representing AWS Deadline Cloud
resources.

### cli

This submodule contains entry points for the CLI applications provided
by the library.

### config

This submodule contains an interface to the machine-specific AWS Deadline Cloud
configuration, specifically settings stored in `~/.deadline/*`

### ui

This submodule contains Qt GUIs, based on PySide(2/6), for common controls
and widgets used in interactive submitters, and to display the status
of various AWS Deadline Cloud resoruces.

### job_bundle

This submodule contains code related to the history of job submissions
performed on the workstation. Its initial functionality is to create
job bundle directories in a standardized manner.

## Compatibility

This library requires:

1. Python 3.7 or higher; and
2. Linux, MacOS, or Windows operating system.

## Versioning

This package's version follows [Semantic Versioning 2.0](https://semver.org/), but is still considered to be in its 
initial development, thus backwards incompatible versions are denoted by minor version bumps. To help illustrate how
versions will increment during this initial development stage, they are described below:

1. The MAJOR version is currently 0, indicating initial development. 
2. The MINOR version is currently incremented when backwards incompatible changes are introduced to the public API. 
3. The PATCH version is currently incremented when bug fixes or backwards compatible changes are introduced to the public API. 

## Downloading

You can download this package from:
- [GitHub releases](https://github.com/casillas2/deadline-cloud/releases)

## Development

See instructions in DEVELOPMENT.md

## Telemetry

This library collects telemetry data by default. Telemetry events contain non-personally-identifiable information that helps us understand how users interact with our software so we know what features our customers use, and/or what existing pain points are.

You can opt out of telemetry data collection by either:

1. Setting the environment variable: `DEADLINE_CLOUD_TELEMETRY_OPT_OUT=true`
2. Setting the config file: `deadline config set telemetry.opt_out true`

Note that setting the environment variable supersedes the config file setting.

# Build / Test / Release

## Build the package.
```
hatch build
```

## Run tests
```
hatch run test
```

## Run integration tests
```
hatch run integ:test
```

## Run linting
```
hatch run lint
```

## Run formating
```
hatch run fmt
```

## Run tests for all supported Python versions.
```
hatch run all:test
```

# Optional Third Party Dependencies - GUI

N.B.: Although this repository is released under the Apache-2.0 license, its optional GUI feature
uses the third party Qt && PySide projects. The Qt and PySide projects' licensing includes the LGPL-3.0 license.
