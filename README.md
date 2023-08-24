# The Amazon Deadline Cloud Client Library (`deadline.client`)

## Overview

This is a shared Python library that implements functionality to support
client applications using Amazon Deadline Cloud.

It is divided into the following submodules:

### api

This submodule contains utilities to call boto3 in a standardized way
using an aws profile configured for Amazon Deadline Cloud, helpers for working with
Nimble Studio login/logout, and objects representing Amazon Deadline Cloud
resources.

### cli

This submodule contains entry points for the CLI applications provided
by the library.

### config

This submodule contains an interface to the machine-specific Amazon Deadline Cloud
configuration, specifically settings stored in `~/.deadline/*`

### ui

This submodule contains Qt GUIs, based on PySide2, for common controls
and widgets used in interactive submitters, and to display the status
of various Amazon Deadline Cloud resoruces.

### job_history

This submodule contains code related to the history of job submissions
performed on the workstation. Its initial functionality is to create
job bundle directories in a standardized manner.

## Development

See instructions in DEVELOPMENT.md

# Build / Test / Release

## Setup Code Artifact
```
export CODEARTIFACT_ACCOUNT_ID=<account-id>
export CODEARTIFACT_DOMAIN=<domain>
export CODEARTIFACT_REPOSITORY=<repository>
export REGION=us-west-2
export CODEARTIFACT_AUTH_TOKEN=`aws codeartifact get-authorization-token --domain $CODEARTIFACT_DOMAIN --domain-owner $CODEARTIFACT_ACCOUNT_ID --query authorizationToken --output text`
```

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

## Publish
```
./publish.sh
```
