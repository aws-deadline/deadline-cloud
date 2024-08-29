# Code organization

This repository is split up into two main modules:
1. `src/client`
2. `src/job_attachments`

The `src/client` organization is laid out below.

For more information on job attachments, see [here](src/deadline/job_attachments/README.md).

### `src/client/api`

This submodule contains utilities to call boto3 in a standardized way
using an aws profile configured for AWS Deadline Cloud, helpers for working with the
AWS Deadline Cloud monitor login/logout, and objects representing AWS Deadline Cloud
resources.

### `src/client/cli`

This submodule contains entry points for the CLI applications provided
by the library.

### `src/client/config`

This submodule contains an interface to the machine-specific AWS Deadline Cloud
configuration, specifically settings stored in `~/.deadline/*`

### `src/client/ui`

This submodule contains Qt GUIs, based on PySide(2/6), for common controls
and widgets used in interactive submitters, and to display the status
of various AWS Deadline Cloud resources.

### `src/client/job_bundle`

This submodule contains code related to the history of job submissions
performed on the workstation. Its initial functionality is to create
job bundle directories in a standardized manner.