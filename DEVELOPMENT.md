# Development documentation

This documentation provides guidance on developer workflows for working with the code in this repository.

Table of Contents:
- [Development documentation](#development-documentation)
  - [Development Environment Setup](#development-environment-setup)
  - [The Development Loop](#the-development-loop)
  - [Documentation](#documentation)
    - [Code Organization](#code-organization)
  - [Testing](#testing)
    - [Writing Tests](#writing-tests)
    - [Unit Tests](#unit-tests)
      - [Running Unit Tests](#running-unit-tests)
      - [Running Docker-based Unit Tests](#running-docker-based-unit-tests)
    - [Integration Tests](#integration-tests)
      - [Running Integration Tests](#running-integration-tests)
    - [Squish GUI Submitter Tests](#squish-gui-submitter-tests)
      - [Running Squish GUI Submitter Tests](#running-squish-gui-submitter-tests)
  - [Changelog Guidelines](#changelog-guidelines)
  - [Things to Know](#things-to-know)
    - [Public Contracts](#public-contracts)
      - [Private Modules](#private-modules)
      - [Public Modules](#public-modules)
      - [On `import os as _os`](#on-import-os-as-_os)
    - [Library Dependencies](#library-dependencies)
      - [Why is a new dependency needed?](#why-is-a-new-dependency-needed)
      - [Quality of the dependency](#quality-of-the-dependency)
      - [Version Pinning](#version-pinning)
      - [Licensing](#licensing)
    - [Qt and Calling AWS (including AWS Deadline Cloud) APIs](#qt-and-calling-aws-including-aws-deadline-cloud-apis)
    - [Pattern 1: Simple Async Operations (Recommended)](#pattern-1-simple-async-operations-recommended)
    - [Pattern 2: Long-Running Operations with Progress](#pattern-2-long-running-operations-with-progress)
- [Profiling in Deadline Cloud](#profiling-in-deadline-cloud)

## Development Environment Setup

To develop the Python code in this repository you will need:

1. Python 3.8 or higher. We recommend [mise](https://github.com/jdx/mise) if you would like to run more than one version
   of Python on the same system. When running unit tests against all supported Python versions, for instance.
2. The [hatch](https://github.com/pypa/hatch) package installed (`pip install --upgrade hatch`) into your Python environment.

You can develop on a Linux, MacOS, or Windows workstation, but you may find that some of the support scripting is specific to
Linux/MacOS workstations.

If you are making changes to the Job Attachments files, then you will also need the following to be able to run the integration
tests:

1. A valid AWS Account
2. An AWS Deadline Cloud Farm and Queue.
   *  You can create these via AWS Deadline Cloud's AWS Console quick Farm create workflow.
      The Queue's configuration must include a Job Attachments bucket. If used only for running these tests then the cost of
      this infrastructure should be negligible, but do keep an eye on your costs and destroy the infrastructure (especially S3 buckets)
      when you no longer need it.

## The Development Loop

We have configured [hatch](https://github.com/pypa/hatch) commands to support a standard development loop. You can run the following
from any directory of this repository:

* `hatch build` - To build the installable Python wheel and sdist packages into the `dist/` directory.
* `hatch run test` - To run the PyTest unit tests found in the `test/unit` directory. See [Testing](#testing).
* `hatch run all:test` - To run the PyTest unit tests against all available supported versions of Python.
* `hatch run integ:test` - To run the PyTest integration tests found in the `test/integ` directory. See [Testing](#testing).
* `hatch run lint` - To check that the package's formatting adheres to our standards.
* `hatch run fmt` - To automatically reformat all code to adhere to our formatting standards.
* `hatch shell` - Enter a shell environment where you can run the `deadline` command-line directly as it is implemented in your
  checked-out local git repository.
* `hatch env prune` - Delete all of your isolated workspace [environments](https://hatch.pypa.io/1.12/environment/)
   for this package.

If you are not sure about how to approach development for this package, then we suggest a development
process along the lines of the following as a starting point:

1. Make your functional changes and make sure that they work.
2. Add unit tests for your changes and ensure that all unit tests pass.
   Iteratively improve your implementation until all unit tests pass. (See [Unit tests](#unit-tests))
3. Add integration tests for your changes if applicable. Ensure that all integration tests pass.
   Iteratively improve your implementation until all integration and unit tests pass. (See [Integration tests](#integration-tests))
4. Add Squish GUI tests for your changes if applicable. Ensure that all Squish GUI tests pass. (See [Squish GUI tests](#squish-tests))

Once you are satisfied with your code, and all relevant tests pass, then run `hatch run fmt` to fix up the formatting of
your code and post your pull request.

Note: Hatch uses [environments](https://hatch.pypa.io/1.12/environment/) to isolate the Python development workspace
for this package from your system or virtual environment Python. If your build/test run is not making sense, then
sometimes pruning (`hatch env prune`) all of these environments for the package can fix the issue.

## Documentation

Work-in-progress documentation for the Deadline Cloud client library is in progress in the [docs](docs/index.html) directory.
Documentation is written in Markdown using [Material for MkDocs](https://squidfunk.github.io/mkdocs-material/).
You can run the command `hatch run docs:serve` to start a server for viewing the documentation on localhost. When the command
starts, it prints the URL for viewing the docs locally, and will automatically update them when the `mkdocs.yml` configuration
or various markdown files are modified. The `hatch run docs:build` will build the documentation to static html content.

### Code Organization

Please see [code organization](docs/code_reference/code_organization.md).

## Testing

The objective for the tests of this package are to act as regression tests to help identify unintended changes to
functionality in the package. As such, we strive to have high test coverage of the different behaviours/functionality
that the package contains. Code coverage metrics are not the goal, but rather are a guide to help identify places
where there may be gaps in testing coverage.

The tests for this package have three forms:

1. Unit tests - Small tests that are narrowly focused on ensuring that function-level behavior
   of the implementation behaves as it is expected to. These can always be run locally on your workstation
   without requiring an AWS account.
2. Integration tests - Tests that ensure that the implementation behaves as expected when run in a real environment.
   Ensuring that code properly interacts as expected with a real Amazon S3 bucket, for instance.
3. Squish GUI Submitter tests - Tests that verify the Deadline GUI using Squish automated framework. Squish tests require a license.

### Writing Tests

If you want assistance developing tests, then please don't hesitate to open a draft pull request and ask for help.
We'll do our best to help you out and point you in the right direction.

Our tests are implemented using the [PyTest](https://docs.pytest.org/en/stable/) testing framework,
and unit tests generally make use of Python's [unittest.mock](https://docs.python.org/3.8/library/unittest.mock.html)
package to avoid runtime dependencies and narrowly focus tests on a specific aspect of the implementation.

If you are not sure how to start writing tests, then we suggest looking at the existing tests
for the same or similar functions for inspiration (search for calls to the function within the `test/`
subdirectories). You will also find both the official [PyTest documentation](https://docs.pytest.org/en/stable/)
and [unitest.mock documentation](https://docs.python.org/3.8/library/unittest.mock.html) very informative (we do).

### Unit Tests

Unit tests are all located under the `test/unit` directory of this repository. If you are adding or modifying
functionality, then you will almost always want to be writing one or more unit tests to demonstrate that your
logic behaves as expected and that future changes do not accidentally break your change.

#### Running Unit Tests

You can run unit tests by running:

* `hatch run test` - To run the unit tests with your default Python runtime.
* `hatch run all:test` - To run the unit tests with all of the supported Python runtime versions that you have installed.

Notes:
* If you are running unit tests on Linux, you may encounter errors such as `INTERNALERROR> ImportError: libEGL.so.1: cannot open shared object file: No such file or directory`. This is because some Qt dependencies are missed on Linux. Please install these [Qt dependencies](https://github.com/aws-deadline/.github/blob/mainline/.github/workflows/reusable_python_build.yml#L46-L49) to resolve this issue.

#### Running Docker-based Unit Tests

Some of the unit tests in this package require a docker environment to run. These tests are marked with `@pytest.mark.docker`.
In order to run these tests, please run the `run_sudo_tests.sh` script located in the `scripts` directory. For detailed instructions,
please refer to [scripts/README.md](./scripts/README.md).

If you make changes to the `download` or `asset_sync` modules, it's highly recommended to run and ensure these tests pass.

### Integration Tests

Integration tests are all located under the `test/integ` directory of this repository. You should consider
adding or modifying an integration test for any change that adds or modifies functionality that directly
interfaces with the local filesystem or an AWS service API.

#### Running Integration Tests

Our integration tests run using infrastructure that is in your AWS Account. A Farm, Queue and Fleet (that associated with 
the Queue) will be required to run the integration tests. The identifiers for these resources are communicated to the 
tests through environment variables that you must define before running the tests. Define the following environment 
variables:

```bash
# Replace with your AWS Account ID
export SERVICE_ACCOUNT_ID=000000000000
# Replace with the region code where your AWS test resources are located (e.g. us-west-2)
export AWS_DEFAULT_REGION=xx-yyyy-nn
# Replace with the ID of your AWS Deadline Cloud Farm
export FARM_ID=farm-00112233445566778899aabbccddeeff
# Replace with the ID of your AWS Deadline Cloud Queue that is configured with a
# Job Attachments bucket.
export QUEUE_ID=queue-00112233445566778899aabbccddeeff

export JOB_ATTACHMENTS_BUCKET=$(
   aws deadline get-queue --farm-id $FARM_ID --queue-id $QUEUE_ID \
    --query 'jobAttachmentSettings.s3BucketName' | tr -d '"'
)
export JA_TEST_ROOT_PREFIX=$(
   aws deadline get-queue --farm-id $FARM_ID --queue-id $QUEUE_ID \
    --query 'jobAttachmentSettings.rootPrefix' | tr -d '"'
)
```

Then you can run the integration tests with:

```bash
hatch run integ:test
```

Notes:
* If you are not one of the AWS Deadline Cloud developers then you may see test failures in tests marked with
  `pytest.mark.cross_account`. That's okay, just ignore them; they'll be tested with the required setup in our CI.
* If you are adding/changing code related to the Job Attachments' file-upload interactions with S3, then if you have a second
  AWS account then we request that you also ensure that the tests marked with the `pytest.mark.cross_account` marker also pass.
  If you don't have a second account, then don't worry about it. These tests will run in our CI. To run these tests:
  1. Create an S3 bucket in the same region as your testing resources but in your second AWS Account. If the bucket doesn't exist, you may see S3 PermanentRedirect error.
  2. Set the access policy of that S3 bucket to allow your first AWS Account to perform all operations on the bucket. Do
     NOT open the bucket up to the world for reading/writing!
  3. `export INTEG_TEST_JA_CROSS_ACCOUNT_BUCKET=<your-bucket-name-in-the-second-account>`
  4. Run the integration tests.
* AWS Developers note: If testing with a non-production deployment of AWS Deadline Cloud then you will have to
define the `AWS_ENDPOINT_URL_DEADLINE` environment variable to the non-production endpoint URL. For example,
production endpoints look like: `export AWS_ENDPOINT_URL_DEADLINE="https://deadline.$AWS_DEFAULT_REGION.amazonaws.com"`

### Squish GUI Submitter Tests

Squish GUI tests are located under the `test/squish` directory of this repository. New tests can be added for the Deadline GUI when necessary (ie: new functionality is introduced and a test can be added for coverage, or existing functionality is modified). When changes are made, Squish automated tests should be run to ensure changes are not breaking Deadline CLI and GUI functionality.

#### Running Squish GUI Submitter Tests

A separate ReadMe for developing/running Squish GUI tests is located in the `test/squish` directory. Please refer to [test/squish/SQUISH_README.md](./test/squish/SQUISH_README.md) on full instructions to use the automated tests. Note that a Squish license is required in order to run the tests. Currently, you may either have your own Squish license or you may file a [pull request](https://help.github.com/articles/creating-a-pull-request/) to the Deadline Cloud team to run or add any tests against any changes to be committed. Please perform any necessary manual tests prior to submitting any changes, in addition to making sure at least a minimal render job test passes.

## Changelog Guidelines

When a new version of `deadline` is being released, we must prepare an update to our change log (`CHANGELOG.md`). This is a semi-automated process. GitHub actions prepares a pull request with an automatically generated draft of the changelog entry. Maintainers are responsible for reviewing the draft, making any necessary changes, and reviewing the changes in the pull request. Please consult in [CHANGELOG_GUIDELINES.md](./CHANGELOG_GUIDELINES.md) for the changelog guidelines. These guidelines ensure consistency in how we communicate changes to users and provide standards for:

* Structuring changelog sections and their ordering
* Writing user-focused descriptions for different types of changes
* Handling breaking changes with proper migration guidance
* Communicating deprecations effectively
* Managing fixes to unreleased changes
* Documenting changes to experimental features

## Things to Know

### Public Contracts

The publicly consumable interfaces of this library and CLI are all considered to be public contracts. Meaning that any
change to them that is not backwards compatible is considered to be a breaking change. We strive to avoid making breaking
changes when possible, but accept that there are sometimes very good reasons for why a breaking change is necessary.

The following are some heuristics to demonstrate how to think about breaking vs non-breaking changes in the public interface.

For the command-line interface:
* Things like adding a non-required argument to a subcommand, or adding a new subcommand are not breaking changes.
* Renaming a subcommand or argument is a breaking change.
* Adding a new required subcommand argument is a breaking change.
* Changing a default value/behaviour is a breaking change.

For the Python library interface:
* We follow the [PEP 8](https://peps.python.org/pep-0008/#descriptive-naming-styles) weak internal use indicator convention
  and name all functions and modules that are internal/private with a leading underscore character.
* All functions and modules whose name does not begin with an underscore are part of the public contract for this package.
* Things like adding a non-required keyword argument to a function, or adding a new public function are not breaking changes.
* Things like renaming a keyword argument, or adding/removing a positional argument in a public function is a breaking change.
* Changing a default argument value is a breaking change.
* Changing the location that a file or directory is created should be considered to be a breaking change. These locations have a tendancy to become
  de-facto parts of the public contract as users build automation that assumes these locations is unchanged.

Note that we enforce our public contract through GitHub actions. See the [API Change Detection section](scripts/README.md#api-change-detection) in the scripts README for more information about generating and validating API changes.

#### Private Modules

New code should reside in private modules (example: `_my_module.py`), which removes the need to mark imports, classes, and functions as private with an underscore.

```python
# _my_module.py
import os

class PublicClass:
    def publicmethod(self):
        pass
    # We still need to mark this as private, since the class will be public
    def _privatemethod(self):
        pass

class PrivateClass:
    def privatemethod(self):
        pass
```

Public contracts in private modules are defined by imports in the corresponding `__init__.py` in the same directory as the private module.

```python
# __init__.py

from _my_module import PublicClass
```

#### Public Modules

A public module (for example `my_module.py`) in this package will be defined with the following style:

```python
# my_module.py

# The os module is not part of this file's external interface
import os as _os

# PublicClass is part of this file's external interface.
class PublicClass:
    def publicmethod(self):
        pass

    def _privatemethod(self):
        pass

# _PrivateClass is not part of this file's external interface.
class _PrivateClass:
    def publicmethod(self):
        pass

    def _privatemethod(self):
        pass
```

#### On `import os as _os`

Every module/symbol that is imported into a Python module becomes a part of that module's interface.
Thus, if we have a module called `foo.py` such as:

```python
# foo.py

import os
```

Then, the `os` module becomes part of the public interface for `foo.py` and a consumer of that module
is free to do:

```python
from foo import os
```

We don't want all (generally, we don't want any) of our imports to become part of the public API for
the module, so we import modules/symbols into a public module with the following style:

```python
import os as _os
from typing import Dict as _Dict
```

### Library Dependencies

Library dependencies are Python packages required to build and run the Deadline Cloud Python project. Dependencies are specified in the `dependencies` section of `pyproject.toml`.

The Deadline Cloud library is designed to be integrated into third-party applications that have bespoke and customized deployment environments. Adding dependencies will increase the chance of library version conflicts and incompatabilities. Please evaluate the addition of each new dependency.

We try to minimize the number of dependencies required to build and run Deadline Cloud. When contributing changes, please consider the following.

#### Why is a new dependency needed?

* Is the dependency library functionality required small enough to have a minimal version added to the Deadline Cloud code base?

#### Quality of the dependency

* Is the dependency active, reputable or maintained by a reputable source? Considerations can include:
    - PyPI download stats
    - GitHub stars
    - GitHub dependency graph showing downstream consumers
* Is it well-maintained?
* Is the library released regularly or recently?

#### Version Pinning

* How should we pin the version of this new dependency?
    - Please consider changes over time such as API or CLI command evolution and breakage.
* Does the library follow a versioning scheme such as semver?

#### Licensing

*   Please ensure the license of the dependency is compatible with the distribution license of this library.
*   Please attribute dependencies in https://github.com/aws-deadline/deadline-cloud/blob/mainline/THIRD_PARTY_LICENSES.

### Qt and Calling AWS (including AWS Deadline Cloud) APIs

> TL;DR Never call an AWS API from the main Qt event loop. Always run it in a separate thread,
> and use a Signal/Slot to send the result back to GUI widget that needs an update. The code
> in the separate thread should watch a boolean flag indicating whether to abandon its work.

AWS APIs, while often quick, can be very slow sometimes. When calling to a distant region,
they can consistently have very high latency.

In Qt, event handling happens in the process's main thread that is running an event
loop. If code performs a slow operation, such as calling an AWS API, that blocks all
interactivity with the GUI.

We can maintain GUI interactivity by running these slow operations in a separate thread.
If the separate thread, however, directly modifies the GUI, this can produce crashes or
undefined behavior. Therefore, the only way the results of these operations should be consumed
is by emitting a Qt Signal from the thread, and consuming it in the Widget.

Another detail is that threads need to finish running before the process can exit. If an
operation in a thread continues indefinitely, this will block program exit, so it should watch
for a signal from the application.

If interacting with the GUI can start multiple background threads, you should also track which
is the latest, so the code only applies the result of the newest operation.

See `deadline_config_dialog.py` for some examples that do all of the above.

### Pattern 1: Simple Async Operations (Recommended)

For simple fetch-and-display operations, use `AsyncTaskRunner`:

```python
from deadline.client.ui.controllers import AsyncTaskRunner

class MyCustomWidget(QWidget):
    def __init__(self, ...):
        self._runner = AsyncTaskRunner(self)
        self._runner.task_error.connect(self._on_error, Qt.QueuedConnection)

    def start_the_refresh(self):
        self.result_widget.set_refreshing_status(True)
        self._runner.run(
            operation_key="my_refresh",
            fn=self._fetch_data,
            on_success=self._handle_result,
            on_error=self._handle_error,
        )

    def _fetch_data(self):
        # This runs in background thread
        return boto3_client.potentially_expensive_api(...)

    def _handle_result(self, result):
        self.result_widget.set_refreshing_status(False)
        self.result_widget.set_message(result)

    def _handle_error(self, error):
        self.result_widget.set_refreshing_status(False)
        QMessageBox.warning(self, "Error", str(error))
```

### Pattern 2: Long-Running Operations with Progress

For complex operations with progress callbacks, use a `QThread` subclass:

```python
from qtpy.QtCore import QThread, Signal, Qt

class MyWorker(QThread):
    progress = Signal(int, str)  # percent, message
    succeeded = Signal(object)
    failed = Signal(BaseException)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._canceled = False

    def cancel(self):
        self._canceled = True

    def run(self):
        try:
            for i, item in enumerate(items):
                if self._canceled:
                    return
                self.progress.emit(i * 100 // len(items), f"Processing {item}")
                process(item)
            self.succeeded.emit(result)
        except Exception as e:
            if not self._canceled:
                self.failed.emit(e)


class MyCustomWidget(QWidget):
    def __init__(self, ...):
        self._worker = MyWorker(self)
        self._worker.progress.connect(self._on_progress, Qt.QueuedConnection)
        self._worker.succeeded.connect(self._on_success, Qt.QueuedConnection)
        self._worker.failed.connect(self._on_error, Qt.QueuedConnection)

    def start_the_operation(self):
        self._worker.start()

    def closeEvent(self, event):
        if self._worker.isRunning():
            self._worker.cancel()
            self._worker.wait()
        super().closeEvent(event)
```

# Profiling in Deadline Cloud

Instead of runnning a deadline command as `deadline ...` run `pyinstrument -r html -m deadline ...`.

This will profile the current `deadline` command and open the results in an interactive window.

# Manual Test Cases

These are the manual test cases for the client software release cycle, covering Deadline CLI and job attachments across Linux, Windows, and macOS.

## Deadline CLI Tests

| Test Case | Test Steps | Notes |
|---|---|---|
| Pre-requisite: Uninstall any previous versions of the Deadline Cloud Submitter Installer | Update PATH if necessary. | |
| Verify Deadline CLI can be successfully installed using the staged individual installer | Run the staged individual installer and verify that it can install. | |
| Verify correct version of Deadline CLI is being tested | Verify correct version using `deadline --version` command. | |
| Verify user can modify workstation configuration settings using Deadline GUI (`deadline config gui`) | Run `deadline config gui` and verify dialogue loads as expected. Verify User can authenticate using DCM Profile using Login button. Using a DCM Profile, verify correct farm/queue resources are pulled, and the existing config can be modified. Verify User can Logout of DCM Profile using Logout button. Verify User can authenticate using an AWS Profile. Using an AWS Profile, verify correct farm/queue resources are pulled, and the existing config can be modified. | |
| Verify user can modify workstation configuration settings using `deadline config set` | Once settings are modified, verify settings were modified correctly using: 1. `deadline config show` 2. `deadline config get <setting_name>` 3. `deadline config gui` (verify modified settings appear correctly in the GUI). Verify settings can be modified back to default using `deadline config clear`. | |
| Verify user can authenticate/login using DCM Profile: `deadline auth login` | In deadline config gui, you should see the profile name with a green checkmark beside it in the bottom left. | Would need to set using DCM profile first. |
| Verify user can logout of DCM Profile: `deadline auth logout` | In deadline config gui, you should see the profile name with a red 'X' beside it in the bottom left. There should be a button to log in on the right. | |
| `deadline auth login` using AWS profile | Run `deadline config gui`, select an AWS profile that uses IAM credentials, and run `deadline auth login`. Confirm that there is an error like "Logging in is only supported for AWS Profiles created by Deadline Cloud monitor". | Verify Login is not supported when using AWS profile. |
| Verify user can authenticate using AWS profile | In deadline config gui, you should see the profile name with a green checkmark beside it in the bottom left. There should be no option to logout. | Need to set AWS region environment variable in Terminal. |
| Submit a render job using `deadline bundle gui-submit --browse` | Verify job bundle can be selected and GUI Submitter dialogue loads correctly with correct default settings based on the job bundle, including any job attachments and host requirements. Verify job bundle can be submitted successfully to the farm. Verify job bundle is created upon submission in the users' job history directory. Verify correct job information appears in DCM and job can be completed successfully. | |
| Submit a render job using `--output json` option (success) | Launch GUI Submitter using `deadline bundle gui-submit --output json <path> > output.txt`. Click submit. When the submission succeeds, click Ok. Close the submitter. Open output.txt and ensure it is JSON structured like: `{"status": "SUBMITTED", "jobId": "<job-id>", "jobHistoryBundleDirectory": "<path>"}` | |
| Submit a render job using `--output json` option (cancel) | Launch GUI Submitter using `deadline bundle gui-submit --output json <path> > output.txt`. Click submit. Immediately click cancel before the submission completes. Close the submitter. Open output.txt and ensure it is JSON exactly like: `{"status": "CANCELED"}` | |
| Submit a render job using a submitter name | Launch GUI Submitter using `deadline bundle gui-submit --submitter-name Testing <path>`. Click submit. Wait for the submission to complete. Click Ok. Ensure the submission process exits. | |
| Verify 'Load a different job bundle' button in GUI Submitter | Launch GUI Submitter using `deadline bundle gui-submit --browse`, select a job bundle. Verify correct defaults/details. Hit 'Load a different job bundle' button and select a second job bundle. Verify correct defaults/details for the second bundle. | |
| Verify 'Export job bundle' button in GUI Submitter | | |
| Verify all GUI Submitter dialogue controls work | Verify all dropdown options, menus, input fields, toggles, checkboxes, radio buttons work as expected. Verify all tabs: Shared job settings, Job-specific settings, Job attachments, Host requirements (both 'Run on all worker hosts' and 'Run on worker hosts that meet the following requirements' options). | |
| Test Deadline Cloud release candidate against currently released DCC Submitter | A Blender manual install might be easiest. Build deadline-cloud from the release candidate branch and pip install it into the submitter dependencies instead of the latest in PyPi. | |

## Job Attachments Tests

| Test Case | Test Steps | Notes |
|---|---|---|
| Submit render job with job attachments | | |
| Verify job attachments are uploaded to s3 during the render submission process | | |
| `deadline attachment upload` command | | |
| `deadline attachment download` command | | |
| `deadline manifest snapshot` | | |
| `deadline manifest diff` command | | |
| `deadline manifest download` | | |
| `deadline manifest upload` | | |
| Path mapping rules | | |
| `deadline job download-output` with overwrite files option | Download output to a folder where output already exists, select overwrite. | |
| `deadline job download-output` with skip option | Download output to a folder where output already exists, select skip. | |
| `deadline job download-output` with create copy/append option | Download output to a folder where output already exists, select create copy/append. | |
