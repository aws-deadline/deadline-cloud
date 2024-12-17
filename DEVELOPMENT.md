# Development documentation

This documentation provides guidance on developer workflows for working with the code in this repository.

Table of Contents:
* [Development Environment Setup](#development-environment-setup)
* [The Development Loop](#the-development-loop)
* [Code Organization](#code-organization)
* [Testing](#testing)
   * [Writing tests](#writing-tests)
   * [Unit tests](#unit-tests)
   * [Integration tests](#integration-tests)
* [Things to Know](#things-to-know)
   * [Public contracts](#public-contracts)
   * [Library Dependencies](#dependencies)
   * [Qt and Calling AWS APIs](#qt-and-calling-aws-including-aws-deadline-cloud-apis)

## Development Environment Setup

To develop the Python code in this repository you will need:

1. Python 3.8 or higher. We recommend [mise](https://github.com/jdx/mise) if you would like to run more than one version
   of Python on the same system. When running unit tests against all supported Python versions, for instance.
2. The [hatch](https://github.com/pypa/hatch) package installed (`pip install --upgrade hatch`) into your Python environment. 

You can develop on a Linux, MacOs, or Windows workstation, but you may find that some of the support scripting is specific to
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

Once you are satisfied with your code, and all relevant tests pass, then run `hatch run fmt` to fix up the formatting of
your code and post your pull request.

Note: Hatch uses [environments](https://hatch.pypa.io/1.12/environment/) to isolate the Python development workspace
for this package from your system or virtual environment Python. If your build/test run is not making sense, then
sometimes pruning (`hatch env prune`) all of these environments for the package can fix the issue.

## Code Organization

Please see [code organization](docs/code_organization.md).

## Testing

The objective for the tests of this package are to act as regression tests to help identify unintended changes to
functionality in the package. As such, we strive to have high test coverage of the different behaviours/functionality
that the package contains. Code coverage metrics are not the goal, but rather are a guide to help identify places
where there may be gaps in testing coverage.

The tests for this package have two forms:

1. Unit tests - Small tests that are narrowly focused on ensuring that function-level behavior
   of the implementation behaves as it is expected to. These can always be run locally on your workstation
   without requiring an AWS account.
2. Integration tests - Tests that ensure that the implementation behaves as expected when run in a real environment.
   Ensuring that code properly interacts as expected with a real Amazon S3 bucket, for instance.

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

Our integration tests run using using infrastructure that is in your AWS Account. The identifiers for
these resources are communicated to the tests through environment variables that you must define before running
the tests. Define the following environment variables:

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

See `deadline_config_dialog.py` for some examples that do all of the above. Here's some
code that was edited to show how it fits together:

```python
class MyCustomWidget(QWidget):
   # Signals for the widget to receive from the thread
   background_exception = Signal(str, BaseException)
   update = Signal(int, BackgroundResult)

   def __init__(self, ...):
      # Save information about the thread
      self.__refresh_thread = None
      self.__refresh_id = 0

      # Set this to True when exiting
      self.canceled = False

      # Connect the Signals to handler functions that run on the main thread
      self.update.connect(self.handle_update)
      self.background_exception.connect(self.handle_background_exception)

    def closeEvent(self, event):
      # Tell background threads when the widget closes
      self.canceled = True
      event.accept()

   def handle_background_exception(self, e: BaseException):
      # Handle the error
      QMessageBox.warning(...)

   def handle_update(self, refresh_id: int, result: BackgroundResult):
      # Apply the refresh if it's still for the latest call
      if refresh_id == self.__refresh_id:
         # Do something with result
         self.result_widget.set_message(result)

    def start_the_refresh(self):
        # This function starts the thread to run in the background

        # Update the GUI state to reflect the update
        self.result_widget.set_refreshing_status(True)

        self.__refresh_id += 1
        self.__refresh_thread = threading.Thread(
            target=self._refresh_thread_function,
            name=f"AWS Deadline Cloud Refresh Thread",
            args=(self.__refresh_id,),
        )
        self.__refresh_thread.start()

   def _refresh_thread_function(self, refresh_id: int):
      # This function is for the background thread
      try:
         # Call the slow operations
         result = boto3_client.potentially_expensive_api(...)
         # Only emit the result if it isn't canceled
         if not self.canceled:
            self.update.emit(refresh_id, result)
      except BaseException as e:
         # Use multiple signals for different meanings, such as handling errors.
         if not self.canceled:
            self.background_exception.emit(f"Background thread error", e)

```

