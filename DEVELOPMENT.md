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
    - [GUI Tests (pytest-qt)](#gui-tests-pytest-qt)
      - [Running GUI Tests](#running-gui-tests)
    - [UI Tests](#ui-tests)
      - [Running UI Tests](#running-ui-tests)
  - [Changelog Guidelines](#changelog-guidelines)
  - [Things to Know](#things-to-know)
    - [Public Contracts](#public-contracts)
      - [Private Modules](#private-modules)
      - [Public Modules](#public-modules)
      - [On `import os as _os`](#on-import-os-as-_os)
    - [Python Version Support](#python-version-support)
      - [What we support today](#what-we-support-today)
      - [Why we don't drop versions on Python EOL alone](#why-we-dont-drop-versions-on-python-eol-alone)
      - [When to drop support for a Python version](#when-to-drop-support-for-a-python-version)
      - [When to add support for a new Python version](#when-to-add-support-for-a-new-python-version)
      - [DCC bundled Python reference](#dcc-bundled-python-reference)
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

1. Python 3.9 or higher. We recommend [mise](https://github.com/jdx/mise) if you would like to run more than one version
   of Python on the same system. When running unit tests against all supported Python versions, for instance.
2. The [hatch](https://github.com/pypa/hatch) package installed (`pip install --upgrade hatch`) into your Python environment.

You can develop on a Linux, MacOS, or Windows workstation, but you may find that some of the support scripting is specific to
Linux/MacOS workstations.

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
4. Add pytest-qt GUI unit tests for widget/dialog behavior, or UI tests for full workflow verification. (See [GUI Tests (pytest-qt)](#gui-tests-pytest-qt) and [UI Tests](#ui-tests))

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
3. GUI unit tests - Tests that verify individual Deadline GUI widgets and dialogs using [pytest-qt](https://pytest-qt.readthedocs.io/).
   These run as part of the unit test suite, use MockDeadlineBackend for API responses, and require no AWS account.
4. UI tests - Subprocess-based tests that launch the real `deadline` GUI commands and drive them through the OS accessibility tree, verifying the UI renders the right widgets and responds correctly. See [UI Tests](#ui-tests).

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
* AWS Developers note: If testing with a non-production deployment of AWS Deadline Cloud then you will have to
define the `AWS_ENDPOINT_URL_DEADLINE` environment variable to the non-production endpoint URL. For example,
production endpoints look like: `export AWS_ENDPOINT_URL_DEADLINE="https://deadline.$AWS_DEFAULT_REGION.amazonaws.com"`

### GUI Tests (pytest-qt)

GUI tests are located under `test/unit/deadline_client/ui/gui/`. They use [pytest-qt](https://pytest-qt.readthedocs.io/) to test Qt widgets and dialogs in-process, with `MockDeadlineBackend` providing fake API responses. No AWS credentials required.

#### Running GUI Tests

```sh
hatch run test test/unit/deadline_client/ui/gui/
```

These tests run automatically in CI as part of the standard unit test suite.

### UI Tests

UI tests are located under the `test/ui` directory of this repository. They launch the real `deadline` GUI commands as a subprocess against an in-process mock Deadline backend and drive the GUI through the OS accessibility tree via [xa11y](https://xa11y.dev/). New UI tests can be added for new dialogs/widgets or to cover regressions in existing GUI behavior. See [test/README.md](./test/README.md) for the full testing layer guide.

#### Running UI Tests

```bash
hatch run ui:test
```

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

### Python Version Support

This library is consumed in two very different ways, and that shapes how we choose which Python
versions to support:

1. As a standalone CLI and Python library, installed into an environment the user controls.
2. As an in-process integration (a submitter plugin, adaptor, or script) running *inside* a
   Digital Content Creation (DCC) application — Maya, Houdini, Nuke, 3ds Max, etc. In this case
   the DCC dictates the Python interpreter, and the user usually cannot change it.

The second case is why our supported range reaches back further than a typical Python library's.
A DCC often ships with, and is locked to, an older bundled Python. If we drop a version that a
widely-used DCC release still depends on, users on that DCC can no longer integrate with Deadline
Cloud at all — there is no "just upgrade Python" option available to them.

#### What we support today

The authoritative declaration of supported versions lives in `pyproject.toml` and `hatch.toml`,
and these must be kept in sync:

* `pyproject.toml` — `requires-python` and the `Programming Language :: Python :: 3.x` classifiers.
* `hatch.toml` — the `[[envs.all.matrix]]` `python` list used by `hatch run all:test`.

CI runs the unit test suite against every version in that matrix, so a version is only "supported"
if it is listed in both places and passing in CI.

When a dependency needs different version constraints across Python versions, prefer
environment markers over dropping a Python version. We already do this — for example:

```toml
"boto3 >= 1.42.89; python_version >= '3.9'",
"boto3 >= 1.36.8; python_version < '3.9'",
```

#### Why we don't drop versions on Python EOL alone

The Python core team's [end-of-life schedule](https://devguide.python.org/versions/) is **not** our
primary signal for dropping a version. A Python version reaching upstream EOL does not mean our users
have stopped using it — DCC vendors lag the upstream Python release cycle by years, and studios pin
DCC versions for the lifetime of a production.

Instead, we weigh three factors:

1. **DCC bundled-Python support** — Is a Python version still the interpreter shipped by a DCC release
   that customers actively use? (See the [reference table](#dcc-bundled-python-reference) below.) A
   version that is the only way to integrate with a popular DCC release is effectively mandatory for
   us regardless of upstream EOL.
2. **Real customer usage** — Do we have evidence (telemetry, support tickets, forum/Slack/GitHub
   issues, direct customer conversations) that people are actually running this library on that
   version? Low-or-no measured usage is a strong signal that dropping is safe; meaningful usage is a
   strong signal to keep it.
3. **Cost to keep supporting it** — What does the version actually cost us? Examples: dependencies
   that no longer release wheels for it (forcing version-split markers like the boto3 example above),
   inability to use newer language features across the codebase, extra CI matrix time, and bugs or
   security fixes we can't pick up because a transitive dependency dropped the version.

A version is a candidate to drop when the cost in (3) is rising **and** both (1) and (2) are low — no
actively-used DCC release depends on it, and we see little or no real usage. The decision is a
judgement call balancing these three, not a date on the upstream EOL calendar.

#### When to drop support for a Python version

Before dropping a version, confirm and document the following in the pull request:

* No actively-supported release of a DCC we care about is locked to it as its bundled interpreter
  (check the [reference table](#dcc-bundled-python-reference)).
* Usage data / customer signals show the version is not (or is no longer meaningfully) used.
* You can articulate the concrete cost of keeping it (a dependency that dropped it, a feature we
  can't adopt, etc.) — "it's old" is not by itself a reason.

Dropping a version is a **breaking change** for any user pinned to it (see
[Public Contracts](#public-contracts)). When you drop one:

1. Update `requires-python` in `pyproject.toml`.
2. Remove the matching `Programming Language :: Python :: 3.x` classifier.
3. Remove the version from the `[[envs.all.matrix]]` list in `hatch.toml`.
4. Clean up now-unnecessary version-conditional dependency markers and `sys.version_info` /
   `typing_extensions` shims that existed only for the dropped version.
5. Call it out in the changelog as a breaking change with migration guidance (see
   [Changelog Guidelines](#changelog-guidelines)).

#### When to add support for a new Python version

We aim to support current Python versions promptly so users on the latest interpreters — and DCCs
that have moved to them — are not blocked. To add a version:

1. Add it to the `[[envs.all.matrix]]` list in `hatch.toml` and get the suite passing against it
   locally and in CI (watch for dependencies that don't yet ship wheels for the new version).
2. Add the matching `Programming Language :: Python :: 3.x` classifier in `pyproject.toml`.
3. Widen the upper bound of `requires-python` if necessary.

#### DCC bundled Python reference

This table is the kind of evidence we use for factor (1) above. It covers the DCCs we ship submitters
for ([the `deadline-cloud-for-*` repositories](https://github.com/aws-deadline/)), and the `Releases`
column lists the DCC versions each submitter currently supports. It is a **point-in-time aid, not an
authoritative source** — bundled versions change between DCC releases, so verify against the specific
DCC release in question (vendor release notes / VFX Reference Platform) before relying on it in a
drop/add decision.

The bundled Python is what matters most: when a submitter or adaptor runs *inside* the DCC, it is
constrained to the interpreter that DCC ships. (After Effects, RenderMan, and ShotGrid are exceptions
— see the notes below the table.)

| DCC | Releases (supported by submitter) | Bundled Python | Reference |
| --- | --- | --- | --- |
| Maya | 2023 | 3.9 | |
| Maya | 2024 | 3.10 | |
| Maya | 2025, 2026 | 3.11 | |
| 3ds Max | 2024 | 3.10 | [3ds Max 2024 Python — What's New](https://help.autodesk.com/cloudhelp/2024/ENU/MAXDEV-Python/files/MAXDEV_Python_what_s_new_in_3ds_max_python_api_html.html) |
| 3ds Max | 2025, 2026 | 3.11 | [3ds Max 2025 Python — What's New](https://help.autodesk.com/cloudhelp/2025/ENU/MAXDEV-Python/files/MAXDEV_Python_what_s_new_in_3ds_max_python_api_html.html) |
| Houdini | 19.5 | 3.9 (3.7 build available) | [Houdini 19.5 Platforms](https://www.sidefx.com/docs/houdini/news/19_5/platforms.html) |
| Houdini | 20.0 | 3.10 (3.9 build available) | [Houdini 20.0 Platforms](https://www.sidefx.com/docs/houdini/news/20/platforms.html) |
| Houdini | 20.5 | 3.11 (3.10 build available) | [Houdini 20.5 Platforms](https://www.sidefx.com/docs/houdini/news/20_5/platforms.html) |
| Houdini | 21.0 | 3.11 (3.10 build available) | [Houdini 21.0 Platforms](https://www.sidefx.com/docs/houdini/news/21/platforms.html) |
| Nuke | 15 | 3.10 | [Nuke 15.2 Python module](https://learn.foundry.com/nuke/15.2v1/content/comp_environment/script_editor/nuke_python_module.html) |
| Nuke | 16 | 3.11 | [Nuke 16 Python module](https://learn.foundry.com/nuke/16.0v1/content/comp_environment/script_editor/nuke_python_module.html) |
| Nuke | 17 | 3.11 | [Nuke 17 Python module](https://learn.foundry.com/nuke/17.0v1/content/comp_environment/script_editor/nuke_python_module.html) |
| Blender | 3.6 LTS | 3.10 | [Blender 3.6 versions.cmake](https://github.com/blender/blender/blob/v3.6.0/build_files/build_environment/cmake/versions.cmake) |
| Blender | 4.0 | 3.10 | [Blender 4.0 versions.cmake](https://github.com/blender/blender/blob/v4.0.0/build_files/build_environment/cmake/versions.cmake) |
| Blender | 4.1, 4.2 LTS, 4.3, 4.4 | 3.11 | [Blender 4.1 versions.cmake](https://github.com/blender/blender/blob/v4.1.0/build_files/build_environment/cmake/versions.cmake) |
| Blender | 4.5 LTS, 5.0 | 3.11 | [Blender 4.5 versions.cmake](https://github.com/blender/blender/blob/v4.5.0/build_files/build_environment/cmake/versions.cmake) |
| Blender | 5.1 | 3.13 | [Blender 5.1 versions.cmake](https://github.com/blender/blender/blob/v5.1.0/build_files/build_environment/cmake/versions.cmake) |
| Cinema 4D | 2024 | 3.11 | [Cinema 4D 2024 Python SDK](https://developers.maxon.net/docs/py/2024_0_0/) |
| Cinema 4D | 2025, 2026 | 3.11 | [Cinema 4D 2025 Python SDK](https://developers.maxon.net/docs/py/2025_1_0/manuals/index.html) |
| KeyShot | 2023 | 3.11 | [KeyShot 2023 scripting manual](https://manuals.keyshot.com/keyshot2023/Content/manual/scripting/index.html) |
| KeyShot | 2024 | 3.12 | [KeyShot 2024 scripting manual](https://manuals.keyshot.com/keyshot2024/manual/scripting.html) |
| KeyShot | 2025 | 3.12 | [KeyShot 2025 scripting manual](https://manuals.keyshot.com/kss2025/en-us/manual/scripting.html) |
| Unreal Engine | 5.4, 5.5 | 3.11.8 | [UE 5.4 Python scripting](https://dev.epicgames.com/documentation/en-us/unreal-engine/scripting-the-unreal-editor-using-python?application_version=5.4) |
| Unreal Engine | 5.6, 5.7 | 3.11.8 | [UE 5.6 Python scripting](https://dev.epicgames.com/documentation/en-us/unreal-engine/scripting-the-unreal-editor-using-python?application_version=5.6) |
| VRED | 2025, 2026 | 3.11 | [VRED 2026 — What's New](https://help.autodesk.com/cloudhelp/2026/ENU/VRED-WhatsNew/files/Whats-New/whatsnew-vred-2026/wn-20260.html) |
| After Effects | 2024 – 2026 | None embedded (ExtendScript/JS); submitter uses an external Python | [After Effects scripting guide](https://ae-scripting.docsforadobe.dev/) |
| RenderMan | 24 – 26 | Uses host app Python; Pro Server bindings target 3.7 / 3.9 / 3.10 / 3.11 | [Installing RenderMan](https://rmanwiki-26.pixar.com/space/REN26/19660949/Installing+on+Linux) |

Notes:
* The [VFX Reference Platform](https://vfxplatform.com/) targets Python 3.11 for CY2024/CY2025 and
  Python 3.13 for CY2026/CY2027.
* Several DCCs (Nuke 13.x, Houdini 19.0, Maya ≤ 2023, 3ds Max ≤ 2023) bundled Python 3.7–3.9 and are
  why the library's floor has historically been below 3.9. Our submitters have since moved their
  minimums up (most now require 3.9+, 3ds Max requires 3.10+), but the `deadline` library itself is
  consumed beyond just our own submitters, so its supported range can extend below what any one
  submitter requires.
* After Effects scripts in ExtendScript/JavaScript and RenderMan host plugins run under the host DCC's
  interpreter — in these cases the relevant Python is the environment's, not a version bundled by the
  application.

The [VFX Reference Platform](https://vfxplatform.com/) is a useful cross-vendor reference for the
Python version the major DCCs target in a given calendar year.

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
