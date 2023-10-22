## Docker Environment for Testing File and Directory Permissions

This Docker environment is set up to test code related to modifying OS group ownership and permissions of files and directories. It adds two different OS groups (and a user in each group,) allowing us to manipulate system-level settings of files and directories. When the container starts, it executes the test script `run_tests.sh`, which only runs unit tests marked with `docker`.

### Usage
To use this Docker environment and run the related tests, navigate to the root of this repository and run the `./scripts/run_sudo_tests.sh` script.
