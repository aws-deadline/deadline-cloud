## Scripts

### Running Tests with Docker for File and Directory Permissions

We have some unit tests that require being run in a specific docker container that is set up for testing with different users. Those unit tests are marked with `docker`, and the `run_sudo_tests.sh` script is provided to facilitate this testing. The script builds the Docker image using the Dockerfile located in `testing_containers/localuser_sudo_environment/`, and then runs the container.

#### Usage
Execute the script from the root of the repository.
```
./scripts/run_sudo_tests.sh --build
```

Or, you can run the hatch script:
```
hatch run test_docker
```

Please make sure that you have the necessary permissions to execute the script.
