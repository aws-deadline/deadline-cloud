# Profiling

The application level profiling tests submit job bundles with invocations wrapped by pyinstrument to generate
profiling data.

## Running

You will need a farm and a queue that you can submit jobs to.

### bash/zsh
```sh
# Replace values with your own
export AWS_DEFAULT_REGION=us-west-2
export AWS_DEFAULT_PROFILE=myprofile
export FARM_ID=farm-myfarmid
export QUEUE_ID=queue-myqueueid

uv venv .venv-profiling
source .venv-profiling/bin/activate
uv pip install -e .
uv pip install -r requirements-testing.txt

python scripted_tests/profiling/profiling.py --output-dir path/where/you/want/output
```

### fish
```fish
# Replace values with your own
set -x AWS_DEFAULT_REGION us-west-2
set -x AWS_DEFAULT_PROFILE myprofile
set -x FARM_ID farm-myfarmid
set -x QUEUE_ID queue-myqueueid

uv venv .venv-profiling
source .venv-profiling/bin/activate.fish
uv pip install -e .
uv pip install -r requirements-testing.txt

python scripted_tests/profiling/profiling.py --output-dir path/where/you/want/output
```

After running the script your output will be in the directory you specified. By default, the output will be an
html file for each test.

