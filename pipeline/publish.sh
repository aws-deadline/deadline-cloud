#!/bin/sh
# Set the -e option
set -e

./pipeline/build.sh
twine upload --repository codeartifact dist/* --verbose