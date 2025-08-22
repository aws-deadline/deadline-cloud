# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

#!/bin/bash
set -euo pipefail

export OUTPUT_PATH="$1"
export FIRST_NAME="$2"
export STEP_NAME="$3"
export LAST_NAME="$4"

mkdir -p $OUTPUT_PATH
cd $OUTPUT_PATH

echo "Files in output dir (before):"
ls -al

for NAME in $(eval "echo {$STEP_NAME..$LAST_NAME}"); do
    if [[ "$NAME" == "$STEP_NAME" ]]; then
        echo "Step $STEP_NAME is correct" > "$NAME.txt"
    else
        echo "Step $STEP_NAME output is wrong - this file should be overwritten later by step $NAME" > "$NAME.txt"
    fi
done

echo "Files in output dir (after):"
ls -al

echo "Step $STEP_NAME Done!"