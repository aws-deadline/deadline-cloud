# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

#!/bin/bash
set -euo pipefail

export OUTPUT_PATH="$1"
export FIRST_NAME="$2"
export LAST_NAME="$3"

cd $OUTPUT_PATH

RESULT=0

echo "Files in output dir:"
ls -al

for NAME in $(eval "echo {$FIRST_NAME..$LAST_NAME}"); do
    if [[ "$(cat "$NAME.txt")" == "Step $NAME is correct" ]]; then
        echo "Verified $NAME.txt"
    else
        echo "Failed verification on $NAME.txt, output is:"
        cat "$NAME.txt"
        RESULT=1
    fi
done

echo "Verification complete"
exit $RESULT