#!/bin/bash

####
# This script demonstrates building on Job Attachments CLI primitives for manifest and attachment handling.
# In this example, we snapshot a directory, make a modification and diff against the snapshot to show to find what files have changed.
# This example is usefule as a cron job to create incrementally updated manifests and upload only modified or new files.
###

mkdir manifest-demo
touch ./manifest-demo/include.file
touch ./manifest-demo/ignored.file
touch ./manifest-demo/exclude.file

GLOB='{
  "include": [
    "include.file"
  ],
  "exclude": [
    "exclude.file"
  ]
}'

# Snapshot the src directory
MANIFEST=$(deadline manifest snapshot --root ./manifest-demo \
    --destination ~/work/manifest/ \
    --name break-time \
    --glob "$GLOB"\
    --json)

# Use jq to parse the JSON output and extract the file path
FILE_PATH=$(echo "$MANIFEST" | jq -r '.manifest')

echo "Created Manifest file: $FILE_PATH"
echo "Formatted Manifest:"
cat $FILE_PATH | jq

# Now use glob as a file that can be persisted as a configuration.
echo "Now to demonstrate Glob settings in a file"
echo "$GLOB" > glob.file
# Snapshot the src directory
MANIFEST=$(deadline manifest snapshot --root ./manifest-demo \
    --destination ~/work/manifest/ \
    --name break-time \
    --glob glob.file\
    --json)

# Use jq to parse the JSON output and extract the file path
FILE_PATH=$(echo "$MANIFEST" | jq -r '.manifest')

echo "Created Manifest file: $FILE_PATH"
echo "Formatted Manifest:"
cat $FILE_PATH | jq

rm -rf manifest-demo