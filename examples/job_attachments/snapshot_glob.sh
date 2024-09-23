#!/bin/bash

####
# This script demonstrates building on Job Attachments CLI primitives for manifest and attachment handling.
# In this example, we snapshot a directory, using file globs to include or exclude specific files.
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

###
# Output: 
# {
#   "manifest": "/Users/hello/work/manifest/breaktime-time-2024-09-17T13-57-33.manifest"
# }
###

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