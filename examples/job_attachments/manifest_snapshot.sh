#!/bin/bash

####
# This script demonstrates building on Job Attachments CLI primitives for manifest and attachment handling.
# In this example, we snapshot a directory, and print out the manifest for reference.
# This example is useful as a ramp up to Job Attachments Manifest CLI commands.
###

# Snapshot the src directory
MANIFEST=$(deadline manifest snapshot --root ./src \
    --destination ~/work/manifest/ \
    --name break-time \
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