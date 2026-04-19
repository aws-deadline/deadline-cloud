#!/bin/bash

export OUTPUT_DIR=$1
mkdir $OUTPUT_DIR

echo "Path mapping rules file: $2"
jq . $2
echo
echo "Creating output file: $OUTPUT_DIR/output.txt"
echo "Script location: $0" >> $OUTPUT_DIR/output.txt
