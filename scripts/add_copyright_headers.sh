#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

if [ $# -eq 0 ]; then
    echo "Usage: add-copyright-headers <file.java> ..." >&2
    exit 1
fi

for file in "$@"; do
    if ! head -1 | grep 'Copyright ' "$file" >/dev/null; then
        case $file in
            *.java)
                CONTENT=$(cat "$file")
                cat > "$file" <<EOF
/* Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved. */

$CONTENT
EOF
            ;;
            *.xml)
                FIRSTLINE=$(head -n 1 "$file")
                if echo "$FIRSTLINE" | grep '^<?xml' >/dev/null; then
                    CONTENT=$(tail -n +2 "$file")
                    cat > "$file" <<EOF
$FIRSTLINE
<!-- Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved. -->
$CONTENT
EOF
                else
                    CONTENT=$(cat "$file")
                    cat > "$file" <<EOF
<!-- Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved. -->
$CONTENT
EOF
                fi
            ;;
            *.py)
                CONTENT=$(cat "$file")
                cat > "$file" <<EOF
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

$CONTENT
EOF
            ;;
            *.yml)
                CONTENT=$(cat "$file")
                cat > "$file" <<EOF
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

$CONTENT
EOF
            ;;
            *.cfg)
                CONTENT=$(cat "$file")
                cat > "$file" <<EOF
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

$CONTENT
EOF
            ;;
            *.ini)
                CONTENT=$(cat "$file")
                cat > "$file" <<EOF
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

$CONTENT
EOF
            ;;
            *)
                echo "Skipping file in unrecognized format: $file" >&2
                exit 1
            ;;
        esac
    fi
done