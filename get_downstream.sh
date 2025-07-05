#!/bin/bash

# Check if target function argument is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <target-function>"
    echo "Example: $0 'network::SnowflakeRestful._request_exec'"
    exit 1
fi

TARGET_FUNCTION="$1"

# Run code2flow with the specified target function
code2flow --output map.json \
    --language py \
    --target-function "$TARGET_FUNCTION" \
    --upstream-depth 0 \
    --downstream-depth 1 \
    -q \
    ./src/snowflake/connector/*.py \
    ./src/snowflake/connector/auth/*.py

# Check if code2flow ran successfully
if [ $? -ne 0 ]; then
    echo "Error: code2flow command failed" >&2
    exit 1
fi

# Check if map.json was created
if [ ! -f map.json ]; then
    echo "Error: map.json was not created" >&2
    exit 1
fi

# Output the contents of map.json to stdout
cat map.json
