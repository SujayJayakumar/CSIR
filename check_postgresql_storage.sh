#!/bin/bash

# Directory to monitor
DATA_DIR="/data/postgresql"

# Get usage percentage as a number (strip %)
USAGE=$(df -h "$DATA_DIR" | awk 'NR==2 {gsub("%",""); print $5}')

# Get current date and time
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

# Threshold
THRESHOLD=80

if [ "$USAGE" -ge "$THRESHOLD" ]; then
    echo "$TIMESTAMP ⚠️  WARNING: PostgreSQL data directory is ${USAGE}% full!"
else
    echo "$TIMESTAMP ✅ PostgreSQL storage usage is healthy (${USAGE}%)."
fi

