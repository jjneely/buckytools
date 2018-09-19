#!/usr/bin/env bash

# Check if the bucky command and daemon are supplied
if [ ! -f bucky ]; then
    echo "ERROR: bucky executable not found. Please copy it in this directory"
    exit 1
fi

if [ ! -f buckyd ]; then
    echo "ERROR: buckyd executable not found. Please copy it in this directory"
    exit 1
fi

# Launch the actual cluster
docker-compose up -d || echo "ERROR: docker-compose failed"
