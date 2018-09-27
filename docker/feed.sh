#!/usr/bin/env bash

echo "example1 $((RANDOM%100)) $(date +%s)" | nc -c 127.0.0.1 2103
echo "example2 $((RANDOM%100)) $(date +%s)" | nc -c 127.0.0.1 2203
echo "example3 $((RANDOM%100)) $(date +%s)" | nc -c 127.0.0.1 2303
