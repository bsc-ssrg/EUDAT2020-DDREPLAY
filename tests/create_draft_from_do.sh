#!/bin/bash

if [[ ! $# -gt 0 ]]; then
    echo "Usage: $0 <PID>"
    exit 1
fi

while [[ $# -gt 0 ]];
do
    PID=$1
    shift
    curl -X PUT "http://localhost:5000/api/v1.0/datasets/${PID}/"
done

