#!/bin/bash

if [[ ! $# -gt 0 ]]; then
    echo "Usage: $0 <dataset_id>"
    exit 1
fi

while [[ $# -gt 0 ]];
do
    draft_id=$1
    shift
    curl -X GET "http://localhost:5000/api/v1.0/datasets/${draft_id}/"
done

