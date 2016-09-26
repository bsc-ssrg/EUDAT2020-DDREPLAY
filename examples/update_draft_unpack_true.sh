#!/bin/bash

API_VERSION="v1.1"

if [[ ! $# -gt 0 ]]; then
    echo "Usage: $0 <draft_id>"
    exit 1
fi

while [[ $# -gt 0 ]];
do
    draft_id=$1
    shift

    curl -X PUT -F payload=@test_data/data_00.tar.gz "http://localhost:5000/api/${API_VERSION}/drafts/${draft_id}?unpack=true&overwrite=true"
done
