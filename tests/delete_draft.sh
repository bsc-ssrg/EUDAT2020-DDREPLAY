#!/bin/bash

if [[ ! $# -gt 0 ]]; then
    echo "Usage: $0 <draft_id>"
    exit 1
fi

while [[ $# -gt 0 ]];
do
    draft_id=$1
    shift
    curl -X DELETE http://localhost:5000/api/v1.0/drafts/"$draft_id"
done

