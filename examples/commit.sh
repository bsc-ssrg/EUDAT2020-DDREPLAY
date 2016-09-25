#!/bin/bash

draft_id=`curl -sX POST "http://localhost:5000/api/v1.0/drafts/" | grep id | cut -d '"' -f 4`

echo "PUT result:"
curl -sX PUT -F payload=@test_data/data_00.tar.gz "http://localhost:5000/api/v1.0/drafts/${draft_id}/foo/bar/?unpack=false&overwrite=false" # > /dev/null

echo "PUBLISH result:"
curl -sX GET "http://localhost:5000/api/v1.0/drafts/${draft_id}/publish?author=alberto.miranda@bsc.es&message=Initial%20version"
