#!/bin/bash

API_VERSION="v1.1"

curl -X GET "http://localhost:5000/api/${API_VERSION}/drafts/"
