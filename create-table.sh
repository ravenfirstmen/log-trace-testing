#!/bin/bash

echo "Localstack deployed. Create dynamodb table..."
source ./local-stack-env.sh
aws dynamodb create-table --no-cli-pager --table-name my-table --endpoint-url ${AWS_ENDPOINT_URL} --cli-input-json file://./dynamodb-table.json 1> /dev/null
echo "Dynamodb tables created."