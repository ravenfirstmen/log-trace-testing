#!/bin/bash

echo "Localstack deployed. Delete dynamodb table..."
source ./local-stack-env.sh
aws dynamodb delete-table --no-cli-pager --table-name my-table --endpoint-url ${AWS_ENDPOINT_URL} 1> /dev/null
echo "Dynamodb tables deleted."