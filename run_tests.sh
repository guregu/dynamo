#!/usr/bin/env bash

docker rm -f dynamodb > /dev/null
docker run --name dynamodb -p 8000:8000 amazon/dynamodb-local > /dev/null &


export DYNAMO_ENDPOINT="http://localhost:8000"
export DYNAMO_TEST_REGION="us-west-2"
export DYNAMO_TEST_TABLE="TestDB"

aws dynamodb delete-table \
--table-name $DYNAMO_TEST_TABLE \
--endpoint-url $DYNAMO_ENDPOINT > /dev/null 2>&1

aws dynamodb create-table \
 --table-name $DYNAMO_TEST_TABLE \
 --attribute-definitions \
 AttributeName=UserID,AttributeType=N \
 AttributeName=Time,AttributeType=S \
 --key-schema \
 AttributeName=UserID,KeyType=HASH \
 AttributeName=Time,KeyType=RANGE \
 --provisioned-throughput ReadCapacityUnits=1000,WriteCapacityUnits=1000 \
 --region $DYNAMO_TEST_REGION \
 --endpoint-url $DYNAMO_ENDPOINT > /dev/null

go test . -cover
