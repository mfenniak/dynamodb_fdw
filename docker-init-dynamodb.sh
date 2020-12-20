#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE EXTENSION multicorn;
    CREATE SERVER multicorn_dynamo FOREIGN DATA WRAPPER multicorn
    options (
        wrapper 'dynamodbfdw.dynamodbfdw.DynamoFdw'
    );
    CREATE FOREIGN TABLE dynamodb (
        oid TEXT,
        region TEXT,
        table_name TEXT,
        partition_key TEXT,
        sort_key TEXT,
        document JSON NOT NULL
    ) server multicorn_dynamo;
EOSQL
