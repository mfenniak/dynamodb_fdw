#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE EXTENSION multicorn;
    CREATE SERVER multicorn_dynamo FOREIGN DATA WRAPPER multicorn
    options (
        wrapper 'dynamodbfdw.dynamodbfdw.DynamoFdw'
    );
EOSQL
