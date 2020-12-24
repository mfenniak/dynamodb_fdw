import boto3
import pytest
import time
from botocore.config import Config

aws_region = 'us-east-1'

@pytest.fixture(scope='session')
def boto_config():
    return Config(region_name=aws_region)

@pytest.fixture(scope='session')
def dynamodb_resource():
    return boto3.resource('dynamodb', region_name=aws_region)

@pytest.fixture(scope='session')
def dynamodb_client():
    return boto3.client('dynamodb', region_name=aws_region)

@pytest.fixture(scope='session')
def string_table(dynamodb_resource, dynamodb_client):
    table_name = 'dynamodbfdw-string-pkey'
    try:
        dynamodb_resource.Table(table_name).key_schema
    except:
        dynamodb_client.create_table(
            AttributeDefinitions=[
                {
                    'AttributeName': 'pkey',
                    'AttributeType': 'S',
                },
            ],
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'pkey',
                    'KeyType': 'HASH',
                },
            ],
            BillingMode='PAY_PER_REQUEST',
        )
        dynamodb_resource.Table(table_name).wait_until_exists()
    return table_name

@pytest.fixture(scope='session')
def string_string_table(dynamodb_resource, dynamodb_client):
    table_name = 'dynamodbfdw-string-pkey-string-skey'
    try:
        dynamodb_resource.Table(table_name).key_schema
    except:
        dynamodb_client.create_table(
            AttributeDefinitions=[
                {
                    'AttributeName': 'pkey',
                    'AttributeType': 'S',
                },
                {
                    'AttributeName': 'skey',
                    'AttributeType': 'S',
                },
            ],
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'pkey',
                    'KeyType': 'HASH',
                },
                {
                    'AttributeName': 'skey',
                    'KeyType': 'RANGE',
                },
            ],
            BillingMode='PAY_PER_REQUEST',
        )
        dynamodb_resource.Table(table_name).wait_until_exists()
    return table_name

@pytest.fixture
def pg_connection():
    import psycopg2
    conn = psycopg2.connect("host=localhost port=5432 dbname=postgres user=postgres password=password")
    yield conn
    conn.close()

@pytest.fixture
def import_schema(pg_connection, string_table, string_string_table):
    with pg_connection.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS dynamodbfdw_import_test CASCADE")
        cur.execute("CREATE SCHEMA dynamodbfdw_import_test")
        cur.execute("""
            IMPORT FOREIGN SCHEMA dynamodb
            FROM SERVER multicorn_dynamo INTO dynamodbfdw_import_test OPTIONS ( aws_region %s )
        """, [aws_region])

def test_import_schema(pg_connection, import_schema, string_table, string_string_table):
    with pg_connection.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS dynamodbfdw_import_test CASCADE")
        cur.execute("CREATE SCHEMA dynamodbfdw_import_test")
        cur.execute("""
            IMPORT FOREIGN SCHEMA dynamodb
            FROM SERVER multicorn_dynamo INTO dynamodbfdw_import_test OPTIONS ( aws_region %s )
        """, [aws_region])
        cur.execute("SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'dynamodbfdw_import_test' AND table_name = %s", [string_table])
        res = cur.fetchone()
        assert res == (string_table, 'FOREIGN')
        cur.execute("SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'dynamodbfdw_import_test' AND table_name = %s", [string_string_table])
        res = cur.fetchone()
        assert res == (string_string_table, 'FOREIGN')

def test_read_table(pg_connection, import_schema, string_table, string_string_table):
    from psycopg2 import sql
    with pg_connection.cursor() as cur:
        cur.execute(sql.SQL('SELECT * FROM dynamodbfdw_import_test.{}').format(sql.Identifier(string_table)))
        cur.fetchall()
        cur.execute(sql.SQL('SELECT * FROM dynamodbfdw_import_test.{}').format(sql.Identifier(string_string_table)))
        cur.fetchall()
