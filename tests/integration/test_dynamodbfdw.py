from botocore.config import Config
from psycopg2 import sql
import boto3
import json
import os
import pytest
import time

aws_region = 'us-east-1'

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
    host = os.getenv('FDW_IP', 'localhost')
    conn = psycopg2.connect(f"host={host} port=5432 dbname=postgres user=postgres password=password")
    yield conn
    conn.close()

@pytest.fixture
def multicorn_dynamo(pg_connection):
    with pg_connection.cursor() as cur:
        cur.execute("DROP SERVER IF EXISTS multicorn_dynamo CASCADE")
        cur.execute("CREATE EXTENSION IF NOT EXISTS multicorn")
        cur.execute("CREATE SERVER multicorn_dynamo FOREIGN DATA WRAPPER multicorn options ( wrapper 'dynamodbfdw.dynamodbfdw.DynamoFdw' )")

@pytest.fixture
def import_schema(pg_connection, multicorn_dynamo, string_table, string_string_table):
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

def test_blind_data_read(pg_connection, import_schema, string_table, string_string_table):
    # Doesn't check any data quality, but serves as a good baseline for whether literally anything is working
    with pg_connection.cursor() as cur:
        cur.execute(sql.SQL('SELECT * FROM dynamodbfdw_import_test.{}').format(sql.Identifier(string_table)))
        assert cur.fetchall() == []
        cur.execute(sql.SQL('SELECT * FROM dynamodbfdw_import_test.{}').format(sql.Identifier(string_string_table)))
        assert cur.fetchall() == []

@pytest.fixture
def string_table_data(pg_connection, string_table):
    with pg_connection.cursor() as cur:
        cur.execute(
            sql.SQL('INSERT INTO dynamodbfdw_import_test.{} (pkey, document) VALUES (%s, %s)').format(sql.Identifier(string_table)),
            ['pkey-value-1', json.dumps({'doc-attr-1': 'doc-value-1'})]
        )
        pg_connection.commit()
    yield
    with pg_connection.cursor() as cur:
        cur.execute(sql.SQL('DELETE FROM dynamodbfdw_import_test.{}').format(sql.Identifier(string_table)))
        pg_connection.commit()

def test_hash_table_data_read(pg_connection, import_schema, string_table, string_table_data):
    with pg_connection.cursor() as cur:
        cur.execute(sql.SQL('SELECT oid, pkey, document FROM dynamodbfdw_import_test.{}').format(sql.Identifier(string_table)))
        data = cur.fetchall()
        assert data == [('{"pkey": "pkey-value-1"}', 'pkey-value-1', {'pkey': 'pkey-value-1', 'doc-attr-1': 'doc-value-1'})]

def assert_contains_rows(output, required_rows):
    """
    Check if the provided output contains all required rows.
    This allows for the presence of additional rows and does not require exact order.

    :param output: The list of tuples representing the output from the query.
    :param required_rows: A list of strings that are critical to appear in the output.
    """
    output_text = "\n".join([row[0] for row in output])  # Convert tuple output to a single string for easier searching
    missing_rows = [row for row in required_rows if row not in output_text]
    assert not missing_rows, f"Missing required rows in output: {missing_rows}"

def test_explain_pkey_equal(pg_connection, import_schema, string_table):
    with pg_connection.cursor() as cur:
        cur.execute(
            sql.SQL('EXPLAIN SELECT * FROM dynamodbfdw_import_test.{} WHERE pkey = %s').format(sql.Identifier(string_table)),
            ['pkey-value-1']
        )
        query_plan = cur.fetchall()
        # It's possible that micro adjustments in this query plan will happen w/ new versions of Postgres or Multicorn,
        # so comments point out the things that are important about the test... should probably be extracted into some
        # kind of test help that just checks specific row presence?
        required_query_plan_rows = [
            "Foreign Scan on \"dynamodbfdw-string-pkey\"",
            "Filter: (pkey = 'pkey-value-1'::text)",
            "Multicorn: DynamoDB: pagination provider",
            # "Query table", rather than "Scan table", means that we're doing a DynamoDB Query operation.  That's what
            # we expect here because we're doing an exact pkey search, and is more efficient than a Scan.
            "Multicorn:   DynamoDB: Query table dynamodbfdw-string-pkey from us-east-1",
            "Multicorn:           \"AttributeValueList\": [",
            # Should have both pkey & skey being sent in the KeyConditions.
            "Multicorn:             \"pkey-value-1\"",
            "Multicorn:           \"ComparisonOperator\": \"EQ\"",
        ]
        assert_contains_rows(query_plan, required_query_plan_rows)

@pytest.fixture
def string_string_table_data(pg_connection, string_string_table):
    with pg_connection.cursor() as cur:
        cur.execute(
            sql.SQL('INSERT INTO dynamodbfdw_import_test.{} (pkey, skey, document) VALUES (%s, %s, %s)').format(sql.Identifier(string_string_table)),
            ['pkey-value-1', 'skey-value-1', json.dumps({'doc-attr-1': 'doc-value-1'})]
        )
        pg_connection.commit()
    yield
    with pg_connection.cursor() as cur:
        cur.execute(sql.SQL('DELETE FROM dynamodbfdw_import_test.{}').format(sql.Identifier(string_string_table)))
        pg_connection.commit()

def test_sort_table_data_read(pg_connection, import_schema, string_string_table, string_string_table_data):
    with pg_connection.cursor() as cur:
        cur.execute(sql.SQL('SELECT oid, pkey, skey, document FROM dynamodbfdw_import_test.{}').format(sql.Identifier(string_string_table)))
        data = cur.fetchall()
        assert data == [(
            '{"pkey": "pkey-value-1", "skey": "skey-value-1"}',
            'pkey-value-1',
            'skey-value-1',
            {'pkey': 'pkey-value-1', 'skey': 'skey-value-1', 'doc-attr-1': 'doc-value-1'}
        )]

def test_explain_pkey_equal(pg_connection, import_schema, string_string_table):
    with pg_connection.cursor() as cur:
        cur.execute(
            sql.SQL('EXPLAIN SELECT * FROM dynamodbfdw_import_test.{} WHERE pkey = %s AND skey = %s').format(sql.Identifier(string_string_table)),
            ['pkey-value-1', 'skey-value-1']
        )
        query_plan = cur.fetchall()
        # It's possible that micro adjustments in this query plan will happen w/ new versions of Postgres or Multicorn,
        # so we try to be a bit flexible and just pick out the important bits we want to test for.
        required_query_plan_rows = [
            "Foreign Scan on \"dynamodbfdw-string-pkey-string-skey\"",
            "Filter: ((pkey = 'pkey-value-1'::text) AND (skey = 'skey-value-1'::text))",
            "Multicorn: DynamoDB: pagination provider",
            # "Query table", rather than "Scan table", means that we're doing a DynamoDB Query operation.  That's what
            # we expect here because we're doing an exact pkey search, and is more efficient than a Scan.
            "Multicorn:   DynamoDB: Query table dynamodbfdw-string-pkey-string-skey from us-east-1",
            "Multicorn:           \"AttributeValueList\": [",
            # Should have both pkey & skey being sent in the KeyConditions.
            "Multicorn:             \"pkey-value-1\"",
            "Multicorn:             \"skey-value-1\"",
            "Multicorn:           \"ComparisonOperator\": \"EQ\"",
        ]
        assert_contains_rows(query_plan, required_query_plan_rows)

@pytest.fixture
def custom_import_schema(pg_connection, multicorn_dynamo, string_table):
    with pg_connection.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS dynamodbfdw_manual_test CASCADE")
        cur.execute("CREATE SCHEMA dynamodbfdw_manual_test")
        cur.execute(f"""
            CREATE FOREIGN TABLE dynamodbfdw_manual_test."{string_table}" (
                oid TEXT,
                pkey TEXT OPTIONS (mapped_attr 'pkey', partition_key 'true'),
                test_text_field TEXT OPTIONS (mapped_attr 'test_attr'),
                test_bytea_field BYTEA OPTIONS (mapped_attr 'test_binary_attr'),
                document JSON OPTIONS (ddb_document 'true')
            ) SERVER multicorn_dynamo OPTIONS (
                aws_region '{aws_region}',
                table_name '{string_table}'
            )
        """)

@pytest.fixture
def string_table_custom_schema_data(pg_connection, string_table):
    with pg_connection.cursor() as cur:
        cur.execute(
            sql.SQL("INSERT INTO dynamodbfdw_manual_test.{} (pkey, test_text_field, test_bytea_field, document) VALUES (%s, %s, %s, %s)").format(sql.Identifier(string_table)),
            ['pkey-value-1', 'test custom field with mapped_attr', b'\x00\x01\x02\x03', json.dumps({'doc-attr-1': 'doc-value-1'})]
        )
        pg_connection.commit()
    yield
    with pg_connection.cursor() as cur:
        cur.execute(sql.SQL('DELETE FROM dynamodbfdw_manual_test.{}').format(sql.Identifier(string_table)))
        pg_connection.commit()

def test_mapped_attr_insert(pg_connection, custom_import_schema, string_table, string_table_custom_schema_data):
    with pg_connection.cursor() as cur:
        # Query to ensure that test_text_field was populated in the DDB record
        cur.execute(sql.SQL('SELECT oid, pkey, test_text_field, test_bytea_field, document FROM dynamodbfdw_manual_test.{}').format(sql.Identifier(string_table)))
        oid, pkey, test_text_field, test_bytea_field, document = cur.fetchall()[0]
        assert pkey == 'pkey-value-1'
        assert test_text_field == 'test custom field with mapped_attr'
        assert test_bytea_field.tobytes() == b'\x00\x01\x02\x03' # psycopg2 returns bytea as a memoryview
        assert document == {
            'pkey': 'pkey-value-1',
            'test_attr': 'test custom field with mapped_attr',
            'test_binary_attr': '\\x00010203',
            'doc-attr-1': 'doc-value-1'
        }
