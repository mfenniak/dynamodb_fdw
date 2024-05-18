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

def test_explain_pkey_join_nested_loop(pg_connection, import_schema, string_table):
    with pg_connection.cursor() as cur:
        cur.execute(
            sql.SQL('''
                EXPLAIN SELECT * FROM
                    dynamodbfdw_import_test.{tbl} a
                    INNER JOIN dynamodbfdw_import_test.{tbl} b ON (a.pkey = b.pkey)
                WHERE a.pkey = %s
                ''').format(tbl=sql.Identifier(string_table)),
            ['pkey-value-1']
        )
        query_plan = cur.fetchall()
        required_query_plan_rows = [
            # The key thing we're looking for here is that get_path_keys() has allowed multicorn->PG to know that the
            # estimated rows for the table aliased "b" is 1.  This will allow the planner to choose a nested loop join
            # if the query is selective enough, rather than a merge join.
            '  ->  Foreign Scan on "dynamodbfdw-string-pkey" b  (cost=20.00..300.00 rows=1 width=300)',
            "        Filter: ((pkey = 'pkey-value-1'::text) AND (a.pkey = pkey))",
        ]
        assert_contains_rows(query_plan, required_query_plan_rows)

def test_explain_pkey_skey_join_nested_loop(pg_connection, import_schema, string_string_table):
    with pg_connection.cursor() as cur:
        # more realistic join than test_explain_pkey_join_nested_loop -- different conditions on the sort key
        cur.execute(
            sql.SQL('''
                EXPLAIN SELECT * FROM
                    dynamodbfdw_import_test.{tbl} a
                    INNER JOIN dynamodbfdw_import_test.{tbl} b ON (a.pkey = b.pkey AND b.skey = 's-key-value-2')
                WHERE a.pkey = %s AND a.skey = %s
                ''').format(tbl=sql.Identifier(string_string_table)),
            ['pkey-value-1', 'skey-value-1']
        )
        query_plan = cur.fetchall()
        for row in query_plan:
            print(row)
        required_query_plan_rows = [
            # The key thing we're looking for here is that get_path_keys() has allowed multicorn->PG to know that the
            # estimated rows for the table aliased "b" is 1.  This will allow the planner to choose a nested loop join
            # if the query is selective enough, rather than a merge join.
            '  ->  Foreign Scan on "dynamodbfdw-string-pkey-string-skey" b  (cost=20.00..400.00 rows=1 width=400)',
            "        Filter: ((pkey = 'pkey-value-1'::text) AND (skey = 's-key-value-2'::text) AND (a.pkey = pkey) AND (skey = 's-key-value-2'::text))",
        ]
        assert_contains_rows(query_plan, required_query_plan_rows)

@pytest.fixture(scope='session')
def string_table_with_string_gsi(dynamodb_resource, dynamodb_client):
    table_name = 'dynamodbfdw-string-pkey-string-gsi'
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
                    'AttributeName': 'gsi-skey',
                    'AttributeType': 'S',
                },
            ],
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'pkey',
                    'KeyType': 'HASH',
                }
            ],
            BillingMode='PAY_PER_REQUEST',
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'gsi-skey',
                    'KeySchema': [
                        {
                            'AttributeName': 'gsi-skey',
                            'KeyType': 'HASH',
                        }
                    ],
                    'Projection': {
                        'ProjectionType': 'ALL',
                    }
                }
            ]
        )
        dynamodb_resource.Table(table_name).wait_until_exists()
    return table_name

@pytest.fixture
def string_table_with_string_gsi_data(pg_connection, string_table_with_string_gsi):
    with pg_connection.cursor() as cur:
        cur.execute(
            sql.SQL('INSERT INTO dynamodbfdw_import_test.{} (pkey, "gsi-skey", document) VALUES (%s, %s, %s)').format(sql.Identifier(string_table_with_string_gsi)),
            ['pkey-value-1', 'gsi-key-value-1', json.dumps({'doc-attr-1': 'doc-value-1'})]
        )
        pg_connection.commit()
    yield
    with pg_connection.cursor() as cur:
        cur.execute(sql.SQL('DELETE FROM dynamodbfdw_import_test.{}').format(sql.Identifier(string_table_with_string_gsi)))
        pg_connection.commit()


def test_query_by_gsi(pg_connection, import_schema, string_table_with_string_gsi, string_table_with_string_gsi_data):
    with pg_connection.cursor() as cur:
        cur.execute(
            sql.SQL('SELECT * FROM dynamodbfdw_import_test.{} WHERE "gsi-skey" = %s').format(sql.Identifier(string_table_with_string_gsi)),
            ['gsi-key-value-1']
        )
        data = cur.fetchall()
        assert data == [(
            '{"pkey": "pkey-value-1"}',
            'pkey-value-1',
            'gsi-key-value-1',
            {'pkey': 'pkey-value-1', 'gsi-skey': 'gsi-key-value-1', 'doc-attr-1': 'doc-value-1'}
        )]

def test_explain_gsikey_equal(pg_connection, import_schema, string_table_with_string_gsi):
    with pg_connection.cursor() as cur:
        cur.execute(
            sql.SQL('EXPLAIN SELECT * FROM dynamodbfdw_import_test.{} WHERE "gsi-skey" = %s').format(sql.Identifier(string_table_with_string_gsi)),
            ['pkey-value-1']
        )
        query_plan = cur.fetchall()
        for row in query_plan:
            print(repr(row))
        # It's possible that micro adjustments in this query plan will happen w/ new versions of Postgres or Multicorn,
        # so we try to be a bit flexible and just pick out the important bits we want to test for.
        required_query_plan_rows = [
            'Foreign Scan on "dynamodbfdw-string-pkey-string-gsi"  (cost=20.00..40000000000.00 rows=100000000 width=400)',
            '  Filter: ("gsi-skey" = \'pkey-value-1\'::text)',
            '  Multicorn: DynamoDB: pagination provider',
            # "Query table", rather than "Scan table", means that we're doing a DynamoDB Query operation.  That's what
            # we expect here because we're doing an exact GSI search, and is more efficient than a Scan.
            '  Multicorn:   DynamoDB: Query table dynamodbfdw-string-pkey-string-gsi from us-east-1',
            '  Multicorn:       "IndexName": "gsi-skey",',
            '  Multicorn:       "KeyConditions": {',
            '  Multicorn:         "gsi-skey": {',
            '  Multicorn:           "AttributeValueList": [',
            '  Multicorn:             "pkey-value-1"',
            '  Multicorn:           ],',
            '  Multicorn:           "ComparisonOperator": "EQ"',
        ]
        assert_contains_rows(query_plan, required_query_plan_rows)

def test_explain_gsi_join(pg_connection, import_schema, string_string_table, string_table_with_string_gsi):
    with pg_connection.cursor() as cur:
        # more realistic join than test_explain_pkey_join_nested_loop -- different conditions on the sort key
        cur.execute(
            sql.SQL('''
                EXPLAIN SELECT * FROM
                    dynamodbfdw_import_test.{string_string_table} a
                    INNER JOIN dynamodbfdw_import_test.{string_table_with_string_gsi} b ON (b."gsi-skey" = a.document->>'sub-id')
                WHERE a.pkey = %s
                ''').format(
                    string_string_table=sql.Identifier(string_string_table),
                    string_table_with_string_gsi=sql.Identifier(string_table_with_string_gsi),
                ),
            ['pkey-value-1']
        )
        query_plan = cur.fetchall()
        for row in query_plan:
            print(row)
        required_query_plan_rows = [
            # The key thing we're looking for here is that get_path_keys() has allowed multicorn->PG to know that the
            # estimated rows for the table aliased "b" is 1.  This will allow the planner to choose a nested loop join
            # if the query is selective enough, rather than a merge join.
            '  ->  Foreign Scan on "dynamodbfdw-string-pkey-string-gsi" b  (cost=20.00..400.00 rows=1 width=400)',
            '        Filter: ("gsi-skey" = (a.document ->> \'sub-id\'::text))',
            '        Multicorn:           "AttributeValueList": [',
            '        Multicorn:             "?"',
            '        Multicorn:           ],',
        ]
        assert_contains_rows(query_plan, required_query_plan_rows)

@pytest.fixture
def gsi_join_data(pg_connection, import_schema, string_table, string_table_with_string_gsi):
    with pg_connection.cursor() as cur:
        cur.execute(
            sql.SQL('INSERT INTO dynamodbfdw_import_test.{} (pkey, document) VALUES (%s, %s)').format(sql.Identifier(string_table)),
            ['pkey-value-1', json.dumps({'name': 'Jack Bauer', 'sub-id': 'doc-value-1'})]
        )
        cur.execute(
            sql.SQL('INSERT INTO dynamodbfdw_import_test.{} (pkey, document) VALUES (%s, %s)').format(sql.Identifier(string_table)),
            ['pkey-value-2', json.dumps({'name': 'Gil Grissom', 'sub-id': 'doc-value-2'})]
        )
        cur.execute(
            sql.SQL('INSERT INTO dynamodbfdw_import_test.{} (pkey, document) VALUES (%s, %s)').format(sql.Identifier(string_table)),
            ['pkey-value-3', json.dumps({'name': 'Taylor Swift'})]
        )
        cur.execute(
            sql.SQL('INSERT INTO dynamodbfdw_import_test.{} (pkey, "gsi-skey", document) VALUES (%s, %s, %s)').format(sql.Identifier(string_table_with_string_gsi)),
            ['other-key-value-1', 'doc-value-1', json.dumps({'occupation': 'Agent'})]
        )
        cur.execute(
            sql.SQL('INSERT INTO dynamodbfdw_import_test.{} (pkey, "gsi-skey", document) VALUES (%s, %s, %s)').format(sql.Identifier(string_table_with_string_gsi)),
            ['other-key-value-2', 'doc-value-2', json.dumps({'occupation': 'CSI'})]
        )
        pg_connection.commit()
    yield
    with pg_connection.cursor() as cur:
        cur.execute(sql.SQL('DELETE FROM dynamodbfdw_import_test.{}').format(sql.Identifier(string_table)))
        cur.execute(sql.SQL('DELETE FROM dynamodbfdw_import_test.{}').format(sql.Identifier(string_table_with_string_gsi)))
        pg_connection.commit()

def test_gsi_join(pg_connection, gsi_join_data, string_table, string_table_with_string_gsi):
    del pg_connection.notices[:] # clear out any notices from previous queries
    with pg_connection.cursor() as cur:
        # more realistic join than test_explain_pkey_join_nested_loop -- different conditions on the sort key
        cur.execute(
            sql.SQL('''
                SELECT
                    a.document->>'name' AS name, b.document->>'occupation' AS occupation
                FROM
                    dynamodbfdw_import_test.{string_table} a
                    INNER JOIN dynamodbfdw_import_test.{string_table_with_string_gsi} b ON (b."gsi-skey" = a.document->>'sub-id')
                WHERE
                    a.pkey IN (%s, %s)
                ORDER BY a.document->>'name'
                ''').format(
                    string_table=sql.Identifier(string_table),
                    string_table_with_string_gsi=sql.Identifier(string_table_with_string_gsi),
                ),
            ['pkey-value-1', 'pkey-value-2']
        )
        assert cur.fetchall() == [
            ("Gil Grissom", "CSI"),
            ("Jack Bauer", "Agent")
        ]
        assert pg_connection.notices == [
            # Each row retrieved from string_table will cause a Query operation to be performed on the GSI table because
            # we're joining on the GSI key (as opposed to scanning the entire table and doing a merge join or something
            # like that); caused by the get_path_keys() method in the FDW.  Absent get_path_keys the functional join
            # works just fine, but it reads the entire table.
            'NOTICE:  DynamoDB FDW retrieved 1 pages containing 1 records; DynamoDB scanned 1 records server-side\n',
            'NOTICE:  DynamoDB FDW retrieved 1 pages containing 1 records; DynamoDB scanned 1 records server-side\n',
            # And one query on the string_table itself:
            'NOTICE:  DynamoDB FDW retrieved 2 pages containing 2 records; DynamoDB scanned 2 records server-side\n',
        ]
