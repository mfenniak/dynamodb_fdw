from multicorn import ForeignDataWrapper, TableDefinition, ColumnDefinition
from multicorn.utils import log_to_postgres, ERROR, WARNING, DEBUG, INFO
from botocore.config import Config
from collections import namedtuple
from functools import lru_cache
import boto3
import simplejson as json
import decimal

def get_dynamodb(aws_region):
    boto_config = Config(region_name=aws_region)
    dynamodb = boto3.resource('dynamodb', config=boto_config)
    return dynamodb

def get_table(aws_region, table_name):
    dynamodb = get_dynamodb(aws_region)
    table = dynamodb.Table(table_name)
    return table

class MyJsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, set):
            return list(o)
        return super().default(o)

not_found_sentinel = object()
KeyField = namedtuple('KeyField', ['pg_field_name', 'ddb_field_name'])

class DynamoFdw(ForeignDataWrapper):
    """
    DynamoDB foreign data wrapper

    Expected/required table schema:
    CREATE FOREIGN TABLE dynamodb (
        oid TEXT,
        partition_key TEXT OPTIONS ( partition_key 'id' ),
        sort_key TEXT OPTIONS ( sort_key 'skey' ),
        document JSON OPTIONS ( ddb_document 'true' )
    ) server multicorn_dynamo OPTIONS (
        aws_region 'us-west-2',
        table_name 'remote_table'
    )
    """

    @classmethod
    def import_schema(self, schema, srv_options, options, restriction_type, restricts):
        log_to_postgres("schema repr: %r" % (schema,), DEBUG)
        log_to_postgres("srv_options repr: %r" % (srv_options,), DEBUG)
        log_to_postgres("options repr: %r" % (options,), DEBUG)
        log_to_postgres("restriction_type repr: %r" % (restriction_type,), DEBUG)
        log_to_postgres("restricts repr: %r" % (restricts,), DEBUG)
        # restriction_type repr: 'limit'  (or 'except', or None)
        # restricts repr: ['table_a', 'table_b']

        aws_region = options['aws_region']
        dynamodb = get_dynamodb(aws_region)
        for table in dynamodb.tables.all():
            if restriction_type == 'limit':
                if table.name not in restricts:
                    continue
            elif restriction_type == 'except':
                if table.name in restricts:
                    continue
            columns = [
                ColumnDefinition('oid', type_name='TEXT'),
            ]
            for key in table.key_schema:
                # FIXME: only string partition/sort keys supported currently
                if key['KeyType'] == 'HASH':
                    columns.append(
                        ColumnDefinition(key['AttributeName'], type_name='TEXT', options={'partition_key': key['AttributeName']})
                    )
                elif key['KeyType'] == 'RANGE':
                    columns.append(
                        ColumnDefinition(key['AttributeName'], type_name='TEXT', options={'sort_key': key['AttributeName']})
                    )
            columns.append(
                ColumnDefinition('document', type_name='JSON', options={'ddb_document': 'true'})
            )
            yield TableDefinition(table.name,
                columns=columns,
                options={
                    'aws_region': aws_region,
                    'table_name': table.name,
                }
            )

    def __init__(self, options, columns):
        super(DynamoFdw, self).__init__(options, columns)
        self.aws_region = options['aws_region']
        self.table_name = options['table_name']
        self.columns = columns
        self.pending_batch_write = []
        if self.partition_key is not_found_sentinel:
            log_to_postgres("DynamoDB FDW table must have a column w/ partition_key option", ERROR)
        if self.document_field is not_found_sentinel:
            log_to_postgres("DynamoDB FDW table must have a column w/ ddb_document option", ERROR)

    @property
    def rowid_column(self):
        return 'oid'

    @property
    @lru_cache()
    def partition_key(self):
        for cname, column in self.columns.items():
            pkey = column.options.get('partition_key', None)
            if pkey is not None:
                return KeyField(pg_field_name=column.column_name, ddb_field_name=pkey)
        return not_found_sentinel

    @property
    @lru_cache()
    def sort_key(self):
        for cname, column in self.columns.items():
            skey = column.options.get('sort_key', None)
            if skey is not None:
                return KeyField(pg_field_name=column.column_name, ddb_field_name=skey)
        return not_found_sentinel

    @property
    @lru_cache()
    def document_field(self):
        for cname, column in self.columns.items():
            ddb_document = column.options.get('ddb_document', None)
            if ddb_document is not None:
                return KeyField(pg_field_name=column.column_name, ddb_field_name=None)
        return not_found_sentinel

    def get_optional_exact_field(self, quals, field):
        for qual in quals:
            if qual.field_name == field and qual.operator == '=':
                return qual.value
        return not_found_sentinel # None is a valid qual.value; so we don't use None here

    def compute_sort_key_condition(self, quals):
        # DynamoDB supports these operators on sort keys in KeyConditionExpression:
        # DynamoDB: EQ, PostgreSQL: =
        # DynamoDB: LT, PostgreSQL: <
        # DynamoDB: LE, PostgreSQL: <=
        # DynamoDB: GT, PostgreSQL: >
        # DynamoDB: GE, PostgreSQL: >=
        # DynamoDB: BETWEEN, PostgreSQL: my_sort_key >= b, my_sort_key <= c
        # DynamoDB: BEGINS_WITH, PostgreSQL: my_sort_key ~~ abc%

        # Search for any conditions on the sort key; we support a max of two conditions, but only specific
        # conditions... and return early with not_found_sentinel of more conditions are present.
        check1, check2 = None, None
        for qual in quals:
            if qual.field_name == self.sort_key.pg_field_name:
                if qual.operator == '<=':
                    if check2 != None:
                        # had multiple conditions; can't support that
                        return not_found_sentinel
                    check2 = qual
                elif qual.operator in ('=', '<', '>', '>=', '~~'):
                    if check1 != None:
                        # had multiple conditions; can't support that
                        return not_found_sentinel
                    check1 = qual

        # Looks like we could be a BETWEEN query...
        if check1 is not None and check2 is not None:
            # Only supported if it's >= and <=, because we can convert that to between
            if check2.operator != '<=' or check1.operator != '>=':
                return not_found_sentinel
            return {
                'ComparisonOperator': 'BETWEEN',
                'AttributeValueList': [
                    check1.value,
                    check2.value,
                ]
            }

        if check2 is not None:
            # half a BETWEEN; we should only get here if check1 was None
            check1 = check2

        if check1 is not None:
            op = check1.operator

            if check1.operator == '~~':
                # Can be converted into BEGINS_WITH if it only has a single % and it's at the end
                # Also have to deal with the fact that there could be \% and \_ in the string -- escaped literals.

                # First check if it's valid to convert to BEGINS_WITH.
                pattern_without_escaped_wildcards = check1.value.replace("\\%", "").replace("\\_", "")
                if not pattern_without_escaped_wildcards.endswith("%") or "_" in pattern_without_escaped_wildcards or "%" in pattern_without_escaped_wildcards[:-1]:
                    # pattern not supported
                    return not_found_sentinel

                # Remove the trailing % wildcard, and convert the escaped literals to literals.
                pattern_with_unescaped_wildcards = check1.value[:-1].replace("\\%", "%").replace("\\_", "_")
                return {
                    'ComparisonOperator': 'BEGINS_WITH',
                    'AttributeValueList': [
                        pattern_with_unescaped_wildcards
                    ]
                }

            op = {
                '=': 'EQ',
                '<': 'LT',
                '<=': 'LE',
                '>': 'GT',
                '>=': 'GE',
            }
            return {
                'ComparisonOperator': op[check1.operator],
                'AttributeValueList': [
                    check1.value
                ]
            }

        return not_found_sentinel

    def execute(self, quals, columns):
        log_to_postgres("quals repr: %r" % (quals,), DEBUG)
        log_to_postgres("columns repr: %r" % (columns,), DEBUG)
        log_to_postgres("columns repr: %r" % (columns,), DEBUG)

        table = get_table(self.aws_region, self.table_name)

        query_params = None
        partition_key_value = self.get_optional_exact_field(quals, self.partition_key.pg_field_name)
        if partition_key_value is not not_found_sentinel:
            log_to_postgres("partition_key_value search for: %r" % (partition_key_value,), DEBUG)
            query_params = {
                'KeyConditions': {}
            }
            query_params['KeyConditions'][self.partition_key.ddb_field_name] = {
                'AttributeValueList': [partition_key_value],
                'ComparisonOperator': 'EQ',
            }

            if self.sort_key is not not_found_sentinel:
                sort_key_cond = self.compute_sort_key_condition(quals)
                log_to_postgres("sort_key_cond repr: %r" % (sort_key_cond,), DEBUG)
                if sort_key_cond is not not_found_sentinel:
                    query_params['KeyConditions'][self.sort_key.ddb_field_name] = sort_key_cond
        log_to_postgres("query_params repr: %r" % (query_params,), DEBUG)

        local_count = 0
        scanned_count = 0
        page_count = 0

        last_evaluated_key = None
        try:
            while True:
                if query_params is not None:
                    my_query_params = {}
                    my_query_params.update(query_params)
                    if last_evaluated_key is not None:
                        my_query_params['ExclusiveStartKey'] = last_evaluated_key
                    log_to_postgres("performing QUERY operation: %r" % (my_query_params,), DEBUG)
                    resp = table.query(**my_query_params)
                else:
                    my_scan_params = {}
                    if last_evaluated_key is not None:
                        my_scan_params['ExclusiveStartKey'] = last_evaluated_key
                    else:
                        log_to_postgres("DynamoDB FDW SCAN operation; this can be costly and time-consuming; use partition_key if possible", WARNING)
                    log_to_postgres("performing SCAN operation: %r" % (my_scan_params,), DEBUG)
                    resp = table.scan(**my_scan_params)

                scanned_count += resp['ScannedCount']
                local_count += resp['Count']
                page_count += 1

                data_page = resp['Items']
                for ddb_row in data_page:
                    pg_row = {}
                    pg_row[self.partition_key.pg_field_name] = ddb_row[self.partition_key.ddb_field_name]
                    if self.sort_key is not not_found_sentinel:
                        pg_row[self.sort_key.pg_field_name] = ddb_row[self.sort_key.ddb_field_name]
                    # at this point, pg_row contains all the unique identifiers of the row; exactly what oid needs to contain
                    pg_row['oid'] = json.dumps(pg_row)
                    pg_row[self.document_field.pg_field_name] = json.dumps(ddb_row, cls=MyJsonEncoder)
                    yield pg_row

                last_evaluated_key = resp.get('LastEvaluatedKey')
                log_to_postgres("LastEvaluatedKey from query/scan: %r" % (last_evaluated_key,), DEBUG)
                if last_evaluated_key is None:
                    break
        finally:
            # this is wrapped in a finally because iteration may be aborted from a query's LIMIT clause, but we still want the log message
            log_to_postgres("DynamoDB FDW retrieved %s pages containing %s records; DynamoDB scanned %s records server-side" % (page_count, local_count, scanned_count), INFO)
    
    def delete(self, oid):
        # called once for each row to be deleted, with the oid value
        # oid value is a json encoded '{"pg_partition_key": "pkey2", "pg_sort_key": "skey2"}'
        log_to_postgres("delete oid: %r" % (oid,), DEBUG)
        oid = json.loads(oid)
        delete_item = {}
        delete_item[self.partition_key.ddb_field_name] = oid[self.partition_key.pg_field_name]
        if self.sort_key is not not_found_sentinel:
            delete_item[self.sort_key.ddb_field_name] = oid[self.sort_key.pg_field_name]
        self.pending_batch_write.append({
            'Delete': delete_item
        })

    def insert(self, value):
        # called once for each value to be inserted, where the value is the structure of the dynamodb FDW table; eg.
        # {'partition_key': 'key74', 'sort_key': None, 'document': '{}'}
        log_to_postgres("insert row: %r" % (value,), DEBUG)
        put_item = {}
        put_item.update(json.loads(value[self.document_field.pg_field_name]))
        put_item[self.partition_key.ddb_field_name] = value[self.partition_key.pg_field_name]
        if self.sort_key is not not_found_sentinel:
            put_item[self.sort_key.ddb_field_name] = value[self.sort_key.pg_field_name]
        self.pending_batch_write.append({
            'PutItem': put_item
        })

    def update(self, oldvalues, newvalues):
        # WARNING:  update oldvalues: 'blahblah3', newvalues: {'partition_key': 'woot', 'sort_key': None, 'document': '{"id": "blahblah3", "string_set": ["s2", "s1", "s5"]}'}
        # WARNING:  update oldvalues: 'idkey2', newvalues: {'partition_key': 'woot', 'sort_key': None, 'document': '{"id": "idkey2", "number_column": 1234.5678}'}
        # WARNING:  update oldvalues: 'idkey7', newvalues: {'partition_key': 'woot', 'sort_key': None, 'document': '{"map_column": {"field1": "value1"}, "id": "idkey7"}'}
        # WARNING:  update oldvalues: 'idkey1', newvalues: {'partition_key': 'woot', 'sort_key': None, 'document': '{"string_column": "This is a string column", "id": "idkey1"}'}
        log_to_postgres("update oldvalues: %r, newvalues: %r" % (oldvalues, newvalues), DEBUG)
        log_to_postgres("UPDATE operation is not currently supported on DynamoDB FDW tables", ERROR)
        pass

    def begin(self, serializable):
        # create an empty batch write buffer
        log_to_postgres("FDW transaction BEGIN", DEBUG)
        self.pending_batch_write = []

    def pre_commit(self):
        # submit the batch write buffer to DynamoDB
        log_to_postgres("pre_commit; %s writes to perform to table %s in region %s" % (len(self.pending_batch_write), self.table_name, self.aws_region), DEBUG)

        # NOOP if the batch write buffer is empty, because pre_commit will be called even if no write operations have occurred
        if len(self.pending_batch_write) == 0:
            return

        table = get_table(self.aws_region, self.table_name)
        with table.batch_writer() as batch:
            for item in self.pending_batch_write:
                item_del_op = item.get('Delete', not_found_sentinel)
                if item_del_op is not not_found_sentinel:
                    batch.delete_item(Key=item_del_op)
                item_ins_op = item.get('PutItem', not_found_sentinel)
                if item_ins_op is not not_found_sentinel:
                    batch.put_item(Item=item_ins_op)

        self.pending_batch_write = []

    def rollback(self):
        # discard the batch write buffer
        log_to_postgres("FDW rollback; clearing %s write operations from buffer" % (len(self.pending_batch_write)), DEBUG)
        self.pending_batch_write = []
