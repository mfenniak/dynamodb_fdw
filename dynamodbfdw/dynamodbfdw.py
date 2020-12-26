from collections import namedtuple
from functools import lru_cache
from queue import Queue, Full
import threading
from multicorn import ForeignDataWrapper, TableDefinition, ColumnDefinition, ANY
from multicorn.utils import log_to_postgres, ERROR, WARNING, DEBUG, INFO
from botocore.config import Config
import boto3
import simplejson as json

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
LocalSecondaryIndex = namedtuple('LocalSecondaryIndex', ['pg_field_name', 'ddb_lsi_name', 'ddb_field_name'])

class RowProvider(object):
    # "abstract" properties expected to be implemented:
    # scanned_count
    # local_count
    # page_count

    def get_rows(self, table):
        raise NotImplementedError()

    def explain(self, verbose, aws_region, table_name):
        raise NotImplementedError()


class PaginatedRowProvider(RowProvider):
    def __init__(self):
        super().__init__()
        self.scanned_count = 0
        self.local_count = 0
        self.page_count = 0

    def get_rows(self, table):
        last_evaluated_key = None
        while True:
            resp = self.get_page(table, last_evaluated_key)

            self.scanned_count += resp['ScannedCount']
            self.local_count += resp['Count']
            self.page_count += 1

            data_page = resp['Items']
            for ddb_row in data_page:
                yield ddb_row
            last_evaluated_key = resp.get('LastEvaluatedKey')
            log_to_postgres("LastEvaluatedKey from query/scan: %r" % (last_evaluated_key,), DEBUG)
            if last_evaluated_key is None:
                break

    def explain(self, verbose, aws_region, table_name):
        yield "DynamoDB: pagination provider"
        for line in self.explain_page(verbose, aws_region, table_name):
            yield "  %s" % line

    def get_page(self, table, last_evaluated_key):
        raise NotImplementedError()

    def explain_page(self, verbose, aws_region, table_name):
        raise NotImplementedError()


class QueryRowProvider(PaginatedRowProvider):
    def __init__(self, query_params):
        super().__init__()
        self.query_params = query_params

    def get_page(self, table, last_evaluated_key):
        my_query_params = {}
        my_query_params.update(self.query_params)
        if last_evaluated_key is not None:
            my_query_params['ExclusiveStartKey'] = last_evaluated_key
        log_to_postgres("performing QUERY operation: %r" % (my_query_params,), DEBUG)
        return table.query(**my_query_params)

    def explain_page(self, verbose, aws_region, table_name):
        yield "DynamoDB: Query table %s from %s" % (table_name, aws_region)
        qp = json.dumps(self.query_params, sort_keys=True, indent=2)
        for line in qp.split('\n'):
            yield '  %s' % line


class ScanRowProvider(PaginatedRowProvider):
    def __init__(self, scan_params):
        super().__init__()
        self.scan_params = scan_params

    def get_page(self, table, last_evaluated_key):
        my_scan_params = {}
        my_scan_params.update(self.scan_params)
        if last_evaluated_key is not None:
            my_scan_params['ExclusiveStartKey'] = last_evaluated_key
        log_to_postgres("performing SCAN operation: %r" % (my_scan_params,), DEBUG)
        return table.scan(**my_scan_params)

    def explain_page(self, verbose, aws_region, table_name):
        yield "DynamoDB: Scan table %s from %s" % (table_name, aws_region)


class MultiQueryRowProvider(RowProvider):
    def __init__(self, partition_key, multi_query_values, addt_query_params):
        super().__init__()
        self.query_providers = [
            self.make_query_provider(partition_key, qv, addt_query_params)
            for qv in multi_query_values
        ]

    def make_query_provider(self, partition_key, query_value, addt_query_params):
        query_params = {}
        query_params.update(addt_query_params)

        # We're going to start mutating KeyConditions; to avoid mutating the same dict
        # stored in addt_query_params multiple times, we've gotta copy it
        kc = {}
        kc.update(query_params.get('KeyConditions', {}))
        query_params['KeyConditions'] = kc

        query_params['KeyConditions'][partition_key.ddb_field_name] = {
            'AttributeValueList': [query_value],
            'ComparisonOperator': 'EQ',
        }
        return QueryRowProvider(query_params)

    @property
    def scanned_count(self):
        return sum([s.scanned_count for s in self.query_providers])

    @property
    def local_count(self):
        return sum([s.local_count for s in self.query_providers])

    @property
    def page_count(self):
        return sum([s.page_count for s in self.query_providers])

    def get_rows(self, table):
        for qp in self.query_providers:
            for row in qp.get_rows(table):
                yield row

    def explain(self, verbose, aws_region, table_name):
        yield "DynamoDB: Consolidate %s Query operations" % (len(self.query_providers),)
        for counter, qp in enumerate(self.query_providers):
            yield "  Query %s:" % (counter)
            for line in qp.explain(verbose, aws_region, table_name):
                yield "    %s" % line


class ParallelScanIterator(object):
    def __init__(self, queue, workers, threads):
        super().__init__()
        self.queue = queue
        self.workers = workers
        self.threads = threads

    def __del__(self):
        for thread in self.threads:
            thread.kill_signal = True

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def next(self):
        while True:
            item = self.queue.get()
            if item is not_found_sentinel:
                self.workers -= 1
                if self.workers == 0:
                    raise StopIteration()
            else:
                return item


class ParallelScanThread(threading.Thread):
    def __init__(self, table, row_provider, queue):
        super().__init__()
        self.row_provider = row_provider
        self.queue = queue
        self.table = table
        self.kill_signal = False

    def run(self):
        for row in self.row_provider.get_rows(self.table):
            while True:
                if self.kill_signal:
                    # This will only happen if our iterator has been deleted, in which case putting
                    # the not_found_sentinel into the queue doesn't matter
                    return
                try:
                    self.queue.put(row, timeout=1)
                    break
                except Full:
                    # we'll try again; but also check for kill_signal
                    pass
        self.queue.put(not_found_sentinel)


class ParallelScanRowProvider(RowProvider):
    def __init__(self, parallel_scan_count):
        super().__init__()
        self.total_segments = parallel_scan_count
        self.scan_providers = [
            ScanRowProvider({ "Segment": i, "TotalSegments": self.total_segments })
            for i in range(self.total_segments)
        ]

    @property
    def scanned_count(self):
        return sum([s.scanned_count for s in self.scan_providers])

    @property
    def local_count(self):
        return sum([s.local_count for s in self.scan_providers])

    @property
    def page_count(self):
        return sum([s.page_count for s in self.scan_providers])

    def get_rows(self, table):
        log_to_postgres("DynamoDB FDW SCAN operation; this can be costly and time-consuming; use partition_key if possible", WARNING)
        queue = Queue(self.total_segments * 10)
        threads = [ParallelScanThread(table, s, queue) for s in self.scan_providers]
        for t in threads:
            t.start()
        return ParallelScanIterator(queue, len(self.scan_providers), threads)

    def explain(self, verbose, aws_region, table_name):
        yield "DynamoDB: parallel scan provider; %s concurrent segments" % (self.total_segments)
        for line in self.scan_providers[0].explain(verbose, aws_region, table_name):
            yield "  %s" % line


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
        parallel_scan_count = options.get('parallel_scan_count', '8')
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
            local_secondary_indexes = table.local_secondary_indexes
            if local_secondary_indexes is not None:
                for lsi in table.local_secondary_indexes:
                    if lsi.get('Projection') != {'ProjectionType': 'ALL'}:
                        # Technically we can read from an LSI that doesn't project all the attributes, but the
                        # record fields in PostgreSQL will randomly have different values depending on whether
                        # we select those LSIs to query.  This will result in really inconsistent looking data...
                        # Probably the only way to get around that is to create separate "tables" for each LSI
                        # and only query them explicitly.  Not supported (yet?)... so we'll just skip any LSI
                        # that isn't an ALL projection.
                        continue
                    ddb_lsi_name = lsi['IndexName']
                    for key in lsi['KeySchema']:
                        if key['KeyType'] == 'RANGE':
                            columns.append(
                                ColumnDefinition(
                                    key['AttributeName'],
                                    type_name='TEXT',
                                    options={
                                        'lsi_name': ddb_lsi_name,
                                        'lsi_key': key['AttributeName']
                                    }
                                )
                            )
            columns.append(
                ColumnDefinition('document', type_name='JSON', options={'ddb_document': 'true'})
            )
            yield TableDefinition(table.name,
                columns=columns,
                options={
                    'aws_region': aws_region,
                    'table_name': table.name,
                    'parallel_scan_count': parallel_scan_count,
                }
            )

    def __init__(self, options, columns):
        super(DynamoFdw, self).__init__(options, columns)
        self.aws_region = options['aws_region']
        self.table_name = options['table_name']
        self.parallel_scan_count = int(options.get('parallel_scan_count', '8'))
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
        for column in self.columns.values():
            pkey = column.options.get('partition_key', None)
            if pkey is not None:
                return KeyField(pg_field_name=column.column_name, ddb_field_name=pkey)
        return not_found_sentinel

    @property
    @lru_cache()
    def sort_key(self):
        for column in self.columns.values():
            skey = column.options.get('sort_key', None)
            if skey is not None:
                return KeyField(pg_field_name=column.column_name, ddb_field_name=skey)
        return not_found_sentinel

    @property
    @lru_cache()
    def local_secondary_indexes(self):
        lsis = []
        for column in self.columns.values():
            lsi_name = column.options.get('lsi_name', None)
            lsi_key = column.options.get('lsi_key', None)
            if lsi_name is not None and lsi_key is not None:
                lsis.append(LocalSecondaryIndex(
                    pg_field_name=column.column_name,
                    ddb_lsi_name=lsi_name,
                    ddb_field_name=lsi_key,
                ))
            elif lsi_name is not None or lsi_key is not None:
                log_to_postgres("DynamoDB FDW column must have both lsi_name and lsi_key", ERROR)
        return lsis

    @property
    @lru_cache()
    def document_field(self):
        for column in self.columns.values():
            ddb_document = column.options.get('ddb_document', None)
            if ddb_document is not None:
                return KeyField(pg_field_name=column.column_name, ddb_field_name=None)
        return not_found_sentinel

    def get_optional_exact_field(self, quals, field):
        for qual in quals:
            if qual.field_name == field and qual.operator == '=':
                return qual.value
        return not_found_sentinel # None is a valid qual.value; so we don't use None here

    def compute_sort_key_condition(self, quals, sort_key):
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
            if qual.field_name == sort_key.pg_field_name:
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

    def plan_query(self, quals):
        # Search for any multi-partition-key qualifiers; eg. partition_key IN ('1, '2')
        # Those will be specialized to a multi-Query operation.
        multi_query = None
        for qual in quals:
            if qual.field_name == self.partition_key.pg_field_name and qual.list_any_or_all is ANY and qual.operator[0] == '=':
                multi_query = qual.value
                break

        if multi_query is not None:
            # init query_params so that we can put sort-key or LSI info into it; but it won't
            # contain the multi_query data
            query_params = {
                'KeyConditions': {}
            }
            log_to_postgres("partition key is a multi-query for: %r" % (multi_query,), DEBUG)
        else:
            partition_key_value = self.get_optional_exact_field(quals, self.partition_key.pg_field_name)
            if partition_key_value is not_found_sentinel:
                # No partition key filtering was found in a supported manner, so... scan that.
                return ParallelScanRowProvider(self.parallel_scan_count)

            log_to_postgres("partition_key_value search for: %r" % (partition_key_value,), DEBUG)
            query_params = {
                'KeyConditions': {}
            }
            query_params['KeyConditions'][self.partition_key.ddb_field_name] = {
                'AttributeValueList': [partition_key_value],
                'ComparisonOperator': 'EQ',
            }

        sort_key_being_queried = False
        if self.sort_key is not not_found_sentinel:
            sort_key_cond = self.compute_sort_key_condition(quals, self.sort_key)
            log_to_postgres("sort_key_cond repr: %r" % (sort_key_cond,), DEBUG)
            if sort_key_cond is not not_found_sentinel:
                query_params['KeyConditions'][self.sort_key.ddb_field_name] = sort_key_cond
                sort_key_being_queried = True

        if not sort_key_being_queried:
            for lsi in self.local_secondary_indexes:
                sort_key_cond = self.compute_sort_key_condition(quals, lsi)
                if sort_key_cond is not not_found_sentinel:
                    query_params['KeyConditions'][lsi.ddb_field_name] = sort_key_cond
                    query_params['IndexName'] = lsi.ddb_lsi_name
                    break # stop iterating LSIs, we can only use one

        log_to_postgres("query_params repr: %r" % (query_params,), DEBUG)
        if multi_query is not None:
            return MultiQueryRowProvider(self.partition_key, multi_query, query_params)
        else:
            return QueryRowProvider(query_params)

    def explain(self, quals, columns, sortkeys=None, verbose=False):
        row_provider = self.plan_query(quals)
        return row_provider.explain(verbose, aws_region=self.aws_region, table_name=self.table_name)

    def execute(self, quals, columns):
        log_to_postgres("quals repr: %r" % (quals,), DEBUG)
        log_to_postgres("columns repr: %r" % (columns,), DEBUG)
        log_to_postgres("columns repr: %r" % (columns,), DEBUG)

        row_provider = self.plan_query(quals)
        table = get_table(self.aws_region, self.table_name)

        try:
            for ddb_row in row_provider.get_rows(table):
                pg_row = {}
                pg_row[self.partition_key.pg_field_name] = ddb_row[self.partition_key.ddb_field_name]
                if self.sort_key is not not_found_sentinel:
                    pg_row[self.sort_key.pg_field_name] = ddb_row[self.sort_key.ddb_field_name]
                # at this point, pg_row contains all the unique identifiers of the row; exactly what oid needs to contain
                pg_row['oid'] = json.dumps(pg_row)
                # populate any local-secondary-index columns for PG-based filtering and consistency/display
                for lsi in self.local_secondary_indexes:
                    pg_row[lsi.pg_field_name] = ddb_row.get(lsi.ddb_field_name)
                pg_row[self.document_field.pg_field_name] = json.dumps(ddb_row, cls=MyJsonEncoder)
                yield pg_row
        finally:
            # this is wrapped in a finally because iteration may be aborted from a query's LIMIT clause, but we still want the log message
            log_to_postgres(
                "DynamoDB FDW retrieved %s pages containing %s records; DynamoDB scanned %s records server-side" %
                (row_provider.page_count, row_provider.local_count, row_provider.scanned_count), INFO)

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
        for lsi in self.local_secondary_indexes:
            v = value.get(lsi.pg_field_name, not_found_sentinel)
            if v is not not_found_sentinel:
                put_item[lsi.ddb_field_name] = v
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
