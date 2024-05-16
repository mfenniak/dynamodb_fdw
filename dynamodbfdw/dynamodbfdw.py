from collections import namedtuple, OrderedDict
from functools import lru_cache
from queue import Queue, Full
import threading
from multicorn import ForeignDataWrapper, TableDefinition, ColumnDefinition, ANY
from multicorn.utils import log_to_postgres, ERROR, WARNING, DEBUG, INFO
from botocore.config import Config
import boto3
import simplejson as json
from boto3.dynamodb.types import Binary

def map_python_types_to_dynamodb(value):
    """
    Recursively convert all bytes objects to boto3's Binary wrapper in the given dictionary.
    """
    if isinstance(value, bytes):
        return Binary(value)
    elif isinstance(value, dict):
        return {k: map_python_types_to_dynamodb(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [map_python_types_to_dynamodb(v) for v in value]
    else:
        return value

def map_dynamodb_types_to_python(value):
    """
    Recursively convert all Binary objects to bytes in the given dictionary.
    """
    if isinstance(value, Binary):
        return value.value
    elif isinstance(value, dict):
        return {k: map_dynamodb_types_to_python(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [map_dynamodb_types_to_python(v) for v in value]
    else:
        return value

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
        elif isinstance(o, Binary):
            # format bytes into json doc as "\x00"...
            return "\\x" + o.value.hex()
        return super().default(o)

not_found_sentinel = object()
exception_sentinel = object()
KeyField = namedtuple('KeyField', ['pg_field_name', 'ddb_field_name'])
LocalSecondaryIndex = namedtuple('LocalSecondaryIndex', ['pg_field_name', 'ddb_lsi_name', 'ddb_field_name'])
GlobalSecondaryIndex = namedtuple('GlobalSecondaryIndex', ['partition_key', 'sort_key', 'ddb_gsi_name'])
QueryPlan = namedtuple('QueryPlan', ['row_provider', 'score'])
SortKeyQueryClause = namedtuple('SortKeyQueryClause', ['query_clause', 'score'])

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
        self.had_exception = False

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
            if item is not_found_sentinel or item is exception_sentinel:
                self.had_exception = self.had_exception or item is exception_sentinel
                self.workers -= 1
                if self.workers == 0:
                    if self.had_exception:
                        for thread in self.threads:
                            if thread.exception is not None:
                                raise thread.exception
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
        self.exception = None

    def run(self):
        try:
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
        except Exception as e:
            self.exception = e
            self.queue.put(exception_sentinel)


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
        partition_key TEXT OPTIONS ( mapped_attribute 'id' partition_key 'true' ),
        sort_key TEXT OPTIONS ( mapped_attribute 'skey' sort_key 'true' ),
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
            columns = OrderedDict()
            columns['oid'] = ColumnDefinition('oid', type_name='TEXT')
            for key in table.key_schema:
                # FIXME: only string partition/sort keys supported currently, but no checking here for that
                if key['KeyType'] == 'HASH':
                    columns[key['AttributeName']] = ColumnDefinition(
                        key['AttributeName'],
                        type_name='TEXT',
                        options={
                            'mapped_attr': key['AttributeName'],
                            'partition_key': 'true',
                        })
                elif key['KeyType'] == 'RANGE':
                    columns[key['AttributeName']] = ColumnDefinition(
                        key['AttributeName'],
                        type_name='TEXT',
                        options={
                            'mapped_attr': key['AttributeName'],
                            'sort_key': 'true'
                        })
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
                    sort_key = None
                    for key in lsi['KeySchema']:
                        if key['KeyType'] == 'RANGE':
                            sort_key = key
                            break
                    else:
                        log_to_postgres("DynamoDB LSI on table %s had a no RANGE key and is not supported" % (table.name), WARNING)
                        continue
                    column = columns.get(sort_key['AttributeName'], not_found_sentinel)
                    if column is not_found_sentinel:
                        column = ColumnDefinition(
                            sort_key['AttributeName'],
                            type_name='TEXT',
                            options={
                                'lsi_name': ddb_lsi_name,
                                'mapped_attr': sort_key['AttributeName']
                            }
                        )
                        columns[column.column_name] = column
                    else:
                        # column may already ahve an lsi_name on them; or may exist already... all in theory,
                        # I don't know why this would really happen.  But it is easy to support.
                        lsi_name = column.options.get('lsi_name', '')
                        lsi_name += ',%s' % ddb_lsi_name
                        if lsi_name.startswith(','):
                            lsi_name = lsi_name[1:]
                        column.options['lsi_name'] = lsi_name
                        column.options['mapped_attr'] = sort_key['AttributeName']

            global_secondary_indexes = table.global_secondary_indexes
            if global_secondary_indexes is not None:
                for gsi in table.global_secondary_indexes:
                    if gsi.get('Projection') != {'ProjectionType': 'ALL'}:
                        # Technically we can read from an GSI that doesn't project all the attributes, but the
                        # record fields in PostgreSQL will randomly have different values depending on whether
                        # we select those GSIs to query.  This will result in really inconsistent looking data...
                        # Probably the only way to get around that is to create separate "tables" for each GSI
                        # and only query them explicitly.  Not supported (yet?)... so we'll just skip any GSI
                        # that isn't an ALL projection.
                        continue
                    ddb_gsi_name = gsi['IndexName']
                    for key in gsi['KeySchema']:
                        option_name_gsi_name = None
                        if key['KeyType'] == 'HASH':
                            option_name_gsi_name = 'gsi_partition_key_gsi_name'
                        elif key['KeyType'] == 'RANGE':
                            option_name_gsi_name = 'gsi_sort_key_gsi_name'
                        else:
                            log_to_postgres("DynamoDB GSI on table %s had an unsupported key type and is not supported" % (table.name), WARNING)
                            break

                        column = columns.get(key['AttributeName'], not_found_sentinel)
                        if column is not_found_sentinel:
                            column = ColumnDefinition(
                                key['AttributeName'],
                                type_name='TEXT',
                                options={
                                    option_name_gsi_name: ddb_gsi_name,
                                    'mapped_attr': key['AttributeName']
                                }
                            )
                            columns[column.column_name] = column
                        else:
                            # column may already have an gsi on it; if so, extend it with a new value
                            gsi_name = column.options.get(option_name_gsi_name, '')
                            gsi_name += ',%s' % ddb_gsi_name
                            if gsi_name.startswith(','):
                                gsi_name = gsi_name[1:]
                            column.options[option_name_gsi_name] = gsi_name
                            column.options['mapped_attr'] = key['AttributeName']

            columns['document'] = ColumnDefinition('document', type_name='JSON', options={'ddb_document': 'true'})
            yield TableDefinition(table.name,
                columns=columns.values(),
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
            mapped_attr = column.options.get('mapped_attr', None)
            if pkey is not None and mapped_attr is not None:
                return KeyField(pg_field_name=column.column_name, ddb_field_name=mapped_attr)
        return not_found_sentinel

    @property
    @lru_cache()
    def sort_key(self):
        for column in self.columns.values():
            skey = column.options.get('sort_key', None)
            mapped_attr = column.options.get('mapped_attr', None)
            if skey is not None and mapped_attr is not None:
                return KeyField(pg_field_name=column.column_name, ddb_field_name=mapped_attr)
        return not_found_sentinel

    @property
    @lru_cache()
    def local_secondary_indexes(self):
        lsis = []
        for column in self.columns.values():
            lsi_names = column.options.get('lsi_name', None)
            mapped_attr = column.options.get('mapped_attr', None)
            if lsi_names is not None and mapped_attr is not None:
                # This supports having multiple LSIs on the same field... I don't know why you'd ever do this
                lsi_names = lsi_names.split(',')
                for lsi_name in lsi_names:
                    lsis.append(LocalSecondaryIndex(
                        pg_field_name=column.column_name,
                        ddb_lsi_name=lsi_name,
                        ddb_field_name=mapped_attr,
                    ))
            elif lsi_names is not None:
                log_to_postgres("DynamoDB FDW column must have both lsi_name and mapped_attr", ERROR)
        return lsis

    @property
    @lru_cache()
    def global_secondary_indexes(self):
        # temp dict gsi_name to gsi object
        gsi_dict = {}

        # first find all the distinct gsi_name's
        gsi_names = set()
        for column in self.columns.values():
            gsi_partition_key_gsi_names = column.options.get('gsi_partition_key_gsi_name', not_found_sentinel)
            if gsi_partition_key_gsi_names is not not_found_sentinel:
                for gsi_partition_key_gsi_name in gsi_partition_key_gsi_names.split(','):
                    gsi_names.add(gsi_partition_key_gsi_name)
            gsi_sort_key_gsi_names = column.options.get('gsi_sort_key_gsi_name', not_found_sentinel)
            if gsi_sort_key_gsi_names is not not_found_sentinel:
                for gsi_sort_key_gsi_name in gsi_sort_key_gsi_names.split(','):
                    gsi_names.add(gsi_sort_key_gsi_name)

        for gsi_name in gsi_names:
            for column in self.columns.values():
                gsi_partition_key_gsi_names = column.options.get('gsi_partition_key_gsi_name', not_found_sentinel)
                if gsi_partition_key_gsi_names is not not_found_sentinel:
                    gsi_partition_key_gsi_names = gsi_partition_key_gsi_names.split(',')
                    if gsi_name in gsi_partition_key_gsi_names:
                        attr = column.options['mapped_attr']
                        gsi = gsi_dict.get(gsi_name, GlobalSecondaryIndex(ddb_gsi_name=gsi_name, partition_key=None, sort_key=not_found_sentinel))
                        gsi = gsi._replace(partition_key=KeyField(pg_field_name=column.column_name, ddb_field_name=attr))
                        gsi_dict[gsi_name] = gsi

                gsi_sort_key_gsi_names = column.options.get('gsi_sort_key_gsi_name', not_found_sentinel)
                if gsi_sort_key_gsi_names is not not_found_sentinel:
                    gsi_sort_key_gsi_names = gsi_sort_key_gsi_names.split(',')
                    if gsi_name in gsi_sort_key_gsi_names:
                        attr = column.options['mapped_attr']
                        gsi = gsi_dict.get(gsi_name, GlobalSecondaryIndex(ddb_gsi_name=gsi_name, partition_key=None, sort_key=not_found_sentinel))
                        gsi = gsi._replace(sort_key=KeyField(pg_field_name=column.column_name, ddb_field_name=attr))
                        gsi_dict[gsi_name] = gsi

        for gsi in gsi_dict.values():
            if gsi.partition_key is None:
                log_to_postgres("DynamoDB GSI column %s had no partition_key" % (gsi.ddb_gsi_name,), ERROR)

        return gsi_dict.values()

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

    def plan_sort_key_query_clauses(self, quals, sort_key):
        # Yield instances of SortKeyQueryClause = namedtuple('SortKeyQueryClause', ['query_clause', 'score'])
        # DynamoDB supports these operators on sort keys in KeyConditionExpression:
        # DynamoDB: EQ, PostgreSQL: =;                                       score 7/7
        # DynamoDB: BETWEEN, PostgreSQL: my_sort_key >= b, my_sort_key <= c; score 6/7
        # DynamoDB: BEGINS_WITH, PostgreSQL: my_sort_key ~~ abc%;            score 5/6
        # DynamoDB: LT, PostgreSQL: <,                                       score 4/7
        # DynamoDB: GT, PostgreSQL: >,                                       score 3/7
        # DynamoDB: LE, PostgreSQL: <=,                                      score 2/7
        # DynamoDB: GE, PostgreSQL: >=,                                      score 1/7
        operator_scores = {
            'EQ': 7,
            'BETWEEN': 6,
            'BEGINS_WITH': 5,
            'LT': 4,
            'GT': 3,
            'LE': 2,
            'GE': 1,
        }
        pg_op_to_ddb_op = {
            '=': 'EQ',
            '<': 'LT',
            '<=': 'LE',
            '>': 'GT',
            '>=': 'GE',
        }
        between_ge, between_le = not_found_sentinel, not_found_sentinel
        for qual in quals:
            if qual.field_name == sort_key.pg_field_name:
                if qual.operator == '<=' and between_le is not_found_sentinel:
                    between_le = qual.value
                elif qual.operator == '>=' and between_ge is not_found_sentinel:
                    between_ge = qual.value

                if qual.operator in pg_op_to_ddb_op:
                    yield SortKeyQueryClause(
                        query_clause={
                            'ComparisonOperator': pg_op_to_ddb_op[qual.operator],
                            'AttributeValueList': [
                                qual.value,
                            ]
                        },
                        score=operator_scores[pg_op_to_ddb_op[qual.operator]] / 7.0,
                    )
                if qual.operator == '~~':
                    # Can be converted into BEGINS_WITH if it only has a single % and it's at the end
                    # Also have to deal with the fact that there could be \% and \_ in the string -- escaped literals.
                    # First check if it's valid to convert to BEGINS_WITH.
                    pattern_without_escaped_wildcards = qual.value.replace("\\%", "").replace("\\_", "")
                    if not pattern_without_escaped_wildcards.endswith("%") or "_" in pattern_without_escaped_wildcards or "%" in pattern_without_escaped_wildcards[:-1]:
                        continue
                    # pattern supported
                    # Remove the trailing % wildcard, and convert the escaped literals to literals.
                    pattern_with_unescaped_wildcards = qual.value[:-1].replace("\\%", "%").replace("\\_", "_")
                    yield SortKeyQueryClause(
                        query_clause={
                            'ComparisonOperator': 'BEGINS_WITH',
                            'AttributeValueList': [
                                pattern_with_unescaped_wildcards
                            ]
                        },
                        score=operator_scores['BEGINS_WITH'] / 7.0,
                    )

        if between_ge is not not_found_sentinel and between_le is not not_found_sentinel:
            # Looks like we could be a BETWEEN query...
            yield SortKeyQueryClause(
                query_clause={
                    'ComparisonOperator': 'BETWEEN',
                    'AttributeValueList': [
                        between_ge,
                        between_le,
                    ]
                },
                score=operator_scores['BETWEEN'] / 7.0,
            )

    def plan_single_query(self, quals, partition_key, sort_key, local_secondary_indexes, index_name, pkey_score_bonus):
        partition_key_value = self.get_optional_exact_field(quals, partition_key.pg_field_name)
        if partition_key_value is not_found_sentinel:
            # Can't filter on this partition_key
            return

        query_params = {
            'KeyConditions': {}
        }
        query_params['KeyConditions'][partition_key.ddb_field_name] = {
            'AttributeValueList': [partition_key_value],
            'ComparisonOperator': 'EQ',
        }
        if index_name is not None:
            query_params['IndexName'] = index_name

        pkey_base_score = (pkey_score_bonus * 50) + 50 # bonus for a single-EQ partition key search
        yield QueryPlan(row_provider=QueryRowProvider(query_params), score=pkey_base_score)

        for qp in self.plan_sort_key_options(query_params, quals, sort_key, local_secondary_indexes, index_name, pkey_base_score):
            yield qp

    def plan_multi_query(self, quals, partition_key, sort_key, local_secondary_indexes, index_name, pkey_score_bonus):
        # Search for any multi-partition-key qualifiers; eg. partition_key IN ('1, '2')
        # Those will be specialized to a multi-Query operation.
        for qual in quals:
            if qual.field_name == partition_key.pg_field_name and qual.list_any_or_all is ANY and qual.operator[0] == '=':
                multi_query = qual.value
                # init query_params so that we can put sort-key or LSI info into it; but it won't
                # contain the multi_query data
                query_params = {
                    'KeyConditions': {}
                }
                log_to_postgres("partition key is a multi-query for: %r" % (multi_query,), DEBUG)
                if index_name is not None:
                    query_params['IndexName'] = index_name

                pkey_base_score = (pkey_score_bonus * 50)
                yield QueryPlan(row_provider=MultiQueryRowProvider(partition_key, multi_query, query_params), score=pkey_base_score)

                for orig_qp in self.plan_sort_key_options(query_params, quals, sort_key, local_secondary_indexes, index_name, pkey_base_score):
                    # convert the row_provider in the qp from a QueryRowProvider to a MultiQueryRowProvider
                    yield QueryPlan(row_provider=MultiQueryRowProvider(partition_key, multi_query, orig_qp.row_provider.query_params), score=orig_qp.score)

    def plan_sort_key_options(self, query_params, quals, sort_key, local_secondary_indexes, index_name, pkey_base_score):
        if sort_key is not not_found_sentinel:
            if index_name is None:
                # querying against primary partition key + sort key is the best
                sort_key_bonus_score = 1
            else:
                # querying against a GSI's partition key is good too
                sort_key_bonus_score = 0.5
            for skqc in self.plan_sort_key_query_clauses(quals, sort_key):
                score = pkey_base_score + (sort_key_bonus_score * skqc.score * 50)
                qp = json.loads(json.dumps(query_params)) # deep clone to avoid mutating
                qp['KeyConditions'][sort_key.ddb_field_name] = skqc.query_clause
                yield QueryPlan(row_provider=QueryRowProvider(qp), score=score)

        for lsi in local_secondary_indexes:
            sort_key_bonus_score = 0.01 # querying LSIs is not as prefered as other sort keys
            for skqc in self.plan_sort_key_query_clauses(quals, lsi):
                score = pkey_base_score + (sort_key_bonus_score * skqc.score * 50)
                qp = json.loads(json.dumps(query_params)) # deep clone to avoid mutating
                qp['KeyConditions'][lsi.ddb_field_name] = skqc.query_clause
                qp['IndexName'] = lsi.ddb_lsi_name
                yield QueryPlan(row_provider=QueryRowProvider(qp), score=score)

    def plan_by_key_pattern(self, quals, partition_key, sort_key, local_secondary_indexes, index_name, pkey_score_bonus):
        for qp in self.plan_multi_query(quals, partition_key, sort_key, local_secondary_indexes, index_name, pkey_score_bonus):
            yield qp
        for qp in self.plan_single_query(quals, partition_key, sort_key, local_secondary_indexes, index_name, pkey_score_bonus):
            yield qp

    def plan_query(self, quals):
        query_plans = [
            QueryPlan(row_provider=ParallelScanRowProvider(self.parallel_scan_count), score=0),
        ]
        query_plans.extend(self.plan_by_key_pattern(quals, self.partition_key, self.sort_key, self.local_secondary_indexes, None, pkey_score_bonus=1))
        for gsi in self.global_secondary_indexes:
            query_plans.extend(self.plan_by_key_pattern(quals, gsi.partition_key, gsi.sort_key, [], gsi.ddb_gsi_name, pkey_score_bonus=0.1))

        query_plans.sort(key=lambda qp: qp.score, reverse=True)
        # FIXME: should find some way to make visibility into the query plans considered, maybe?
        log_to_postgres("plan_query found %s valid query plans; selecting the top scoring at score = %s" % (len(query_plans), query_plans[0].score,), DEBUG)
        return query_plans[0].row_provider

    def explain(self, quals, columns, sortkeys=None, verbose=False):
        row_provider = self.plan_query(quals)
        return row_provider.explain(verbose, aws_region=self.aws_region, table_name=self.table_name)

    def execute(self, quals, columns):
        log_to_postgres("quals repr: %r" % (quals,), DEBUG)
        log_to_postgres("columns repr: %r" % (columns,), DEBUG)

        row_provider = self.plan_query(quals)
        table = get_table(self.aws_region, self.table_name)

        try:
            # FIXME: pass `columns` into get_rows and use them as a projection to only retrieve the columns requested
            for ddb_row in row_provider.get_rows(table):
                pg_row = {}
                pg_row[self.partition_key.pg_field_name] = ddb_row[self.partition_key.ddb_field_name]
                if self.sort_key is not not_found_sentinel:
                    pg_row[self.sort_key.pg_field_name] = ddb_row[self.sort_key.ddb_field_name]
                # at this point, pg_row contains all the unique identifiers of the row; exactly what oid needs to contain
                pg_row['oid'] = json.dumps(pg_row)

                # populate any secondary-index columns for PG-based filtering and consistency/display
                for lsi in self.local_secondary_indexes:
                    pg_row[lsi.pg_field_name] = ddb_row.get(lsi.ddb_field_name)
                for gsi in self.global_secondary_indexes:
                    pg_row[gsi.partition_key.pg_field_name] = ddb_row.get(gsi.partition_key.ddb_field_name)
                    if gsi.sort_key is not not_found_sentinel:
                        pg_row[gsi.sort_key.pg_field_name] = ddb_row.get(gsi.sort_key.ddb_field_name)

                # populate the document field
                pg_row[self.document_field.pg_field_name] = json.dumps(ddb_row, cls=MyJsonEncoder)

                # populate any other mapped attributes
                # FIXME: does this also replace all the LSI & GSI logic above, since they also have mapped_attr on them?
                # Maybe, but needs testing... integration tests don't cover these cases yet so I have no confidence in
                # it.
                for column_name, column in self.columns.items():
                    if column_name not in columns:
                        continue
                    mapped_attr = column.options.get('mapped_attr', not_found_sentinel)
                    if mapped_attr is not not_found_sentinel:
                        ddb_value = ddb_row.get(mapped_attr, not_found_sentinel)
                        if ddb_value is not not_found_sentinel:
                            pg_row[column_name] = map_dynamodb_types_to_python(ddb_value)

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

        # Add the document field to the put_item
        document_data = json.loads(value[self.document_field.pg_field_name])
        put_item.update(map_python_types_to_dynamodb(document_data))

        # Add the partition key and sort key
        put_item[self.partition_key.ddb_field_name] = map_python_types_to_dynamodb(value[self.partition_key.pg_field_name])
        if self.sort_key is not not_found_sentinel:
            put_item[self.sort_key.ddb_field_name] = map_python_types_to_dynamodb(value[self.sort_key.pg_field_name])

        # Add Local Secondary Index (LSI) fields
        for lsi in self.local_secondary_indexes:
            v = value.get(lsi.pg_field_name, not_found_sentinel)
            if v is not not_found_sentinel:
                put_item[lsi.ddb_field_name] = map_python_types_to_dynamodb(v)

        # Add Global Secondary Index (GSI) fields
        for gsi in self.global_secondary_indexes:
            gsi_pkey_value = value.get(gsi.partition_key.pg_field_name, not_found_sentinel)
            if gsi_pkey_value is not not_found_sentinel:
                put_item[gsi.partition_key.ddb_field_name] = map_python_types_to_dynamodb(gsi_pkey_value)
            if gsi.sort_key is not not_found_sentinel:
                gsi_skey_value = value.get(gsi.sort_key.pg_field_name, not_found_sentinel)
                if gsi_skey_value is not not_found_sentinel:
                    put_item[gsi.sort_key.ddb_field_name] = map_python_types_to_dynamodb(gsi_skey_value)

        # Add other fields marked with mapped_attr
        #
        # FIXME: does this also replace all the LSI & GSI logic above, since they also have mapped_attr on them? Maybe,
        # but needs testing... integration tests don't cover these cases yet so I have no confidence in it.
        for column_name, column in self.columns.items():
            mapped_attr = column.options.get('mapped_attr', not_found_sentinel)
            if mapped_attr is not not_found_sentinel:
                field_value = value.get(column_name, not_found_sentinel)
                if field_value is not not_found_sentinel:
                    put_item[mapped_attr] = map_python_types_to_dynamodb(field_value)

        self.pending_batch_write.append({
            'PutItem': put_item
        })

    def update(self, oldvalues, newvalues):
        # WARNING:  update oldvalues: 'blahblah3', newvalues: {'partition_key': 'woot', 'sort_key': None, 'document': '{"id": "blahblah3", "string_set": ["s2", "s1", "s5"]}'}
        # WARNING:  update oldvalues: 'idkey2', newvalues: {'partition_key': 'woot', 'sort_key': None, 'document': '{"id": "idkey2", "number_column": 1234.5678}'}
        # WARNING:  update oldvalues: 'idkey7', newvalues: {'partition_key': 'woot', 'sort_key': None, 'document': '{"map_column": {"field1": "value1"}, "id": "idkey7"}'}
        # WARNING:  update oldvalues: 'idkey1', newvalues: {'partition_key': 'woot', 'sort_key': None, 'document': '{"string_column": "This is a string column", "id": "idkey1"}'}
        #
        # Challenges to implementation:
        # - if oid doesn't change then it's the same logic as insert(), but if oid changes then it's a delete and insert
        #   which isn't transactionally safe...
        # - update() might theoretically, in the future, support partial updates (where newvalues doesn't contain every
        #   field) from multicorn; that wouldn't work with put_item
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

    def get_path_keys(self):
        path_keys = [
            # (('oid',), 1) # theoretically a search for oid would return a single row... but I don't think we actually
            # look for that in `execute`'s quals to support that search, so, don't offer it as a good option here.
        ]

        # Partition key + sort key should give us a single row; but if we don't have a sort key then the same is true
        # for just partition key.
        if self.partition_key is not not_found_sentinel:
            if self.sort_key is not not_found_sentinel:
                path_keys.append(((self.partition_key.pg_field_name, self.sort_key.pg_field_name), 1))
                # hypothetically we could add just the partition key as a path key... but we wouldn't know how many rows
                # it might return.  It could make sense to provide a value that is less than the default 100000000 to
                # indicate that this is still more reduced data set... but I'm not sure if that's a good idea or not
                # because the data in the table could still be any size.
                # path_keys.append(((self.partition_key.pg_field_name,), 50000000))
            else:
                path_keys.append(((self.partition_key.pg_field_name,), 1))

        # GSIs are also potential path keys where we know we could get a single row back.
        for gsi in self.global_secondary_indexes:
            if gsi.sort_key is not not_found_sentinel:
                path_keys.append(((gsi.partition_key.pg_field_name, gsi.sort_key.pg_field_name), 1))
                # Again, hypothetically we could add just the partition key as a path key...
                # path_keys.append(((gsi.partition_key.pg_field_name,), 50000000))
            else:
                path_keys.append(((gsi.partition_key.pg_field_name,), 1))

        # LSIs could be potential path keys but they don't guarantee a single row back.  Similar to the returning just
        # the partition key when we have (pkey/sortkey), it's plausible to include them with some synthetic row value
        # that is lower than the default, but I don't know if that's a good idea or not.
        # for lsi in self.local_secondary_indexes:
        #     if lsi.sort_key is not not_found_sentinel:
        #         path_keys.append(((lsi.pg_field_name, lsi.sort_key.pg_field_name), 50000000))
        #     else:
        #         path_keys.append(((lsi.pg_field_name,), 50000000))

        return path_keys
