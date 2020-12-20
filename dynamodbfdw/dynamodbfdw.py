from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres, ERROR, WARNING, DEBUG, INFO
from botocore.config import Config
import boto3
import simplejson as json
import decimal

# "MVP" List:
# write up a single "start docker container"
# push docker container to docker hub
# (CI docker push?)
# cleanup unused files in repo
# rewrite README
# add warnings to output the number of scan/query API calls, and total number of records processed; Count & ScannedCount results
# reduce logging from WARNING
#
# Future:
# support multiple partition key values in a Query, rather than just one exact value
# Somehow support secondary indexes
# support multiple parallel scan operations if we can?
# send upstream sort key searches as part of query -- with support for EQ | LE | LT | GE | GT | BEGINS_WITH | BETWEEN
# not sure, but Query might support EQ | LE | LT | GE | GT | BEGINS_WITH | BETWEEN on partition keys as well -- we could use Query in more than just EQ case

def get_table(aws_region, table_name):
    boto_config = Config(region_name=aws_region)
    dynamodb = boto3.resource('dynamodb', config=boto_config)
    table = dynamodb.Table(table_name)
    return table

# Probably not doable: concept for automatically creating views w/ more easily accessible document structure
# I though I could automatically generate a view that has a typed definition for the table.  But DynamoDB columns
# can vary types between different records (excluding the paritition & sort keys), and DynamoDB doesn't provide
# metadata on the attributes and their types defined in the table.  So; I think this idea is dead, but I kept
# around the code for a bit in case something else comes to my mind here.
#
# def make_column(plpy, attr, src):
#     attr_type = attr['AttributeType']
#     attr_name = attr['AttributeName']
#     if attr_type == 'N':
#         src = '(%s)::numeric' % src
#     elif attr_type == 'B':
#         raise Exception('binary field types not supported yet')
#     # FIXME: other attr_type here
#     return '%s as %s' % (src, plpy.quote_ident(attr_name))
# 
# def find_attr(attribute_definitions, attr_name):
#     for pot_attr in attribute_definitions:
#         if pot_attr['AttributeName'] == attr_name:
#             return pot_attr
#     raise Exception('unable to find definition of HASH key attribute')
# 
# def define_dynamodb(plpy, aws_region, table_name):
#     table = get_table(aws_region, table_name)
#     view_name = table.table_name
# 
#     columns = []
#     key_attributes = set()
# 
#     for key in table.key_schema:
#         if key['KeyType'] == 'HASH':
#             attr = find_attr(table.attribute_definitions, key['AttributeName'])
#             if attr['AttributeType'] != 'S':
#                 # FIXME: To support non-string paritition keys, we're going to have to add multiple partition_key fields
#                 # to the FDW table so that we can work with the correct Postgres types without casts; it's doable
#                 raise Exception('only string partition keys are currently supported')
#             key_attributes.add(key['AttributeName'])
#             columns.append(make_column(plpy, attr, 'partition_key'))
#         elif key['KeyType'] == 'RANGE':
#             # FIXME: untested
#             attr = find_attr(table.attribute_definitions, key['AttributeName'])
#             if attr['AttributeType'] != 'S':
#                 # FIXME: To support non-string paritition keys, we're going to have to add multiple partition_key fields
#                 # to the FDW table so that we can work with the correct Postgres types without casts; it's doable
#                 raise Exception('only string sort keys are currently supported')
#             key_attributes.add(key['AttributeName'])
#             columns.append(make_column(plpy, attr, 'sort_key'))
# 
#     plpy.warning("attribute_definitions: %r" % table.attribute_definitions)
#     for attr in table.attribute_definitions:
#         attr_name = attr['AttributeName']
#         if attr_name in key_attributes:
#             # don't define them twice
#             continue
#         columns.append(make_column(plpy, attr, 'document->>[%s]' % plpy.quote_literal(attr_name)))
# 
#     sql = "CREATE VIEW %s AS SELECT " % plpy.quote_ident(view_name)
#     for c in columns:
#         sql += c
#         sql += ","
#     sql = sql[:-1]
#     sql += " FROM dynamodb WHERE "
#     sql += "region = %s AND " % plpy.quote_literal(aws_region)
#     sql += "table_name = %s" % plpy.quote_literal(table_name)
#     plpy.warning("sql: %r" % sql)
# 
#     plpy.execute("DROP VIEW IF EXISTS %s" % plpy.quote_ident(view_name))
#     plpy.execute(sql)
# 
#     return view_name

class MyJsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, set):
            return list(o)
        return super().default(o)

not_found_sentinel = object()

class DynamoFdw(ForeignDataWrapper):
    """
    DynamoDB foreign data wrapper

    Expected/required table schema:
    CREATE FOREIGN TABLE dynamodb (
        oid TEXT,
        region TEXT,
        table_name TEXT,
        partition_key TEXT,
        sort_key TEXT,
        document JSON NOT NULL
    ) server multicorn_dynamo
    """

    def __init__(self, options, columns):
        super(DynamoFdw, self).__init__(options, columns)
        self.columns = columns
        self.pending_batch_write = []
        # FIXME: validate that the columns are exactly as expected, maybe?

    @property
    def rowid_column(self):
        return 'oid'

    def get_required_exact_field(self, quals, field):
        value = self.get_optional_exact_field(quals, field)
        if value is not_found_sentinel:
            log_to_postgres("You must query for a specific target %s" % field, ERROR)
        return value

    def get_optional_exact_field(self, quals, field):
        for qual in quals:
            if qual.field_name == field and qual.operator == '=':
                return qual.value
        return not_found_sentinel # None is a valid qual.value; so we don't use None here

    def execute(self, quals, columns):
        log_to_postgres("quals repr: %r" % (quals,), DEBUG)
        log_to_postgres("columns repr: %r" % (columns,), DEBUG)

        aws_region = self.get_required_exact_field(quals, 'region')
        table_name = self.get_required_exact_field(quals, 'table_name')

        table = get_table(aws_region, table_name)
        key_schema = table.key_schema # cache; not sure if this causes API calls on every access

        query_params = None
        partition_key_value = self.get_optional_exact_field(quals, 'partition_key')
        if partition_key_value is not not_found_sentinel:
            log_to_postgres("partition_key_value search for: %r" % (partition_key_value,), DEBUG)
            query_params = {
                'KeyConditions': {}
            }
            for key in key_schema:
                if key['KeyType'] == 'HASH':
                    query_params['KeyConditions'][key['AttributeName']] = {
                        'AttributeValueList': [partition_key_value],
                        'ComparisonOperator': 'EQ',
                    }
                    break
            else:
                raise Exception('unable to find hash key in key_schema')

        local_count = 0
        scanned_count = 0
        page_count = 0

        last_evaluated_key = None
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
                pg_row = {
                    'region': aws_region,
                    'table_name': table_name,
                }
                for key in key_schema:
                    if key['KeyType'] == 'HASH':
                        pg_row['partition_key'] = ddb_row[key['AttributeName']]
                    elif key['KeyType'] == 'RANGE':
                        pg_row['sort_key'] = ddb_row[key['AttributeName']]
                # at this point, pg_row contains all the unique identifiers of the row... which... is exactly what oid needs to contain
                pg_row['oid'] = json.dumps(pg_row)
                # FIXME: should I remove the keys from the document, so that they can't be used for conditions that won't be translated to queries?
                pg_row['document'] = json.dumps(ddb_row, cls=MyJsonEncoder)
                yield pg_row

            last_evaluated_key = resp.get('LastEvaluatedKey')
            log_to_postgres("LastEvaluatedKey from query/scan: %r" % (last_evaluated_key,), DEBUG)
            if last_evaluated_key is None:
                break

        log_to_postgres("DynamoDB FDW retrieved %s pages containing %s records; DynamoDB scanned %s records server-side" % (page_count, local_count, scanned_count), INFO)
    
    def delete(self, oid):
        # called once for each row to be deleted, with the oid value
        # oid value is a json encoded '{"region": "us-west-2", "table_name": "fdwtest2", "partition_key": "pkey2", "sort_key": "skey2"}'
        log_to_postgres("delete oid: %r" % (oid,), DEBUG)
        oid = json.loads(oid)
        self.pending_batch_write.append({
            'region': oid['region'],
            'table_name': oid['table_name'],
            'Delete': {
                'partition_key': oid['partition_key'],
                'sort_key': oid.get('sort_key', not_found_sentinel),
            }
        })

    def insert(self, value):
        # called once for each value to be inserted, where the value is the structure of the dynamodb FDW table; eg.
        # {'region': 'us-west-2', 'table_name': 'fdwtest', 'partition_key': 'key74', 'sort_key': None, 'document': '{}'}
        log_to_postgres("insert row: %r" % (value,), DEBUG)
        self.pending_batch_write.append({
            'region': value['region'],
            'table_name': value['table_name'],
            'PutItem': {
                'partition_key': value['partition_key'],
                'sort_key': value['sort_key'],
                'document': json.loads(value['document'])
            }
        })

    def update(self, oldvalues, newvalues):
        # WARNING:  update oldvalues: 'blahblah3', newvalues: {'region': 'us-west-2', 'table_name': 'fdwtest', 'partition_key': 'woot', 'sort_key': None, 'document': '{"id": "blahblah3", "string_set": ["s2", "s1", "s5"]}'}
        # WARNING:  update oldvalues: 'idkey2', newvalues: {'region': 'us-west-2', 'table_name': 'fdwtest', 'partition_key': 'woot', 'sort_key': None, 'document': '{"id": "idkey2", "number_column": 1234.5678}'}
        # WARNING:  update oldvalues: 'idkey7', newvalues: {'region': 'us-west-2', 'table_name': 'fdwtest', 'partition_key': 'woot', 'sort_key': None, 'document': '{"map_column": {"field1": "value1"}, "id": "idkey7"}'}
        # WARNING:  update oldvalues: 'idkey1', newvalues: {'region': 'us-west-2', 'table_name': 'fdwtest', 'partition_key': 'woot', 'sort_key': None, 'document': '{"string_column": "This is a string column", "id": "idkey1"}'}
        log_to_postgres("update oldvalues: %r, newvalues: %r" % (oldvalues, newvalues), DEBUG)
        log_to_postgres("UPDATE operation is not currently supported on DynamoDB FDW tables", ERROR)
        pass

    def begin(self, serializable):
        # create an empty batch write buffer
        log_to_postgres("FDW transaction BEGIN", DEBUG)
        self.pending_batch_write = []

    def pre_commit(self):
        # submit the batch write buffer to DynamoDB
        log_to_postgres("pre_commit; %s write operations in buffer" % (len(self.pending_batch_write)), DEBUG)

        # NOOP if the batch write buffer is empty, because pre_commit will be called even if no write operations have occurred
        if len(self.pending_batch_write) == 0:
            return

        # group all the pending batch writes by region & table
        by_region = {}
        for write in self.pending_batch_write:
            region = write['region']
            table_name = write['table_name']
            within_region = by_region.setdefault(region, {})
            within_table = within_region.setdefault(table_name, [])
            within_table.append(write)
         
        for region, tables in by_region.items():
            for table_name, items in tables.items():
                log_to_postgres("pre_commit; %s writes to perform to table %s in region %s" % (len(items), table_name, region), DEBUG)
                table = get_table(region, table_name)
                
                key_schema = table.key_schema
                partition_key_attr_name = None
                sort_key_attr_name = None
                for key in key_schema:
                    if key['KeyType'] == 'HASH':
                        partition_key_attr_name = key['AttributeName']
                    elif key['KeyType'] == 'RANGE':
                        sort_key_attr_name = key['AttributeName']
                if partition_key_attr_name == None:
                    log_to_postgres("unable to find partition key in key_schema for table %s" % (table_name,), ERROR)

                with table.batch_writer() as batch:
                    for item in items:
                        item_del_op = item.get('Delete', not_found_sentinel)
                        if item_del_op is not not_found_sentinel:
                            delete_key = {}
                            delete_key[partition_key_attr_name] = item_del_op['partition_key']
                            skey = item_del_op['sort_key']
                            if skey is not not_found_sentinel:
                                if sort_key_attr_name == None:
                                    log_to_postgres("unable to find sort key in key_schema for table %s" % (table_name,), ERROR)
                                delete_key[sort_key_attr_name] = skey
                            batch.delete_item(Key=delete_key)

                        item_ins_op = item.get('PutItem', not_found_sentinel)
                        if item_ins_op is not not_found_sentinel:
                            put_item = {}
                            put_item.update(item_ins_op['document'])
                            put_item[partition_key_attr_name] = item_ins_op['partition_key']
                            skey = item_ins_op['sort_key']
                            if skey is not None:
                                if sort_key_attr_name == None:
                                    log_to_postgres("unable to find sort key in key_schema for table %s" % (table_name,), ERROR)
                                put_item[sort_key_attr_name] = skey
                            batch.put_item(Item=put_item)

        self.pending_batch_write = []

    def rollback(self):
        # discard the batch write buffer
        log_to_postgres("FDW rollback; clearing %s write operations from buffer" % (len(self.pending_batch_write)), DEBUG)
        self.pending_batch_write = []
