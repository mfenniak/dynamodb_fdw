from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres, ERROR, WARNING, DEBUG
from botocore.config import Config
import boto3
import simplejson as json
import decimal

# "MVP" List:
# support insert
# support delete
# put error in place for update
# write up a single "start docker container"
# push docker container to docker hub
# (CI docker push?)
# cleanup unused files in repo
# rewrite README
# add warnings to output the number of scan/query API calls, and total number of records processed; Count & ScannedCount results
# test whether logging an error interrupts work, or needs to have a raise as well
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
        # FIXME: validate that the columns are exactly as expected, maybe?

    @property
    def rowid_column(self):
        return 'oid'

    def get_required_exact_field(self, quals, field):
        value = self.get_optional_exact_field(quals, field)
        if value is not_found_sentinel:
            log_to_postgres("You must query for a specific target %s" % field, ERROR)
            # FIXME: raise?
        return value

    def get_optional_exact_field(self, quals, field):
        for qual in quals:
            if qual.field_name == field and qual.operator == '=':
                return qual.value
        return not_found_sentinel # None is a valid qual.value; so we don't use None here

    def execute(self, quals, columns):
        log_to_postgres("quals repr: %r" % (quals,), WARNING)
        log_to_postgres("columns repr: %r" % (columns,), WARNING)

        aws_region = self.get_required_exact_field(quals, 'region')
        table_name = self.get_required_exact_field(quals, 'table_name')

        table = get_table(aws_region, table_name)
        key_schema = table.key_schema # cache; not sure if this causes API calls on every access
        # log_to_postgres("key_schema repr: %r" % (key_schema,), WARNING)

        query_params = None

        partition_key_value = self.get_optional_exact_field(quals, 'partition_key')
        if partition_key_value is not not_found_sentinel:
            log_to_postgres("partition_key_value search for: %r" % (partition_key_value,), WARNING)
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

        last_evaluated_key = None
        while True:
            if query_params is not None:
                my_query_params = {}
                my_query_params.update(query_params)
                if last_evaluated_key is not None:
                    my_query_params['ExclusiveStartKey'] = last_evaluated_key
                log_to_postgres("performing QUERY operation: %r" % (my_query_params,), WARNING)
                resp = table.query(**my_query_params)
            else:
                my_scan_params = {}
                if last_evaluated_key is not None:
                    my_scan_params['ExclusiveStartKey'] = last_evaluated_key
                log_to_postgres("performing SCAN operation: %r" % (my_scan_params,), WARNING)
                resp = table.scan(**my_scan_params)

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
            log_to_postgres("LastEvaluatedKey from query/scan: %r" % (last_evaluated_key,), WARNING)
            if last_evaluated_key is None:
                break
    
    def delete(self, oldvalues):
        # WARNING:  delete oldvalues: 'blahblah3'
        # WARNING:  delete oldvalues: 'idkey2'
        # WARNING:  delete oldvalues: 'idkey7'
        # WARNING:  delete oldvalues: 'idkey1'
        log_to_postgres("delete oldvalues: %r" % (oldvalues,), WARNING)
        pass

    def insert(self, values):
        # WARNING:  insert values: {'region': 'us-west-2', 'table_name': 'fdwtest', 'partition_key': 'key74', 'sort_key': None, 'document': '{}'}
        log_to_postgres("insert values: %r" % (values,), WARNING)
        pass

    def update(self, oldvalues, newvalues):
        # WARNING:  update oldvalues: 'blahblah3', newvalues: {'region': 'us-west-2', 'table_name': 'fdwtest', 'partition_key': 'woot', 'sort_key': None, 'document': '{"id": "blahblah3", "string_set": ["s2", "s1", "s5"]}'}
        # WARNING:  update oldvalues: 'idkey2', newvalues: {'region': 'us-west-2', 'table_name': 'fdwtest', 'partition_key': 'woot', 'sort_key': None, 'document': '{"id": "idkey2", "number_column": 1234.5678}'}
        # WARNING:  update oldvalues: 'idkey7', newvalues: {'region': 'us-west-2', 'table_name': 'fdwtest', 'partition_key': 'woot', 'sort_key': None, 'document': '{"map_column": {"field1": "value1"}, "id": "idkey7"}'}
        # WARNING:  update oldvalues: 'idkey1', newvalues: {'region': 'us-west-2', 'table_name': 'fdwtest', 'partition_key': 'woot', 'sort_key': None, 'document': '{"string_column": "This is a string column", "id": "idkey1"}'}
        log_to_postgres("update oldvalues: %r, newvalues: %r" % (oldvalues, newvalues), WARNING)
        pass

    def begin(self, serializable):
        # concept: create an empty batch write buffer
        # FIXME: need to see if instances of this class are re-used across connections?  Is buffering in `self` safe?
        log_to_postgres("begin", WARNING)
        pass

    def pre_commit(self):
        # concept: submit the batch write buffer
        # FIXME: NOOP if the batch write buffer is empty, because pre_commit will be called even if no write operations have occurred
        log_to_postgres("pre_commit", WARNING)
        pass

    def rollback(self):
        # concept: discard teh batch write buffer
        log_to_postgres("rollback", WARNING)
        pass

