from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres, ERROR, WARNING, DEBUG
from botocore.config import Config
import boto3
import simplejson as json
import decimal

class MyJsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, set):
            return list(o)
        return super().default(o)

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

    def get_table(self, aws_region, table_name):
        boto_config = Config(region_name=aws_region)
        dynamodb = boto3.resource('dynamodb', config=boto_config)
        table = dynamodb.Table(table_name)
        return table

    def get_required_exact_field(self, quals, field):
        for qual in quals:
            if qual.field_name == field and qual.operator == '=':
                return qual.value
        else:
            log_to_postgres("You must query for a specific target %s" % field, ERROR)

    def execute(self, quals, columns):
        # log_to_postgres("quals repr: %r" % (quals,), WARNING)
        # log_to_postgres("columns repr: %r" % (columns,), WARNING)

        aws_region = self.get_required_exact_field(quals, 'region')
        table_name = self.get_required_exact_field(quals, 'table_name')

        table = self.get_table(aws_region, table_name)
        # log_to_postgres("key_schema repr: %r" % (table.key_schema,), WARNING)

        # FIXME: pagination of results
        # FIXME: multiple parallel scan/querys?
        # FIXME: send some simple conditions upstream, especially PK & sort key operations, with query?

        key_schema = table.key_schema # cache; not sure if this causes API calls on every access

        for ddb_row in table.scan()['Items']:
            pg_row = {
                'region': aws_region,
                'table_name': table_name,
            }

            for key in key_schema:
                if key['KeyType'] == 'HASH':
                    pg_row['partition_key'] = ddb_row[key['AttributeName']]
                elif key['KeyType'] == 'RANGE':
                    # FIXME: untested
                    pg_row['sort_key'] = ddb_row[key['AttributeName']]

            # FIXME: should I remove the keys from the document, so that they can't be used for conditions that won't be translated to queries?
            pg_row['document'] = json.dumps(ddb_row, cls=MyJsonEncoder)

            yield pg_row
