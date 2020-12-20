from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres, ERROR, WARNING, DEBUG
from botocore.config import Config
import boto3
import json


class DynamoFdw(ForeignDataWrapper):
    """
    A DynamoDB foreign data wrapper.

    """

    def __init__(self, options, columns):
         super(DynamoFdw, self).__init__(options, columns)
         self.columns = columns
         try:
            # self.aws_access_key_id = options['aws_access_key_id']
            # self.aws_secret_access_key = options['aws_secret_access_key']
            self.aws_region = options['aws_region']
            self.remote_table = options['remote_table']
            self.boto_config = Config(
                region_name = self.aws_region
            )
            self.dynamodb = boto3.resource('dynamodb', config=self.boto_config)
         except KeyError:
            log_to_postgres("You must specify these options when creating the FDW: aws_region,remote_table",ERROR)


    def get_table(self):
        # dynamodb = boto3.resource('dynamodb')
        # FIXME: should we cache this?
        table = self.dynamodb.Table(self.remote_table)

        # conn = boto.dynamodb.connect_to_region(self.aws_region,aws_access_key_id=self.aws_access_key_id,aws_secret_access_key=self.aws_secret_access_key)
        # table = conn.get_table(self.remote_table)
        # log_to_postgres(json.dumps(table),DEBUG)
        return table


    def filter_condition(self,quals):
        # for qual in quals:
        #     if qual.field_name == 'customer' and qual.operator == '=':
        #         return qual.value
        return None

    def execute(self, quals, columns):
        table = self.get_table()
        #result = table.scan()

        # customer = self.filter_condition(quals)

        # FIXME: pagination
        # FIXME: multiple parallel scan/querys?
        # FIXME: send some simple conditions upstream, especially PK & sort key operations, with query?

        for ddb_row in table.scan()['Items']:
            pg_row = {}
            for key, value in ddb_row.items():
                if isinstance(value, dict):
                    # convert to json text; expecting the table to have JSON as a field
                    pg_row[key] = json.dumps(value)
                elif isinstance(value, set):
                    # convert to an array, then to JSON text; theoretically it could be a postgres array but I think a JSON array will be easier to work with
                    pg_row[key] = json.dumps([x for x in value])
                else:
                    pg_row[key] = value
            # log_to_postgres("ddb repr: %r" % (ddb_row,), WARNING)
            # log_to_postgres("pg  repr: %r" % (pg_row,), WARNING)
            yield pg_row

        # try:
        #     log_to_postgres('Asking dynamodb for this columns: ' + json.dumps(list(columns)),DEBUG)
        #     result = table.query(customer,attributes_to_get=list(columns))
        # except:
        #     # TODO Dangerous query, replace to Error message
        #     log_to_postgres('Performing table.scan()')
        #     result = table.scan()
        #     
        # for item in result:
        #    #log_to_postgres(json.dumps(item),WARNING)
        #    yield item


