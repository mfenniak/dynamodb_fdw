# DynamoDB Foreign Data Wrapper for PostgreSQL

dynamodb_fdw allows for the querying and modification of data stored in AWS DynamoDB tables from PostgreSQL.

## Why?

Yes, this is the most important question!  Do not use this project!  I mean, other than when you really have to.

DynamoDB is an incredible NoSQL database.  When used correctly, as a developer you need to understand the access patterns that will be used to retrieve data, and create the appropriate partition keys, sort keys, and secondary indexes to make those access patterns fast and efficient.

But when you actually start *operating* a system that uses DynamoDB, someday you will find yourself in a situation where you want to know information that doesn't match your application's access patterns.  Here are some examples:

- Your application is experiencing an error due to unexpected data in the DynamoDB system.  But the application isn't logging useful information like the partition key & sort key, so you can't *find* the item to see what it is and start figuring out why.

- The size of your DynamoDB tables is larger than expected, and you want to perform an analysis of the data to understand the distribution of data across different dimensions.

These types of "I didn't design the system for this" workflows can't be done by DynamoDB alone.  So; you've got to go write some custom software to scan a DynamoDB table and... *snore*.

Enter PostgreSQL + dynamodb_fdw.  You write a quick SQL query; dynamodb_fdw deals with querying or scanning the DynamoDB data for you, and PostgreSQL provides you all the necessary filtering, analysis, and aggregation systems.  You can even link up to external systems that support connecting to PostgreSQL, as well.

## Testimonials

"I'm... not sure how to feel about that.  Impressed and kinda a little disgusted all at the same time." - Anonymous

## How to Use

To make it super easy, dynamodb_fdw comes in a Docker container with the software configured and ready to go.  All you need to do is:

```
docker run -d \
    -p 5432:5432 \
    -e POSTGRES_PASSWORD=a-postgres-password \
    -v $HOME/.aws:/var/lib/postgresql/.aws \
    mfenniak/dynamodb_fdw:latest
```

Here you're providing the AWS access keys that will be used to access AWS, and a password that you can use to connect to Postgres.  AWS credentials in this example were provided by sharing `$HOME/.aws` into the container, but can also be provided by any boto3 supported mechanism (eg. environment variables, instance profile metadata).  Any other options supported by the [docker standard PostgreSQL image](https://hub.docker.com/_/postgres) can also be used to customize the PostgreSQL server.

Once running, you can use any PostgreSQL client to access the DB and start running SQL.

Next you have to create one PostgreSQL table for every remote DynamoDB table that you want to interact with.  You can do this very quickly by using the [IMPORT FOREIGN SCHEMA](https://www.postgresql.org/docs/12/sql-importforeignschema.html) functionality.  In the below example, the PG schema ddb_usw2 is created, and all DynamoDB tables in the us-west-2 region are imported into that schema:

```
CREATE SCHEMA ddb_usw2;
IMPORT FOREIGN SCHEMA dynamodb
    FROM SERVER multicorn_dynamo
    INTO ddb_usw2
    OPTIONS ( aws_region 'us-west-2' );
```

Options supported in schema import:
- `aws_region`
  - Name of the AWS region which will be scanned for DynamoDB tables.
- `parallel_scan_count`
  - (optional) Imported tables have this `parallel_scan_count` value set on them; see the table-level documentation for description.  Defaults to 8 if not specified.

DynamoDB is a schema-less system except for the partition & sort keys.  dynamodb_fdw represents that accurately by providing most of the data in a `document` json field.  Your table schema, either imported, or manually created if you like, would look like this:

```
CREATE FOREIGN TABLE fdwtest2 (
    oid TEXT,
    partition_key TEXT OPTIONS ( partition_key 'id' ),
    sort_key TEXT OPTIONS ( sort_key 'skey' ),
    document JSON OPTIONS ( ddb_document 'true' )
) SERVER multicorn_dynamo OPTIONS (
    aws_region 'us-west-2',
    table_name 'fdwtest2'
)
```

Options supported on the table are:
- `aws_region`
  - Name of the AWS region which will be scanned for DynamoDB tables.
- `table_name`
  - Name of the DynamoDB table.
- `parallel_scan_count`
  - (optional) When DynamoDB scan operations need to be performed, they are split into multiple segments that are scanned in parallel to improve performance.  This is the number of parallel segmented scans used.  Defaults to 8 if not specified.

The fields in this example table are:
- `oid`
  - Internal composite primary key of `partition_key`, and `sort_key`, used to support write operations.  It can be ignored; don't query it, and don't bother providing it when INSERTing into a table.
- `partition_key`
  - The partition key of the DynamoDB table.  Any name can be used for the PostgreSQL column.  Only string partition keys are supported currently.  The option `partition_key` must be set to the name of the partition key on the DynamoDB table.  It is highly recommended that when querying `dynamodb`, you provide an exact `partition_key` query condition.  One field marked with `partition_key` option must be present.  Foreign schema import will set the PostgreSQL field name to the DynamoDB sort key name, which often requires quoting if it is not entirely lower-cased and alphanumeric.
  - One specific PostgreSQL query operation on the partition key will be translated into an optimized DynamoDB query; an exact match equality check on the partition key.
- `sort_key`
  - The sort key of the DynamoDB table.  Any name can be used for the PostgreSQL column.  Only string sort keys are supported currently.  The option `sort_key` must be set to the name of the sort key on the DynamoDB table.  If the DynamoDB table has no sort key, this field can be omitted.  Foreign schema import will set the PostgreSQL field name to the DynamoDB sort key name, which often requires quoting if it is not entirely lower-cased and alphanumeric.
  - Specific PostgreSQL query operations on the sort key will be translated into optimized DynamoDB queries.  Those operations include: single equality check, range checks (>, <, >=, <=), between checks, and LIKE operators that have a single wildcard at the end (eg. "begins with" filters).  All other filters will result in records being downloaded and filtered in PostgreSQL.
- `document`
  - JSON-structured version of the entire DynamoDB record.  Any name can be used for the PostgreSQL column.  One field marked with the option `ddb_document` must be present.

So, what can you do now?  Let's start simple, by querying a DynamoDB table:

```
=> SELECT document FROM fdwtest2 LIMIT 10;
WARNING:  DynamoDB FDW SCAN operation; this can be costly and time-consuming; use partition_key if possible
NOTICE:  DynamoDB FDW retrieved 1 pages containing 2004 records; DynamoDB scanned 2004 records server-side
                          document
------------------------------------------------------------
 {"text": "hello 830", "pkey": "key830", "skey": "key2830"}
 {"text": "hello 830", "pkey": "key830", "skey": "key830"}
 {"text": "hello 989", "pkey": "key989", "skey": "key2989"}
 {"text": "hello 989", "pkey": "key989", "skey": "key989"}
 {"text": "hello 453", "pkey": "key453", "skey": "key2453"}
 {"text": "hello 453", "pkey": "key453", "skey": "key453"}
 {"text": "hello 776", "pkey": "key776", "skey": "key2776"}
 {"text": "hello 776", "pkey": "key776", "skey": "key776"}
 {"text": "hello 877", "pkey": "key877", "skey": "key2877"}
 {"text": "hello 877", "pkey": "key877", "skey": "key877"}
(10 rows)
```

Neat!  Notice that there's a warning here about a SCAN operation being used.  If you can, it's possible to avoid that warning by providing a parition_key search:

```
=> SELECT document FROM fdwtest2 WHERE partition_key = 'key877';
NOTICE:  DynamoDB FDW retrieved 1 pages containing 2 records; DynamoDB scanned 2 records server-side
                          document
------------------------------------------------------------
 {"text": "hello 877", "pkey": "key877", "skey": "key2877"}
 {"text": "hello 877", "pkey": "key877", "skey": "key877"}
(2 rows)
```

You can always perform an EXPLAIN operation on a query to determine what type of DynamoDB interaction it will cause.  In the demo below, you can see that performing a search on the sort_key results in a scan operation; DynamoDB cannot perform a lookup on a sort key without specifying the partition key.  But in the second query where both a partition key and a sort key are provided, an efficient server-side query operation is performed rather than a complete table scan.

```
=> explain SELECT * FROM fdwtest2 WHERE sort_key = '123' LIMIT 1;
                                      QUERY PLAN
---------------------------------------------------------------------------------------
 Limit  (cost=20.00..420.00 rows=1 width=400)
   ->  Foreign Scan on fdwtest2  (cost=20.00..40000000000.00 rows=100000000 width=400)
         Filter: (sort_key = '123'::text)
         Multicorn: DynamoDB: parallel scan provider; 8 concurrent segments
         Multicorn:   DynamoDB: pagination provider
         Multicorn:     DynamoDB: Scan table fdwtest2 from us-west-2
(6 rows)

=> explain SELECT * FROM fdwtest2 WHERE partition_key = 'woot' and sort_key like 'abc%' LIMIT 1;
                                      QUERY PLAN
---------------------------------------------------------------------------------------
 Limit  (cost=20.00..420.00 rows=1 width=400)
   ->  Foreign Scan on fdwtest2  (cost=20.00..40000000000.00 rows=100000000 width=400)
         Filter: ((sort_key ~~ 'abc%'::text) AND (partition_key = 'woot'::text))
         Multicorn: DynamoDB: pagination provider
         Multicorn:   DynamoDB: Query table fdwtest2 from us-west-2
         Multicorn:     {
         Multicorn:       "KeyConditions": {
         Multicorn:         "pkey": {
         Multicorn:           "AttributeValueList": [
         Multicorn:             "woot"
         Multicorn:           ],
         Multicorn:           "ComparisonOperator": "EQ"
         Multicorn:         },
         Multicorn:         "skey": {
         Multicorn:           "AttributeValueList": [
         Multicorn:             "abc"
         Multicorn:           ],
         Multicorn:           "ComparisonOperator": "BEGINS_WITH"
         Multicorn:         }
         Multicorn:       }
         Multicorn:     }
(21 rows)
```

All of those things are basically things that DynamoDB can already do, though.  What can we do with the foreign-data wrapper that we can't do natively in DynamoDB?  How about an aggregation...


```
=> SELECT partition_key, count(*)
   FROM fdwtest2
   GROUP BY partition_key ORDER BY count desc LIMIT 5;
WARNING:  DynamoDB FDW SCAN operation; this can be costly and time-consuming; use partition_key if possible
NOTICE:  DynamoDB FDW retrieved 1 pages containing 2004 records; DynamoDB scanned 2004 records server-side
 partition_key | count
---------------+-------
 key2          |     4
 key1          |     4
 key100        |     2
 key101        |     2
 key1000       |     2
(5 rows)
```

Cool, any PostgreSQL aggregation will work on DynamoDB data.  It could be very, very slow if the table is large... but it will work.  How about filtering based upon the contents of the DynamoDB table, rather than the keys?

```
=> SELECT document FROM fdwtest2
   WHERE document->>'text' = 'hello 453';
WARNING:  DynamoDB FDW SCAN operation; this can be costly and time-consuming; use partition_key if possible
NOTICE:  DynamoDB FDW retrieved 1 pages containing 2004 records; DynamoDB scanned 2004 records server-side
                          document
------------------------------------------------------------
 {"text": "hello 453", "pkey": "key453", "skey": "key2453"}
 {"text": "hello 453", "pkey": "key453", "skey": "key453"}
(2 rows)
```

Again, it will tend to perform a full-scan and be slow... but that's neat!  Two more little tricks...

```
=> DELETE FROM fdwtest2
   WHERE document->>'text' = 'hello 453';
WARNING:  DynamoDB FDW SCAN operation; this can be costly and time-consuming; use partition_key if possible
NOTICE:  DynamoDB FDW retrieved 1 pages containing 2004 records; DynamoDB scanned 2004 records server-side
DELETE 2

=> INSERT INTO fdwtest2 (partition_key, sort_key, document)
   SELECT
     'key' || s,
     'key3' || s,
     json_build_object('text', 'hello ' || s, 'another-key', 'else')
   FROM generate_series(1, 2) s RETURNING partition_key, sort_key, document;
 partition_key | sort_key |                   document
---------------+----------+----------------------------------------------
 key1          | key31    | {"text" : "hello 1", "another-key" : "else"}
 key2          | key32    | {"text" : "hello 2", "another-key" : "else"}
(2 rows)

INSERT 0 2
```

DELETE & INSERT operations are both supported.  UPDATE is not currently.  Write operations are even transaction-aware -- if you make modifications in a PostgreSQL transaction, then they will not be written to DynamoDB until the transaction is committed.  Atomic PostgreSQL & DynamoDB updates are not guaranteed.

## Future Plans

dynamodb_fdw could be a bit more still, I think.  Here are some areas that it could be improved in the future:

- Partition & sort key support is exactly one-to-one mapped with what is supported by DynamoDB.  However, dynamodb_fdw could break down the conditions and perform multiple queries that are joined together... for example, (partition_key = 'a' or partition_key = 'b') would result in a full table scan now, but it could result in two query operations instead.
- Secondary indexes aren't ever used.  It seems possible to automatically match up query attempts with available secondary indexes.
- Most filtering is done by PostgreSQL, excluding the partition key query.  More filtering operations could be sent to DynamoDB to reduce the amount of data being retrieved.
- Haven't performed any testing on how the FDW works when DynamoDB is throttling API requests; I suspect it will not work well.

## Thanks

This is a fork of Fabio Rueda's original DynamoDB FDW implementation, https://github.com/avances123/dynamodb_fdw.  Not much remains of that original code base, but I thank Fabio for providing a great starting point!

## License

dynamodb_fdw carries forward the GPL3 license of its original implementation.
