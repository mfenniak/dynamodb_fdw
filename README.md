# DynamoDB Foreign Data Wrapper for PostgreSQL

dynamodb_fdw allows for the querying and modification of data stored in AWS DyanmoDB tables from PostgreSQL.

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
    -ePOSTGRES_PASSWORD=a-postgres-password \
    -eAWS_ACCESS_KEY_ID=AKIA...access.key... \
    -eAWS_SECRET_ACCESS_KEY=...secret.access.key... \
    mfenniak/dynamodb_fdw:latest
```

Here you're providing the AWS access keys that will be used to access AWS, and a password that you can use to connect to Postgres.  Any other options supported by the (docker standard PostgreSQL image)[https://hub.docker.com/_/postgres] can also be used.

Once running, you can con use any PostgreSQL client to access the DB and start running SQL.

The docker container has a pre-built foreign table called `dynamodb` with this structure:

```
CREATE FOREIGN TABLE dynamodb (
    oid TEXT,
    region TEXT,
    table_name TEXT,
    partition_key TEXT,
    sort_key TEXT,
    document JSON NOT NULL
) server multicorn_dynamo;
```

The fields in this table are:
- `oid` -- a composite primary key of `region`, `table_name`, `partition_key`, and `sort_key` used because multicorn (Python FDW wrapper) only supports a single column for write operations.  Basically, ignore this.
- `region` -- AWS region name, eg. `us-west-2`; you *must* filter on this when querying `dynamodb`
- `table_name` -- DynamoDB table name, eg. `my-dynamodb-table`; you *must* filter on this when querying `dynamodb`
- `partition_key` -- the partition key of the DynamoDB table.  Only string partition keys are supported currently.  It is highly recommended that when querying `dynamodb`, you provide an exact `partition_key` query condition.
- `sort_key` -- the sort key of the DynamoDB table.  Only string sort keys are supported currently.
- `document` -- a JSON-structured version of the entire DynamoDB record.


You *must* always provide a filter on `region` and `table_name` when querying this table.  It's a bit awkard, I'll admit.

So, what can you do?

Querying:

```
postgres=# SELECT document FROM dynamodb WHERE region = 'us-west-2' AND table_name = 'fdwtest2' LIMIT 10;
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

Neat!  If you can, it's possible to avoid that warning by providing a parition_key search...

```
postgres=# SELECT document FROM dynamodb WHERE region = 'us-west-2' AND table_name = 'fdwtest2' AND partition_key = 'key877';
NOTICE:  DynamoDB FDW retrieved 1 pages containing 2 records; DynamoDB scanned 2 records server-side
                          document
------------------------------------------------------------
 {"text": "hello 877", "pkey": "key877", "skey": "key2877"}
 {"text": "hello 877", "pkey": "key877", "skey": "key877"}
(2 rows)
```

Alright, well that's kinda boring.  How about some things that DynamoDB can't do natively?


```
postgres=# SELECT partition_key, count(*)
   FROM dynamodb WHERE region = 'us-west-2' AND table_name = 'fdwtest2'
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
postgres=# SELECT document FROM dynamodb WHERE region = 'us-west-2' AND table_name = 'fdwtest2' AND document->>'text' = 'hello 453';
WARNING:  DynamoDB FDW SCAN operation; this can be costly and time-consuming; use partition_key if possible
NOTICE:  DynamoDB FDW retrieved 1 pages containing 2004 records; DynamoDB scanned 2004 records server-side
                          document
------------------------------------------------------------
 {"text": "hello 453", "pkey": "key453", "skey": "key2453"}
 {"text": "hello 453", "pkey": "key453", "skey": "key453"}
(2 rows)
```

Again, it will tend to perform a full-scan and be slow... but that's neat!  Two more little tricks, though...

```
postgres=# DELETE FROM dynamodb WHERE region = 'us-west-2' AND table_name = 'fdwtest2' AND document->>'text' = 'hello 453';
WARNING:  DynamoDB FDW SCAN operation; this can be costly and time-consuming; use partition_key if possible
NOTICE:  DynamoDB FDW retrieved 1 pages containing 2004 records; DynamoDB scanned 2004 records server-side
DELETE 2

postgres=# INSERT INTO dynamodb (region, table_name, partition_key, sort_key, document)
  SELECT 'us-west-2', 'fdwtest2', 'key' || s, 'key3' || s, json_build_object('text', 'hello ' || s, 'another-key', 'else')
  FROM generate_series(1, 2) s RETURNING partition_key, sort_key, document;
 partition_key | sort_key |                   document
---------------+----------+----------------------------------------------
 key1          | key31    | {"text" : "hello 1", "another-key" : "else"}
 key2          | key32    | {"text" : "hello 2", "another-key" : "else"}
(2 rows)

INSERT 0 2
```

DELETE & INSERT operations are both supported.  UPDATE is not currently.  Write operations are even transaction-aware -- if you make modifications in a PostgreSQL transaction, then they will not be written to DynamoDB until the transaction is commited.  Atomic PostgreSQL & DynamoDB updates are not guaranteed.

## Future Plans

dynamodb_fdw could be a bit more still, I think.  Here are some areas that it could be improved in the future:

- Currently only performs a "Query" operation when you do an exact search for a partition_key.  Some additional query operations could be supported.
- Secondary indexes aren't ever used.  It seems possible to automatically match up query attempts with available secondary indexes.
- "Scan" operations are done sequentially.  DynamoDB's API does support parallel scans, which could be implemented.
- Most filtering is done by PostgreSQL, excluding the partition key query.  More filtering operations could be sent to DynamoDB to reduce the amount of data being retrieved.
- Haven't performed any testing on how the FDW works when DynamoDB is throttling API requests; I suspect it will not work well.

## Thanks

This is a fork of Fabio Rueda's original DynamoDB FDW implementation, https://github.com/avances123/dynamodb_fdw.  Not much remains of that original code base, but I thank Fabio for providing a great starting point!

## License

dynamodb_fdw carries forward the GPL3 license of it's original implementation.
