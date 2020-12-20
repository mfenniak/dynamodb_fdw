FROM postgres:12

RUN apt-get update && \
    apt-get install -y \
    postgresql-12-python3-multicorn \
    postgresql-plpython3-12 \
    python3-setuptools \
    python3-boto3 \
    python3-simplejson \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*
COPY docker-init-dynamodb.sh /docker-entrypoint-initdb.d/docker-init-dynamodb.sh
COPY setup.py /tmp/dynamodb_fdw/setup.py
COPY dynamodbfdw /tmp/dynamodb_fdw/dynamodbfdw
RUN cd /tmp/dynamodb_fdw && python3 setup.py install && rm -rf /tmp/dynamodb_fdw
