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
RUN mkdir /tmp/dynamodb_fdw
COPY setup.py /tmp/dynamodb_fdw
COPY dynamodbfdw /tmp/dynamodb_fdw/dynamodbfdw
RUN cd /tmp/dynamodb_fdw && python3 setup.py install

# FIXME: add postgresql-plpython3-12
# FIXME: cleanup apt-get update artifacts & caches

#    pgxnclient \
#    build-essential \
#    python3-dev \
#    postgresql-server-dev-12 \
#    python3-setuptools \
#    python3-boto3
# RUN pgxn install multicorn
