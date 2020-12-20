from setuptools import setup
import os 

setup(
    name='DynamodbFdw',
    version='mf-0.2.0',
    author='Mathieu Fenniak',
    author_email='mathieu@fenniak.net',
    packages=['dynamodbfdw'],
    url='https://github.com/mfenniak/dynamodb_fdw',
    license='LICENSE.txt',
    description='Postgresql Foregin Data Wrapper mapping Amazon DynamoDB',
    install_requires=["boto3"],
)
