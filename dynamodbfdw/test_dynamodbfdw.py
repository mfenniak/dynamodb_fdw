from . import dynamodbfdw
from boto3.dynamodb.types import Binary
import json

def test_json_encoder_set():
    json.dumps({ 'a set': set([1, 2, 3]) }, cls=dynamodbfdw.MyJsonEncoder)

def test_json_binary_set():
    json.dumps({ 'a binary': Binary(b'123') }, cls=dynamodbfdw.MyJsonEncoder)

def test_convert_bytes_to_binary():
    # Test with a single bytes object
    input_data = b'test_bytes'
    output_data = dynamodbfdw.map_python_types_to_dynamodb(input_data)
    assert isinstance(output_data, Binary)
    assert output_data.value == input_data

def test_convert_nested_dict_with_bytes():
    # Test with a nested dictionary containing bytes
    input_data = {
        'key1': b'bytes_data',
        'key2': {
            'nested_key1': b'nested_bytes_data',
            'nested_key2': 'string_data'
        },
        'key3': [b'list_bytes_data', 'list_string_data']
    }
    output_data = dynamodbfdw.map_python_types_to_dynamodb(input_data)

    assert isinstance(output_data['key1'], Binary)
    assert output_data['key1'].value == b'bytes_data'
    assert isinstance(output_data['key2']['nested_key1'], Binary)
    assert output_data['key2']['nested_key1'].value == b'nested_bytes_data'
    assert output_data['key2']['nested_key2'] == 'string_data'
    assert isinstance(output_data['key3'][0], Binary)
    assert output_data['key3'][0].value == b'list_bytes_data'
    assert output_data['key3'][1] == 'list_string_data'

def test_convert_list_with_bytes():
    # Test with a list containing bytes
    input_data = [b'bytes_data', 'string_data', {'nested_key': b'nested_bytes_data'}]
    output_data = dynamodbfdw.map_python_types_to_dynamodb(input_data)

    assert isinstance(output_data[0], Binary)
    assert output_data[0].value == b'bytes_data'
    assert output_data[1] == 'string_data'
    assert isinstance(output_data[2]['nested_key'], Binary)
    assert output_data[2]['nested_key'].value == b'nested_bytes_data'

def test_no_conversion_needed():
    # Test with a dictionary containing no bytes objects
    input_data = {
        'key1': 'string_data',
        'key2': 123,
        'key3': {
            'nested_key1': 'nested_string_data',
            'nested_key2': [1, 2, 3]
        }
    }
    output_data = dynamodbfdw.map_python_types_to_dynamodb(input_data)
    assert output_data == input_data
