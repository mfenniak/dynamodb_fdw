from . import dynamodbfdw
import json

def test_json_encoder_set():
    json.dumps({ 'a set': set([1, 2, 3]) }, cls=dynamodbfdw.MyJsonEncoder)
