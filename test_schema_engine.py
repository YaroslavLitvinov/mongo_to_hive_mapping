#!/usr/bin/env python

import json
import bson
from bson.json_util import loads

from schema_engine import SchemaNode, SchemaEngine


def prepare_engine(collection_name):
    with open("test_files/json_schema2.txt", "r") as input_schema_f, open("test_files/bson_data2.txt", "r") as input_data_f:
        schema = [json.load(input_schema_f)]
        data = bson.json_util.loads( input_data_f.read() )
        return SchemaEngine(collection_name, schema, data)

def test_locate_parents():
    full_path = ['quotes', 'comments', 'items']
    list_path = full_path[1:]
    engine = prepare_engine( full_path[0] )
    engine_res = engine.locate(list_path)
    assert("items" == engine.locate(list_path).name)
    parents = [i.name for i in engine.locate(list_path).get_all_parents() if i.name is not None]
    assert(full_path==parents)


