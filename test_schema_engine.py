#!/usr/bin/env python

import json
import bson
from bson.json_util import loads

from schema_engine import SchemaNode, SchemaEngine, SqlTable


def prepare_engine(collection_name):
    with open("test_files/json_schema2.txt", "r") as input_schema_f, \
         open("test_files/bson_data2.txt", "r") as input_data_f:
        schema = [json.load(input_schema_f)]
        data = bson.json_util.loads( input_data_f.read() )
        return SchemaEngine(collection_name, schema, data)

def get_test_node(full_path):
    engine = prepare_engine( full_path[0] )
    if len(full_path) > 1:
        return engine.locate(full_path[1:])
    else:
        return engine.root_node

def test_locate_parents():
    full_path = ['quotes', 'comments', 'items']
    root = get_test_node([full_path[0]])
    assert("items" == root.locate(full_path[1:]).name)
    parents = [i.name \
               for i in root.locate(full_path[1:]).all_parents() \
               if i.name is not None]
    assert(full_path==parents)

def test_all_aliases():
    def test_alias(full_path, assert_name):
        node1 = get_test_node(full_path)
        assert(assert_name == node1.short_alias())
        
    test_alias(['quotes', 'comments', '_id', 'oid'], 'id_oid')
    test_alias(['quotes', 'comments', 'quotes_id_oid'], 'quotes_id_oid')

    test_alias(['quotes', 'comments', 'items', 'quotes_id_oid'], 'quotes_id_oid')
    test_alias(['quotes', 'comments', 'items', 'quotes_comments_id_oid'], 'quotes_comments_id_oid')

def test_one_column(sqltable, colname, values):
    sqlcol = sqltable.sql_columns[colname]
    assert(sqlcol.name == colname)
    assert(sqlcol.values == values)

def test_quotes_table():
    quotes = SqlTable(get_test_node(['quotes']))
    test_one_column(quotes, 'body', ['body3'])
    

def test_tables():
    print SqlTable(get_test_node(['quotes', 'comments']))
    print SqlTable(get_test_node(['quotes', 'comments', 'items']))


