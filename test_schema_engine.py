#!/usr/bin/env python

import os
import json
import bson
from bson.json_util import loads

#from schema_engine import create_schema_engine, SchemaNode, SchemaEngine, SqlTable, Tables
import schema_engine

files = {'a_inserts': ('test_files/json_schema2.txt',
                       'test_files/bson_data2.txt')}

def get_schema_engine(collection_name):
    dirpath=os.path.dirname(os.path.abspath(__file__))
    schema_fname = files[collection_name][0]
    schema_path = os.path.join(dirpath, schema_fname)
    return schema_engine.create_schema_engine(collection_name, schema_path)

def get_schema_tables(schema_engine_obj):
    collection_name = schema_engine_obj.root_node.name
    dirpath=os.path.dirname(os.path.abspath(__file__))
    data_fname = files[collection_name][1]
    data_path = os.path.join(dirpath, data_fname)
    return schema_engine.create_tables_load_file(schema_engine_obj, \
                                                 data_path)

def get_test_node(full_path):
    engine = get_schema_engine( full_path[0] )
    if len(full_path) > 1:
        return engine.locate(full_path[1:])
    else:
        return engine.root_node

def test_locate_parents():
    full_path = ['a_inserts', 'comments', 'items']
    root = get_test_node([full_path[0]])
    assert("items" == root.locate(full_path[1:]).name)
    parents = [i.name \
               for i in root.locate(full_path[1:]).all_parents() \
               if i.name is not None]
    assert(full_path==parents)

def test_all_aliases():
    def test_alias(full_path, short_alias, long_alias, long_plural_alias):
        node1 = get_test_node(full_path)
        assert(short_alias == node1.short_alias())
        assert(long_alias == node1.long_alias())
        assert(long_plural_alias == node1.long_plural_alias())
#test parental field name
    test_alias(['a_inserts', 'comments', 'a_inserts_id_oid'], \
               'a_inserts_id_oid', 'a_inserts_id_oid', 'a_inserts_id_oid')
#test structrure field name
    test_alias(['a_inserts', 'comments', 'body'], \
               'body', 'a_inserts_comments_body', \
               'a_insert_comment_body')
#test nested struct field name
    test_alias(['a_inserts', 'comments', '_id', 'oid'], \
               'id_oid', 'a_inserts_comments_id_oid', \
               'a_insert_comment_id_oid')
#test 1 level array name
    test_alias(['a_inserts'], 'a_inserts', 'a_inserts', 'a_inserts')
#test nested array name
    test_alias(['a_inserts', 'comments', 'items'], \
               'items', 'a_inserts_comments_items', \
               'a_insert_comment_items')

def check_one_column(sqltable, colname, values):
    sqlcol = sqltable.sql_columns[colname]
    assert(sqlcol.name == colname)
    assert(sqlcol.values == values)

def generate_insert_queries(table):
    """ get output as list of tuples :format string, parameters as tuple """
    queries = []
    fmt_string = "INSERT INTO table %s (%s) VALUES(%s);" \
                 % (table.table_name, \
                    ', '.join(table.sql_column_names), \
                    ', '.join(['%s' for i in table.sql_column_names]))
    firstcolname = table.sql_column_names[0]
    reccount = len(table.sql_columns[firstcolname].values)
    for val_i in xrange(reccount):
        values = [table.sql_columns[i].values[val_i] for i in table.sql_column_names]
        queries.append( (fmt_string, tuple(values)) )
    return queries

def check_a_inserts_table(tables):
    sqltable = tables.tables["a_inserts"]
    check_one_column(sqltable, 'body', ['body3'])
    check_one_column(sqltable, 'id_oid', ['56b8f05cf9fcee1b00000010'])
    check_one_column(sqltable, 'idx', [1])
    queries = generate_insert_queries(sqltable)
    print sqltable.table_name
    print queries
    assert(len(queries)==1)

def check_comments_table(tables):
    sqltable = tables.tables["a_insert_comments"]
    check_one_column(sqltable, 'id_oid', ['56b8f05cf9fcee1b00000110',\
                                          '56b8f05cf9fcee1b00000011'])
    check_one_column(sqltable, 'body', ['body3', 'body2'])
    check_one_column(sqltable, 'idx', [1, 2])
    check_one_column(sqltable, 'a_inserts_idx', [1, 1])
    queries = generate_insert_queries(sqltable)
    print sqltable.table_name
    print queries
    assert(len(queries)==2)

def check_items_table(tables):
    sqltable = tables.tables["a_insert_comment_items"]
    check_one_column(sqltable, 'data', ['1', '2'])
    check_one_column(sqltable, 'idx', [1, 2])
    check_one_column(sqltable, 'a_inserts_idx', [1, 1])
    check_one_column(sqltable, 'a_inserts_comments_idx', [1, 2])

def test_all_tables():
    collection_name = 'a_inserts'
    schema_engine_obj = get_schema_engine( collection_name )
    tables = get_schema_tables(schema_engine_obj)
    assert(tables.tables.keys() == ['a_insert_comment_items', \
                                    'a_inserts',
                                    'a_insert_comments'])
    check_a_inserts_table(tables)
    check_comments_table(tables)
    check_items_table(tables)


    
