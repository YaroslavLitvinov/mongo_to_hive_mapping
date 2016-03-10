#!/usr/bin/env python

import os
import json
import bson
from bson.json_util import loads

from schema_engine import SchemaNode, SchemaEngine, SqlTable, Tables


def prepare_engine(collection_name):
    dirpath=os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(dirpath,"test_files/json_schema2.txt"), "r") \
         as input_schema_f, \
         open(os.path.join(dirpath,"test_files/bson_data2.txt"), "r") as input_data_f:
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
    def test_alias(full_path, short_alias, long_alias, long_plural_alias):
        node1 = get_test_node(full_path)
        assert(short_alias == node1.short_alias())
        assert(long_alias == node1.long_alias())
        assert(long_plural_alias == node1.long_plural_alias())
#test parental field name
    test_alias(['quotes', 'comments', 'quotes_id_oid'], \
               'quotes_id_oid', 'quotes_id_oid', 'quotes_id_oid')
#test structrure field name
    test_alias(['quotes', 'comments', 'body'], \
               'body', 'quotes_comments_body', \
               'quote_comment_body')
#test nested struct field name
    test_alias(['quotes', 'comments', '_id', 'oid'], \
               'id_oid', 'quotes_comments_id_oid', \
               'quote_comment_id_oid')
#test 1 level array name
    test_alias(['quotes'], 'quotes', 'quotes', 'quotes')
#test nested array name
    test_alias(['quotes', 'comments', 'items'], \
               'items', 'quotes_comments_items', \
               'quote_comment_items')

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

def check_quotes_table(tables):
    sqltable = tables.tables["quotes"]
    check_one_column(sqltable, 'body', ['body3'])
    check_one_column(sqltable, 'id_oid', ['56b8f05cf9fcee1b00000010'])
    check_one_column(sqltable, 'idx', [1])
    queries = generate_insert_queries(sqltable)
    print sqltable.table_name
    print queries
    assert(len(queries)==1)

def check_comments_table(tables):
    sqltable = tables.tables["quote_comments"]
    check_one_column(sqltable, 'id_oid', ['56b8f05cf9fcee1b00000110',\
                                          '56b8f05cf9fcee1b00000011'])
    check_one_column(sqltable, 'body', ['body3', 'body2'])
    check_one_column(sqltable, 'idx', [1, 2])
    check_one_column(sqltable, 'quotes_idx', [1, 1])
    queries = generate_insert_queries(sqltable)
    print sqltable.table_name
    print queries
    assert(len(queries)==2)

def check_items_table(tables):
    sqltable = tables.tables["quote_comment_items"]
    check_one_column(sqltable, 'data', ['1', '2'])
    check_one_column(sqltable, 'idx', [1, 2])
    check_one_column(sqltable, 'quotes_idx', [1, 1])
    check_one_column(sqltable, 'quotes_comments_idx', [1, 2])

def test_all_tables():
    collection_name = 'quotes'
    schema_engine = prepare_engine( collection_name )
    tables = Tables(schema_engine)
    tables.load_all()
    assert(tables.tables.keys() == ['quote_comment_items', \
                                    'quotes',
                                    'quote_comments'])
    check_quotes_table(tables)
    check_comments_table(tables)
    check_items_table(tables)


    
