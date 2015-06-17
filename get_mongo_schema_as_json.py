#!/usr/bin/env python

"""Read data from mongo collection and create 'schema' that
corresponding to read data. Export schema as json."""

import sys
import json
import argparse
import bson
import datetime
from pymongo.mongo_client import MongoClient

def message(mes):
    sys.stderr.write( mes + '\n')

def get_mongo_collection_schema(source_data, schema):
    if type(source_data) is dict:
        if type(schema) is not dict:
            schema = {}
        for key in source_data:
            nested_schema = {}
            #add to schema
            if ( schema.get(key) == None ):
                schema[key] = {}
            else:
                nested_schema = schema[key]
            tmp_schema = get_mongo_collection_schema(source_data[key], nested_schema)
            #trying to resolve conflicts automatically, do not overwrite schema by empty data
            if type(tmp_schema) == type:
                schema[key] = tmp_schema
            elif type(schema[key]) == None or type(schema[key]) == type(None) or \
                    ( (type(schema[key]) is dict or type(schema[key]) is list) \
                          and len(tmp_schema) >= len(schema[key]) ) :
                schema[key] = tmp_schema
    elif type(source_data) is list:
        if type(schema) is list:
            schema_as_list = schema
        else:
            schema_as_list = [schema]
        nested_schema = schema_as_list
        for item in source_data:
            nested_schema[0] = get_mongo_collection_schema(item, nested_schema[0])
        #trying to resolve conflicts automatically
        if type(nested_schema[0]) == dict and len(nested_schema[0]) == 0:
            nested_schema = type(None)
        elif type(schema_as_list[0]) is dict and type(nested_schema[0]) is dict \
                and len(schema_as_list)>len(nested_schema):
            nested_schema = schema_as_list
        schema = nested_schema
    else:
        if type(source_data) is float:
            if (source_data - int(source_data)) > 0:
                schema = float
            else:
                schema = int
        elif type(source_data) is bson.objectid.ObjectId:
                schema = { 'oid': str, 'bsontype': int }
        else:
            schema = type(source_data)
    return schema

def python_type_as_str(t):
    if t is str or t is unicode:
        return "STRING"
    elif t is int:
        return "INT"
    elif t is float:
        return "DOUBLE"
    elif t is type(None):
        return "TINYINT"
    elif t is datetime.datetime:
        return "TIMESTAMP"
    elif t is bool:
        return "BOOLEAN"
    elif t is bson.int64.Int64:
        return "BIGINT"
    else:
        raise Exception("Can't handle type ", schema)


def prepare_schema_for_serialization(schema):
    if type(schema) is type:
        return python_type_as_str(schema)
    for key in schema:
        if type(schema[key]) is list:
            schema[key][0] = prepare_schema_for_serialization(schema[key][0])
        else:
            schema[key] = prepare_schema_for_serialization(schema[key])
    return schema


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", help="Mongo db host name", type=str)
    parser.add_argument("-cn", "--collection-name", help="Mongo collection name that is expected in format db_name.collection_name", type=str)
    parser.add_argument("-of", action="store", 
                        help="File name with schema data encoded as json(stdout by default)", type=argparse.FileType('w'))
    parser.add_argument("-js-request", help="Mongo db search request in json format. Default request is {'_id': {'$gt':0}}", type=str)

    args = parser.parse_args()

    if args.of == None:
        args.of = sys.stdout
        message( "using stdout for output schema")

    if args.host == None or args.collection_name == None:
        parser.print_help()
        exit(1)

    split_name = args.collection_name.split('.')
    if len(split_name) != 2:
        message("collection name is expected in format db_name.collection_name")
        exit(1)

    message("Connecting to mongo server "+args.host)
    split_host = args.host.split(':')
    if len(split_host) > 1:
        client = MongoClient(split_host[0], int(split_host[1]))
    else:
        client = MongoClient(args.host, 27017)

    search_request = {'_id': {'$gt':0}}
    if args.js_request != None:
        search_request = json.loads(args.js_request)

    db = client[split_name[0]]
    collection_names = db.collection_names()
    quotes = db[split_name[1]]
    rec_list = quotes.find( search_request )

    schema={}
    for r in rec_list:
        schema = get_mongo_collection_schema(r, schema)

    schema = prepare_schema_for_serialization(schema)
    json.dump(schema, args.of, indent=4)

    
