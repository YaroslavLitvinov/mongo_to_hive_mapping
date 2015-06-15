#!/usr/bin/env python

"""Read data from mongo collection and create 'schema' that corresponding to read data.\
Excessive schema fields can be filtered by providing list of fields to filtering."""

import sys
import json
import argparse
from pymongo.mongo_client import MongoClient

def message(mes):
    sys.stderr.write( mes + '\n')

#schema and schema_branches are calculates separately and not depends on each other
def get_mongo_collection_schema(source_data, schema):
    schema_branches = nested_branches = {}
    if type(source_data) is dict:
        if type(schema) is not dict:
            schema = {}
        for key in source_data:
            nested_schema = {}
            schema_branches[key] = 1
            #modify schema field to be compatible with hive
            hive_key = key.replace('?', '')
            if len(key) > 0 and key[0] == '_' :
                hive_key = key[1:]
            #add to schema
            if ( schema.get(hive_key) == None ):
                schema[hive_key] = {}
            else:
                nested_schema = schema[hive_key]
            tmp_schema, nested_branches = get_mongo_collection_schema(source_data[key], nested_schema)
            #trying to resolve conflicts automatically, do not overwrite schema by empty data
            if type(tmp_schema) == type:
                schema[hive_key] = tmp_schema
            elif type(schema[hive_key]) == None or type(schema[hive_key]) == type(None) or \
                    ( (type(schema[hive_key]) is dict or type(schema[hive_key]) is list) \
                          and len(tmp_schema) >= len(schema[hive_key]) ) :
                schema[hive_key] = tmp_schema
            for nb in nested_branches:
                schema_branches[key+'.'+nb] = 1
    elif type(source_data) is list:
        if type(schema) is list:
            schema_as_list = schema
        else:
            schema_as_list = [schema]
        nested_schema = schema_as_list
        for item in source_data:
            nested_schema[0], nested_branches = get_mongo_collection_schema(item, nested_schema[0])
            for nb in nested_branches:
                schema_branches[nb] = 1
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
    return (schema, schema_branches)


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", help="Mongo db host name", type=str)
    parser.add_argument("-t", "--table", help="table name that is expected in format db_name.table_name", type=str)
    parser.add_argument("-b", "--output-branches-file", action="store", 
                        help="File with list of branches to be used in further", type=argparse.FileType('w'))
    parser.add_argument("-output-schema-file", action="store", 
                        help="File name with schema data encoded as json(stdout by default)", type=argparse.FileType('w'))
    parser.add_argument("-js-request", help="Mongo db search request in json format", type=str)

    args = parser.parse_args()

    if args.output_schema_file == None:
        args.output_file = sys.stdout
        message( "using stdout for output schema")

    message("Connecting to mongo server "+args.host)
    split_host = args.host.split(':')
    if len(split_host) > 1:
        client = MongoClient(split_host[0], int(split_host[1]))
    else:
        client = MongoClient(args.host, 27017)

    split_name = args.table.split('.')
    if len(split_name) != 2:
        message("table name is expected in format db_name.table_name")
        exit(1)

    search_request = {'_id': {'$gt':0}}
    if args.js_request != None:
        search_request = json.loads(args.js_request)

    db = client[split_name[0]]
    collection_names = db.collection_names()
    quotes = db[split_name[1]]
    rec_list = quotes.find( search_request )
    
    json.dumps(schema, args.output_schema_file)
    
    #pickle.load(args.storage_file_r)        
