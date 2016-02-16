#!/usr/bin/env python

__author__      = "Yaroslav Litvinov"
__copyright__   = "Copyright 2016, Rackspace Inc."
__email__       = "yaroslav.litvinov@rackspace.com"

import sys
import os
import argparse
import json
import bson
from bson.json_util import loads
from bson_processing import BsonProcessing

def message(mes):
    sys.stderr.write( mes + '\n')

def callback(table, schema, data, fieldname):
    columns=[]
    #print 'callback', schema, data
    for type_item in schema:
        if type(schema[type_item]) is dict:
            for dict_item in schema[type_item]:
                columns.append('_'.join([fieldname, dict_item]))
        elif type(schema[type_item]) is not list:
            columns.append(type_item)
    for record in data:
        values=[]
        for type_item in schema:
            if type(schema[type_item]) is dict:
                for dict_item in schema[type_item]:
                    values.append(record[type_item][dict_item])
            elif type(schema[type_item]) is not list:
                values.append(str(record[type_item]))
        print "INSERT INTO {table}({columns})\nVALUES({values});"\
            .format(table=table, columns=','.join(columns), values=','.join(values))

   
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-si", "--statement-insert", help="Generate insert statements", action='store_true')
    parser.add_argument("-dataf", "--input-data-file", action="store", help="Input file with json data", type=file)
    parser.add_argument("-tn", "--table-name", help="Hive's table base name for substitution", type=str)
    parser.add_argument("-ifs", "--input-file-schema", action="store",
                        help="Input file with json schema, (stdin by default)", type=file)
    parser.add_argument("-od", "--output-dir", help="Directory to save hiveql scripts", type=str)    

    args = parser.parse_args()

    if args.table_name == None or args.output_dir == None:
        parser.print_help()
        exit(1)

    if args.input_file_schema == None:
        args.input_file_schema = sys.stdin
        message( "using stdin to read schema in json format")

    data = bson.json_util.loads( args.input_data_file.read() )
    #print data
    schema = json.load(args.input_file_schema)
    #print schema
    tables = {}

    bt = BsonProcessing(callback)
    bt.get_tables_structure([schema], data, "quotes", "", tables)


