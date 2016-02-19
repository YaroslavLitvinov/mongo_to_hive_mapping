#!/usr/bin/env python

__author__      = "Yaroslav Litvinov"
__copyright__   = "Copyright 2016, Rackspace Inc."
__email__       = "yaroslav.litvinov@rackspace.com"

import bson
from bson.json_util import loads


class BsonProcessing:
    """ Get tables structure by processing bson object. """

    def __init__(self, callback, result):
        self.callback = callback
        self.result = result

    def get_tables_structure(self, schema, data, collection, fieldname, tables):
        """ generate sql statements from schema + data and return result as list of sqls"""
        if type(schema) is list:
            if fieldname:
                compound_collection_list = [collection, fieldname]
            else:
                compound_collection_list = [collection]
            table_name = '_'.join(compound_collection_list)
            if type(schema[0]) is dict:
                for dict_l in data:
                    #dict type array
                    for key, value in schema[0].iteritems():
                        if key in dict_l:
                            self.get_tables_structure(value, dict_l[key], table_name, key, tables)
            else:
                #base type array
                for data_l in data:
                    self.get_tables_structure(schema[0], data_l, '_'.join(compound_collection_list), fieldname, tables)
                #print "base type array", schema, data
            self.result.append( 
                self.callback(table_name, 
                              tables[table_name]['schema'], 
                              tables[table_name]['data'], fieldname ) )
        if type(schema) is dict:
            if type(data) is bson.objectid.ObjectId:
                id_oid=str(data)
                id_bsontype = 7
                self.get_tables_structure(schema['oid'], id_oid, '_'.join([collection]), '_'.join([fieldname, 'oid']), tables)
                self.get_tables_structure(schema['bsontype'], id_bsontype, '_'.join([collection]), '_'.join([fieldname, 'bsontype']), tables)
            else:
                for key, value in schema.iteritems():
                    if type(value) is list or type(value) is dict:
                        self.get_tables_structure(value, data[key], '_'.join([collection, key]), key, tables)
                    else:
                        self.get_tables_structure(value, data, '_'.join([collection]), key, tables)
                #print "struct type array", collection, type(schema), schema, '\n'
        else:
            #produced tables schema should not contain array fields
            if type(schema) is list:
                return
            #saving schema
            if collection not in tables.keys():
                tables[collection] = {'schema':{}, 'data':[{}]}
            if fieldname not in tables[collection]['schema'].keys():
                #prevent type rewrite
                tables[collection]['schema'][fieldname] = schema
            #saving data
            rec_count = len(tables[collection]['data'])
            if rec_count and fieldname not in tables[collection]['data'][rec_count-1].keys():
                tables[collection]['data'][rec_count-1][fieldname] = data
            else:
                #new record
                tables[collection]['data'].append( {fieldname:data} )
        return self.result
    
if __name__ == "__main__":
    #todo add test
    assert(0)
