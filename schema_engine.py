#!/usr/bin/env python

__author__      = "Yaroslav Litvinov"
__copyright__   = "Copyright 2016, Rackspace Inc."
__email__       = "yaroslav.litvinov@rackspace.com"

import json
import bson
from bson.json_util import loads

class SchemaNode:
    
    type_struct = 'STRUCT'
    type_array = 'ARRAY'

    def __init__(self, parent):
        self.parent = parent
        self.children = []
        self.name = None
        self.value = None

    def __repr__(self):
        gap = ''.join(['----' for s in xrange(len(self.get_all_parents()))])
        s = "%s%s : %s" % (gap, self.name, self.value)
        for c in self.children:
            s += "\n"+c.__repr__()
        return s

    def short_alias(self):
        name = ''
        if self.parent and self.parent.value == self.type_struct and self.parent.parent:
            name = self.parent.short_alias()
        if self.name:
            if len(name):
                return '_'.join( [name, self.name] )
            else:
                return self.name
        else:
            return ''

    def full_alias(self):
        l = []
        p = self.get_all_parents()
        p.reverse()
        n=0
        if not self.parent:
            l = [ self.name ]
        for item in p:
            n+=1
            if item.value == self.type_array and n!=len(p):
                l.append(item.name[:-1])
            elif item.name:
                l.append(item.name)
        return '_'.join( l )

    def get_nested_array_type_nodes(self):
        l = []
        for i in self.children:
            l.extend( i.get_nested_array_type_nodes() )
            if i.value == self.type_array:
                return l + [i]
        return l
        
    def get_all_parents(self):
        if self.parent:
            return [self] + self.parent.get_all_parents()
        else:
            return []

    def locate(self, names_list):
        for item in self.children:
            if not item.name and item.value == self.type_struct:
                return item.locate(names_list)
            elif item.name == names_list[0]:
                rest = names_list[1:]
                if not len(rest):
                    return item
                else:
                    return item.locate(rest)
        return None
            
    def load(self, name, json_schema):
        self.name = name
        if type(json_schema) is dict:
            self.value = self.type_struct
            for k, v in json_schema.iteritems():
                child = SchemaNode(self)
                child.load(k, v)
                self.children.append( child )
        elif type(json_schema) is list:
            self.value = self.type_array
            for v in json_schema:
                child = SchemaNode(self)
                child.load(None, v)
                self.children.append( child )
        else:
            self.value = json_schema

class SchemaEngine:

    def __init__(self, name, schema, data):
        self.root_node = SchemaNode(None)
        self.root_node.load( name, schema )
        self.schema = schema
        self.data = data

    def locate(self, fields_list):
        return self.root_node.locate( fields_list )

    def get_tables_list(self):
        print [i.name for i in schema_engine.root_node.get_nested_array_type_nodes()]
        pass

    def get_table_fields(self):
        pass

    def get_table_parent_fields(self):
        """ list of parent objects """
        l = []
        for p in self.get_all_parents():
            ll = [i.name  for i in p.get_all_parents() if i.name != None]
            l.append('_'.join(ll))
        return l

if __name__ == "__main__":
    from test_schema_engine import prepare_engine
    schema_engine = prepare_engine("quotes")
    print schema_engine.root_node
    print [i.name for i in schema_engine.root_node.get_nested_array_type_nodes()]
    print schema_engine.locate( ['comments', '_id', 'oid'] ).full_alias()
    print schema_engine.locate( ['comments', 'items', 'data'] ).full_alias()
    print schema_engine.locate( ['comments', 'items'] ).full_alias()
    print schema_engine.locate( ['comments'] ).full_alias()
    print schema_engine.root_node.full_alias()
    print schema_engine.root_node.short_alias()
    print schema_engine.locate( ['comments'] ).short_alias()
    print schema_engine.locate( ['comments', 'items'] ).short_alias()
    print schema_engine.locate( ['comments', 'items', 'data'] ).short_alias()
    print schema_engine.locate( ['comments', '_id', 'oid'] ).short_alias()
