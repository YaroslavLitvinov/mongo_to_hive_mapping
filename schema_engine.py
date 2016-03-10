#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import json
import bson

class SqlColumn:
    def __init__(self, root, node):
        self.root = root
        self.node = node
        self.values = []
        if node.value == node.type_array:
            self.typo = 'BIGINT'
            if node.long_alias() == root .long_alias():
                self.name = 'idx'
            else:
                self.name = node.long_alias()+'_idx'
        else:
            self.name = node.short_alias()
            self.typo = node.value

    def set_node(self, node):
        self.node = node

    def __repr__(self):
        return '\n' + str(self.name) + ': ' + self.typo + \
            '; values: ' + str(self.values)

class SqlTable:
    def __init__(self, root):
        """ Logical structure of table """
        assert(root.value == root.type_array)
        self.sql_column_names = []
        self.sql_columns = {}
        self.root = root
        self.table_name = root.long_plural_alias()
        for node in root.list_non_array_nodes():
            sqlcol = SqlColumn(root, node)
            self.sql_column_names.append(sqlcol.name)
            self.sql_columns[sqlcol.name] = sqlcol
        parent_arrays = [i for i in root.all_parents() \
                         if i.value == i.type_array]
        for parent_array in parent_arrays: 
#add indexes
            sqlcol = SqlColumn(root, parent_array)
            self.sql_column_names.append(sqlcol.name)
            self.sql_columns[sqlcol.name] = sqlcol

    def __repr__(self):
        return self.table_name + ' ' + str(self.sql_columns)

class SchemaNode:
    type_struct = 'STRUCT'
    type_array = 'ARRAY'

    def __init__(self, parent):
        self.parent = parent
        self.children = []
        self.name = None
        self.value = None
        self.reference = None

    def __repr__(self):
        gap = ''.join(['----' for s in xrange(len(self.all_parents()))])
        s = "%s%s : %s" % (gap, self.external_name(), self.value)
        for c in self.children:
            s += "\n"+c.__repr__()
        return s

    def get_nested_array_type_nodes(self):
        l = []
        for i in self.children:
            l.extend(i.get_nested_array_type_nodes())
            if i.value == self.type_array:
                return [i] + l
        return l

    def all_parents(self):
        if self.parent:
            return self.parent.all_parents() + [self]
        elif self.name:
            return [self]
        else:
            return []

    def get_id_node(self):
        node_parent_id = self.locate(['_id']) or self.locate(['id'])
        if node_parent_id:
            node_parent_id_oid = node_parent_id.locate(['oid'])
            if node_parent_id_oid:
                return node_parent_id_oid
            else:
                return node_parent_id

    def list_non_array_nodes(self):
        fields = []
        for item in self.children:
            if item.value != self.type_array:
                if item.value != self.type_struct:
                    fields.append(item)
                fields.extend( item.list_non_array_nodes() )
        return fields

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
                self.children.append(child)
        elif type(json_schema) is list:
            self.value = self.type_array
            for v in json_schema:
                child = SchemaNode(self)
                child.load(None, v)
                self.children.append(child)
        else:
            self.value = json_schema

    def add_parents_references(self):
        for arr in [i for i in self.all_parents() if i.value == i.type_array]:
            idnode = arr.get_id_node()
            if self.long_alias() != arr.long_alias() and idnode:
                child = SchemaNode(self)
                child.name = '_'.join([item.external_name() \
                                       for item in idnode.all_parents() \
                                       if item.name])
                child.value = idnode.value
                child.reference = idnode
                self.children.append(child)

#methods related to naming conventions

    def external_name(self):
        if self.name and len(self.name) and self.name[0] == '_':
            return self.name[1:]
        else:
            return self.name

    def short_alias(self):
        if self.reference:
#reference items name always is long_alias()
           return self.name
        else:
            name = ''
            if self.parent \
               and self.parent.value == self.type_struct \
               and self.parent.parent:
                name = self.parent.short_alias()
            if self.external_name():
                if len(name):
                    return '_'.join([name, self.external_name()])
                else:
                    return self.external_name()
            else:
                return ''

    def long_alias(self):
        if self.reference:
            return self.name
        else:
            p = self.all_parents()
            return '_'.join([item.external_name() for item in p if item.name])

    def long_plural_alias(self):
        if self.reference:
#reference items name always is long_alias()
           return self.name
        else:
            l = []
            p = self.all_parents()
            n = 0
            for item in p:
                n += 1
                if item.value == self.type_array and n != len(p):
                    l.append(item.external_name()[:-1])
                elif item.name:
                    l.append(item.external_name())
            return '_'.join(l)


class SchemaEngine:
    def __init__(self, name, schema):
        self.root_node = SchemaNode(None)
        self.root_node.load(name, schema)
        for item in self.root_node.get_nested_array_type_nodes():
            if item.children[0].value == item.type_struct:
                item.children[0].add_parents_references()
            else:
                item.add_parents_references()
        self.schema = schema

    def locate(self, fields_list):
        return self.root_node.locate(fields_list)

    def get_tables_list(self):
        print [i.name for i in \
               schema_engine.root_node.get_nested_array_type_nodes()]


class DataEngine:
    def __init__(self, root, bson_data, callback, callback_param):
        self.root = root
        self.data = bson_data
        self.callback = callback
        self.callback_param = callback_param
        self.cursors = {}
        self.indexes = {}

    def inc_single_index(self, key_name):
        if key_name not in self.indexes:
            self.indexes[key_name] = 0
        self.indexes[key_name] += 1

    def load_data_recursively(self, data, node):
        """ Do initial data load. Calculate data indexes, 
            exec callback for every new array"""
        if node.value == node.type_struct:
            for child in node.children:
                if child.value == child.type_struct or \
                   child.value == child.type_array:
                    self.load_data_recursively(data[child.name], child)
        elif node.value == node.type_array:
            key_name = node.long_alias()
            self.cursors[key_name] = 0
            for data_i in data:
                self.inc_single_index(key_name)
                self.load_data_recursively(data_i, node.children[0])
                self.callback(self.callback_param, node)
                if self.cursors[key_name]+1 < len(data):
                    self.cursors[key_name] += 1

    def get_current_record_data(self, node):
        """ Get current data pointed by cursors"""
        if node.reference:
            return self.get_current_record_data(node.reference)
        curdata = self.data
        components = [i.name for i in node.all_parents()[1:] if i.name]
        component_idx = 0

        for parnode in node.all_parents():
            if parnode.value == parnode.type_array:
                cursor = self.cursors[parnode.long_alias()]
                curdata = curdata[cursor]
            elif parnode.value == parnode.type_struct:
                if type(curdata) is bson.objectid.ObjectId:
                    pass
                else:
                    curdata = curdata[components[component_idx]]
                    component_idx += 1

        if node.parent and \
           node.parent.value == node.type_struct and \
           type(curdata) is bson.objectid.ObjectId:
            if node.name == 'oid':
                curdata = str(curdata)
            elif node.name == 'bsontype':
                curdata = 7
        return curdata

def load_table_callback(tables, node):
    table_name = node.long_plural_alias()
    if table_name not in tables.tables:
        tables.tables[table_name] = SqlTable(node)
    sqltable = tables.tables[table_name]
    for column_name, column in sqltable.sql_columns.iteritems():
        if column.node.value == column.node.type_array: #index
            idxcolkey = column.node.long_alias()
            column.values.append( tables.data_engine.indexes[idxcolkey] )
        else:
            column.values.append( tables.data_engine.get_current_record_data(column.node) )

class Tables:
    def __init__(self, schema_engine, bson_data):
        self.tables = {}
        self.data = bson_data
        self.schema_engine = schema_engine
        self.data_engine = \
                DataEngine(schema_engine.root_node, self.data, \
                           load_table_callback, self)

    def load_all(self):
        root = self.schema_engine.root_node
        array = [root] + root.get_nested_array_type_nodes()
        self.data_engine.load_data_recursively(self.data, root)


def create_schema_engine(collection_name, schemapath):
    with open(schemapath, "r") as input_schema_f:
        schema = [json.load(input_schema_f)]
        return SchemaEngine(collection_name, schema)

def create_tables_load_data(schema_engine, data):
    bson_data = bson.json_util.loads(data)
    tables = Tables(schema_engine, bson_data)
    tables.load_all()
    return tables

def create_tables_load_file(schema_engine, datapath):
    with open(datapath, "r") as input_f:
        return create_tables_load_data(schema_engine, input_f.read())

if __name__ == "__main__":
    from test_schema_engine import test_all_tables
    test_all_tables()

