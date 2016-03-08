#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import bson

class SqlColumn:
    def __init__(self, name, value, is_index):
        self.name = name
        self.typo = value
        self.values = []
        self.is_index = is_index
        self.node = None

    def set_node(self, node):
        self.node = node

    def __repr__(self):
        return '\n' + str(self.name) + ': ' + self.typo + '; index: ' \
            + str(self.is_index) + '; values: ' + str(self.values)

class SqlTable:
    def __init__(self, root):
        """ Logical structure of table """
        self.sql_column_names = []
        self.sql_columns = {}
        self.table_name = root.long_plural_alias()
        for node in root.list_non_array_nodes():
            name = node.short_alias()
            self.sql_columns[name] = SqlColumn(name, node.value, False)
            self.sql_columns[name].set_node(node)
        self.add_parent_indexes([i for i in root.all_parents() \
                                 if i != root])

    def __repr__(self):
        return self.table_name + ' ' + str(self.sql_columns)

    def add_parent_indexes(self, parents):
        """ Add every parent's indexes & ids to columns """
        for item in parents:
            idxname = item.long_alias()+'_idx'
            if idxname not in self.sql_columns:
                self.sql_column_names.append(idxname)
                self.sql_columns[idxname] = SqlColumn(idxname, 'BIGINT', True)

    def add_id_idx_values(self, ids_indexes):
        for colname, colval in ids_indexes.iteritems():
            if colname in self.sql_columns:
                self.sql_columns[colname].values.append(colval)

class TablesCollection:
    def __init__(self):
        self.indexes = {}
        self.tables = {}

    def callback_for_populate(self, node, value):
        table_node = node.get_host_node()
        table_name = table_node.long_plural_alias()
        colname = node.short_alias()

        self.tables[table_name].sql_columns[colname].values.append(value)
        print node.get_host_node().long_plural_alias(), \
            node.long_alias(), value, self.indexes

    def callback_for_populate_ids_indexes(self, node, value):
        table_name = node.long_plural_alias()

        if table_name not in self.tables:
            self.tables[table_name] = SqlTable(node)
        self.tables[table_name].add_id_idx_values(self.indexes)

        print "populate_ids_indexes", node.value, self.tables[table_name]


    def populate_all(self, root, data):
        idx = 0
        for l in data:
            idx += 1
            self.indexes[root.get_index_name()] = idx
            if root.get_id_node():
                long_idname = root.get_id_node().long_alias()
                self.indexes[long_idname] = l[root.get_id_node().name]
            self.populate(root, l, newrow = True)

    def populate(self, node, data, newrow):
        if newrow == True:
            self.callback_for_populate_ids_indexes(node, data)
        if node.value == node.type_struct:
            if type(data) is bson.objectid.ObjectId:
                if node.children[0].name == 'oid':
                    self.populate(node.children[0], str(data), newrow = False)
                else:
                    self.children[1].populate(node, 7, newrow = False)
            else:
                for item in node.children:
                    self.populate(item, data[item.name], newrow = False)
        elif node.value == node.type_array:
            idx = 0
            for d in data:
                idx += 1
                self.indexes[node.get_index_name()] = idx
                if node.get_id_node():
                    long_idname = node.get_id_node().long_alias()
                    self.indexes[long_idname] = data[node.get_id_node().name]
                self.populate(node.children[0], d, newrow = True)
        else:
            self.callback_for_populate(node, data)
    

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

    def get_host_node(self):
        """ get node w/o parent or array """
        if self.parent:
            if self.parent.value == self.type_array:
                return self.parent
            else:
                return self.parent.get_host_node()
        else:
            return self

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

    def get_index_name(self):
        if self.value == self.type_array or not self.parent:
            return self.long_alias()+'_idx'
        else:
            return None

    def iterate_all_nested(self, callback):
        for item in self.children:
            if item.value != self.type_struct:
                callback(item)
            item.iterate_all_nested(callback)

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
                if ((item.value == self.type_array and item.children[0].name) or not item.parent) \
                   and n != len(p):
                    l.append(item.external_name()[:-1])
                elif item.name:
                    l.append(item.external_name())
            return '_'.join(l)


class SchemaEngine:
    def __init__(self, name, schema, data):
        self.root_node = SchemaNode(None)
        self.root_node.load(name, schema)
        for item in self.root_node.get_nested_array_type_nodes():
            if item.children[0].value == item.type_struct:
                item.children[0].add_parents_references()
            else:
                item.add_parents_references()
        self.schema = schema
        self.data = data

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
                self.callback(self.callback_param, data_i, node.children[0])
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

def load_schema(collection):
    from test_schema_engine import prepare_engine
    schema_engine = prepare_engine(collection)
    print schema_engine.root_node
    return schema_engine


def load_table_callback(tables, data, node):
    print "load_callback", node.long_alias()
    table_name = node.long_plural_alias()
    if table_name not in tables.tables:
        tables.tables[table_name] = SqlTable(node)
    sqltable = tables.tables[table_name]
    for column_name, column in sqltable.sql_columns.iteritems():
        if column.node:
            column.values.append( tables.data_engine.get_current_record_data(column.node) )
        else: #index
            pass

class Tables:
    def load_array_items_callback(item):
        print item.long_alias(), "=", self.data_engine.get_current_record_data(item)
        pass


    def __init__(self, schema_engine):
        self.tables = {}
        self.schema_engine = schema_engine
        self.data_engine = DataEngine(schema_engine.root_node, \
                                      schema_engine.data, \
                                      load_table_callback, \
                                      self)

    def load_all(self):
        root = self.schema_engine.root_node
        array = [root] + root.get_nested_array_type_nodes()
        self.data_engine.load_data_recursively(schema_engine.data, root)


def load_data(schema_engine):
    tables = Tables(schema_engine)
    tables.load_all()
    return tables

if __name__ == "__main__":
    schema_engine = load_schema("quotes")
    tables = load_data(schema_engine)
    

    

    print SqlTable(schema_engine.root_node)
    print SqlTable(schema_engine.root_node.locate(['comments']))
    print SqlTable(schema_engine.root_node.locate(['comments', 'items']))

    #from test_schema_engine import test_locate_parents
    #test_locate_parents()

