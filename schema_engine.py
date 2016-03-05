#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import bson

class SqlColumn:
    def __init__(self, name, value, parental):
        self.name = name
        self.typo = value
        self.values = []
        self.parental = parental

    def __repr__(self):
        return '\n' + str(self.name) + ': ' + self.typo + '; parent: ' \
            + str(self.parental) + '; values: ' + str(self.values)

class SqlTable:
    def table_structure_filler_callback(self, unused, node):
        self.sql_columns[node.short_alias()] = \
                SqlColumn(node.short_alias(), node.value, False)

    def __repr__(self):
        return self.table_name + ' ' + str(self.sql_columns)

    def __init__(self, root):
        self.sql_column_names = []
        self.sql_columns = {}
        self.table_name = root.long_plural_alias()
        root.iterate_no_nested_arrays( \
                                       self.table_structure_filler_callback, None)
        self.add_parent_ids_indexes([i for i in root.all_parents() \
                                     if i != root])

    def add_parent_ids_indexes(self, parents):
        """ Add every parent's indexes & ids to columns """
        for item in parents:
            idxname = item.get_index_name()
            if idxname not in self.sql_columns:
                self.sql_column_names.append(idxname)
                self.sql_columns[idxname] = SqlColumn(idxname, 'BIGINT', True)
            id = item.get_id_node()
            if id:
                idname = id.long_alias()
                if idname not in self.sql_columns:
                    self.sql_column_names.append(idname)
                    self.sql_columns[idname] = SqlColumn(idname, id.value, True)

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

    def iterate_no_nested_arrays(self, callback, callback_param):
        for item in self.children:
            if item.value != self.type_array:
                if item.value != self.type_struct:
                    callback(callback_param, item)
                item.iterate_no_nested_arrays(callback, callback_param)

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
        for item in [i for i in self.all_parents() if i.value == i.type_array]:
            idnode = item.get_id_node()
            if item != self and idnode:
                print "reference", idnode.long_alias()
                child = SchemaNode(self)
                child.name = idnode.name
                child.value = idnode.value
                child.reference = idnode
                self.children.append(child)

#naming conventions

    def external_name(self):
        node = self
        if self.reference:
            node = self.reference
        if node.name and len(node.name) and node.name[0] == '_':
            return node.name[1:]
        else:
            return node.name

    def short_alias(self):
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
        p = self.all_parents()
        return '_'.join([item.external_name() for item in p if item.name])

    def long_plural_alias(self):
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
    def __init__(self, root, bson_data, callback):
        self.root = root
        self.data = bson_data
        self.callback = callback
        self.cursors = {}
        self.indexes = {}

    def inc_single_index(self, key_name):
        if key_name not in self.indexes:
            self.indexes[key_name] = 0
        self.indexes[key_name] += 1

    def load_data_recursively(self, data, node):
        """ Go through children, run callback on every new record"""
        if node.value == node.type_struct:
            for child in node.children:
                if child.value == child.type_struct or \
                   child.value == child.type_array:
                    self.load_data_recursively(data[child.name], child)
        elif node.value == node.type_array:
            key_name = node.long_alias()
            self.cursors[key_name] = 0
            for item_l in data:
                self.inc_single_index(key_name)
                self.load_data_recursively(item_l, node.children[0])
                self.callback(self, item_l, node.children[0])
                if self.cursors[key_name]+1 < len(data):
                    self.cursors[key_name] += 1

    def get_data(self, node):
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

def load_data(schema_engine):
    def load_callback(data_engine, data, node):
        node.iterate_no_nested_arrays(load_array_items_callback, data_engine)

    def load_array_items_callback(data_engine, item):
        #print item.long_alias(), "=", data_engine.get_data(item)
        pass

    root = schema_engine.root_node
    de = DataEngine(schema_engine.root_node, schema_engine.data, load_callback)
    array = [root] + root.get_nested_array_type_nodes()
    de.load_data_recursively( schema_engine.data, root)

if __name__ == "__main__":
    schema_engine = load_schema("quotes")
    load_data(schema_engine)

    print SqlTable(schema_engine.root_node)
    print SqlTable(schema_engine.root_node.locate(['comments']))
    print SqlTable(schema_engine.root_node.locate(['comments', 'items']))



