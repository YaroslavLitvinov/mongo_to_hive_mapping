#!/usr/bin/env python

"""Import schema from json file.
Excessive schema fields can be filtered by providing list of fields for filtering.\
As result it output hiveql source code for creating hive external table and 
hiveql scripts for generating series of plain tables corresponding to arrays."""

__author__      = "Yaroslav Litvinov"
__copyright__   = "Copyright 2015, Rackspace Inc."
__email__       = "yaroslav.litvinov@rackspace.com"

import sys
import os
import argparse
import json

def message(mes):
    sys.stderr.write( mes + '\n')

def get_exclude_branches_structure(exclude_branches_list):
    exclude_branches_structure = {}
    exclude_branches_list.sort()
    current = ""
    nested_exclude_branches_list = []
    for key in exclude_branches_list:
        splits = key.split('.', 1)
        if current != splits[0]:
            if len(current)>0:
                exclude_branches_structure[ current ] = get_exclude_branches_structure( nested_exclude_branches_list )
            current = splits[0]
            del nested_exclude_branches_list[:]
        if len(splits) > 1:
            nested_exclude_branches_list.append(splits[1])
    if len(current)>0 and exclude_branches_structure.get(current) == None:
        exclude_branches_structure[ current ] = get_exclude_branches_structure( nested_exclude_branches_list )
    return exclude_branches_structure


def remove_excluded_branches_from_schema(schema, exclude_branches_structure):
    for key, value in exclude_branches_structure.iteritems():
        if type(schema) is dict:
            if len(value)>0:
                if schema.get(key) == None:
                    message("can't locate attr key="+key+" at schema")
                else:
                    remove_excluded_branches_from_schema(schema[key], value)
            else:
                if schema.get(key) == None:
                    message("can't exclude key="+key+" as not located")
                else:
                    del schema[key]
        elif type(schema) is list:
            if len(value)>0:
                if len(schema)>0 and type(schema[0]) is dict:
                    if schema[0].get(key) == None:
                        message("can't locate in array key="+key+" at schema")
                    else:
                        remove_excluded_branches_from_schema(schema[0][key], value)
                else:
                    message("can't locate array key="+key+" at schema")
            elif schema[0].get(key) != None:
                del schema[0][key]


def generate_external_hive_table(indirection_level, schema):
    indirection_level += 1
    output = ''
    indent = '    '
    indent_struct = indent
    delim = ' '
    struct_open = '(\n'
    struct_close = '\n)\n'
    if indirection_level > 1:
        indent_struct = indent * (indirection_level-1)
        indent = indent * indirection_level
        delim = ':'
        struct_open = 'STRUCT\n'+indent_struct+'<\n'
        struct_close = '\n'+indent_struct+'>'
    array_open = 'ARRAY\n'+indent_struct+'<\n'
    array_close = '\n'+indent_struct+'>'

    if type(schema) is dict:
        output += struct_open
        for key in schema.keys()[:-1]:
            output += indent + '`' + key + '`' + delim + generate_external_hive_table(indirection_level, schema[key]) + ',\n'
        if len(schema.keys()) > 0:
            key = schema.keys()[-1]
            output += indent + '`' +key + '`' + delim + generate_external_hive_table(indirection_level, schema[key])
        output += indent + struct_close
    elif type(schema) is list:
        output += array_open
        for item in schema[:-1]:
            output += indent + generate_external_hive_table(indirection_level, item) + ',\n'
        if len(schema) > 0:
            output += indent + generate_external_hive_table(indirection_level, schema[-1])
        output += indent + array_close
    elif type(schema) is str or type(schema) is unicode:
        output += schema
    else:
        raise Exception("unknown schema", type(schema))
    return output

def get_branches_from_schema_recursively(schema):
    branches = []
    if type(schema) is not dict and type(schema) is not list:
        return branches

    for key, value in schema.iteritems():
        k=""
        if type(value) is dict:
            l = get_branches_from_schema_recursively(value)
            for item in l:
                branches.append(key+'.'+item)
        elif type(value) is list:
            try:
                l = get_branches_from_schema_recursively(value[0])
            except:
                message("Data type not specified. Empty arrays like [] not allowed")
                raise
            for item in l:
                branches.append(key+'.'+item)
        else:
            branches.append(key)
    return branches


def get_canonical_hive_schema_recursively(schema):
    if type(schema) is not list and type(schema) is not dict:
        return schema

    canonical_schema = {}
    for key, value in schema.iteritems():
        if key[0] == '_':
            key = key[1:]
        key = key.replace('?','')
        if type(value) is list:
            #do not propogate array of empty structs
            if not(len(value)>0 and type(value[0]) is dict and len(value[0]) == 0):
                canonical_schema[key] = [get_canonical_hive_schema_recursively(value[0])]
        elif type(value) is dict:
            #do not propogate empty structs
            if len(value) != 0:
                canonical_schema[key] = get_canonical_hive_schema_recursively(value)
        else:
            canonical_schema[key] = value
    return canonical_schema


def get_struct_fields_recursively(schema):
    select_fields = []
    for key, value in schema.iteritems():
        if type(value) is dict:
            for item in get_struct_fields_recursively(value):
                if type(item) is list:
                    s = [key]
                    s.extend(item)
                    select_fields.append( s )
                else:
                    select_fields.append( [key, item] )
        elif type(value) is not list:
            select_fields.append(key)
    return select_fields

def create_keys_mapping(branches):
    mappings = {}
    for item in branches:
        if (len(item) > 0 and item[0] == '_') or item.find('._') != -1 or item.find('?') != -1:
            new_item = item.replace('._', '.')
            new_item = new_item.replace('?','')
            if new_item[0] == '_' :
                new_item = new_item[1:]
            #get rid from unicode
            mappings[str(new_item)] = str(item)
    return mappings

class HiveTableGenerator:
    create_fmt = "drop table {0}; create table {0} {1} as\n"
    select_fmt = "SELECT\n {0}{1}{2} \nFROM "
    foreignk_fmt = ",\n{0}_exp.id AS {1}_id"
    foreignk_fmt2 = ",\n{0}_exp.id.oid AS {1}_id"
    select_item_fmt = ",\n{0}_exp.{1} AS {2}"
    select_item_fmt2 = "{0} AS {1}"
    primaryk_fmt = "row_number() OVER(ORDER BY {0}_exp.id) AS {1}"
    explode_as_fmt = " AS {0}_exp LATERAL VIEW EXPLODE({0}_exp.{1}) {1}_e AS {1}_exp"

    def __init__(self, schema, ext_table_name, base_table_name, tables_folder_name, table_custom_properties, hive_opts, short_column_names):
        self.helper_structure = {}
        self.ext_table_name = ext_table_name
        self.tables_folder_name = tables_folder_name
        self.create_structure_for_plain_hive_tables([base_table_name], schema, self.helper_structure)
        self.table_custom_properties = table_custom_properties
        self.hive_opts = hive_opts
        self.short_column_names = short_column_names

    def create_structure_for_plain_hive_tables(self,nesting_list, schema, res_tables):
        select_fields = []
        output = ""
        if type(schema) is dict:
            schema_as_dict = schema
        elif type(schema) is list:
            schema_as_dict = schema[0]
        else:
            return

        select_fields = []
        for key, value in schema_as_dict.iteritems():
            if type(value) is list:
                self.create_structure_for_plain_hive_tables(nesting_list+[key], value[0], res_tables) 
            elif type(value) is dict:
                struct_fields = get_struct_fields_recursively(value)
                for item in struct_fields:
                    if type(item) is list:
                        s = [key]
                        s.extend(item)
                        select_fields.append( s )
                    else:
                        select_fields.append( [key] + [item] )
            else:
                select_fields.append(key)

        compound_name = ""
        for i in xrange(len(nesting_list)):
            nest = nesting_list[i]
            name = nest[:-1]
            if len(compound_name) != 0 :
                compound_name += "-"
            compound_name += name
        res_tables[compound_name+'s'] = (select_fields, nesting_list)


    def helper_structure_by_name_component(self, name):
        for table_name, table_struct in self.helper_structure.iteritems():
            if name == table_name:
                return table_struct
        return None

    def hiveql_gen_nested_plain_tables(self):
        for table_name, table_struct in self.helper_structure.iteritems():
            file_name = table_name
            table_name = table_name.replace('-','_')
            query_str = ""
            select_str = ""
            name_components = table_struct[1]
            #skip base table
            if len(name_components) == 1:
                continue
            for name_component_idx in xrange(len(name_components)):
                if name_component_idx == 0:
                    continue
                prev_name_component = name_components[name_component_idx-1]
                name_component = name_components[name_component_idx]
                next_name_component = ""
                if name_component_idx+1 < len(name_components):
                    next_name_component = name_components[name_component_idx+1]

                explode_as_str = self.explode_as_fmt.format(prev_name_component, name_component)

                if len(next_name_component) == 0:
                    #if main select
                    select_items_str = ""
                    for t in table_struct[0]:
                        main_sel_item = ""
                        if type(t) is list:
                            main_sel_item = '.'.join(t)
                        else:
                            main_sel_item = t
                        if self.short_column_names:
                            column_name = main_sel_item.replace('.', '_')
                        else:
                            column_name = table_name[:-1]+"_"+main_sel_item.replace('.', '_')
                        select_items_str += \
                            self.select_item_fmt.format(name_component, main_sel_item, column_name)
                    #use special names for foreign,parent columns to prefent name conflicts
                    
                    #handle situation when foreign key is ObjectId and not just int
                    prev_table_struct = self.helper_structure_by_name_component(prev_name_component)
                    if prev_table_struct and \
                            'id' not in prev_table_struct[0] and '_id' not in prev_table_struct[0]:
                        foreignk_str = self.foreignk_fmt2.format(prev_name_component,
                                                                "_".join(name_components[:-1]))
                    else:
                        foreignk_str = self.foreignk_fmt.format(prev_name_component,
                                                                "_".join(name_components[:-1]))
                    pk_str = self.primaryk_fmt.format( prev_name_component,
                                                       "_".join(name_components)+"_id" )
                    select_str = self.select_fmt.format(pk_str, foreignk_str, select_items_str)
                else:
                    #if nested selects
                    select_exp_str = self.select_item_fmt.format(name_component, next_name_component, next_name_component)
                    pk_str = self.primaryk_fmt.format( prev_name_component, "id" )
                    select_str = self.select_fmt.format(pk_str, select_exp_str, "")
                if len(query_str) == 0:
                    query_str = select_str + self.ext_table_name
                else:
                    query_str = select_str + "("+query_str+")"
                query_str += "\n" + explode_as_str
            query_str += ";"

            complete_script = self.create_fmt.format(table_name, self.table_custom_properties.replace('{TABLE_NAME}', table_name))+query_str
            with open(self.tables_folder_name+"/"+file_name+".sql", 'w') as plain_table_file:
                plain_table_file.write(self.hive_opts)
                plain_table_file.write(complete_script)
                plain_table_file.close()
                message(plain_table_file.name)

    def hiveql_gen_base_plain_table(self):
        for table_name, table_struct in self.helper_structure.iteritems():
            name_components = table_struct[1]
            #skip all nested structures
            if len(name_components) > 1:
                continue
            select_items_str = ""
            for t in table_struct[0]:
                main_sel_item = ""
                if type(t) is list:
                    main_sel_item = '.'.join(t)
                    main_sel_item = self.select_item_fmt2.format(main_sel_item, main_sel_item.replace('.', '_'))
                else:
                    main_sel_item = t
                if len(select_items_str):
                    main_sel_item = ',\n'+main_sel_item
                select_items_str += main_sel_item
            select_str = self.select_fmt.format(select_items_str, "", "")
            query_str = select_str + self.ext_table_name + ';'
            complete_script = self.create_fmt.format(table_name, self.table_custom_properties.replace('{TABLE_NAME}', table_name))+query_str
            with open(self.tables_folder_name+"/"+table_name+".sql", 'w') as plain_table_file:
                plain_table_file.write(self.hive_opts)
                plain_table_file.write(complete_script)
                plain_table_file.close()
                message(plain_table_file.name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-tn", "--table-name", help="Hive's table base name for substitution", type=str)
    parser.add_argument("-mu", "--mongouri", help="Hive's external table 'mongo.uri' parameter", type=str)
    parser.add_argument("-ifs", "--input-file-schema", action="store",
                        help="Input file with json schema, (stdin by default)", type=file)
    parser.add_argument("-od", "--output-dir", help="Directory to save hiveql scripts", type=str)    
    parser.add_argument("-fexclude", action="store",
                        help="Input file with list of branches to exclude, see 'ofb' option", type=file)
    parser.add_argument("-output-branches", action="store", help="Output file with list of all branches", type=argparse.FileType('w'))
    parser.add_argument("-table-custom-properties",
                        help="Optional hive's table properties like ROW FORMAT, STORED AS, LOCATION.\
Will substitute substring {TABLE_NAME} if provided by real table name", 
                        type=str)
    parser.add_argument("-fhive-mongo-opts", action="store",
                        help="Input file with hive mongodb options to be added into output sql files, when needed.", type=file)
    parser.add_argument("-fhive-opts", action="store",
                        help="Input file with generic hive options to be added into output sql files.", type=file)
    parser.add_argument("-big-table-optimization",
                        help="If specified then intermediate native table will be created", action='store_true')
    parser.add_argument("-short-column-names", help="If specified then short column names will be used", action='store_true')


    args = parser.parse_args()

    if args.table_name == None or args.mongouri == None or args.output_dir == None:
        parser.print_help()
        exit(1)

    if args.input_file_schema == None:
        args.input_file_schema = sys.stdin
        message( "using stdin to read schema in json format")

    if args.table_custom_properties == None:
        args.table_custom_properties = ""

    ext_table_name = 'mongo'+args.table_name

    schema = json.load(args.input_file_schema)
    schema_branches=get_branches_from_schema_recursively(schema)

    if args.output_branches != None:
        for item in schema_branches:
            args.output_branches.writelines(item+'\n')

    if args.fexclude != None:
        exclude_branches_list = []
        for line in args.fexclude:
            exclude_branches_list.append(line.strip())
        exclude_branches_structure = get_exclude_branches_structure(exclude_branches_list)
        remove_excluded_branches_from_schema(schema, exclude_branches_structure)

    hive_mongo_opts = hive_opts = ""
    if args.fhive_opts is not None:
        hive_opts = args.fhive_opts.read()
    if args.fhive_mongo_opts is not None:
        hive_mongo_opts = args.fhive_mongo_opts.read()

    keys_mapping = create_keys_mapping(schema_branches)
    #rewrite current schema after getting keys mapping, it's used original names of fields
    schema = get_canonical_hive_schema_recursively(schema)

    tables_folder_name = args.output_dir
    if os.path.isdir(tables_folder_name) == True:
        message('Directory '+tables_folder_name+' is exist, exiting.')
        exit(1)
    os.mkdir(tables_folder_name)
    #generate native flat tables
    message('Saved plain tables: ')

    if args.big_table_optimization:
        hive_gen = HiveTableGenerator(schema, ext_table_name, args.table_name, tables_folder_name, 
                                      args.table_custom_properties, hive_opts,
                                      args.short_column_names)
    else:
        hive_gen = HiveTableGenerator(schema, ext_table_name, args.table_name, tables_folder_name, 
                                      args.table_custom_properties, hive_mongo_opts+hive_opts, 
                                      args.short_column_names)
    hive_gen.hiveql_gen_nested_plain_tables()
    hive_gen.hiveql_gen_base_plain_table()

    #generate external nested table
    table_schema  = generate_external_hive_table(0, schema)
    templ_dict = {"mongouri"   : args.mongouri,
                  "table_name" : ext_table_name,
                  "schema"     : table_schema,
                  "mappings"   : str(keys_mapping).replace("'", '"') }
    external_table = ""

    #depending on parameter will be chosed one or another template file
    template_fname="template.txt"
    if args.big_table_optimization:
        template_fname="template_optimized.txt"

    with open(os.path.dirname(os.path.abspath(__file__))+'/'+template_fname, 'r') as templ_file:
        templ_str = templ_file.read()
        external_table = templ_str % templ_dict
        templ_file.close()

    message('Saved external table: ')
    with open(tables_folder_name+'/'+ext_table_name+'.sql', 'w') as ext_table_file:
        #write hive mongodb options
        ext_table_file.write(hive_mongo_opts)
        if args.big_table_optimization:
            #hive options needed for heavy intermediate table
            ext_table_file.write(hive_opts)
        ext_table_file.write(external_table)
        ext_table_file.close()
        message(ext_table_file.name)
