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
import shutil
import pprint
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
            else:
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
            l = get_branches_from_schema_recursively(value[0])
            for item in l:
                branches.append(key+'.'+item)
        else:
            branches.append(key)
    return branches


def get_struct_fields_recursively(schema):
    select_fields = []
    for key, value in schema.iteritems():
        if type(value) is dict:
            select_fields = select_fields+get_struct_fields_recursively(value)
        elif type(value) is type:
            select_fields.append(key)
    return select_fields
    

def create_structure_for_plain_hive_tables(nesting_list, schema, res_tables):
    select_fields = []
    selects_primaryk = []
    output = ""
    if type(schema) is dict:
        schema_as_dict = schema
    elif type(schema) is list:
        schema_as_dict = schem[0]
    else:
        return

    select_fields = []
    for key, value in schema_as_dict.iteritems():
        if type(value) is list:
            create_structure_for_plain_hive_tables(nesting_list+[key], value[0], res_tables) 
        elif type(value) is dict:
            struct_fields = get_struct_fields_recursively(value)
            for item in struct_fields:
                select_fields.append( [key] + [item] )
        else:
            select_fields.append(key)

    compound_name = ""
    for i in xrange(len(nesting_list)):
        nest = nesting_list[i]
        name = nest[:-1]
        if len(compound_name) != 0 :
            compound_name += "_"
        compound_name += name
        grp = nest+"_exp"
        pk = "row_number() over(order by {0}.id) {1}_id".format( grp, name )
        if i == len(nesting_list)-1:
            pk = "row_number() over(order by {0}.id) {1}_id".format( grp, compound_name )
        selects_primaryk.append(pk)
    res_tables[compound_name+'s'] = (select_fields, nesting_list, selects_primaryk)


def create_keys_mapping(branches):
    mappings = {}
    for item in branches:
        if (len(item) > 0 and item[0] == '_') or item.find('._') != -1:
            new_item = item.replace('._', '.')
            new_item = new_item.replace('?','')
            if new_item[0] == '_' :
                new_item = new_item[1:]
            #get rid from unicode
            mappings[str(item)] = str(new_item)
    return mappings

def hiveql_create_table_scripts(ext_table_name, base_table_name, tables_folder_name):
    res_tables = {}
    create_structure_for_plain_hive_tables([base_table_name], schema, res_tables)

    create_fmt = "drop table {0}; create table {0} stored as orc as\n"
    select_fmt = "SELECT\n {0}{1}{2} \nFROM "
    foreignk_fmt = ",\n{0}_exp.id AS {1}_id"
    select_exp_fmt = ",\n{0}_exp.{1}.id AS id"
    select_item_fmt = ",\n{0}_exp.{1} AS {2}"
    primaryk_fmt = "row_number() OVER(ORDER BY {0}_exp.id) AS {1}"
    explode_as_fmt = " AS {0}_exp LATERAL VIEW EXPLODE({0}_exp.{1}) {1}_e AS {1}_exp"
    for table_name, table_struct in res_tables.iteritems():
        query_str = ""
        select_str = ""
        nest_items = table_struct[1]
        for nest_idx in xrange(len(nest_items)):
            if nest_idx == 0:
                continue
            prev_nest = nest_items[nest_idx-1]
            nest = nest_items[nest_idx]
            next_nest = ""
            if nest_idx+1 < len(nest_items):
                next_nest = nest_items[nest_idx+1]
                
            explode_as_str = explode_as_fmt.format(prev_nest, nest)
            pk_str = primaryk_fmt.format( nest, table_name[:-1]+"_id" )

            if len(next_nest) == 0:
                #if main select
                select_items_str = ""
                for t in table_struct[0]:
                    main_sel_item = ""
                    if type(t) is list:
                        main_sel_item = '.'.join(t)
                    else:
                        main_sel_item = t
                    select_items_str += \
                        select_item_fmt.format(nest, 
                                               main_sel_item, 
                                               table_name[:-1]+"_"+main_sel_item)
                foreignk_str = foreignk_fmt.format(nest, 
                                                   "_".join(nest_items[:-1])[:-1])
                select_str = select_fmt.format(pk_str, foreignk_str, select_items_str)
            else:
                #if nested selects
                select_exp_str = select_exp_fmt.format(nest, next_nest)
                select_str = select_fmt.format(pk_str, select_exp_str, "")
            if len(query_str) == 0:
                query_str = select_str + ext_table_name
            else:
                query_str = select_str + "("+query_str+")"
            query_str += "\n" + explode_as_str
        query_str += ";"

        complete_script = create_fmt.format(table_name)+query_str
        with open(tables_folder_name+"/"+table_name+".sql", 'w') as plain_table_file:
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

    args = parser.parse_args()

    if args.input_file_schema == None:
        args.input_file_schema = sys.stdin
        message( "using stdin to read schema in json format")

    ext_table_name = 'mongo'+args.table_name

    pp = pprint.PrettyPrinter(indent=0)

    schema = json.load(args.input_file_schema)
    schema_branches=get_branches_from_schema_recursively(schema)

    if args.fexclude != None:
        exclude_branches_list = []
        for line in args.fexclude:
            exclude_branches_list.append(line.strip())
        exclude_branches_structure = get_exclude_branches_structure(exclude_branches_list)
        remove_excluded_branches_from_schema(schema, exclude_branches_structure)
    
    keys_mapping = create_keys_mapping(schema_branches)

    tables_folder_name = args.output_dir
    if os.path.isdir(tables_folder_name) == True:
        shutil.rmtree(tables_folder_name)
    os.mkdir(tables_folder_name)
    #generate native flat tables
    message('Saved plain tables: ')
    hiveql_create_table_scripts(ext_table_name, args.table_name, tables_folder_name)

    #generate external nested table
    table_schema  = generate_external_hive_table(0, schema)
    templ_dict = {"mongouri"   : args.mongouri,
                  "table_name" : ext_table_name,
                  "schema"     : table_schema,
                  "mappings"   : str(keys_mapping).replace("'", '"') }
    external_table = ""
    with open('template.txt', 'r') as templ_file:
        templ_str = templ_file.read()
        external_table = templ_str % templ_dict
        templ_file.close()

    message('Saved external table: ')
    with open(tables_folder_name+'/'+ext_table_name+'.sql', 'w') as ext_table_file:
        ext_table_file.write(external_table)
        ext_table_file.close()
        message(ext_table_file.name)
