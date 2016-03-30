import copy
import json
import argparse

"""Creating new schema files with type fixes from specified error data.
format of lines of branches to fix
'wrong value (STRING) for quotes.customer.title(TINYINT)': 59 
wrong value (INT) for quotes.customer.region(TINYINT)': 59 """

def update_data_on_branch_recursively(schema, branch, new_value):
    if type(schema) is not list and type(schema) is not dict:
        return new_value

    updated_schema = {}
    for key, value in schema.iteritems():
        if key != branch.split('.')[0]:
            updated_schema[key] = value
            continue

        if type(value) is list:
            #do not propogate array of empty structs
            if not(len(value)>0 and type(value[0]) is dict and len(value[0]) == 0):
                res = update_data_on_branch_recursively( value[0], '.'.join(branch.split('.')[1:]), new_value )
                if type(res) is list:
                    updated_schema[key] = res
                else:
                    updated_schema[key] = [res]
                
        elif type(value) is dict:
            #do not propogate empty structs
            if len(value) != 0:
                updated_schema[key] = update_data_on_branch_recursively( value, '.'.join(branch.split('.')[1:]), new_value )
        else:
            if key == branch:
                updated_schema[key] = new_value
            else:
                updated_schema[key] = value
    return updated_schema



parser = argparse.ArgumentParser()
parser.add_argument("--primary-schema", action="store", help="", type=argparse.FileType('r'))
parser.add_argument("--branches-to-patch", action="store", help="", type=argparse.FileType('r'))
parser.add_argument("--patched-schema", action="store", help="", type=argparse.FileType('w'))
args = parser.parse_args()

if args.primary_schema is None or args.patched_schema is None or args.branches_to_patch is None :
    parser.print_help()
    exit(1)

primary_schema = json.load(args.primary_schema)
branches_to_fix = args.branches_to_patch.readlines()
for item in branches_to_fix:
    s = item.split("(")
    if len(s):
        res = s[1].split(')')
        newtype = res[0]
        branch_name = res[1].split()[-1].split('.', 1)[1]
        print 'set', branch_name, newtype
        primary_schema = \
            update_data_on_branch_recursively(primary_schema, 
                                              branch_name, newtype)

json.dump(primary_schema, args.patched_schema, indent=4, sort_keys=True)



