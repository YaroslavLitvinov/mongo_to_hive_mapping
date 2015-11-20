import copy
import json
import argparse

def get_branches_from_schema_recursively(schema):
    branches = []
    if type(schema) is not dict and type(schema) is not list:
        return branches

    for key, value in schema.iteritems():
        if type(value) is dict:
            l = get_branches_from_schema_recursively(value)
            if not len(l):
                branches.append(key)
            for item in l:
                branches.append(key+'.'+item)
        elif type(value) is list:
            try:
                l = get_branches_from_schema_recursively(value[0])
            except:
                message("Data type not specified. Empty arrays like [] not allowed")
                raise
            if not len(l):
                branches.append(key)
            for item in l:
                branches.append(key+'.'+item)
        else:
            branches.append(key)
    return branches


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


def get_data_from_branch_recursively(schema, branch):
    res = None
    if type(schema) is not list and type(schema) is not dict:
        return res

    for key, value in schema.iteritems():
        if key != branch.split('.')[0]:
            continue

        if type(value) is list:
            if key == branch:
                #res = value[0]
                res = value
            #do not propogate array of empty structs
            elif not(len(value)>0 and type(value[0]) is dict and len(value[0]) == 0):
                res = get_data_from_branch_recursively( value[0], '.'.join(branch.split('.')[1:]) )
        elif type(value) is dict:
            if key == branch:
                res = value
            #do not propogate empty structs
            elif len(value) != 0:
                res = get_data_from_branch_recursively( value, '.'.join(branch.split('.')[1:]) )
        else:
            res = value

        if res is not None:
            return res
    return res



parser = argparse.ArgumentParser()
parser.add_argument("--primary-schema", action="store", help="", type=argparse.FileType('r'))
parser.add_argument("--secondary-schema", action="store", help="", type=argparse.FileType('r'))
parser.add_argument("--merged-schema", action="store", help="", type=argparse.FileType('w'))
args = parser.parse_args()

if args.primary_schema is None or args.secondary_schema is None or args.merged_schema is None :
    parser.print_help()
    exit(1)

primary_schema = json.load(args.primary_schema)
print 0
secondary_schema = json.load(args.secondary_schema)

print 1
primary_branches = get_branches_from_schema_recursively(primary_schema)
print primary_branches
print 2

new_schema_with_derived_datatypes = copy.copy(primary_schema)
print 3
for branch in primary_branches:
    data_primary = get_data_from_branch_recursively(primary_schema, branch)
    data_secondary = get_data_from_branch_recursively(secondary_schema, branch)
    if branch == "options.groupings.items.reference_documents":
        pass
    if data_primary is not None and data_secondary is not None \
            and data_secondary != data_primary \
            and data_secondary != 'TINYINT':
        print branch, data_primary, data_secondary
        new_schema_with_derived_datatypes = \
            update_data_on_branch_recursively(new_schema_with_derived_datatypes, 
                                              branch, data_secondary)

json.dump(new_schema_with_derived_datatypes, args.merged_schema, indent=4, sort_keys=True)



