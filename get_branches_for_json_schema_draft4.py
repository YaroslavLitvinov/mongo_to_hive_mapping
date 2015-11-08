
import argparse
import json

def get_branches_from_schema_recursively(schema):
    branches = []
    if type(schema) is not dict and type(schema) is not list:
        return branches

    for key, value in schema.iteritems():
        v = None
        if type(value) is dict:
            v = value
        elif type(value) is list:
            v = value[0]
        
        if v is not None:
            l = get_branches_from_schema_recursively(v)

            for item in l:
                if key == 'properties' and schema['type'] == 'object':
                    branches.append(item)
                elif key == 'items' and ('type' in schema.keys() and schema['type'] == 'array'):
                    branches.append(item)
                elif key == 'anyOf' and len(schema.keys()) == 1:
                    branches.append(item)
                else:
                    branches.append(key+'.'+item)
        else:
            branches.append(key)
    return branches

if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema-draft4-file", 
                        help="Input file with json-schema.org/draft-04/schema#", type=argparse.FileType('r'))

    args = parser.parse_args()

    if args.schema_draft4_file is None:
        parser.print_help()
        exit(1)

    #get list of branches
    schema = json.load(args.schema_draft4_file)
    schema_branches=get_branches_from_schema_recursively(schema)

    #remove exscessive last split from every branch
    for item in schema_branches:
        item = '.'.join(item.split('.')[:-1])
        print item
