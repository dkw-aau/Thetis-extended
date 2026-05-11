"""
json_converter
-------

This file converts the table json files used in the WWW18 wikitables into a singular json file format used
by the WTR paper. Once converted this new json file can be fed in the indexer (i.e., elasticsearch)


python json_converter.py --input_dir ../data/tables/wikipages/tables/ --output_dir data/wikipages/ \
--table_id_to_entities_path ../data/index/wikipages/tableIDToEntities.ttl
"""

import json
import argparse
import os

from tqdm import tqdm
from pathlib import Path

def get_table_to_entities_dict(table_id_to_entities_path):
    '''
    Given the path to a tableIDToEntities.ttl file return a dictionary mapping each tableID its list of entities
    '''
    table_to_ents_dict = {}

    print("\nConstructing the table_to_ents_dict.")
    with open(table_id_to_entities_path) as file:
        for line in tqdm(file):
            vals = line.rstrip().split()
            tableID = vals[0].split('/')[-1][:-1] + '.json'
            entity = vals[2].split('/')[-1][:-1]
            
            if tableID not in table_to_ents_dict:
                table_to_ents_dict[tableID] = set()
            
            table_to_ents_dict[tableID].add(entity)

    # Convert the sets to lists
    for tableID in table_to_ents_dict:
        table_to_ents_dict[tableID] = list(table_to_ents_dict[tableID])

    print("Finished constructing the table_to_ents_dict.\n")
    return table_to_ents_dict

def get_relation(input_json):
    relation = []

    headers = [header['text'] for header in input_json['headers']]
    for header in headers:
        relation.append([header])

    for row in input_json['rows']:
        for i in range(len(row)):
            cell_val = row[i]['text']
            relation[i].append(cell_val)

    return relation


def get_new_json(input_json, table_to_ents_dict):
    new_json = {}

    new_json['relation'] = get_relation(input_json)
    new_json['pageTitle'] = input_json['pgTitle']
    new_json['title'] = ""
    new_json['url'] = ""
    new_json['hasHeader'] = True
    new_json['headerPosition'] = "FIRST_ROW"
    new_json['tableType'] = "RELATION"
    new_json['tableNum'] = ""
    new_json['s3Link'] = ""
    new_json['recordEndOffset'] = ""
    new_json['recordOffset'] = ""
    new_json['tableOrientation'] = "HORIZONTAL"
    new_json['TableContextTimeStampBeforeTable'] = ""
    new_json['TableContextTimeStampAfterTable'] = ""
    new_json['textBeforeTable'] = ""
    new_json['textAfterTable'] = ""
    new_json['hasKeyColumn'] = ""
    new_json['keyColumnIndex'] = 0
    new_json['headerRowIndex'] = 0
    new_json['json_loc'] = "table-"+str(input_json['_id']+'.json')

    # Extract the list of entities for the current table
    if new_json['json_loc'] in table_to_ents_dict:
        new_json['entities'] = table_to_ents_dict[new_json['json_loc']]
    else:
        new_json['entities'] = []

    return new_json




def main(args):

    # Mapping of each table ID to the list of entities found in it
    table_to_ents_dict = get_table_to_entities_dict(args.table_id_to_entities_path)

    new_json = open(args.output_dir+'tables.json','w')

    # Loop over all input json files
    json_files = sorted(os.listdir(args.input_dir))
    for filename in tqdm(json_files):
        with open(args.input_dir + filename) as json_file:
            input_file = json.load(json_file)
            
        # Extract the necessary features to update new_json
        cur_new_json = get_new_json(input_file, table_to_ents_dict)
        new_json.write(json.dumps(cur_new_json) + '\n')

    new_json.close()

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_dir', help='Path to the directory containing all the json files waiting to be converted', required=True)
    parser.add_argument('--output_dir', help='Path to the directory where the converted singular json file is to be stored', required=True)
    parser.add_argument('--table_id_to_entities_path', help='Path to the tableIDToEntities.ttl file corresponding to \
        the tables specified in the `input_tables_dir`', required=True)

    args = parser.parse_args()

    # Create the `output_dir`` if it doesn't exist
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)
    
    main(args)
