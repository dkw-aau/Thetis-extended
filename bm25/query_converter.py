"""
query_converter
-------

This file converts the set of queries used for the WikiPages dataset into a single queries.txt file used by pool_ranker.py 


python query_converter.py --queries_dir ../data/queries/wikipages/queries/ --output_dir data/wikipages/
"""

import json
import argparse
import os
import itertools
import numpy as np
import pandas as pd

from tqdm import tqdm
from pathlib import Path

def get_keywords_from_entities(query_file, max_size=2048):
    entities = list(itertools.chain.from_iterable(query_file['queries']))
    
    keyword_str = ""
    words_set = set()

    for ent in entities:
        ent_raw = ent.split('/')[-1]

        words = ent_raw.split('_')
        for word in words:
            words_set.add(word)

    for word in words_set:
        if (len(keyword_str.encode('utf-8')) + len(word.encode('utf-8')) <= max_size):
            keyword_str += word + ' '
        else:
            break

    return keyword_str

def get_table_rows(json_path):
    '''
    Return the raw text of all rows given table json file
    '''
    with open(json_path) as json_file:
        file = json.load(json_file)

    text_rows = []

    for row in file['rows']:
        text_row = []
        for i in range(len(row)):
            cell_val = str(row[i]['text'])
            text_row.append(cell_val)
        text_rows.append(text_row)

    return text_rows

def convert_selected_rows_to_keyword_query(selected_rows, max_size=2048):
    '''
    Given a list of lists of the selected rows convert it to a single keyword string

    TODO: Maybe filter out numerical values from being added in the keywords_str?
    '''
    keywords_str = ""
    for row in selected_rows:
        for val in row:
            if (len(keywords_str.encode('utf-8')) + len(val.encode('utf-8')) <= max_size):
                keywords_str += val + ' '
            else:
                return keywords_str

    return keywords_str

def get_keywords_from_text(args, queries_file):
    '''
    Write all keyword queries using the text mode in the specified `queries_file`
    '''
    queries_df = pd.read_pickle(args.queries_df)
    queries_df = queries_df[queries_df['selected_table'].notna()]

    for idx, row in tqdm(queries_df.iterrows(), total=len(queries_df.index)):
            query_id = row['wikipage_id']
            selected_table = row['selected_table']
            row_ids = row['selected_row_ids']

            if selected_table != np.nan:
                # Get the raw text from the selected rows
                text_rows = get_table_rows(args.tables_dir + selected_table)
                selected_rows = [text_rows[i] for i in row_ids]

                keyword_query = convert_selected_rows_to_keyword_query(selected_rows, max_size=args.max_keyword_size)
                queries_file.write(str(query_id) + ' ' + keyword_query + '\n')


def main(args):
    queries_file = open(args.output_dir+'queries.txt','w')

    if args.query_mode == 'text':
        get_keywords_from_text(args, queries_file)       
    elif args.query_mode == 'entities':
        # Loop over all query json files in args.queries_dir
        json_files = sorted(os.listdir(args.queries_dir))
        for filename in tqdm(json_files):
            with open(args.queries_dir + filename) as json_file:
                input_file = json.load(json_file)

            q_id = filename.split('_')[1].split('.')[0]
            keywords = get_keywords_from_entities(input_file)
            queries_file.write(q_id + ' ' + keywords + '\n')

    queries_file.close()

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--queries_df', help='Path to the dataframe containing the query information. \
        The dataframe specifies the wikitable used to construct each query as well as what rows were used.\
        When building the query to be used by the WTR pipeline extract all raw text from the selected rows', required=True)

    parser.add_argument('--tables_dir', help='Path to the tables directory that contain the json files for each table.', required=True)
    parser.add_argument('--output_dir', help='Path to the directory where converted queries.txt will be saved at.', required=True)

    parser.add_argument('--query_mode', choices=['text', 'entities'], default='text', help='Mode for constructing the keyword queries. \
        If the query mode is `text` then raw text from the selected queries is used. If the query mode is `entities` \
        then the mappable entities from the selected queries are used to constructe the keyword queries.')

    parser.add_argument('--queries_dir', help='Path to where the queries are stored. Only used if the query_mode is `entities`.')

    parser.add_argument('--max_keyword_size', type=int, default=2048, help='Maximum size of the keyword queries in bytes')


    args = parser.parse_args()

    # Create the `output_dir`` if it doesn't exist
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)
    
    main(args)