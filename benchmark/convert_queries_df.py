import numpy as np
import pandas as pd
from tqdm import tqdm
import json
import sys

def table_to_text(json_table):
    with open(json_table) as handle:
        table = json.load(handle)
        text_rows = []

        for row in table['rows']:
            text_row = []

            for i in range(len(row)):
                cell_value = str(row[i]['text'])
                text_row.append(cell_value)

            text_rows.append(text_row)

        return text_rows

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print('Missing queries_df file and/or tables directory')
        exit(1)

    queries_df_file = sys.argv[1]
    tables_dir = sys.argv[2]
    query_tuples = int(sys.argv[3])
    df = pd.read_pickle(queries_df_file)
    df = df[df['selected_table'].notna()]
    query_mapping = dict()

    for idx, row in tqdm(df.iterrows(), total=len(df.index)):
        query_id = row['wikipage_id']
        selected_table = row['selected_table']
        row_ids = list(range(0, query_tuples))

        if selected_table != np.nan:
            text_rows = table_to_text(tables_dir + selected_table)
            selected_rows = [text_rows[i] for i in row_ids]
            keyword_query = ''

            for row in selected_rows:
                for value in row:
                    keyword_query += value + ' '

            query_mapping[query_id] = keyword_query

    with open('keyword_' + str(query_tuples) + '-tuples_queries.json', 'w') as handle:
        json.dump(query_mapping, handle)
