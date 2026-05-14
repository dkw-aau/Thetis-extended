import json
import os
import pickle
import operator
import numpy as np
from sklearn.metrics import ndcg_score

def ground_truth(query_filename, ground_truth_folder, pickle_mapping_file):
    query = None
    relevances = list()
    wikipages = None
    mapping = None

    with open(query_filename, 'r') as file:
        query = json.load(file)['queries']

    with open(ground_truth_folder + '/' + query_filename.split('/')[-1].split('_')[1], 'r') as file:
        wikipages = json.load(file)

    with open(pickle_mapping_file, 'rb') as file:
        mapping = pickle.load(file)

    for key in mapping['wikipage'].keys():
        wiki_id = mapping['wikipage'][key].split('/')[-1]

        if wiki_id in wikipages.keys():
            relevance = wikipages[wiki_id]

            for table in mapping['tables'][key]:
                relevances.append([relevance, table])

    return query, relevances

gt_folder = 'SemanticTableSearchDataset/ground_truth/2013/wikipedia_categories/'
corpus = 'SemanticTableSearchDataset/table_corpus/tables_2013/'
mapping = 'SemanticTableSearchDataset/table_corpus/wikipages_df_2013.pickle'

semantic_results = 'output/search_output/'
keyword_results  = 'bm25_output/search_output/'
queries = os.listdir(keyword_results)

for query in queries:
    keyword_file = keyword_results + query + '/filenameToScore.json'
    semantic_file = semantic_results + query + '/filenameToScore.json'
    keyword_tables = []
    semantic_tables = []

    with open(keyword_file, 'r') as handle:
        result = json.load(handle)

        for table in result['scores']:
            table_id = table['tableID']
            keyword_tables.append(table_id)

    with open(semantic_file, 'r') as handle:
        result = json.load(handle)

        for table in result['scores']:
            table_id = table['tableID']
            semantic_tables.append(table_id)

    overlap_tables = set(keyword_tables).intersection(set(semantic_tables))
    non_overlap_tables = set()

    for table in keyword_tables:
        if not table in semantic_tables:
            non_overlap_tables.add(table)

    gt = ground_truth('SemanticTableSearchDataset/queries/2013/1_tuples_per_query/' + query + '.json', gt_folder, mapping)
    gt_relevance_scores = {table:0 for table in os.listdir(corpus)}
    relevances = list(sorted(gt[1], key = operator.itemgetter(0), reverse = True))
    gt_tables = set()

    for relevance in relevances:
        table_file = relevance[1]
        gt_tables.add(table_file)

    overlap_tables = overlap_tables.intersection(gt_tables)
    non_overlap_tables = non_overlap_tables.intersection(gt_tables)

    #print(len(overlap_tables))
    #print(str(len(overlap_tables) / len(keyword_tables)).replace('.', ','))
    #print(len(non_overlap_tables))
    print(str(len(non_overlap_tables) / len(keyword_tables)).replace('.', ','))
