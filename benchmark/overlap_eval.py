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

def recall(predicted, gt):
    count = 0

    for table in predicted:
        if table in gt:
            count += 1

    if len(predicted) == 0:
        return 0.0

    elif count > len(gt):
        return 1.0

    return float(count) / len(gt)

k = 100
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
    keyword_scores = {table:0 for table in os.listdir(corpus)}

    with open(keyword_file, 'r') as handle:
        result = json.load(handle)

        for table in result['scores']:
            table_id = table['tableID']
            keyword_tables.append(table_id)
            keyword_scores[table_id] = table['score']

    with open(semantic_file, 'r') as handle:
        result = json.load(handle)

        for table in result['scores']:
            table_id = table['tableID']
            semantic_tables.append(table_id)

    #eval_tables = set(keyword_tables).intersection(set(semantic_tables))
    """eval_tables = []

    for table in keyword_tables:
        if not table in semantic_tables:
            eval_tables.append(table)"""

    gt = ground_truth('SemanticTableSearchDataset/queries/2013/1_tuples_per_query/' + query + '.json', gt_folder, mapping)
    gt_relevance_scores = {table:0 for table in os.listdir(corpus)}
    relevances = list(sorted(gt[1], key = operator.itemgetter(0), reverse = True))
    gt_tables = []

    for relevance in relevances:
        table_file = relevance[1]
        gt_tables.append(table_file)
        gt_relevance_scores[table_file] = relevance[0]

    for table in keyword_scores.keys():
        if table in semantic_tables:
            keyword_scores[table] = 0.0

    gt_tables = gt_tables[:k]
    #recall_score = recall(eval_tables, gt_tables)
    ndcg = ndcg_score(np.array([list(gt_relevance_scores.values())]), np.array([list(keyword_scores.values())]), k = k)
    #print(str(recall_score).replace('.', ','))
    print(str(ndcg).replace('.', ','))
