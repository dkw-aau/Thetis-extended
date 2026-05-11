import os
import pickle
import json

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
#query_files = os.listdir('SemanticTableSearchDataset/queries/2013/1_tuples_per_query/')
query_files = os.listdir('hnsw_conf_queries/')
gt_folder = 'SemanticTableSearchDataset/ground_truth/2013/wikipedia_categories/'
corpus = 'SemanticTableSearchDataset/table_corpus/tables_2013/'
mapping = 'SemanticTableSearchDataset/table_corpus/wikipages_df_2013.pickle'

for query_file in query_files:
    gt = ground_truth('SemanticTableSearchDataset/queries/2013/1_tuples_per_query/' + query_file, gt_folder, mapping)
    gt_tables = []

    for relevance in gt[1]:
        table_file = relevance[1]
        gt_tables.append(table_file)

    score_file = 'output/search_output/' + query_file.replace('.json', '') + '/filenameToScore.json'
    predicted_tables = []

    with open(score_file, 'r') as handle:
        results = json.load(handle)['scores']
        count = 0

        for table in results:
            predicted_tables.append(table['tableID'])
            count += 1

            if count >= k:
                break

    score = recall(predicted_tables, gt_tables)
    print(str(score).replace('.', ','))
