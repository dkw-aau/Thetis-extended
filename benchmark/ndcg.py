import os
import numpy as np
from sklearn.metrics import ndcg_score
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

k = 100
query_files = os.listdir('5-tuple/')
gt_folder = 'SemanticTableSearchDataset/ground_truth/2013/wikipedia_categories/'
corpus = 'SemanticTableSearchDataset/table_corpus/tables_2013/'
mapping = 'SemanticTableSearchDataset/table_corpus/wikipages_df_2013.pickle'

for query_file in query_files:
    gt = ground_truth('SemanticTableSearchDataset/queries/2013/1_tuples_per_query/' + query_file, gt_folder, mapping)
    gt_relevance_scores = {table:0 for table in os.listdir(corpus)}

    for relevance in gt[1]:
        gt_relevance_scores[relevance[1]] = relevance[0]

    scores = {table:0 for table in os.listdir(corpus)}
    score_file = 'combined_output/search_output/' + query_file.replace('.json', '') + '/filenameToScore.json'

    with open(score_file, 'r') as handle:
        results = json.load(handle)['scores']

        for table in results:
            scores[table['tableID']] = table['score']

    ndcg = ndcg_score(np.array([list(gt_relevance_scores.values())]), np.array([list(scores.values())]), k = k)
    print(str(ndcg).replace('.', '.'))
