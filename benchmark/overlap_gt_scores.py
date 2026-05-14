import os
import json
import pickle

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

keyword_dir = 'bm25_output/search_output/'
semantic_dir = 'output/search_output/'
queries = os.listdir(keyword_dir)
gt_folder = 'SemanticTableSearchDataset/ground_truth/2013/wikipedia_categories/'
mapping = 'SemanticTableSearchDataset/table_corpus/wikipages_df_2013.pickle'
corpus = 'SemanticTableSearchDataset/table_corpus/tables_2013/'

for query in queries:
    query_file = 'SemanticTableSearchDataset/queries/2013/5_tuples_per_query/' + query + '.json'
    keyword_result_file = keyword_dir + query + '/filenameToScore.json'
    semantic_result_file = semantic_dir + query + '/filenameToScore.json'
    keyword_results = set()
    semantic_results = set()

    with open(keyword_result_file, 'r') as handle:
        file = json.load(handle)

        for result in file['scores']:
            keyword_results.add(result['tableID'])

    with open(semantic_result_file, 'r') as handle:
        file = json.load(handle)

        for result in file['scores']:
            semantic_results.add(result['tableID'])

    overlapping = set()
    non_overlapping = set()

    for table in keyword_results:
        if table in semantic_results:
            overlapping.add(table)

        else:
            non_overlapping.add(table)

    gt = ground_truth(query_file, gt_folder, mapping)
    gt_scores = {table:0 for table in os.listdir(corpus)}

    for relevance in gt[1]:
        gt_scores[relevance[1]] = relevance[0]

    print('\nOVERLAPPING')

    for table in overlapping:
        if gt_scores[table] > 0:
            print(str(gt_scores[table]).replace('.', ','))

    print('\nNON-OVERLAPPING')

    for table in non_overlapping:
        if gt_scores[table] > 0:
            print(str(gt_scores[table]).replace('.', ','))
