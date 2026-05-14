import os
import json

keyword_dir = 'bm25_output/search_output/'
semantic_dir = 'output/search_output/'
queries = os.listdir(keyword_dir)

for query in queries:
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

    #intersection = len(keyword_results.intersection(semantic_results))
    #overlap = intersection / len(keyword_results) if intersection > 0 else 0
    #print(overlap)

    difference = len(semantic_results - keyword_results)
    non_overlap = difference / len(keyword_results) if difference > 0 else 0
    print(non_overlap)
