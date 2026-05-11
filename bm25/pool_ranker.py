'''
unsupervised baselines:
1. BM25 for different fields
2. MLM

Output
    1. trec formated result file from each baseline
    2. raw table content from pooled results

python pool_ranker.py --output_dir ./data/wikipages/ --input_dir ./data/wikipages/ --index_name wikipages --topn 100
'''
from scorer import ScorerMLM
from elastic import Elastic
from metadata import *
from data_loader import WikiTables
from glob import glob
import json
import argparse

from tqdm import tqdm
from pathlib import Path

def run_WDC_singleField(output_dir, input_dir, index_name, topn):
    es = Elastic(index_name=index_name)
    wiki_loader = WikiTables(input_dir)
    q_dict = wiki_loader.get_queries()
    queries = [es.analyze_query({'text': q_dict[q]}) for q in q_dict]
    
    # Get the correct id for each query
    index_to_q_id = {}
    query_ids = list(q_dict.keys())
    for i in range(len(queries)):
        index_to_q_id[i] = query_ids[i]

    fields = ['content','textBefore','textAfter','pageTitle','title','header','catchall']
    for field in tqdm(fields):
        rs = es.bulk_search(queries,field)

        #generate result file
        f_rank = open(os.path.join(output_dir, field+'.txt'), 'w')
        for q_id, query in enumerate(queries):
            rank = 1
            for each_rs in sorted(rs[q_id].items(), key=lambda kv: kv[1], reverse=True)[:topn]:
                corrected_q_id = index_to_q_id[q_id]
                f_rank.write(corrected_q_id + "\tQ0\t" + each_rs[0] + "\t" + str(rank) + "\t" + str(
                    each_rs[1]) + "\t" + field + "\n")
                rank += 1
        f_rank.close()

def collect_pooled_WDC_tables(output_dir, index_name):
    # collect table ids from all result files
    pool_files = glob(os.path.join(output_dir,'*'))

    top_tids = set()
    for pool_file in pool_files:
        f = open(pool_file,'r')
        for line in f:
            top_tids.add(line.split('\t')[2])
        f.close()

    # get table content from elasticsearch
    f_table = open(os.path.join(output_dir,'pool.json'),'w')
    es = Elastic(index_name=index_name)
    for tid in top_tids:
        doc = es.get_doc(tid)
        f_table.write(json.dumps(doc['_source'])+'\n')
    f_table.close()


def run_WDC_multiField(topn=20):
    es = Elastic(index_name=webtable_index_name)
    wiki_loader = WikiTables('./data/www2018')
    q_dict = wiki_loader.get_queries()
    queries = [es.analyze_query({'text': q_dict[q]}) for q in q_dict]
    fields = ['content', 'textBefore', 'textAfter', 'pageTitle', 'title', 'header']
    field_weights = {
        'content':0.6,
        'textBefore':0.1,
        'textAfter':0.1,
        'pageTitle':0.1,
        'title':0.05,
        'header':0.05,
    }
    params = {"fields": field_weights}
    for query in queries:
        rs = ScorerMLM(es,query.params).score_doc()


if __name__  == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output_dir', help='Path to the directory where results are stored', required=True)
    parser.add_argument('--input_dir', help='Path to the directory where the queries.txt file is located', required=True)
    parser.add_argument('--index_name', help='Name of the index on elasticsearch', required=True)
    parser.add_argument('--topn', type=int, help='The number of top-N results for each query', default=20)

    args = parser.parse_args()

    # Create the `output_dir`` if it doesn't exist (Remove all files if any in it)
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)
    for f in os.listdir(args.output_dir):
        os.remove(os.path.join(args.output_dir, f))

    run_WDC_singleField(output_dir=args.output_dir, input_dir=args.input_dir ,index_name=args.index_name, topn=args.topn)
    # collect_pooled_WDC_tables(output_dir=args.output_dir, index_name=args.index_name)
