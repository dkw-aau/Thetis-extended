import os
import json

results_dir = 'output/search_output/'
output_ids = os.listdir(results_dir)

for output in output_ids:
    file = results_dir + output + '/filenameToScore.json'

    with open(file, 'r') as handle:
        table = json.load(handle)
        runtime = table['runtime'] / 1000000000
        print(str(runtime).replace('.', ','))
