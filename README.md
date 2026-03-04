# TableSearch

## Table Search Algorithm based on Semantic Relevance

Semantically Augmented Table Search with Thetis++.

### Outline

1. Input: Set of tables & and a KG

2. Preprocessing: 
    
   Take tables and output an index that has <tableId, rowId, cellId, uriToEntity>

3. On-Line:
    
   Take input a set of entity tuples:
   
        <Entity1, Entity2>
        <Entity3, Entity4>

   Return a set of ranked tables
        T1, T2, T3 --> ranked based on relevance score

## Data Preparation

### KG

The reference KG is DBpedia.

1. Enter the `data/kg/dbpedia/` directory and download the files with the command 

   ```bash
   ./download-dbpedia.sh dbpedia_files.txt 
   ```

2. Load the data into a database. In this case Neo4j
   
   Take a look at: https://gist.github.com/kuzeko/7ce71c6088c866b0639c50cf9504869a for more details on setting up Neo4j.

### Embeddings

Generate RDF embeddings by following the steps in the README in the <a href="https://github.com/EDAO-Project/DBpediaEmbedding">DBpediaEmbedding</a> repository. 
Create a folder `embeddings/` in `data/`. Move the embeddings file `vectors.txt` into the `data/embeddings/` folder.

Enter the project root directory. Pull the Postgres image and setup a database

```
docker pull postgres
docker run -e POSTGRES_USER=<USERNAME> -e POSTGRES_PASSWORD=<PASSWORD> -e POSTGRES_DB=embeddings --name db -d postgres
```

Choose a username and password and substitute `<USERNAME>` and `<PASSWORD>` with them.

Extract the IP address of the Postgress container

```
docker exec -it db hostname -I
```

Remember the IP address for later.
With the command `docker exec -it db psql -U thetis embeddings`, you can connect to the `embeddings` database and modify and query it as you like.

Now, exit the Docker interactive mode with `Ctrl+d` and start inserting embeddings into Postgres

```
docker run -v $(pwd)/Thetis:/src -v $(pwd)/data:/data  --network="host" -it --rm --entrypoint /bin/bash maven:3.8.4-openjdk-17
cd /src
mvn package
java -jar target/Thetis.0.1.jar embedding -f /data/embeddings/vectors.txt -db postgres -h <POSTGRES IP> -p 5432 -dbn embeddings -u <USERNAME> -pw <PASSWORD>
```

Insert the IP address from the previous step instead of `<POSTGRES IP>`.
Add the option `-dp` or `--disable-parsing` to skip pre-parsing the embeddings file before insertion. Pre-parsing is very time-consuming but avoids populating the Postgres instance if there are syntax errors in the vector file.

### Table Datasets

The table dataset is Wikitables, consisting of tables extracted from Wikipedia pages.
This dataset come with Wikipedia page annotations for entity mentions, which allows us to link these entities to their corresponding DBpedia entities.

## WikiTables
The WikiTables corpus originates from the TabEL paper
> Bhagavatula, C. S., Noraset, T., & Downey, D. (2015, October). TabEL: entity linking in web tables. In International Semantic Web Conference (pp. 425-441). Springer, Cham.

We use the WikiTables corpus as provided in the STR paper (this is the same corpus as described in TabEL paper but with different filenames so we can appropriately compare our method to STR) 
>  Zhang, S., & Balog, K. (2018, April). Ad hoc table retrieval using semantic similarity. In Proceedings of the 2018 world wide web conference (pp. 1553-1562).

We published a resource paper containing the full WikiTables dataset with ground truth
> Leventidis, A., & Christensen, M. P., & Di Rocco, L., & Lissandrini, M., Hose, K., & Miller, R. (2024, July). A Large Scale Test Corpus for Semantic Table Search (pp. 1142–1151).

1. Download the raw corpus and unzip it

   ```bash
   mkdir -p data/tables/wikitables/files/wikitables_raw/
   wget -P data/tables/wikitables http://iai.group/downloads/smart_table/WP_tables.zip

   unzip data/tables/wikitables/WP_tables.zip -d data/tables/wikitables/files/wikitables_raw/
   mv data/tables/wikitables/files/wikitables_raw/tables_redi2_1/* data/tables/wikitables/files/wikitables_raw/
   rm -rf data/tables/wikitables/files/wikitables_raw/tables_redi2_1/
   ```
  
2. Run preprocessing script for extracting tables
   ```bash
   # Create and install dependencies in a python virtual environment
   python3 -m venv .virtualenv
   source .virtualenv/bin/activate
   pip install -r requirements.txt

   cd data/tables/wikitables

   # Create one json file for each table in the wikitables_raw/ directory  
   python extract_tables.py --input_dir_raw files/wikitables_raw/ --output_dir_clean files/wikitables_one_json_per_table/

   # Parse each json file in wikitables_one_json_per_table/ and extract the appropriate json format for each table in the dataset
   # Notice that we also remove tables with less than 10 rows and/or 2 columns
   python extract_tables.py --input_dir files/wikitables_one_json_per_table/ --output files/wikitables_parsed/ --min-rows 10 --max-rows 0 --min-cols 2   
   ```

3. Run preprocessing script for indexing. Notice that we first create a docker container and then run all commands within it

   ```bash
   docker run -v $(pwd)/Thetis:/src -v $(pwd)/data:/data  --network="host" -it --rm --entrypoint /bin/bash maven:3.8.4-openjdk-17
   cd /src
   mvn package
   
   # From inside Docker
   java -Xms25g -jar target/Thetis.0.1.jar index --table-type wikitables --table-dir  /data/tables/wikitables/files/wikitables_parsed/tables_10_MAX/ --output-dir /data/index/wikitables/ -t 4 -pv 15 -bf 0.2 -bc 20
   ```

   `-pv` is number of permutation vectors for Locality-Sensitive Index (LSH) index of entity types and this number also defines the number of projections in the vector/embedding LSH index. `-bf` is the size of LSH bands defined as the fraction of the signature size of each entity. `-bc` is the number of bucket in the LSH indexes.

4. Materialize table to entity edges in the Graph

   Running the indexing in step 3 will generate the ``tableIDToEntities.ttl`` which contains the mappings of each entity as well as the ``tableIDToTypes.ttl``file.
   Copy these two files to the ``data/kg/dbpedia/files_wikitables/`` directory using:
   ```bash
   mkdir -p data/kg/dbpedia/files_wikitables/
   cp data/index/wikitables/tableIDToEntities.ttl data/index/wikitables/tableIDToTypes.ttl data/kg/dbpedia/files_wikitables/
   ```
      
   We update the neo4j database by introducing table nodes which are connected to all the entities found in them.
   To perform this run the ``generate_table_nodes.sh`` script found in the ``data/kg/dbpedia/`` directory.

### Wikitable Queries (Generate query tuples)
The STR paper is evaluated over 50 keyword queries and for each query a set of tables were labeled as highly-relevant, relevant and not relevant.
Our method is using tuples of entities as query input. For each keyword query in the STR paper we extract a table labeled as highly-relevant that has the largest horizontal mapping of entities (i.e., the table for which the can identify the largest tuple of entities). We can construct the query tuples for each of the 50 keyword queries with the following commands:
```bash
cd data/queries/www18_wikitables/

python generate_queries.py --relevance_queries_path qrels.txt \
--min_rows 10 --min_cols 2 --index_dir ../../index/wikitables/ \
--data_dir ../../tables/wikitables/files/wikitables_parsed/tables_10_MAX/ \
--q_output_dir queries/ --tuples_per_query all \
--filtered_tables_output_dir ../../tables/wikitables/files/wikitables_per_query/ \
--embeddings_path ../../embeddings/embeddings.json
```
Notice that we skip labeled tables that have less than 10 rows and/or less than 2 columns so there will be less than 50 queries after the filtering process.
Also note that the following command will generate a new tables directories at `/tables/wikitables/files/wikitables_per_query/`, one for each query which is used to specify the set of tables the search module will look through. 

### Wikitable Search

In this section we describe how to run our algorithms once all the tables have been indexed.

Run Column-Types Similarity Baseline and Embedding Baseline over all queries in `www18_wikitables/queries/`.
For all baselines each query entity is mapped to a single column.
```bash
# Inside docker run the following script
./run_www18_wikitable_queries.sh
```

Run PPR over all queries in `www18_wikitables/queries/`
```bash
./run_www18_wikitable_queries_ppr.sh
```