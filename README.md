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

We published a resource paper containing the full WikiTables dataset with queries and ground truth
> Leventidis, A., & Christensen, M. P., & Di Rocco, L., & Lissandrini, M., Hose, K., & Miller, R. (2024, July). A Large Scale Test Corpus for Semantic Table Search (pp. 1142–1151).

1. Download evaluation suite from Zenodo <a href="https://zenodo.org/records/8082116">here</a> into `data/` and follow the instructions to prepare the data

2. Run preprocessing script for indexing. Notice that we first create a docker container and then run all commands within it

   ```bash
   mkdir -p data/index/wikitables/
   docker run -v $(pwd)/Thetis:/src -v $(pwd)/data:/data  --network="host" -it --rm --entrypoint /bin/bash maven:3.8.4-openjdk-17
   cd /src
   mvn package
   
   # From inside Docker
   java -Xms25g -jar target/Thetis.0.1.jar index --table-type wikitables --table-dir /data/SemanticTableSearchDataset/table_corpus/tables_2013/ --output-dir /data/index/wikitables/ -t 4
   ```

3. Materialize table to entity edges in the Graph

   Running the indexing in step 2 will generate the ``data/index/wikitables/tableIDToEntities.ttl`` which contains the mappings of each entity as well as the ``data/index/wikitables/tableIDToTypes.ttl``file.
   Copy these two files to the ``data/kg/dbpedia/files_wikitables/`` directory using:
   
   ```bash
   mkdir -p data/kg/dbpedia/files_wikitables/
   cp data/index/wikitables/tableIDToEntities.ttl data/index/wikitables/tableIDToTypes.ttl data/kg/dbpedia/files_wikitables/
   ```

   We update the neo4j database by introducing table nodes which are connected to all the entities found in them.
   To perform this run the ``generate_table_nodes.sh`` script found in the ``data/kg/dbpedia/`` directory.

### Wikitable Search

In this section we describe how to run our algorithms once all the tables have been indexed.

The standard approach to perform top-100 semantic table search is the following:

```bash
# From inside Docker
mkdir -p /data/output/
java -Xms25g -jar target/Thetis.0.1.jar search --search-mode analogous -topK 100 -i /data/index/wikitables/ -prop types \
    -q /data/SemanticTableSearchDataset/queries/2013/1_tuples_per_query/ -td /data/SemanticTableSearchDataset/table_corpus/tables_2013/ -od /data/output/ \
   -t 4 -pf HNSW --singleColumnPerQueryEntity --useMaxSimilarityPerColumn
```

The `-prop` flag determines the entity similarity function. Usniog `types` uses Jaccard similarity of entity types, whereas `predicates` uses the Jaccard similarity of entity predicates.
Alternatively, `embeddings` uses the cosine similarity of entity embeddings.
For this option, you must additionally specify the cosine function using the flag `--embeddingSimilarityFunction` and pass one of `norm_cos`, `abs_cos`, and `ang_cos`.

The flag `-q` specifies the directory in which the queries reside, and `-t` specifies the number of threads.
The flag `-pf` specifies search space prefiltering with HNSW.
Available values for this flag are `LSH_TYPES`, `LSH_PREDICATES`, and `LSH_EMBEDDINGS`.

The results can now be found in `data/output/`.

#### Keyword Search

We also allow performing keyword search using the query table entities as keywords.
Similar to the previous search command, exchange the value for the `--search-mode` flag with `lucene` to perform keyword search.
We recommend not using prefiltering with the `-pf` flag when performing this type of search.