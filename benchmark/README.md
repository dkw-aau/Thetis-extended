# Benchmark Setup
During setup, we must index Thetis++ and load tables in Elasticsearch for BM25 keyword search.
First, clone the semantic table search benchmark repository:

```bash
git clone https://github.com/EDAO-Project/SemanticTableSearchDataset.git
```

Follow the instructions in the <a href="https://github.com/EDAO-Project/SemanticTableSearchDataset/blob/main/README.md">README.md</a> file to unpack the repository.

## Indexing Thetis++
Download first embeddings:

```bash
wget -O embeddings.zip https://zenodo.org/records/6384728/files/embeddings.zip?download=1
unzip embeddings.zip
```

Now, we download the knowledge graph (KG) and load it into Neo4j:

```bash

```

Now, move the project root directory.

Note that we open a Docker container in interactive mode.
We must remain attached to this container for other Thetis++ operations.

```bash
docker pull postgres
docker run -e POSTGRES_USER=<USERNAME> -e POSTGRES_PASSWORD=<PASSWORD> -e POSTGRES_DB=embeddings --name db -d postgres
IP=$(docker exec -it db hostname -I)

docker run -v $(pwd)/Thetis:/src -v $(pwd)/benchmark:/data  --network="host" -it --rm --entrypoint /bin/bash maven:3.8.4-openjdk-17
cd /src
mvn package
java -jar target/Thetis.0.1.jar embedding -f /data/embeddings/vectors.txt -db postgres -h ${IP} -p 5432 -dbn embeddings -u <USERNAME> -pw <PASSWORD> -dp
```

Finally, we index the Wikitables into Thetis++:

```bash
mkdir -p /data/index/
java -Xms25g -jar target/Thetis.0.1.jar index --table-type wikitables --table-dir /data/SemanticTableSearchDataset/table_corpus/tables_2013/ --output-dir /data/index/ -t 4
```

## Loading Elasticsearch BM25

## Evaluation Setup
Finally, we setup the evaluation data.
We convert table queries to a keyword queries and store them in a mapping file for Thetis to use:


```bash
python convert_queries_df.py SemanticTableSearchDataset/queries/2013/queries_df.pickle SemanticTableSearchDataset/table_corpus/tables_2013/ 1
python convert_queries_df.py SemanticTableSearchDataset/queries/2013/queries_df.pickle SemanticTableSearchDataset/table_corpus/tables_2013/ 5
```
