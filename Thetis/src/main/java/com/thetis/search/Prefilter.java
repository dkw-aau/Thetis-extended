package com.thetis.search;

import com.thetis.store.EmbeddingsIndex;
import com.thetis.store.EntityLinking;
import com.thetis.store.EntityTable;
import com.thetis.store.EntityTableLink;
import com.thetis.store.hnsw.HNSW;
import com.thetis.structures.Id;
import com.thetis.structures.Pair;
import com.thetis.structures.table.DynamicTable;
import com.thetis.structures.table.Table;

import java.util.*;

/**
 * Searches corpus using specified LSH index
 * This class is used for pre-filtering the search space
 */
public class Prefilter extends AbstractSearch
{
    private long elapsed = -1;
    private HNSW hnsw;
    private LuceneSearch lucene;
    private static final int SIZE_THRESHOLD = 8;
    private static final int SPLITS_SIZE = 3;
    private static final int MIN_EXISTS_IN = 2;

    private Prefilter(EntityLinking linker, EntityTable entityTable, EntityTableLink entityTableLink, EmbeddingsIndex<Id> embeddingsIndex)
    {
        super(linker, entityTable, entityTableLink, embeddingsIndex);
    }

    public Prefilter(EntityLinking linker, EntityTable entityTable, EntityTableLink entityTableLink,
                     EmbeddingsIndex<Id> embeddingsIndex, HNSW hnsw)
    {
        this(linker, entityTable, entityTableLink, embeddingsIndex);
        this.hnsw = hnsw;
        this.lucene = null;
    }

    public Prefilter(EntityLinking linker, EntityTable entityTable, EntityTableLink entityTableLink,
                     EmbeddingsIndex<Id> embeddingsIndex, LuceneSearch lucene)
    {
        this(linker, entityTable, entityTableLink, embeddingsIndex);
        this.hnsw = null;
        this.lucene = lucene;
    }

    @Override
    protected Result abstractSearch(Table<String> query)
    {
        long start = System.nanoTime();

        if (this.lucene != null)
        {
            Result result = this.lucene.search(query);
            this.elapsed = System.nanoTime() - start;
            return result;
        }

        List<Pair<String, Double>> candidates = new ArrayList<>();
        List<Table<String>> subQueries = List.of(query);
        Map<String, Integer> tableCounter = new HashMap<>();
        boolean isQuerySplit = false;

        if (query.rowCount() >= SIZE_THRESHOLD)
        {
            subQueries = split(query, SPLITS_SIZE);
            isQuerySplit = true;
        }

        for (Table<String> subQuery : subQueries)
        {
            Set<String> subCandidates = searchFromTable(subQuery);
            subCandidates.forEach(t -> tableCounter.put(t, tableCounter.containsKey(t) ? tableCounter.get(t) + 1 : 1));
        }

        for (Map.Entry<String, Integer> entry : tableCounter.entrySet())
        {
            if (isQuerySplit && entry.getValue() >= MIN_EXISTS_IN)
            {
                candidates.add(new Pair<>(entry.getKey(), -1.0));
            }

            else if (!isQuerySplit)
            {
                candidates.add(new Pair<>(entry.getKey(), -1.0));
            }
        }

        this.elapsed = System.nanoTime() - start;
        return new Result(candidates.size(), candidates);
    }

    private Set<String> searchFromTable(Table<String> query)
    {
        Set<String> candidates = new HashSet<>();

        if (query.rowCount() == 0)
        {
            return candidates;
        }

        int rows = query.rowCount(), columns = query.getRow(0).size();

        for (int column = 0; column < columns; column++)
        {
            Set<String> entities = new HashSet<>(rows);

            for (int row = 0; row < rows; row++)
            {
                if (column < query.getRow(row).size())
                {
                    entities.add(query.getRow(row).get(column));
                }
            }

            candidates.addAll(this.lucene != null ? searchLucene(entities) : searchHNSW(entities));
        }

        return candidates;
    }

    private static List<Table<String>> split(Table<String> table, int splitSize)
    {
        List<Table<String>> subTables = new ArrayList<>();
        int rows = table.rowCount();

        for (int i = 0; i < rows;)
        {
            Table<String> subTable = new DynamicTable<>();

            for (int j = 0; j < splitSize && i < rows; i++, j++)
            {
                subTable.addRow(table.getRow(i));
            }

            subTables.add(subTable);
        }

        return subTables;
    }

    private Set<String> searchHNSW(Set<String> entities)
    {
        Set<String> tables = new HashSet<>();

        for (String entity : entities)
        {
            tables.addAll(this.hnsw.find(entity));
        }

        return tables;
    }

    private Set<String> searchLucene(Set<String> entities)
    {
        Table<String> query = new DynamicTable<>(List.of(new ArrayList<>(entities)));
        Result result = this.lucene.search(query);
        Iterator<Pair<String, Double>> resultIter = result.getResults();
        Set<String> resultSet = new HashSet<>();

        while (resultIter.hasNext())
        {
            resultSet.add(resultIter.next().getFirst());
        }

        return resultSet;
    }

    @Override
    protected long abstractElapsedNanoSeconds()
    {
        return this.elapsed;
    }
}
