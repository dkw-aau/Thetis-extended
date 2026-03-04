package com.thetis.search;

import com.thetis.store.lucene.LuceneIndex;
import com.thetis.structures.table.Table;
import com.thetis.system.Logger;

import java.io.IOException;

public class LuceneSearch extends AbstractSearch
{
    private long elapsed = -1;
    private final QueryBuilder<String> queryBuilder;
    private final LuceneIndex lucene;

    public LuceneSearch(String luceneDir, int k) throws IOException
    {
        this(new LuceneIndex(luceneDir, k));
    }

    public LuceneSearch(LuceneIndex index)
    {
        super(null, null, null, null);
        this.lucene = index;
        this.queryBuilder = (table) -> {
            StringBuilder builder = new StringBuilder();
            int rows = table.rowCount(), columns = table.columnCount();

            for (int row = 0; row < rows; row++)
            {
                for (int column = 0; column < columns; column++)
                {
                    builder.append(table.getRow(row).get(column)).append(" ");
                }
            }

            return builder.toString().trim();
        };
    }

    public LuceneSearch(LuceneIndex index, int k)
    {
        this(index);
        this.lucene.setK(k);
    }

    @Override
    protected Result abstractSearch(Table<String> query)
    {
        long start = System.nanoTime();
        String keywordQuery = this.queryBuilder.build(query);
        Result result = this.lucene.find(keywordQuery);
        this.elapsed = System.nanoTime() - start;
        Logger.log(Logger.Level.INFO, "Keyword search elapsed time: " + this.elapsed / 1000000 + "ms");

        return result;
    }

    @Override
    protected long abstractElapsedNanoSeconds()
    {
        return this.elapsed;
    }
}
