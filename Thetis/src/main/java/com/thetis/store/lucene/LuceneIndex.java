package com.thetis.store.lucene;

import com.thetis.search.Result;
import com.thetis.store.Index;
import com.thetis.structures.Pair;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Index to perform keyword search using Lucene.
 * Loading of the index happens first after which the index no longer can be populated.
 */
public class LuceneIndex implements Index<String, Result>, AutoCloseable
{
    private int k;
    private final Directory dir;
    private final Analyzer analyzer;
    private final IndexReader reader;
    private final IndexSearcher searcher;
    private final QueryParser parser;
    private final boolean applyNormalization;
    static final String DOC_FIELD = "text";
    static final String ID_FIELD = "id";

    public LuceneIndex(String directory, int k, boolean normalizeScores) throws IOException
    {
        this.dir = FSDirectory.open(Path.of(directory));
        this.analyzer = new StandardAnalyzer();
        this.reader = DirectoryReader.open(this.dir);
        this.searcher = new IndexSearcher(this.reader);
        this.parser = new QueryParser(DOC_FIELD, this.analyzer);
        this.k = k;
        this.applyNormalization = normalizeScores;
    }

    public static LuceneBuilder builder(String indexDir) throws IOException
    {
        return new LuceneBuilder(indexDir);
    }

    public void setK(int k)
    {
        this.k = k;
    }

    @Override
    public void insert(String key, Result value)
    {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public boolean remove(String key)
    {
        IndexWriterConfig config = new IndexWriterConfig(this.analyzer);

        try (IndexWriter writer = new IndexWriter(this.dir, config))
        {
            writer.deleteDocuments(new Term(ID_FIELD, key));
            return true;
        }

        catch (IOException e)
        {
            return false;
        }
    }

    @Override
    public Result find(String keywordQuery)
    {
        try
        {
            Query query = this.parser.parse(keywordQuery);
            TopDocs topDocs = this.searcher.search(query, this.k);
            List<Pair<String, Double>> ranking = new ArrayList<>();

            for (ScoreDoc scoreDoc : topDocs.scoreDocs)
            {
                double score = scoreDoc.score;
                String tableId = this.searcher.doc(scoreDoc.doc).get(ID_FIELD);
                ranking.add(new Pair<>(tableId, score));
            }

            if (this.applyNormalization)
            {
                ranking = normalizeResults(ranking);
            }

            return new Result(this.k, ranking);
        }

        catch (IOException | ParseException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static List<Pair<String, Double>> normalizeResults(List<Pair<String, Double>> results)
    {
        List<Pair<String, Double>> normalized = new ArrayList<>(results.size());

        for (Pair<String, Double> result : results)
        {
            double sigmoid = 1 / (1 + Math.exp(-result.getSecond()));
            normalized.add(new Pair<>(result.getFirst(), sigmoid));
        }

        return normalized;
    }

    // This is a slow implementation, as we are initializing all of the necessary classes for every call of this function
    @Override
    public boolean contains(String key)
    {
        try
        {
            QueryParser idQueryParser = new QueryParser(ID_FIELD, this.analyzer);
            Query query = idQueryParser.parse(key);
            TopDocs topDocs = this.searcher.search(query, size());

            for (ScoreDoc scoreDoc : topDocs.scoreDocs)
            {
                if (this.searcher.doc(scoreDoc.doc).get(ID_FIELD).equals(key))
                {
                    return true;
                }
            }

            return false;
        }

        catch (IOException | ParseException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int size()
    {
        return this.reader.numDocs();
    }

    @Override
    public void clear()
    {
        IndexWriterConfig config = new IndexWriterConfig(this.analyzer);

        try (IndexWriter writer = new IndexWriter(this.dir, config))
        {
            writer.deleteAll();
        }

        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close()
    {
        try
        {
            this.reader.close();
            this.analyzer.close();
        }

        catch (IOException ignored) {}
    }
}
