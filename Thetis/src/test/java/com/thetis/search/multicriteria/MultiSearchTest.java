package com.thetis.search.multicriteria;

import com.thetis.connector.DBDriverBatch;
import com.thetis.connector.MockEmbeddingsDB;
import com.thetis.connector.MockNeo4jEndpoint;
import com.thetis.connector.Neo4jSemanticDriver;
import com.thetis.loader.IndexWriter;
import com.thetis.loader.MockLinker;
import com.thetis.search.AnalogousSearch;
import com.thetis.search.LuceneSearch;
import com.thetis.search.Result;
import com.thetis.structures.Pair;
import com.thetis.structures.table.SimpleTable;
import com.thetis.structures.table.Table;
import com.thetis.system.Configuration;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MultiSearchTest
{
    private final File outDir = new File("testing/output");
    private AnalogousSearch semanticSearch;
    private LuceneSearch keywordSearch;

    @Before
    public void setup() throws IOException
    {
        Configuration.reloadConfiguration();
        Configuration.setEmbeddingsDimension(200);
        Neo4jSemanticDriver endpoint = new MockNeo4jEndpoint();
        DBDriverBatch<List<Double>, String> embeddingsDB = new MockEmbeddingsDB(200);
        List<Path> paths = List.of(Path.of("table-0072-223.json"), Path.of("table-0314-885.json"),
                Path.of("table-0782-820.json"), Path.of("table-1019-555.json"),
                Path.of("table-1260-258.json"), Path.of("table-0001-1.json"));
        paths = paths.stream().map(t -> Path.of("testing/data/" + t.toString())).collect(Collectors.toList());
        IndexWriter indexWriter = new IndexWriter(paths, this.outDir, new MockLinker(), endpoint,
                1, embeddingsDB, "http://www.wikipedia.org/", "http://dbpedia.org/");
        indexWriter.performIO();

        this.semanticSearch = new AnalogousSearch(indexWriter.getEntityLinker(), indexWriter.getEntityTable(), indexWriter.getEntityTableLinker(),
                indexWriter.getEmbeddingsIndex(), 5, 1, AnalogousSearch.EntitySimilarity.JACCARD_TYPES,
                false, false, true, false,
                false, AnalogousSearch.SimilarityMeasure.EUCLIDEAN);
        this.keywordSearch = new LuceneSearch(indexWriter.getLuceneIndex(), 5);
    }

    @Test
    public void testSearch()
    {
        CombinerPipeline pipeline = MultiSearch.createPipeline(new Pareto(), new Pareto());
        MultiSearch search = new MultiSearch(pipeline, this.keywordSearch, this.semanticSearch);
        Table<String> query = new SimpleTable<>(List.of(List.of("http://dbpedia.org/resource/Windows_Mobile", "http://dbpedia.org/resource/Cosworth")));
        Result result = search.search(query);
        Pair<String, Double> best = result.getResults().next();
        assertTrue(result.getResults().hasNext());
        assertEquals(5, result.getSize());
        assertTrue(best.getSecond() > 0.5);
        assertEquals("table-0782-820.json", best.getFirst());
    }
}
