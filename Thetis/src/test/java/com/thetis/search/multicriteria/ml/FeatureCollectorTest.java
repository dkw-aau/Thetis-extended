package com.thetis.search.multicriteria.ml;

import com.thetis.connector.DBDriverBatch;
import com.thetis.connector.MockEmbeddingsDB;
import com.thetis.connector.MockNeo4jEndpoint;
import com.thetis.loader.IndexWriter;
import com.thetis.loader.MockLinker;
import com.thetis.store.EntityLinking;
import com.thetis.store.EntityTableLink;
import com.thetis.system.Configuration;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class FeatureCollectorTest
{
    private EntityLinking linker;
    private EntityTableLink entityTableLink;
    private MockNeo4jEndpoint endpoint;
    private final File outDir = new File("testing/output");

    @Before
    public void setup() throws IOException
    {
        Configuration.reloadConfiguration();
        Configuration.setEmbeddingsDimension(200);
        this.endpoint = new MockNeo4jEndpoint();
        DBDriverBatch<List<Double>, String> embeddingsDB = new MockEmbeddingsDB(200);
        List<Path> paths = List.of(Path.of("table-0072-223.json"), Path.of("table-0314-885.json"),
                Path.of("table-0782-820.json"), Path.of("table-1019-555.json"),
                Path.of("table-1260-258.json"), Path.of("table-0001-1.json"));
        paths = paths.stream().map(t -> Path.of("testing/data/" + t.toString())).collect(Collectors.toList());
        IndexWriter indexWriter = new IndexWriter(paths, this.outDir, new MockLinker(), this.endpoint,
                1, embeddingsDB, "http://www.wikipedia.org/", "http://dbpedia.org/");
        indexWriter.performIO();

        this.linker = indexWriter.getEntityLinker();
        this.entityTableLink = indexWriter.getEntityTableLinker();
    }

    @Test
    public void testEntityFrequencyFeature()
    {
        String entity = "http://dbpedia.org/resource/WebOS";
        int label = 0;
        FrequencyFeature.EntityFrequencyFeature entityFeature = FeatureCollector.entityFrequencyFeature(entity, this.endpoint, this.linker, this.entityTableLink, label);
        assertNotNull(entityFeature);
        assertEquals(label, entityFeature.getLabel());
        assertEquals(3, entityFeature.getFeature().size());
        assertTrue(entityFeature.getFeature().get(0) <= entityFeature.getFeature().get(1));
        assertTrue(entityFeature.getFeature().get(2) > 0);
    }

    @Test
    public void testFrequencyFeatures()
    {
        List<String> entities = List.of("http://dbpedia.org/resource/WebOS", "http://dbpedia.org/resource/1963_Formula_One_season", "http://dbpedia.org/resource/Kisei");
        int label = 1;
        FrequencyFeature feature = FeatureCollector.frequencyFeatures(entities, this.endpoint, this.linker, this.entityTableLink, label);
        assertEquals(label, feature.getLabel());
        assertFalse(feature.getFeature().isEmpty());
        assertFalse(feature.getFeature().stream().anyMatch(Objects::isNull));
        assertEquals(3, feature.getFeature().size());
        assertEquals(9, feature.flatten().length);
    }
}
