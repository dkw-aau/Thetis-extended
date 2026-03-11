package com.thetis.loader;

import com.thetis.TestUtils;
import com.thetis.connector.*;
import com.thetis.store.EntityLinking;
import com.thetis.store.EntityTable;
import com.thetis.store.EntityTableLink;
import com.thetis.structures.Id;
import com.thetis.structures.graph.Entity;
import com.thetis.structures.graph.Type;
import com.thetis.system.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

public class IndexReaderTest
{
    private static IndexReader reader;
    private static final File outDir = new File("testing/output");

    @BeforeClass
    public static void setup() throws IOException
    {
        synchronized (TestUtils.lock)
        {
            Configuration.reloadConfiguration();
            Configuration.setEmbeddingsDimension(200);
            DBDriverBatch<List<Double>, String> embeddingsDB = new MockEmbeddingsDB(200);
            Neo4jSemanticDriver endpoint = new MockNeo4jEndpoint();
            List<Path> paths = List.of(Path.of("table-0072-223.json"), Path.of("table-0314-885.json"),
                    Path.of("table-0782-820.json"), Path.of("table-1019-555.json"), Path.of("table-1260-258.json"));
            paths = paths.stream().map(t -> Path.of("testing/data/" + t.toString())).collect(Collectors.toList());
            IndexWriter writer = new IndexWriter(paths, outDir, new MockLinker(), endpoint, 1,
                    embeddingsDB, "http://www.wikipedia.org/", "http://dbpedia.org/");
            writer.performIO();
            reader = new IndexReader(outDir, false, true, embeddingsDB);
            reader.performIO();
        }
    }

    @AfterClass
    public static void tearDown()
    {
        synchronized (TestUtils.lock)
        {
            File luceneDir = new File(outDir.getAbsolutePath() + "/lucene/"), statDir = new File(outDir.getAbsolutePath() + "/statistics/");

            for (File file : Objects.requireNonNull(luceneDir.listFiles()))
            {
                file.delete();
            }

            for (File file : Objects.requireNonNull(statDir.listFiles()))
            {
                file.delete();
            }

            for (File file : Objects.requireNonNull(outDir.listFiles()))
            {
                file.delete();
            }

            luceneDir.delete();
            statDir.delete();
            outDir.delete();
        }
    }

    @Test
    public void testIndexes()
    {
        EntityLinking linker = reader.getLinker();
        EntityTable entityTable = reader.getEntityTable();
        EntityTableLink entityTableLink = reader.getEntityTableLink();
        assertEquals(entityTable.size(), entityTableLink.size());

        int count = 0;
        Iterator<Id> iter = linker.kgUriIds();

        while (iter.hasNext())
        {
            count++;
            iter.next();
        }

        assertEquals(entityTable.size(), count);
    }

    @Test
    public void testLinker()
    {
        EntityLinking linker = reader.getLinker();
        assertEquals("http://dbpedia.org/resource/1963_Formula_One_season", linker.mapTo("http://www.wikipedia.org/wiki/1963_Formula_One_season"));
        assertEquals("http://www.wikipedia.org/wiki/1963_Formula_One_season", linker.mapFrom("http://dbpedia.org/resource/1963_Formula_One_season"));
        assertEquals("http://dbpedia.org/resource/Windows_Phone_7", linker.mapTo("http://www.wikipedia.org/wiki/Windows_Phone_7"));
        assertEquals("http://www.wikipedia.org/wiki/Windows_Phone_7", linker.mapFrom("http://dbpedia.org/resource/Windows_Phone_7"));

        assertNotNull(linker.kgUriLookup("http://dbpedia.org/resource/1963_Formula_One_season"));
        assertNotNull(linker.inputUriLookup("http://www.wikipedia.org/wiki/1963_Formula_One_season"));
        assertNotNull(linker.kgUriLookup("http://dbpedia.org/resource/Windows_Phone_7"));
        assertNotNull(linker.inputUriLookup("http://www.wikipedia.org/wiki/Windows_Phone_7"));
    }

    @Test
    public void testEntityTable()
    {
        EntityTable entityTable = reader.getEntityTable();
        EntityLinking linker = reader.getLinker();
        Entity ent1 = entityTable.find(linker.kgUriLookup("http://dbpedia.org/resource/Boston_Bruins")),
                ent2 = entityTable.find(linker.kgUriLookup("http://dbpedia.org/resource/NEC_Cup"));
        Set<String> ent1Types = Set.of("https://www.w3.org/2002/07/owl#Thing", "https://dbpedia.org/ontology/Person", "https://www.wikidata.org/wiki/Q5"),
                ent2Types = ent1Types;

        // Checking URIs
        assertEquals("http://dbpedia.org/resource/Boston_Bruins", ent1.getUri());
        assertEquals("http://dbpedia.org/resource/NEC_Cup", ent2.getUri());

        // Checking types
        assertEquals(ent1Types.size(), ent1.getTypes().size());
        assertEquals(ent2Types.size(), ent2.getTypes().size());

        for (Type t : ent1.getTypes())
        {
            assertTrue(ent1Types.contains(t.getType()));
            assertTrue(t.getIdf() > 0);
        }

        for (Type t : ent2.getTypes())
        {
            assertTrue(ent2Types.contains(t.getType()));
            assertTrue(t.getIdf() > 0);
        }
    }

    @Test
    public void testEntityTableLink()
    {
        EntityTableLink entityTableLink = reader.getEntityTableLink();
        EntityLinking linking = reader.getLinker();

        assertEquals(1, entityTableLink.find(linking.kgUriLookup("http://dbpedia.org/resource/1963_Formula_One_season")).size());
        assertEquals("table-0072-223.json", entityTableLink.find(linking.kgUriLookup("http://dbpedia.org/resource/1963_Formula_One_season")).get(0));
        assertEquals(1, entityTableLink.getLocations(linking.kgUriLookup("http://dbpedia.org/resource/1963_Formula_One_season"), "table-0072-223.json").size());

        assertEquals(1, entityTableLink.find(linking.kgUriLookup("http://dbpedia.org/resource/Windows_Phone_7")).size());
        assertEquals("table-0782-820.json", entityTableLink.find(linking.kgUriLookup("http://dbpedia.org/resource/Windows_Phone_7")).get(0));
        assertEquals(2, entityTableLink.getLocations(linking.kgUriLookup("http://dbpedia.org/resource/Windows_Phone_7"), "table-0782-820.json").size());
    }
}
