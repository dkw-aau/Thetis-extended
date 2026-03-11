package com.thetis.store.hnsw;

import com.thetis.store.EmbeddingsIndex;
import com.thetis.store.EntityLinking;
import com.thetis.store.EntityTable;
import com.thetis.store.EntityTableLink;
import com.thetis.structures.Id;
import com.thetis.structures.graph.Entity;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class HNSWTest
{
    private static final File EMBEDDINGS_FILE = new File("src/test/resources/embeddings.txt");
    private HNSW hnsw;
    private EntityLinking linker;
    private EntityTable entityTable;
    private EntityTableLink tableLinks;
    private EmbeddingsIndex<Id> embeddingsIndex;

    @Before
    public void setup()
    {
        List<String> tables = List.of("file1", "file2", "file3");
        this.linker = new EntityLinking("", "");
        this.entityTable = new EntityTable();
        this.tableLinks = new EntityTableLink();
        this.embeddingsIndex = new EmbeddingsIndex<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(EMBEDDINGS_FILE)))
        {
            String line;

            while ((line = reader.readLine()) != null)
            {
                String[] tokens = line.split(" ");
                String uri = tokens[0];
                List<Double> embedding = new ArrayList<>(200);

                for (int i = 1; i < tokens.length; i++)
                {
                    embedding.add(Double.parseDouble(tokens[i]));
                }

                Entity entity = new Entity(uri, List.of(), List.of());
                this.linker.addMapping(uri.replace("dbpedia", "wikipedia"), uri);

                Id id = this.linker.kgUriLookup(uri);
                this.entityTable.insert(id, entity);
                this.tableLinks.insert(id, tables);
                this.embeddingsIndex.insert(id, embedding);
            }

            Iterator<Id> idIterator = this.linker.kgUriIds();
            this.hnsw = new HNSW(entity -> this.embeddingsIndex.find(this.linker.kgUriLookup(entity.getUri())),
                    200, 2000, 10, this.linker, this.entityTable, this.tableLinks, "");

            while (idIterator.hasNext())
            {
                String uri = this.linker.kgUriLookup(idIterator.next());
                this.hnsw.insert(uri, new HashSet<>(tables));
            }
        }

        catch (IOException e)
        {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testSize()
    {
        assertEquals(1592, this.hnsw.size());
    }

    @Test
    public void testFind()
    {
        Set<String> tables = this.hnsw.find("http://dbpedia.org/resource/Joe_Pigott");
        assertEquals(3, tables.size());
    }

    @Test
    public void testClear()
    {
        assertEquals(3, this.hnsw.find("http://dbpedia.org/resource/Joe_Pigott").size());
        assertEquals(1592, this.hnsw.size());

        this.hnsw.clear();
        assertEquals(0, this.hnsw.find("http://dbpedia.org/resource/Joe_Pigott").size());
    }
}
