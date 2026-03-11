package com.thetis.store.hnsw;

import com.stepstone.search.hnswlib.jna.QueryTuple;
import com.stepstone.search.hnswlib.jna.SpaceName;
import com.stepstone.search.hnswlib.jna.exception.OnceIndexIsClearedItCannotBeReusedException;
import com.stepstone.search.hnswlib.jna.exception.QueryCannotReturnResultsException;
import com.thetis.store.EntityLinking;
import com.thetis.store.EntityTable;
import com.thetis.store.EntityTableLink;
import com.thetis.store.Index;
import com.thetis.structures.Id;
import com.thetis.structures.graph.Entity;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

public class HNSW implements Index<String, Set<String>>
{
    private transient Function<Entity, List<Double>> embeddingsGen;
    private com.stepstone.search.hnswlib.jna.Index hnsw;
    private int embeddingsDim, k, findError = 0;
    private long capacity;
    private EntityLinking linker;
    private EntityTable entityTable;
    private EntityTableLink entityTableLink;
    private String indexPath;
    private static final int M = 24;
    private static final int EF = 300;

    public HNSW(Function<Entity, List<Double>> embeddingsGenerator, int embeddingsDimension, long capacity, int neighborhoodSize,
                EntityLinking linker, EntityTable entityTable, EntityTableLink entityTableLink, String indexPath)
    {
        this.embeddingsGen = embeddingsGenerator;
        this.embeddingsDim = embeddingsDimension;
        this.capacity = capacity;
        this.k = neighborhoodSize;
        this.linker = linker;
        this.entityTable = entityTable;
        this.entityTableLink = entityTableLink;
        this.indexPath = indexPath;
        this.hnsw = new com.stepstone.search.hnswlib.jna.Index(SpaceName.COSINE, embeddingsDimension);
        this.hnsw.initialize((int) capacity, M, EF, 100);
    }

    public void setLinker(EntityLinking linker)
    {
        this.linker = linker;
    }

    public void setEntityTable(EntityTable entityTable)
    {
        this.entityTable = entityTable;
    }

    public void setEntityTableLink(EntityTableLink entityTableLink)
    {
        this.entityTableLink = entityTableLink;
    }

    public void setEmbeddingGenerator(Function<Entity, List<Double>> embeddingGenerator)
    {
        this.embeddingsGen = embeddingGenerator;
    }

    public int getEmbeddingsDimension()
    {
        return this.embeddingsDim;
    }

    public long getCapacity()
    {
        return this.capacity;
    }

    public int getNeighborhoodSize()
    {
        return this.k;
    }

    public String getIndexPath()
    {
        return this.indexPath;
    }

    private static float[] toPrimitiveEmbeddings(List<Double> embedding)
    {
        float[] embeddingsArray = new float[embedding.size()];
        int i = 0;

        for (double e : embedding)
        {
            embeddingsArray[i++] = (float) e;
        }

        return embeddingsArray;
    }

    public void setCapacity(long capacity)
    {
        this.capacity = capacity;
        this.hnsw.initialize((int) capacity);
    }

    public void setK(int k)
    {
        this.k = k;
    }

    /**
     * Inserts an entry into the HSNW index that maps an entity to a set of tables
     * If the mapping already exists, the tables will be added to the co-domain of the mapping
     * @param key Entity to be inserted
     * @param tables Tables to be inserted into the index and mapped to by the given entity (ignore this parameter since we retrieve the tables from another index)
     */
    @Override
    public void insert(String key, Set<String> tables)
    {
        Id id = this.linker.kgUriLookup(key);

        if (id == null)
        {
            return;
        }

        Entity entity = this.entityTable.find(id);
        List<Double> embedding = this.embeddingsGen.apply(entity);

        if (embedding != null)
        {
            float[] primitiveEmbedding = toPrimitiveEmbeddings(embedding);
            this.hnsw.addItem(primitiveEmbedding, id.getId());
        }
    }

    /**
     * Removes the entity from the HNSW index
     * @param key Entity to be removed
     * @return True if the entity has an ID and thereby can be removed, otherwise false
     */
    @Override
    public boolean remove(String key)
    {
        Id id = this.linker.kgUriLookup(key);

        if (id == null)
        {
            return false;
        }

        this.hnsw.markDeleted(id.getId());
        return true;
    }

    /**
     * Finds tables containing K-nearest neighbors
     * @param key Query entity
     * @return Set of tables
     */
    @Override
    public Set<String> find(String key)
    {
        Id id = this.linker.kgUriLookup(key);

        if (id == null)
        {
            return Collections.emptySet();
        }

        try
        {
            Entity entity = this.entityTable.find(id);
            List<Double> embedding = this.embeddingsGen.apply(entity);

            if (embedding == null)
            {
                return Collections.emptySet();
            }

            float[] primitiveEmbedding = toPrimitiveEmbeddings(embedding);
            QueryTuple results = this.hnsw.knnQuery(primitiveEmbedding, this.k);
            Set<String> tables = new HashSet<>();

            for (int resultId : results.getIds())
            {
                tables.addAll(this.entityTableLink.find(new Id(resultId)));
            }

            if (tables.isEmpty())
            {
                this.hnsw.setEf(this.hnsw.getEf() * 2);
                this.findError++;

                if (this.findError > 10)
                {
                    this.findError = 0;
                    return tables;
                }

                return find(key);
            }

            this.findError = 0;
            return tables;
        }

        catch (QueryCannotReturnResultsException e)
        {
            this.hnsw.setEf(this.hnsw.getEf() * 2);
            this.findError++;

            if (this.findError > 10)
            {
                this.findError = 0;
                return Collections.emptySet();
            }

            return find(key);
        }

        catch (OnceIndexIsClearedItCannotBeReusedException e)
        {
            return Collections.emptySet();
        }
    }

    /**
     * Approximately checks whether a given entity exists in the HNSW index
     * @param key Entity to check
     * @return True if the entity is in the top-K nearest neighbors
     */
    @Override
    public boolean contains(String key)
    {
        Id id = this.linker.kgUriLookup(key);

        if (id == null)
        {
            return false;
        }

        return find(key).stream().anyMatch(key::equals);
    }

    /**
     * Number of entities in the HNSW index
     *
     * @return The size of the HNSW index in terms of number of stored entities
     */
    @Override
    public int size()
    {
        return this.hnsw.getLength();
    }

    /**
     * Clears the HNSW index
     */
    @Override
    public void clear()
    {
        this.hnsw.clear();
    }

    public void save()
    {
        this.hnsw.save(Path.of(this.indexPath));
    }

    public void load()
    {
        this.hnsw.load(Path.of(this.indexPath), (int) this.capacity);
    }
}
