package com.thetis.search.multicriteria.ml;

import com.thetis.connector.DBDriverBatch;
import com.thetis.connector.Neo4jSemanticDriver;
import com.thetis.store.EmbeddingsIndex;
import com.thetis.store.EntityLinking;
import com.thetis.store.EntityTableLink;
import com.thetis.structures.Id;
import com.thetis.structures.table.Table;
import com.thetis.utilities.Utils;

import java.util.ArrayList;
import java.util.List;

public final class FeatureCollector
{
    /**
     * Given an entity, it constructs a 3-dimensional array containing the min and max entity type frequency and the entity freqyency itself
     * @param entity Entity to collect features from
     * @return 3-dimensional feature array
     */
    public static FrequencyFeature.EntityFrequencyFeature entityFrequencyFeature(String entity, Neo4jSemanticDriver neo4j, EntityLinking linker,
                                                                                 EntityTableLink entityTableLink, int label)
    {
        Id id = linker.kgUriLookup(entity);

        if (id == null)
        {
            return new FrequencyFeature.EntityFrequencyFeature(label, List.of(0L, 0L, 0L));
        }

        List<String> types = neo4j.searchTypes(entity);
        long min = Integer.MAX_VALUE, max = Integer.MIN_VALUE;

        for (String type : types)
        {
            long frequency = neo4j.typeFrequency(type);

            if (frequency > 0)
            {
                min = Math.min(min, frequency);
                max = Math.max(max, frequency);
            }
        }

        long entityFrequency = entityTableLink.find(id).size();
        return new FrequencyFeature.EntityFrequencyFeature(label, List.of(entityFrequency, min < Integer.MAX_VALUE ? min : 0, Math.max(max, 0)));
    }

    /**
     * It constructs entity frequency features for all entities in a list
     * @param entities List of entities
     * @return List of entity frequency features
     */
    public static FrequencyFeature frequencyFeatures(List<String> entities, Neo4jSemanticDriver neo4j, EntityLinking linker,
                                                     EntityTableLink entityTableLink, int label)
    {
        List<FrequencyFeature.EntityFrequencyFeature> entityFeatures = new ArrayList<>(entities.size());

        for (String entity : entities)
        {
            FrequencyFeature.EntityFrequencyFeature entityFeature = entityFrequencyFeature(entity, neo4j, linker, entityTableLink, label);

            if (entityFeature != null)
            {
                entityFeatures.add(entityFeature);
            }
        }

        return new FrequencyFeature(label, entityFeatures);
    }

    public static EmbeddingsFeature queryEmbeddingFeature(Table<String> query, int label, EntityLinking linker, EmbeddingsIndex<Id> embeddingsIndex)
    {
        int rows = query.rowCount();
        int columns = query.columnCount();
        List<List<Double>> sumVectors = new ArrayList<>(rows);

        for (int row = 0; row < rows; row++)
        {
            List<Double> sumVector = null;

            for (int column = 0; column < columns; column++)
            {
                String entity = query.getRow(row).get(column);
                Id id = linker.kgUriLookup(entity);

                if (id != null)
                {
                    List<Double> embedding = embeddingsIndex.find(id);
                    int dimension = embedding.size();


                    if (sumVector == null)
                    {
                        sumVector = embedding;
                    }

                    else
                    {
                        for (int dim = 0; dim < dimension; dim++)
                        {
                            sumVector.set(dim, sumVector.get(dim) + embedding.get(dim));
                        }
                    }
                }
            }

            sumVectors.add(sumVector);
        }

        if (sumVectors.isEmpty())
        {
            return null;
        }

        return new EmbeddingsFeature(Utils.averageVector(sumVectors), label);
    }
}
