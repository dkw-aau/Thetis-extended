package com.thetis.search.multicriteria.ml;

import com.thetis.connector.Neo4jEndpoint;
import com.thetis.connector.Neo4jSemanticDriver;
import com.thetis.store.EntityLinking;
import com.thetis.store.EntityTableLink;
import com.thetis.structures.Id;

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
        List<Long> features = new ArrayList<>(3);
        List<String> types = neo4j.searchTypes(entity);
        long min = Integer.MAX_VALUE, max = Integer.MIN_VALUE;

        for (String type : types)
        {
            long frequency = neo4j.typeFrequency(type);
            min = Math.min(min, frequency);
            max = Math.max(max, frequency);
        }

        Id id = linker.kgUriLookup(entity);
        features.add(min);
        features.add(max);

        if (id != null)
        {
            long entityFrequency = entityTableLink.find(id).size();
            features.add(entityFrequency);

            return new FrequencyFeature.EntityFrequencyFeature(label, features);
        }

        return null;
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
}
