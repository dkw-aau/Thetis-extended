package com.thetis.search.multicriteria.ml;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class FrequencyFeatureTest
{
    @Test
    public void testGetLabel()
    {
        FrequencyFeature.EntityFrequencyFeature entityFeature1 = new FrequencyFeature.EntityFrequencyFeature(0, List.of(1L, 2L, 3L)),
                entityFeature2 = new FrequencyFeature.EntityFrequencyFeature(1, List.of(2L, 3L, 4L)),
                entityFeature3 = new FrequencyFeature.EntityFrequencyFeature(0, List.of(5L));
        FrequencyFeature feature = new FrequencyFeature(0, List.of(entityFeature1, entityFeature2, entityFeature3));
        assertEquals(0, feature.getLabel());
    }

    @Test
    public void testGetFeature()
    {
        FrequencyFeature.EntityFrequencyFeature entityFeature1 = new FrequencyFeature.EntityFrequencyFeature(0, List.of(1L, 2L, 3L)),
                entityFeature2 = new FrequencyFeature.EntityFrequencyFeature(1, List.of(2L, 3L, 4L)),
                entityFeature3 = new FrequencyFeature.EntityFrequencyFeature(0, List.of(5L));
        FrequencyFeature feature = new FrequencyFeature(0, List.of(entityFeature1, entityFeature2, entityFeature3));
        assertEquals(3, feature.getFeature().size());
    }

    @Test
    public void testFlatten()
    {
        FrequencyFeature.EntityFrequencyFeature entityFeature1 = new FrequencyFeature.EntityFrequencyFeature(0, List.of(1L, 2L, 3L)),
                entityFeature2 = new FrequencyFeature.EntityFrequencyFeature(1, List.of(2L, 3L, 4L)),
                entityFeature3 = new FrequencyFeature.EntityFrequencyFeature(0, List.of(5L));
        FrequencyFeature feature = new FrequencyFeature(0, List.of(entityFeature1, entityFeature2, entityFeature3));
        assertEquals(7, feature.flatten().length);
    }

    @Test
    public void testTransformToLibsvm()
    {
        FrequencyFeature.EntityFrequencyFeature entityFeature1 = new FrequencyFeature.EntityFrequencyFeature(0, List.of(1L, 2L, 3L)),
                entityFeature2 = new FrequencyFeature.EntityFrequencyFeature(1, List.of(2L, 3L, 4L)),
                entityFeature3 = new FrequencyFeature.EntityFrequencyFeature(0, List.of(5L, 6L, 7L));
        FrequencyFeature feature = new FrequencyFeature(0, List.of(entityFeature1, entityFeature2, entityFeature3));
        String[] libsvm = feature.transformToLibsvm();
        assertEquals(10, libsvm.length);
        assertEquals("0", libsvm[0]);
        assertEquals("1:1", libsvm[1]);
        assertEquals("2:2", libsvm[2]);
        assertEquals("3:3", libsvm[3]);
        assertEquals("4:2", libsvm[4]);
        assertEquals("5:3", libsvm[5]);
        assertEquals("6:4", libsvm[6]);
        assertEquals("7:5", libsvm[7]);
        assertEquals("8:6", libsvm[8]);
        assertEquals("9:7", libsvm[9]);
    }
}
