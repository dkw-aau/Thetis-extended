package com.thetis.search.multicriteria.ml;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class EntityFrequencyFeatureTest
{
    @Test
    public void testGetLabel()
    {
        FrequencyFeature.EntityFrequencyFeature entityFeature = new FrequencyFeature.EntityFrequencyFeature(0, List.of(1L, 2L, 3L));
        assertEquals(0, entityFeature.getLabel());
    }

    @Test
    public void testGetFeature()
    {
        FrequencyFeature.EntityFrequencyFeature entityFeature = new FrequencyFeature.EntityFrequencyFeature(0, List.of(1L, 2L, 3L));
        assertEquals(List.of(1L, 2L, 3L), entityFeature.getFeature());
    }

    @Test
    public void testFlatten()
    {
        FrequencyFeature.EntityFrequencyFeature entityFeature = new FrequencyFeature.EntityFrequencyFeature(0, List.of(1L, 2L, 3L));
        assertEquals(3, entityFeature.flatten().length);
        assertEquals(1f, entityFeature.flatten()[0], 0.0001);
        assertEquals(2f, entityFeature.flatten()[1], 0.0001);
        assertEquals(3f, entityFeature.flatten()[2], 0.0001);
    }

    @Test
    public void testTransformToLibsvm()
    {
        FrequencyFeature.EntityFrequencyFeature entityFeature = new FrequencyFeature.EntityFrequencyFeature(0, List.of(1L, 2L, 3L));
        String[] libsvm = entityFeature.transformToLibsvm();
        assertEquals(4, libsvm.length);
        assertEquals("0", libsvm[0]);
        assertEquals("1:1", libsvm[1]);
        assertEquals("2:2", libsvm[2]);
        assertEquals("3:3", libsvm[3]);
    }
}
