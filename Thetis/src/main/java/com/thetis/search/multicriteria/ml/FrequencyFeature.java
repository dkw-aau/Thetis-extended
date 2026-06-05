package com.thetis.search.multicriteria.ml;

import java.util.ArrayList;
import java.util.List;

public class FrequencyFeature implements Feature<List<FrequencyFeature.EntityFrequencyFeature>>
{
    private final int label;
    private final List<EntityFrequencyFeature> entityFrequencies;

    public static class EntityFrequencyFeature implements Feature<List<Long>>
    {
        private final int label;
        private final List<Long> frequencies;

        public EntityFrequencyFeature(int label, List<Long> frequencies)
        {
            this.label = label;
            this.frequencies = frequencies;
        }

        @Override
        public int getLabel()
        {
            return this.label;
        }

        @Override
        public List<Long> getFeature()
        {
            return this.frequencies;
        }

        @Override
        public String[] transformToLibsvm()
        {
            int count = this.frequencies.size();
            String[] lsvmInput = new String[count + 1];
            lsvmInput[0] = String.valueOf(this.label);

            for (int i = 1; i <= count; i++)
            {
                lsvmInput[i] = i + ":" + this.frequencies.get(i - 1);
            }

            return lsvmInput;
        }

        @Override
        public float[] flatten()
        {
            int count = this.frequencies.size();
            float[] flattened = new float[count];

            for (int i = 0; i < count; i++)
            {
                flattened[i] = this.frequencies.get(i);
            }

            return flattened;
        }
    }

    FrequencyFeature(int label, List<EntityFrequencyFeature> entityFrequencies)
    {
        this.label = label;
        this.entityFrequencies = entityFrequencies;
    }

    @Override
    public int getLabel()
    {
        return this.label;
    }

    @Override
    public List<EntityFrequencyFeature> getFeature()
    {
        return this.entityFrequencies;
    }

    @Override
    public String[] transformToLibsvm()
    {
        int count = this.entityFrequencies.size(), columnCount = 1;
        String[] lsvmInput = new String[count * 3 + 1];
        lsvmInput[0] = String.valueOf(this.label);

        for (EntityFrequencyFeature entityFeature : this.entityFrequencies)
        {
            lsvmInput[columnCount] = columnCount++ + ":" + entityFeature.getFeature().get(0);
            lsvmInput[columnCount] = columnCount++ + ":" + entityFeature.getFeature().get(1);
            lsvmInput[columnCount] = columnCount++ + ":" + entityFeature.getFeature().get(2);
        }

        return lsvmInput;
    }

    @Override
    public float[] flatten()
    {
        List<Float> flattened = new ArrayList<>();

        for (EntityFrequencyFeature entityFeature : this.entityFrequencies)
        {
            entityFeature.getFeature().forEach(frequency -> flattened.add((float) frequency));
        }

        float[] flattenedArray = new float[flattened.size()];

        for (int i = 0; i < flattenedArray.length; i++)
        {
            flattenedArray[i] = flattened.get(i);
        }

        return flattenedArray;
    }
}
