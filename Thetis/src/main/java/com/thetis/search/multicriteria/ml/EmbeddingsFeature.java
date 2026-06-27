package com.thetis.search.multicriteria.ml;

import java.util.List;

public class EmbeddingsFeature implements Feature<List<Double>>
{
    private final List<Double> embedding;
    private final int label;

    public EmbeddingsFeature(List<Double> embedding, int label)
    {
        this.embedding = embedding;
        this.label = label;
    }

    @Override
    public int getLabel()
    {
        return this.label;
    }

    @Override
    public List<Double> getFeature()
    {
        return this.embedding;
    }

    @Override
    public String[] transformToLibsvm()
    {
        String[] lsvmInput = new String[this.embedding.size() + 1];
        int columnCount = 1;
        lsvmInput[0] = String.valueOf(this.label);

        for (double val : this.embedding)
        {
            lsvmInput[columnCount] = columnCount++ + ":" + val;
        }

        return lsvmInput;
    }

    @Override
    public float[] flatten()
    {
        float[] flattenedArray = new float[this.embedding.size()];
        int dimension = this.embedding.size();

        for (int i = 0; i < dimension; i++)
        {
            flattenedArray[i] = this.embedding.get(i).floatValue();
        }

        return flattenedArray;
    }
}
