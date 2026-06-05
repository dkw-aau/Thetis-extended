package com.thetis.search.multicriteria.ml;

public interface Feature<D>
{
    int getLabel();
    D getFeature();
    String[] transformToLibsvm();
    float[] flatten();
}
