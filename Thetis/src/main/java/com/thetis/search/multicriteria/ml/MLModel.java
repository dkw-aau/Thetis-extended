package com.thetis.search.multicriteria.ml;

public interface MLModel<D>
{
    // Train and predict
    void train(D data);
    int predict(Feature<?> input);
}
