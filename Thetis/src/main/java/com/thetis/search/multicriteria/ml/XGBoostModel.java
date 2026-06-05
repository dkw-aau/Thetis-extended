package com.thetis.search.multicriteria.ml;

import ml.dmlc.xgboost4j.java.Booster;
import ml.dmlc.xgboost4j.java.DMatrix;
import ml.dmlc.xgboost4j.java.XGBoost;
import ml.dmlc.xgboost4j.java.XGBoostError;

import java.util.Iterator;

class XGBoostModel implements MLModel<Iterator<? extends Feature<?>>>
{
    private final double testSplitFraction;
    private final String modelPath;
    private boolean isModelLoaded;
    private Booster model;

    public XGBoostModel(double testSplitFraction, String modelPath)
    {
        this.testSplitFraction = testSplitFraction;
        this.modelPath = modelPath;
        this.isModelLoaded = false;
    }

    public XGBoostModel(String modelPath) throws XGBoostError
    {
        this.testSplitFraction = -1.0;
        this.modelPath = modelPath;
        this.model = XGBoost.loadModel(modelPath);
        this.isModelLoaded = true;
    }

    @Override
    public void train(Iterator<? extends Feature<?>> data)
    {
        if (this.testSplitFraction == -1.0)
        {
            throw new IllegalArgumentException("Object not initialized for training");
        }

        TrainPipeline pipeline = new TrainPipeline(this.testSplitFraction, this.modelPath, data);
        pipeline.run();
        pipeline.evalModel();
        this.isModelLoaded = false;
    }

    @Override
    public int predict(Feature<?> input)
    {
        if (!this.isModelLoaded)
        {
            throw new IllegalArgumentException("Model requires training first");
        }

        try
        {
            float[] flattenedInput = input.flatten();
            DMatrix matrix = new DMatrix(flattenedInput, 1, flattenedInput.length, Float.NaN);
            return (int) this.model.predict(matrix)[0][0];
        }

        catch (XGBoostError e)
        {
            throw new RuntimeException(e.getMessage());
        }
    }
}
