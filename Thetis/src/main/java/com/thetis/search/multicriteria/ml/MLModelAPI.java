package com.thetis.search.multicriteria.ml;

import ml.dmlc.xgboost4j.java.XGBoostError;

import java.util.Iterator;

public class MLModelAPI implements MLModel<Iterator<? extends Feature<?>>>
{
    public enum EngineLabel
    {
        THETIS(0), BM25(1);

        private final int id;

        EngineLabel(int id)
        {
            this.id = id;
        }

        public int getId()
        {
            return this.id;
        }

        public static EngineLabel valueOf(int label)
        {
            return switch (label) {
                case 0 -> THETIS;
                case 1 -> BM25;
                default -> null;
            };
        }
    }

    private MLModel<Iterator<? extends Feature<?>>> model;

    private MLModelAPI(MLModel<Iterator<? extends Feature<?>>> model)
    {
        this.model = model;
    }

    public static MLModelAPI getXGBoostModel(double testSplitFraction, String modelPath)
    {
        MLModel<Iterator<? extends Feature<?>>> model = new XGBoostModel(testSplitFraction, modelPath);
        return new MLModelAPI(model);
    }

    public static MLModelAPI getGXBoostModel(String modelPath) throws XGBoostError
    {
        MLModel<Iterator<? extends Feature<?>>> model = new XGBoostModel(modelPath);
        return new MLModelAPI(model);
    }

    @Override
    public void train(Iterator<? extends Feature<?>> data)
    {
        this.model.train(data);
    }

    @Override
    public int predict(Feature<?> input)
    {
        return this.model.predict(input);
    }
}
