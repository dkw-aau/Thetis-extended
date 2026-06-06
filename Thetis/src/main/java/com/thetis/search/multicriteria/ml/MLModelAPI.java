package com.thetis.search.multicriteria.ml;

import ml.dmlc.xgboost4j.java.XGBoostError;

import java.util.Iterator;

public class MLModelAPI implements MLModel<Iterator<? extends Feature<?>>>
{
    public enum EngineLabel
    {
        THETIS(0, "Thetis"), BM25(1, "BM25");

        private final int id;
        private String label;

        EngineLabel(int id, String label)
        {
            this.id = id;
            this.label = label;
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

        @Override
        public String toString()
        {
            return this.label;
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
