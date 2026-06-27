package com.thetis.search.multicriteria.ml;

import com.opencsv.CSVWriter;
import com.thetis.system.Logger;
import ml.dmlc.xgboost4j.java.Booster;
import ml.dmlc.xgboost4j.java.DMatrix;
import ml.dmlc.xgboost4j.java.XGBoost;
import ml.dmlc.xgboost4j.java.XGBoostError;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.random.RandomGenerator;

class TrainPipeline implements Runnable
{
    private final double testFraction;
    private final String modelPath, testPath, trainPath;
    private final Iterator<? extends Feature<?>> featureIterator;
    private final RandomGenerator randomGenerator = new Random();
    private final Map<String, Object> config = new HashMap<>();
    private Booster booster;
    private final List<Integer> trainGT = new ArrayList<>(), testGT = new ArrayList<>();
    private static final String MODEL_NAME = "model.bin";
    private static final int TRAIN_ROUND = 10;

    public TrainPipeline(double testSplitFraction, String modelPath, Iterator<? extends Feature<?>> featureIterator)
    {
        if (testSplitFraction < 0.0 || testSplitFraction > 1.0)
        {
            throw new IllegalArgumentException("Test split fraction must be within [0, 1]");
        }

        this.testFraction = testSplitFraction;
        this.modelPath = modelPath;
        this.testPath = modelPath + "/test.svm.text";
        this.trainPath = modelPath + "/train.svm.txt";
        this.featureIterator = featureIterator;

        this.config.put("eta", "0.1");
        this.config.put("max_depth", "3");
        this.config.put("objective", "multi:softmax");
        this.config.put("eval_metric", "merror");
        this.config.put("num_class", "7");
    }

    public String getModelPath()
    {
        return this.modelPath + "/" + MODEL_NAME;
    }

    public String getTrainPath()
    {
        return this.trainPath;
    }

    public String getTestPath()
    {
        return this.testPath;
    }

    public Map<String, Object> getConfig()
    {
        return this.config;
    }

    @Override
    public void run()
    {
        long startTime = System.currentTimeMillis();
        Logger.logNewLine(Logger.Level.INFO, "Splitting dataset into train and test sets");

        try (CSVWriter testWriter = new CSVWriter(new FileWriter(this.testPath), ' ', CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER, "\n");
             CSVWriter trainWriter = new CSVWriter(new FileWriter(this.trainPath), ' ', CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER, "\n"))
        {
            while (this.featureIterator.hasNext())
            {
                Feature<?> feature = this.featureIterator.next();

                if (feature == null)
                {
                    continue;
                }

                if (this.randomGenerator.nextDouble() < this.testFraction)
                {
                    testWriter.writeNext(feature.transformToLibsvm());
                    this.testGT.add(feature.getLabel());
                }

                else
                {
                    trainWriter.writeNext(feature.transformToLibsvm());
                    this.trainGT.add(feature.getLabel());
                }
            }
        }

        catch (IOException e)
        {
            Logger.logNewLine(Logger.Level.ERROR, e.getMessage());
            throw new RuntimeException(e);
        }

        Logger.logNewLine(Logger.Level.INFO, "Done constructing training and test sets in " + (System.currentTimeMillis() - startTime) + "ms");
        Logger.logNewLine(Logger.Level.INFO, "Training model");

        try
        {
            DMatrix trainMatrix = new DMatrix(this.trainPath + "?format=libsvm"),
                    testMatrix = new DMatrix(this.testPath + "?format=libsvm");
            Map<String, DMatrix> watches = new HashMap<>();
            watches.put("train", trainMatrix);
            watches.put("test", testMatrix);

            this.booster = XGBoost.train(trainMatrix, this.config, TRAIN_ROUND, watches, null, null);
            this.booster.saveModel(this.modelPath + "/" + MODEL_NAME);
        }

        catch (XGBoostError e)
        {
            Logger.logNewLine(Logger.Level.ERROR, e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void evalModel()
    {
        try
        {
            DMatrix trainMatrix = new DMatrix(this.trainPath + "?format=libsvm"),
                    testMatrix = new DMatrix(this.testPath + "?format=libsvm");
            float[][] trainPredictions = this.booster.predict(trainMatrix);
            float[][] testPredictions = this.booster.predict(testMatrix);
            int trueTrainPredictions = evalPredictions(trainPredictions, this.trainGT),
                    trueTestPredictions = evalPredictions(testPredictions, this.testGT);
            double trainAccuracy = ((double) trueTrainPredictions / (trainPredictions.length - trueTrainPredictions) + trueTrainPredictions) * 100,
                    testAccuracy = ((double) trueTestPredictions / (testPredictions.length - trueTestPredictions) + trueTestPredictions) * 100;
            Logger.logNewLine(Logger.Level.INFO, "Train accuracy: " + trainAccuracy + "%");
            Logger.logNewLine(Logger.Level.INFO, "Test accuracy: " + testAccuracy + "%");
        }

        catch (XGBoostError e)
        {
            Logger.logNewLine(Logger.Level.ERROR, "Failed evaluation XGBoost model: " + e.getMessage());
        }
    }

    private static int evalPredictions(float[][] predictions, List<Integer> gt)
    {
        int count = 0;

        for (int i = 0; i < predictions.length; i++)
        {
            int prediction = (int) predictions[i][0],
                    gtLabel = gt.get(i);

            if (prediction == gtLabel)
            {
                count++;
            }
        }

        return count;
    }
}
