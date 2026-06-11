package com.thetis.commands;

import com.thetis.connector.Neo4jEndpoint;
import com.thetis.loader.IndexReader;
import com.thetis.search.multicriteria.ml.FeatureCollector;
import com.thetis.search.multicriteria.ml.FrequencyFeature;
import com.thetis.search.multicriteria.ml.MLModelAPI;
import com.thetis.store.EntityLinking;
import com.thetis.store.EntityTableLink;
import com.thetis.system.Logger;
import picocli.CommandLine;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

@picocli.CommandLine.Command(name = "train", description = "Train ML model on query set to predict optimal search engine")
public class Train extends Command
{
    //********************* Command Line Arguments *********************//
    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec; // injected by picocli

    private File dataFile = null;
    @CommandLine.Option(names = {"-df", "--data-file"}, description = "File containing labeled training data", required = true)
    public void setDataDir(File value)
    {
        if (!value.exists())
        {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("InvaRoyaltylid value '%s' for option '--data-file': " +
                            "the file does not exists.", value));
        }

        this.dataFile = value;
    }

    private File indexDir = null;
    @CommandLine.Option(names = { "-i", "--index-dir" }, paramLabel = "INDEX_DIR", description = "Directory of loaded indexes", defaultValue = "../data/index/wikitables/")
    public void setHashMapDirectory(File value)
    {
        if(!value.exists())
        {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("Invalid value '%s' for option '--index-dir': " + "the directory does not exists.", value));
        }

        if (!value.isDirectory())
        {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("Invalid value '%s' for option '--index-dir': " + "the path does not point to a directory.", value));
        }

        if (!value.canWrite())
        {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("Invalid value '%s' for option '--index-dir': " +
                            "the directory is not writable.", value));
        }

        this.indexDir = value;
    }

    private File configFile = null;
    @CommandLine.Option(names = { "-cf", "--config"}, paramLabel = "CONF", description = "configuration file", required = true, defaultValue = "./config.properties" )
    public void setConfigFile(File value)
    {
        if(!value.exists())
        {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("Invalid value '%s' for option '--config': " +
                            "the file does not exists.", value));
        }

        if (value.isDirectory())
        {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("Invalid value '%s' for option '--config': " +
                            "the path should point to a file not to a directory.", value));
        }

        this.configFile = value;
    }

    @CommandLine.Option(names = {"-ts", "--test-split-size"}, description = "Fraction of test split size", required = true)
    private double testSplitFraction;

    @Override
    public Integer call()
    {
        try (BufferedReader reader = new BufferedReader(new FileReader(this.dataFile)))
        {
            Neo4jEndpoint neo4jEndpoint = new Neo4jEndpoint(this.configFile);
            IndexReader indexReader = new IndexReader(this.indexDir, true, true);
            indexReader.performIO();

            int maxEntityCount = maxEntities();
            EntityLinking linker = indexReader.getLinker();
            EntityTableLink entityTableLink = indexReader.getEntityTableLink();
            Iterator<FrequencyFeature> frequencyFeatureIterator = new Iterator<>() {
                private String line;

                @Override
                public boolean hasNext()
                {
                    try
                    {
                        this.line = reader.readLine();
                        return this.line != null;
                    }

                    catch (IOException ignored)
                    {
                        return false;
                    }
                }

                @Override
                public FrequencyFeature next()
                {
                    String[] split = this.line.split(",");
                    MLModelAPI.EngineLabel label = MLModelAPI.EngineLabel.valueOf(Integer.parseInt(split[1]));
                    List<String> entities = new ArrayList<>(List.of(split[2].split(";")));

                    if (entities.size() < maxEntityCount)
                    {
                        entities.addAll(new ArrayList<>(Collections.nCopies(maxEntityCount - entities.size(), "http://dbpedia.org/resource/null")));
                    }

                    return FeatureCollector.frequencyFeatures(entities, neo4jEndpoint, linker, entityTableLink, label.getId());
                }
            };
            MLModelAPI model = MLModelAPI.getXGBoostModel(this.testSplitFraction, this.indexDir.getAbsolutePath());
            long start = System.currentTimeMillis();
            Logger.logNewLine(Logger.Level.INFO, "Training XGBoost model");
            model.train(frequencyFeatureIterator);

            Logger.logNewLine(Logger.Level.INFO, "Training complete in " + (System.currentTimeMillis() - start) + "ms");
        }

        catch (IOException e)
        {
            Logger.logNewLine(Logger.Level.ERROR, "Error reading data file: " + e.getMessage());
            return -1;
        }

        return 0;
    }

    private int maxEntities()
    {
        try (BufferedReader reader = new BufferedReader(new FileReader(this.dataFile)))
        {
            String line;
            int max = 0;

            while ((line = reader.readLine()) != null)
            {
                max = Math.max(max, line.split(",").length);
            }

            return max;
        }

        catch (IOException e)
        {
            return -1;
        }
    }
}
