package com.thetis.commands;

import com.thetis.commands.parser.TableParser;
import com.thetis.loader.IndexReader;
import com.thetis.search.multicriteria.ml.EmbeddingsFeature;
import com.thetis.search.multicriteria.ml.FeatureCollector;
import com.thetis.search.multicriteria.ml.MLModelAPI;
import com.thetis.store.EmbeddingsIndex;
import com.thetis.store.EntityLinking;
import com.thetis.structures.Id;
import com.thetis.structures.table.Table;
import com.thetis.system.Logger;
import picocli.CommandLine;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@picocli.CommandLine.Command(name = "train", description = "Train ML model on query set to predict optimal search engine")
public class Train extends Command
{
    //********************* Command Line Arguments *********************//
    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec; // injected by picocli

    private File queriesLocation;
    private List<Path> queryFiles;
    @CommandLine.Option(names = { "-q", "--queries" }, paramLabel = "QUERY", description = "Path to directory of query json files", required = true)
    public void setQueryFile(File value)
    {
        if (!value.exists())
        {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("Invalid value '%s' for option '--queries': " + "the directory does not exists.", value));
        }

        this.queriesLocation = value;

        if (value.isFile())
        {
            this.queryFiles = List.of(value.toPath());
        }

        else
        {
            try
            {
                Stream<Path> queryStream = Files.find(value.toPath(), Integer.MAX_VALUE,
                        (filePath, fileAttr) -> fileAttr.isRegularFile() && filePath.getFileName().toString().endsWith(".json"));
                this.queryFiles = queryStream.collect(Collectors.toList());
            }

            catch (IOException e)
            {
                Logger.logNewLine(Logger.Level.ERROR, "Exception when finding query files: " + e.getMessage());
                System.exit(1);
            }
        }
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

    private File labelFile = null;
    @CommandLine.Option(names = {"-lf", "--label-file"}, paramLabel = "LABEL", description = "label file of binary values", required = true)
    public void setLabelFile(File value)
    {
        if (!value.exists())
        {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("Invalid value '%s' for option '--label-file': " +
                            "the file does not exists.", value));
        }

        if (value.isDirectory())
        {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("Invalid value '%s' for option '--label-file': " +
                            "the path should point to a file not to a directory.", value));
        }

        this.labelFile = value;
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
        try
        {
            IndexReader indexReader = new IndexReader(this.indexDir, true, true);
            indexReader.performIO();

            EntityLinking linker = indexReader.getLinker();
            EmbeddingsIndex<Id> embeddingsIdx = indexReader.getEmbeddingsIndex();
            Iterator<Path> queryPathIterator = this.queryFiles.iterator();
            Map<String, MLModelAPI.EngineLabel> labelMap = readLabels();
            Iterator<EmbeddingsFeature> embeddingFeatureIterator = new Iterator<>() {
                private EmbeddingsFeature embeddingsFeature = null;
                File nextQueryFile = null;

                @Override
                public boolean hasNext()
                {
                    while (queryPathIterator.hasNext() &&
                            !labelMap.containsKey((this.nextQueryFile = queryPathIterator.next().toFile())
                                    .getName().replace(".json", "")));


                    return queryPathIterator.hasNext();
                }

                @Override
                public EmbeddingsFeature next()
                {
                    Table<String> queryTable = TableParser.toTable(this.nextQueryFile);
                    MLModelAPI.EngineLabel label = labelMap.get(this.nextQueryFile.getName().replace(".json", ""));

                    return FeatureCollector.queryEmbeddingFeature(queryTable, label.getId(), linker, embeddingsIdx);
                }
            };
            MLModelAPI model = MLModelAPI.getXGBoostModel(this.testSplitFraction, this.indexDir.getAbsolutePath());
            long start = System.currentTimeMillis();
            Logger.logNewLine(Logger.Level.INFO, "Training XGBoost model");
            model.train(embeddingFeatureIterator);

            Logger.logNewLine(Logger.Level.INFO, "Training complete in " + (System.currentTimeMillis() - start) + "ms");
        }

        catch (IOException e)
        {
            Logger.logNewLine(Logger.Level.ERROR, "Error reading data file: " + e.getMessage());
            return -1;
        }

        return 0;
    }

    private Map<String, MLModelAPI.EngineLabel> readLabels()
    {
        try (BufferedReader reader = new BufferedReader(new FileReader(this.labelFile)))
        {
            String line;
            Map<String, MLModelAPI.EngineLabel> labels = new HashMap<>();

            while ((line = reader.readLine()) != null)
            {
                String[] tokens = line.split(":");
                labels.put(tokens[0], MLModelAPI.EngineLabel.valueOf(Integer.parseInt(tokens[1])));
            }

            return labels;
        }

        catch (IOException e)
        {
            return null;
        }
    }
}
