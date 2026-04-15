package com.thetis.search.multicriteria;

import com.thetis.search.Result;
import com.thetis.structures.Pair;

import java.util.*;
import java.util.function.Function;

public class OrderedCombiner implements CombinerPipeline
{
    private final List<RankingCombiner> combiners;
    private final Function<List<Double>, Double> aggregator;

    public OrderedCombiner(List<RankingCombiner> combiners)
    {
        this.combiners = combiners;
        this.aggregator = lst -> lst.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
    }

    public OrderedCombiner(List<RankingCombiner> combiners, Function<List<Double>, Double> entryAggregator)
    {
        this.combiners = combiners;
        this.aggregator = entryAggregator;
    }

    @Override
    public Result combine(CombinedRanking results)
    {
        CombinedRanking current = results;

        // This is not ideal, but if the results contains chunks (i.e., the ranking is of type ChunkedCombinedRanking),
        // then we execute the next combiner on each of these chunks and flatten each ranking into one large ranking
        // The first chunk contains the highest ranking items and so on
        // E.g., we execute first Pareto, which returns chunks, and then execute TOPSIS on each chunk
        // This results in TOPSIS only being applied locally whilst retaining the global ranking from Pareto
        for (RankingCombiner combiner : this.combiners)
        {
            if (current instanceof ChunkedCombinedRanking chunkedRanking)
            {
                int chunks = chunkedRanking.chunks();
                List<CombinedRanking.Entry> entries = new ArrayList<>();

                for (int chunk = 0; chunk < chunks; chunk++)
                {
                    CombinedRanking subRanking = combiner.rank(chunkedRanking.getChunk(chunk));

                    for (CombinedRanking.Entry entry : subRanking)
                    {
                        entries.add(entry);
                    }
                }

                current = new CombinedRanking(entries);
            }

            else
            {
                current = combiner.rank(current);
            }
        }

        List<Pair<String, Double>> alignedRanking = enforceOrderingScores(current);
        return new Result(alignedRanking.size(), alignedRanking);
    }

    private List<Pair<String, Double>> enforceOrderingScores(CombinedRanking ranking)
    {
        Map<String, Double> aggregatedScores = new HashMap<>();
        double minScore = Double.MAX_VALUE, maxScore = Double.MIN_VALUE;

        for (CombinedRanking.Entry entry : ranking)
        {
            double score = this.aggregator.apply(entry.scores());
            aggregatedScores.put(entry.id(), score);
            minScore = Math.min(minScore, score);
            maxScore = Math.max(maxScore, score);
        }

        int i = 0;
        double degradationRate = (maxScore - minScore) / ranking.size();
        List<Pair<String, Double>> alignedScores = new ArrayList<>(ranking.size());

        for (Map.Entry<String, Double> entry : aggregatedScores.entrySet())
        {
            double alignedScore = maxScore - i++ * degradationRate;
            alignedScores.add(new Pair<>(entry.getKey(), alignedScore));
        }

        return alignedScores;
    }
}
