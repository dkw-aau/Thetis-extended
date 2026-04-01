package com.thetis.search.multicriteria;

import com.thetis.search.AbstractSearch;
import com.thetis.search.Result;
import com.thetis.structures.Pair;
import com.thetis.structures.table.Table;

import java.util.*;
import java.util.function.Function;

// Entrypoint for searching with multiple search engines and unifying their rankings
public class MultiSearch extends AbstractSearch
{
    private final List<AbstractSearch> engines;
    private final CombinerPipeline pipeline;
    private long elapsed = -1;

    public MultiSearch(CombinerPipeline pipeline, AbstractSearch ... engines)
    {
        super(null, null, null, null);
        this.pipeline = pipeline;
        this.engines = List.of(engines);
    }

    public static CombinerPipeline createPipeline(RankingCombiner ... combiners)
    {
        return new OrderedCombiner(List.of(combiners));
    }

    public static CombinerPipeline createPipeline(Function<List<Double>, Double> entryAggregator, RankingCombiner ... combiners)
    {
        return new OrderedCombiner(List.of(combiners), entryAggregator);
    }

    @Override
    protected Result abstractSearch(Table<String> query)
    {
        long start = System.nanoTime();
        CombinedRanking engineResults = new CombinedRanking();
        int criteria = 0;

        for (AbstractSearch engine : this.engines)
        {
            Result result = engine.search(query);
            engineResults = join(engineResults, criteria++, result);
        }

        Result combinedResult = this.pipeline.combine(engineResults);
        this.elapsed = System.nanoTime() - start;

        return combinedResult;
    }

    // Full outer join
    private static CombinedRanking join(CombinedRanking ranking, int criteria, Result newResult)
    {
        Set<String> newResultIds = new HashSet<>();

        for (Pair<String, Double> result : newResult)
        {
            if (!newResultIds.contains(result.getFirst()))
            {
                if (ranking.contains(result.getFirst()))
                {
                    ranking.getEntry(result.getFirst()).scores().add(result.getSecond());
                }

                else
                {
                    CombinedRanking.Entry entry = new CombinedRanking.Entry(result.getFirst(), new ArrayList<>(Collections.nCopies(criteria, 0.0)));
                    entry.scores().add(result.getSecond());
                    ranking.addEntry(entry);
                }

                newResultIds.add(result.getFirst());
            }
        }

        for (CombinedRanking.Entry entry : ranking)
        {
            if (!newResultIds.contains(entry.id()))
            {
                entry.scores().add(0.0);
            }
        }

        return ranking;
    }

    @Override
    protected long abstractElapsedNanoSeconds()
    {
        return this.elapsed;
    }
}
