package com.thetis.search.multicriteria;

import com.thetis.search.AnalogousSearch;
import com.thetis.search.Result;
import com.thetis.structures.Pair;
import com.thetis.structures.table.Table;
import org.apache.jena.base.Sys;

import java.util.*;

// Only works for combining results of two search engines
public class OverlapCombiner implements CombinerPipeline
{
    private final Topsis topsis;
    private final AnalogousSearch analogousSearch;
    private final Table<String> query;
    private final int topK;

    public OverlapCombiner(Topsis topsis, int topK)
    {
        this.topsis = topsis;
        this.analogousSearch = null;
        this.query = null;
        this.topK = topK;
    }

    public OverlapCombiner(AnalogousSearch analogousSearch, Table<String> query, int topK)
    {
        this.topsis = null;
        this.analogousSearch = analogousSearch;
        this.query = query;
        this.topK = topK;
    }

    @Override
    public Result combine(CombinedRanking results)
    {
        if (!results.iterator().hasNext())
        {
            throw new IllegalArgumentException("No results provided to combine");
        }

        else if (results.iterator().next().scores().size() != 2)
        {
            throw new IllegalArgumentException("Only two engines supported");
        }

        CombinedRanking overlappingRanking = new CombinedRanking();
        List<CombinedRanking.Entry> distinctEngine1 = new ArrayList<>(), distinctEngine2 = new ArrayList<>();

        for (CombinedRanking.Entry entry : results)
        {
            if (entry.scores().stream().allMatch(score -> score > 0.0)) // Overlapping table
            {
                overlappingRanking.addEntry(entry);
            }

            else if (entry.scores().get(0) > 0.0)   // Distinct for engine 1
            {
                distinctEngine1.add(entry);
            }

            else    // Distinct for engine 2
            {
                distinctEngine2.add(entry);
            }
        }

        CombinedRanking newResults = overlappingRanking;
        int remaining = this.topK - overlappingRanking.size(),
                engine1Remaining = (int) Math.ceil((double) remaining / 2),
                engine2Remaining = (int) Math.floor((double) remaining / 2);
        Comparator<? super CombinedRanking.Entry> entryComparator = (e1, e2) -> {
            double e1Score = e1.scores().get(0) > 0.0 ? e1.scores().get(0) : e1.scores().get(1),
                    e2Score = e2.scores().get(0) > 0.0 ? e2.scores().get(0) : e2.scores().get(1);
            return Double.compare(e2Score, e1Score);
        };
        distinctEngine1.sort(entryComparator);
        distinctEngine2.sort(entryComparator);
        distinctEngine1.subList(0, Math.min(distinctEngine1.size(), engine1Remaining)).forEach(newResults::addEntry);
        distinctEngine2.subList(0, Math.min(distinctEngine2.size(), engine2Remaining)).forEach(newResults::addEntry);

        return this.topsis != null ? topsisRank(newResults) : analogousRank(newResults);
    }

    private Result analogousRank(CombinedRanking resultSet)
    {
        Set<String> searchSpace = new HashSet<>(resultSet.size());
        resultSet.forEach(entry -> searchSpace.add(entry.id()));
        this.analogousSearch.setCorpus(searchSpace);

        return this.analogousSearch.search(this.query);
    }

    private Result topsisRank(CombinedRanking resultSet)
    {
        CombinedRanking ranking = this.topsis.rank(resultSet);
        List<Pair<String, Double>> results = new ArrayList<>(this.topK);
        ranking.forEach(entry -> results.add(new Pair<>(entry.id(), entry.scores().get(0))));   // TOPSIS only returns a single score per entry

        return new Result(this.topK, results);
    }
}
