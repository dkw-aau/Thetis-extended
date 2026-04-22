package com.thetis.search.multicriteria;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Pareto extends RankingCombiner
{
    // We rely on the ranking scores to be normalized and within [0, 1]
    @Override
    public CombinedRanking rank(CombinedRanking multiCriteriaResults)
    {
        List<CombinedRanking> fronts = new ArrayList<>();
        CombinedRanking remaining = multiCriteriaResults.clone();   // Might be slow yo clone since the constructor will re-iterate all elements

        while (remaining.size() > 0)
        {
            CombinedRanking currentFront = new CombinedRanking();

            for (CombinedRanking.Entry entryI : remaining)
            {
                boolean dominated = false;

                for (CombinedRanking.Entry entryJ : remaining)
                {
                    if (!entryI.id().equals(entryJ.id()) && dominates(entryI, entryJ))
                    {
                        dominated = true;
                        break;
                    }
                }

                if (!dominated)
                {
                    currentFront.addEntry(entryI);
                }
            }

            fronts.add(currentFront);

            for (CombinedRanking.Entry entry : currentFront)
            {
                remaining.removeEntry(entry.id());
            }
        }

        Collections.reverse(fronts);
        return new ChunkedCombinedRanking(fronts);
    }

    private boolean dominates(CombinedRanking.Entry dominator, CombinedRanking.Entry dominated)
    {
        boolean containsStrictlyGreater = false;
        int scores = dominator.scores().size();

        if (scores != dominated.scores().size())
        {
            throw new IllegalArgumentException("Mis-match in number of search engines");
        }

        for (int engine = 0; engine < scores; engine++)
        {
            double dominatorScore = dominator.scores().get(engine), dominatedScore = dominated.scores().get(engine);

            if (dominatorScore < 0.0 || dominatorScore > 1.0 || dominatedScore < 0.0 || dominatedScore > 1.0)
            {
                throw new RuntimeException("Scores must be within [0, 1]");
            }

            else if (dominatorScore + 0.05 < dominatedScore)   // Added magic number such that near-equality is treated as equality
            {
                return false;
            }

            else if (dominatorScore > dominatedScore + 0.05)    // Added magic number such that near-equality is treated as equality
            {
                containsStrictlyGreater = true;
            }
        }

        return containsStrictlyGreater;
    }
}
