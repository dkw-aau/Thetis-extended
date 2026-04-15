package com.thetis.search.multicriteria;

import java.util.ArrayList;
import java.util.List;

// Requires input scores to be normalized within [0, 1]
public class Topsis extends RankingCombiner
{
    @Override
    public CombinedRanking rank(CombinedRanking multiCriteriaResults)
    {
        CombinedRanking unifiedRanking = new CombinedRanking();
        List<CombinedRanking.Entry> orderedRanking = new ArrayList<>();

        // Assumes scores have been normalized
        for (CombinedRanking.Entry entry : multiCriteriaResults)
        {
            int columns = entry.scores().size();
            double bestDistanceSum = 0, worstDistanceSum = 0;

            for (int column = 0; column < columns; column++)
            {
                double score = entry.scores().get(column);
                bestDistanceSum += Math.pow(score - 1, 2);
                worstDistanceSum += Math.pow(score, 2);
            }

            double bestDistance = Math.sqrt(bestDistanceSum), worstDistance = Math.sqrt(worstDistanceSum),
                    relativeDistance = worstDistance / (bestDistance + worstDistance);
            orderedRanking.add(new CombinedRanking.Entry(entry.id(), List.of(relativeDistance)));
        }

        orderedRanking.sort((e1, e2) -> Double.compare(e2.scores().get(0), e1.scores().get(0)));
        orderedRanking.forEach(unifiedRanking::addEntry);
        return unifiedRanking;
    }
}
