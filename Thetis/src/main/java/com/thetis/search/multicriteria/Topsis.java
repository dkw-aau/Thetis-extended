package com.thetis.search.multicriteria;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class Topsis extends RankingCombiner
{
    @Override
    public CombinedRanking rank(CombinedRanking multiCriteriaResults)
    {
        CombinedRanking unifiedRanking = new CombinedRanking();
        List<CombinedRanking.Entry> orderedRanking = new ArrayList<>();
        List<Double> squaredSums = columnsSquaredSums(multiCriteriaResults);

        // Assumes scores have been normalized
        for (CombinedRanking.Entry entry : multiCriteriaResults)
        {
            int columns = entry.scores().size();
            double bestDistanceSum = 0, worstDistanceSum = 0;

            for (int column = 0; column < columns; column++)
            {
                double normalizedScore = entry.scores().get(column) / squaredSums.get(column);
                bestDistanceSum += Math.pow(normalizedScore - 1, 2);
                worstDistanceSum += Math.pow(normalizedScore, 2);
            }

            double bestDistance = Math.sqrt(bestDistanceSum), worstDistance = Math.sqrt(worstDistanceSum),
                    relativeDistance = worstDistance / (bestDistance + worstDistance);
            orderedRanking.add(new CombinedRanking.Entry(entry.id(), List.of(relativeDistance)));
        }

        orderedRanking.sort((e1, e2) -> Double.compare(e2.scores().get(0), e1.scores().get(0)));
        orderedRanking.forEach(unifiedRanking::addEntry);
        return unifiedRanking;
    }

    private static List<Double> columnsSquaredSums(CombinedRanking ranking)
    {
        int columns = ranking.iterator().next().scores().size();
        List<Double> squaredSums = new ArrayList<>(Collections.nCopies(columns, 0.0));

        for (CombinedRanking.Entry entry : ranking)
        {
            for (int column = 0; column < columns; column++)
            {
                double prevValue = squaredSums.get(column);
                squaredSums.set(column, prevValue + Math.pow(entry.scores().get(column), 2));
            }
        }

        return squaredSums.stream().map(Math::sqrt).collect(Collectors.toList());
    }
}
