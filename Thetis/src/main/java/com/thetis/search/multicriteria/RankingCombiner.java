package com.thetis.search.multicriteria;

public abstract class RankingCombiner
{
    public abstract CombinedRanking rank(CombinedRanking multiCriteriaResults);
}
