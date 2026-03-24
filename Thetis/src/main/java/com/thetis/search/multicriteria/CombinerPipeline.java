package com.thetis.search.multicriteria;

import com.thetis.search.Result;

public interface CombinerPipeline
{
    Result combine(CombinedRanking results);
}
