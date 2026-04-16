package com.thetis.search.multicriteria;

import com.thetis.search.Result;
import com.thetis.structures.Pair;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class OrderedCombinerTest
{
    @Test
    public void testCombine()
    {
        CombinedRanking.Entry e1 = new CombinedRanking.Entry("id1", List.of(0.0, 0.5, 0.8)),
                e2 = new CombinedRanking.Entry("id2", List.of(0.1, 0.1, 0.5)),
                e3 = new CombinedRanking.Entry("id3", List.of(0.8, 0.4, 0.1)),
                e4 = new CombinedRanking.Entry("id4", List.of(0.4, 0.3, 0.9)),
                e5 = new CombinedRanking.Entry("id5", List.of(0.4, 0.8, 0.5)),
                e6 = new CombinedRanking.Entry("id6", List.of(0.1, 0.0, 0.9)),
                e7 = new CombinedRanking.Entry("id7", List.of(0.9, 0.3, 0.4)),
                e8 = new CombinedRanking.Entry("id8", List.of(0.7, 0.1, 0.2)),
                e9 = new CombinedRanking.Entry("id9", List.of(0.7, 0.3, 0.9)),
                e10 = new CombinedRanking.Entry("id10", List.of(0.2, 0.4, 0.8));
        CombinedRanking input = new CombinedRanking(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10);
        CombinerPipeline pipeline = new OrderedCombiner(List.of(new Pareto(), new Topsis(List.of(0.3, 0.3, 0.4))));
        Result result = pipeline.combine(input);
        Iterator<Pair<String, Double>> resultIter = result.getResults();
        List<Pair<String, Double>> top3 = new ArrayList<>(3);
        assertEquals(10, result.getSize());

        while (resultIter.hasNext() && top3.size() < 3)
        {
            top3.add(resultIter.next());
        }

        assertEquals("id9", top3.get(0).getFirst());
        assertEquals("id4", top3.get(1).getFirst());
        assertEquals("id1", top3.get(2).getFirst());
    }
}
