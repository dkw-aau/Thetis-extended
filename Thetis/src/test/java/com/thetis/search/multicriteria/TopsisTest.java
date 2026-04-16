package com.thetis.search.multicriteria;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TopsisTest
{
    @Test
    public void testRank()
    {
        CombinedRanking.Entry e1 = new CombinedRanking.Entry("id1", List.of(1.0, 1.5, 1.8)),
                e2 = new CombinedRanking.Entry("id2", List.of(5.1, 3.1, 0.5)),
                e3 = new CombinedRanking.Entry("id3", List.of(10.8, 2.4, 1.1)),
                e4 = new CombinedRanking.Entry("id4", List.of(6.4, 9.3, 7.9)),
                e5 = new CombinedRanking.Entry("id5", List.of(4.4, 2.8, 5.5)),
                e6 = new CombinedRanking.Entry("id6", List.of(23.1, 18.0, 11.9)),
                e7 = new CombinedRanking.Entry("id7", List.of(0.9, 20.3, 13.4)),
                e8 = new CombinedRanking.Entry("id8", List.of(8.7, 9.1, 14.2)),
                e9 = new CombinedRanking.Entry("id9", List.of(2.7, 25.3, 18.9));
        Topsis t = new Topsis(List.of(0.5, 0.2, 0.3));
        CombinedRanking preRanking = new CombinedRanking(e1, e2, e3, e4, e5, e6, e7, e8, e9),
                postRanking = t.rank(preRanking);
        List<CombinedRanking.Entry> top3 = new ArrayList<>(3);
        assertEquals(9, postRanking.size());

        for (CombinedRanking.Entry e : postRanking)
        {
            assertEquals(1, e.scores().size());

            if (top3.size() < 3)
            {
                top3.add(e);
            }
        }

        assertEquals("id5", top3.get(0).id());
        assertEquals("id4", top3.get(1).id());
        assertEquals("id2", top3.get(2).id());
    }
}
