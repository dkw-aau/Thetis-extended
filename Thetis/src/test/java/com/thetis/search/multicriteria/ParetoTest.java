package com.thetis.search.multicriteria;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ParetoTest
{
    @Test
    public void testRank()
    {
        CombinedRanking.Entry e1 = new CombinedRanking.Entry("id1", List.of(0.0, 0.5, 0.8)),
                e2 = new CombinedRanking.Entry("id2", List.of(0.1, 0.1, 0.5)),
                e3 = new CombinedRanking.Entry("id3", List.of(0.8, 0.4, 0.1)),
                e4 = new CombinedRanking.Entry("id4", List.of(0.4, 0.3, 0.9)),
                e5 = new CombinedRanking.Entry("id5", List.of(0.4, 0.8, 0.5)),
                e6 = new CombinedRanking.Entry("id6", List.of(0.1, 0.0, 0.9)),
                e7 = new CombinedRanking.Entry("id7", List.of(0.9, 0.3, 0.4)),
                e8 = new CombinedRanking.Entry("id8", List.of(0.7, 0.1, 0.2)),
                e9 = new CombinedRanking.Entry("id9", List.of(0.7, 0.3, 0.9));
        Pareto p = new Pareto();
        CombinedRanking preRanking = new CombinedRanking(e1, e2, e3, e4, e5, e6, e7, e8, e9),
                postRanking = p.rank(preRanking);
        assertEquals(9, postRanking.size());
        assertTrue(postRanking instanceof ChunkedCombinedRanking);

        ChunkedCombinedRanking chunkedPostRanking = (ChunkedCombinedRanking) postRanking;
        assertEquals(3, chunkedPostRanking.chunks());
        assertEquals(9, chunkedPostRanking.size());
        assertEquals(1, chunkedPostRanking.getChunk(0).size());

        for (CombinedRanking.Entry e : chunkedPostRanking.getChunk(0))
        {
            assertEquals(3, e.scores().size());
        }

        assertEquals("id9", chunkedPostRanking.getChunk(0).iterator().next().id());
    }
}
