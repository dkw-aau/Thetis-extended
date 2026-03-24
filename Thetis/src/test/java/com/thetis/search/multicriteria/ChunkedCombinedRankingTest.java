package com.thetis.search.multicriteria;

import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class ChunkedCombinedRankingTest
{
    private ChunkedCombinedRanking ranking;

    @Before
    public void setup()
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
        CombinedRanking chunk1 = new CombinedRanking(e1, e2, e3), chunk2 = new CombinedRanking(e4, e5), chunk3 = new CombinedRanking(e6, e7, e8, e9);
        this.ranking = new ChunkedCombinedRanking(List.of(chunk1, chunk2, chunk3));
    }

    @Test
    public void testChunkCount()
    {
        assertEquals(3, this.ranking.chunks());
    }

    @Test
    public void testSize()
    {
        assertEquals(9, this.ranking.size());
    }

    @Test
    public void testGetChunk()
    {
        CombinedRanking chunk1 = this.ranking.getChunk(0), chunk2 = this.ranking.getChunk(1), chunk3 = this.ranking.getChunk(2);
        assertEquals(3, chunk1.size());
        assertTrue(chunk1.contains("id1"));
        assertTrue(chunk1.contains("id2"));
        assertTrue(chunk1.contains("id3"));

        assertEquals(2, chunk2.size());
        assertTrue(chunk2.contains("id4"));
        assertTrue(chunk2.contains("id5"));

        assertEquals(4, chunk3.size());
        assertTrue(chunk3.contains("id6"));
        assertTrue(chunk3.contains("id7"));
        assertTrue(chunk3.contains("id8"));
        assertTrue(chunk3.contains("id9"));

        try
        {
            this.ranking.getChunk(4);
            fail("No exception was thrown");
        }

        catch (IndexOutOfBoundsException ignored) {}
    }
}
