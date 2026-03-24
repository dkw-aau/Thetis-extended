package com.thetis.search.multicriteria;

import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class CombinedRankingTest
{
    private CombinedRanking ranking;

    @Before
    public void setup()
    {
        CombinedRanking.Entry e1 = new CombinedRanking.Entry("id1", List.of(1.0, 1.5, 1.8)),
                e2 = new CombinedRanking.Entry("id2", List.of(5.1, 3.1, 0.5)),
                e3 = new CombinedRanking.Entry("id3", List.of(10.8, 2.4, 1.1)),
                e4 = new CombinedRanking.Entry("id4", List.of(6.4, 9.3, 7.9)),
                e5 = new CombinedRanking.Entry("id5", List.of(4.4, 2.8, 5.5));
        this.ranking = new CombinedRanking(e1, e2, e3, e4, e5);
    }

    @Test
    public void testSize()
    {
        assertEquals(5, this.ranking.size());
    }

    @Test
    public void testAddEntry()
    {
        this.ranking.addEntry(new CombinedRanking.Entry("id6", List.of(0.1, 0.2, 0.3)));
        assertEquals(6, this.ranking.size());

        try
        {
            this.ranking.addEntry(new CombinedRanking.Entry("id3", List.of(0.1, 0.3, 0.5)));
            fail("No exception was thrown");    // An IllegalArgumentException should be thrown before reaching this statement
        }

        catch (IllegalArgumentException ignored) {}
    }

    @Test
    public void testGetEntry()
    {
        assertEquals("id2", this.ranking.getEntry("id2").id());
        assertEquals("id5", this.ranking.getEntry("id5").id());
        assertNotEquals("id1", this.ranking.getEntry("id4").id());

        try
        {
            this.ranking.getEntry("id6");
            fail("No exception was thrown");    // "id6" should not exist
        }

        catch (IllegalArgumentException ignored) {}
    }

    @Test
    public void testRemoveEntry()
    {
        assertEquals("id2", this.ranking.getEntry("id2").id());

        this.ranking.removeEntry("id2");
        assertEquals(4, this.ranking.size());

        try
        {
            this.ranking.getEntry("id2");
            fail("No exception was thrown");
        }

        catch (IllegalArgumentException ignored) {}
    }
}
