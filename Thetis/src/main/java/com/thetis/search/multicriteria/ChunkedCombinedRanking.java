package com.thetis.search.multicriteria;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

// Chunks must be ID-disjoint, as otherwise the inverted index would not be able to disambiguate ranking position of a given entry ID
public class ChunkedCombinedRanking extends CombinedRanking
{
    private final List<Integer> chunkOffsets = new ArrayList<>();

    public ChunkedCombinedRanking(List<CombinedRanking> chunks)
    {
        int i = 0;
        Set<String> entryIds = new HashSet<>();

        for (CombinedRanking chunk : chunks)
        {
            for (Entry entry : chunk.ranking)
            {
                if (entryIds.contains(entry.id()))
                {
                    throw new IllegalArgumentException("Chunks are not ID-disjoint");
                }

                entryIds.add(entry.id());
                super.ranking.add(entry);
                super.invertedIndex.put(entry.id(), i++);
            }

            this.chunkOffsets.add(i);
        }
    }

    public int chunks()
    {
        return this.chunkOffsets.size();
    }

    public CombinedRanking getChunk(int index)
    {
        if (index < 0 || index >= this.chunkOffsets.size())
        {
            throw new IndexOutOfBoundsException();
        }

        int end = this.chunkOffsets.get(index), start = index == 0 ? 0 : this.chunkOffsets.get(index - 1);
        return new CombinedRanking(super.ranking.subList(start, end));
    }
}
