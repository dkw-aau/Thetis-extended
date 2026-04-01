package com.thetis.search.multicriteria;

import java.util.*;

public class CombinedRanking implements Iterable<CombinedRanking.Entry>, Cloneable
{
    public record Entry(String id, List<Double> scores) {}

    protected final List<Entry> ranking = new ArrayList<>();
    protected final Map<String, Integer> invertedIndex = new HashMap<>();

    public CombinedRanking() {}

    public CombinedRanking(List<Entry> entries)
    {
        entries.forEach(this::addEntry);
    }

    public CombinedRanking(Entry ... entries)
    {
        this(List.of(entries));
    }

    public CombinedRanking addEntry(Entry entry)
    {
        if (contains(entry.id))
        {
            throw new IllegalArgumentException("Entry with ID '" + entry.id() + "' already exists");
        }

        this.ranking.add(entry);
        this.invertedIndex.put(entry.id(), this.ranking.size() - 1);
        return this;
    }

    public Entry getEntry(String id)
    {
        if (!contains(id))
        {
            throw new IllegalArgumentException("ID " + id + " not found");
        }

        int pos = this.invertedIndex.get(id);
        return this.ranking.get(pos);
    }

    public void removeEntry(String id)
    {
        if (!contains(id))
        {
            throw new IllegalArgumentException("ID " + id + " not found");
        }

        int pos = this.invertedIndex.get(id);
        this.ranking.remove(pos);
        this.invertedIndex.remove(id);

        int size = size();

        for (; pos < size; pos++)
        {
            this.invertedIndex.replace(this.ranking.get(pos).id(), pos);
        }
    }

    public int size()
    {
        return this.ranking.size();
    }

    public boolean contains(String id)
    {
        return this.invertedIndex.containsKey(id);
    }

    @Override
    public Iterator<Entry> iterator()
    {
        return this.ranking.iterator();
    }

    @Override
    public CombinedRanking clone()
    {
        return new CombinedRanking(this.ranking);   // Only works as long as the constructor is cloning the elements
    }
}
