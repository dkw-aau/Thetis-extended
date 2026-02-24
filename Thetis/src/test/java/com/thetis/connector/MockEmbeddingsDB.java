package com.thetis.connector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class MockEmbeddingsDB implements DBDriverBatch<List<Double>, String>
{
    private final Random rand = new Random();
    int dimension;

    public MockEmbeddingsDB(int dimension)
    {
        this.dimension = dimension;
    }

    @Override
    public boolean batchInsert(List<String> iris, List<List<Float>> vectors)
    {
        return false;
    }

    @Override
    public Map<String, List<Double>> batchSelect(List<String> iris)
    {
        return Map.of();
    }

    @Override
    public List<Double> select(String query)
    {
        List<Double> vector = new ArrayList<>(this.dimension);

        for (int i = 0; i < this.dimension; i++)
        {
            vector.add(this.rand.nextDouble());
        }

        return vector;
    }

    @Override
    public boolean update(String query)
    {
        return true;
    }

    @Override
    public boolean updateSchema(String query)
    {
        return true;
    }

    @Override
    public boolean close()
    {
        return true;
    }

    @Override
    public boolean drop(String query)
    {
        return true;
    }
}
