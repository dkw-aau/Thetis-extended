package com.thetis.connector;

public interface Neo4jDriver
{
    Long getNumEdges();
    Long getNumNodes();
    Long getNumNeighbors(String node);
}
