package com.thetis.connector;

import com.thetis.structures.Pair;
import org.neo4j.driver.Record;

import java.util.List;

public class MockNeo4jEndpoint implements Neo4jSemanticDriver
{
    @Override
    public String getPredicate(String predicateLabel)
    {
        return "";
    }

    @Override
    public List<String> searchLinks(Iterable<String> links)
    {
        return List.of();
    }

    @Override
    public List<String> searchLink(String link)
    {
        return List.of();
    }

    @Override
    public List<String> searchTypes(String entity)
    {
        return List.of("https://www.w3.org/2002/07/owl#Thing", "https://dbpedia.org/ontology/Person", "https://www.wikidata.org/wiki/Q5");
    }

    @Override
    public List<Record> entityLabels()
    {
        return List.of();
    }

    @Override
    public List<String> searchPredicates(String entity)
    {
        return List.of("https://dbpedia.org/ontology/birthPlace", "https://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                "https://dbpedia.org/property/as");
    }

    @Override
    public List<Pair<String, String>> searchLinkMentions(List<String> links)
    {
        return List.of();
    }

    @Override
    public Long getNumEdges()
    {
        return 0L;
    }

    @Override
    public Long getNumNodes() {
        return 0L;
    }

    @Override
    public Long getNumNeighbors(String node) {
        return 0L;
    }
}
