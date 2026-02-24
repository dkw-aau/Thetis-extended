package com.thetis.connector;

import com.thetis.structures.Pair;
import org.neo4j.driver.Record;

import java.util.List;

public interface Neo4jSemanticDriver extends Neo4jDriver
{
    String getPredicate(String predicateLabel);
    List<String> searchLinks(Iterable<String> links);
    List<String> searchLink(String link);
    List<String> searchTypes(String entity);
    List<Record> entityLabels();
    List<String> searchPredicates(String entity);
    List<Pair<String, String>> searchLinkMentions(List<String> links);
}
