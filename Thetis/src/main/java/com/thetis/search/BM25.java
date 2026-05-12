package com.thetis.search;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.thetis.structures.Pair;
import com.thetis.structures.table.Table;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BM25 extends AbstractSearch
{
    private long elapsedNs = -1;
    private final String indexName;
    private final boolean normalizeResults;
    private int k;

    public BM25(String indexName, boolean normalizeResults, int k)
    {
        super(null, null, null, null);
        this.indexName = indexName;
        this.normalizeResults = normalizeResults;
        this.k = k;
    }

    public void setK(int k)
    {
        this.k = k;
    }

    @Override
    protected Result abstractSearch(Table<String> query)
    {
        long start = System.nanoTime();

        try (RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200)).build())
        {
            List<Pair<String, Double>> results = new ArrayList<>();
            ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
            ElasticsearchClient client = new ElasticsearchClient(transport);
            String keywordQuery = convertQuery(query);
            SearchResponse<JsonData> search = client.search(s -> s
                    .index(this.indexName)
                    .size(this.k)
                    .query(q -> q
                            .match(t -> t
                                    .field("content")
                                    .query(keywordQuery))), JsonData.class);

            for (Hit<JsonData> hit : search.hits().hits())
            {
                String tableId = hit.id();
                double score = hit.score();
                results.add(new Pair<>(tableId, score));
            }

            if (this.normalizeResults)
            {
                results = normalizeResults(results, search.maxScore());
            }

            this.elapsedNs = System.nanoTime() - start;
            return new Result(this.k, results);
        }

        catch (IOException e)
        {
            return new Result(-1, List.of());
        }
    }

    private static String convertQuery(Table<String> query)
    {
        StringBuilder queryBuilder = new StringBuilder();
        int rows = query.rowCount();

        for (int row = 0; row < rows; row++)
        {
            int columns = query.getRow(row).size();

            for (int column = 0; column < columns; column++)
            {
                String[] textTokens = query.getRow(row).get(column).split("/");
                String text = textTokens[textTokens.length - 1].replace("_", " ");
                queryBuilder.append(text).append(" ");
            }
        }

        return queryBuilder.toString();
    }

    private static List<Pair<String, Double>> normalizeResults(List<Pair<String, Double>> results, double maxScore)
    {
        List<Pair<String, Double>> normalized = new ArrayList<>(results.size());

        for (Pair<String, Double> result : results)
        {
            double normalizedScore = Math.log(1 + result.getSecond()) / Math.log(1 + maxScore);
            normalized.add(new Pair<>(result.getFirst(), normalizedScore));
        }

        return normalized;
    }

    @Override
    protected long abstractElapsedNanoSeconds()
    {
        return this.elapsedNs;
    }
}
