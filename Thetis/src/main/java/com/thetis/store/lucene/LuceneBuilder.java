package com.thetis.store.lucene;

import co.elastic.clients.elasticsearch._types.analysis.StopAnalyzer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Path;

public class LuceneBuilder implements AutoCloseable
{
    private final String path;
    private final Directory dir;
    private final Analyzer analyzer;
    private final IndexWriterConfig config;
    private final IndexWriter writer;

    LuceneBuilder(String dir) throws IOException
    {
        this.path = dir;
        this.dir = FSDirectory.open(Path.of(dir));
        this.analyzer = getAnalyzer();
        this.config = new IndexWriterConfig(this.analyzer);
        this.writer = new IndexWriter(this.dir, this.config);
    }

    public static Analyzer getAnalyzer()
    {
        return new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String s)
            {
                StandardTokenizer tokenizer = new StandardTokenizer();
                TokenStream stream = tokenizer;
                stream = new StopFilter(stream, EnglishAnalyzer.ENGLISH_STOP_WORDS_SET);
                stream = new PorterStemFilter(stream);
                return new Analyzer.TokenStreamComponents(tokenizer, stream);
            }
        };
    }

    public <D> void addDocument(LuceneDocument<D> document) throws IOException
    {
        Document doc = new Document();
        doc.add(new Field(LuceneIndex.DOC_FIELD, document.getText(), TextField.TYPE_STORED));
        doc.add(new Field(LuceneIndex.ID_FIELD, document.id(), TextField.TYPE_STORED));
        this.writer.addDocument(doc);
    }

    public synchronized LuceneIndex build()
    {
        try
        {
            this.writer.close();
            return new LuceneIndex(this.path, 100, true);
        }

        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close()
    {
        try
        {
            this.writer.close();
            this.analyzer.close();
            this.dir.close();
        }

        catch (IOException ignored) {}
    }
}
