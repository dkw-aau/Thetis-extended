package com.thetis.store.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
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
        this.analyzer = new StandardAnalyzer();
        this.config = new IndexWriterConfig(this.analyzer);
        this.writer = new IndexWriter(this.dir, this.config);
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
