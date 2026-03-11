package com.thetis.store.lucene;

import java.util.function.Function;

public record LuceneDocument<D>(D document, String id, Function<D, String> textGenerator)
{
    @Override
    public String toString()
    {
        return getText();
    }

    public String getText()
    {
        return textGenerator.apply(this.document);
    }
}
