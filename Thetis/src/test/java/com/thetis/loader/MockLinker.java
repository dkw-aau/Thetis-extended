package com.thetis.loader;

public class MockLinker implements Linker
{
    @Override
    public String link(String mention) throws IllegalArgumentException
    {
        String postfix = mention.substring(mention.lastIndexOf("/"));
        return "http://dbpedia.org/resource" + postfix;
    }
}
