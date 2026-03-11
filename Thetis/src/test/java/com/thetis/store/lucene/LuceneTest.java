package com.thetis.store.lucene;

import com.thetis.commands.parser.TableParser;
import com.thetis.search.Result;
import com.thetis.structures.Pair;
import com.thetis.tables.JsonTable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.junit.Assert.*;

public class LuceneTest
{
    private LuceneIndex index;

    @Before
    public void load()
    {
        try (LuceneBuilder builder = LuceneIndex.builder("testing/lucene/"))
        {
            File tableDir = new File("testing/data/");

            for (File file : Objects.requireNonNull(tableDir.listFiles()))
            {
                LuceneDocument<File> document = new LuceneDocument<>(file, file.getName(), (f) -> {
                    JsonTable table = TableParser.parse(f);
                    StringBuilder strBuilder = new StringBuilder();

                    for (List<JsonTable.TableCell> row : table.rows)
                    {
                        for (JsonTable.TableCell cell : row)
                        {
                            strBuilder.append(cell.text).append(" ");
                        }
                    }

                    return strBuilder.toString();
                });
                builder.addDocument(document);
            }

            this.index = builder.build();
        }

        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @After
    public void cleanup()
    {
        File indexDir = new File("testing/lucene/");

        for (File file : Objects.requireNonNull(indexDir.listFiles()))
        {
            file.delete();
        }

        indexDir.delete();
        this.index.close();
    }

    @Test
    public void testSize()
    {
        assertEquals(7, this.index.size());
    }

    @Test
    public void testSimpleSearch()
    {
        Result result = this.index.find("Chicago");
        boolean table1Covered = false, table2Covered = false;
        assertEquals(2, result.getSize());

        for (Pair<String, Double> element : result)
        {
            if (element.getFirst().equals("table-0001-1.json"))
            {
                table1Covered = true;
            }

            else if (element.getFirst().equals("table-0314-885.json"))
            {
                table2Covered = true;
            }
        }

        assertTrue(table1Covered && table2Covered);
    }

    @Test
    public void testAdvancedSearch()
    {
        Result result = this.index.find("Chicago Android");
        boolean table1Covered = false, table2Covered = false, table3Covered = false;
        assertEquals(3, result.getSize());

        for (Pair<String, Double> element : result)
        {
            if (element.getFirst().equals("table-0001-1.json"))
            {
                table1Covered = true;
            }

            else if (element.getFirst().equals("table-0314-885.json"))
            {
                table2Covered = true;
            }

            else if (element.getFirst().equals("table-0782-820.json"))
            {
                table3Covered = true;
            }
        }

        assertTrue(table1Covered && table2Covered && table3Covered);
    }

    @Test
    public void testContains()
    {
        assertTrue(this.index.contains("table-0001-1.json"));
        assertTrue(this.index.contains("table-0001-2.json"));
        assertTrue(this.index.contains("table-0072-223.json"));
        assertTrue(this.index.contains("table-0314-885.json"));
        assertTrue(this.index.contains("table-0782-820.json"));
        assertTrue(this.index.contains("table-1019-555.json"));
        assertTrue(this.index.contains("table-1260-258.json"));
        assertFalse(this.index.contains("table-1111-1.json"));
        assertFalse(this.index.contains("null"));
    }
}
