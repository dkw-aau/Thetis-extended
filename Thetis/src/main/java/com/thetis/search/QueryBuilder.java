package com.thetis.search;

import com.thetis.structures.table.Table;

public interface QueryBuilder<R>
{
    R build(Table<String> queryTable);
}
