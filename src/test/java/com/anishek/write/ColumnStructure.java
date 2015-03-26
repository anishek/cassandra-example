package com.anishek.write;

import com.datastax.driver.core.querybuilder.Insert;

public interface ColumnStructure {
    Insert populate(Insert insert);
}
