package com.anishek.datastructure;

import com.anishek.write.ColumnStructure;
import com.datastax.driver.core.querybuilder.Insert;

public class SingleColumnStructure implements ColumnStructure {
    private PointDefinition pointDefinition = new PointDefinition();

    @Override
    public Insert populate(Insert insert) {
        return insert.value("definition", pointDefinition.serialize());
    }
}
