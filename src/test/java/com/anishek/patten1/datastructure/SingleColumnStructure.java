package com.anishek.patten1.datastructure;

import com.anishek.patten1.write.ColumnStructure;
import com.datastax.driver.core.querybuilder.Insert;

public class SingleColumnStructure implements ColumnStructure {
    private PointDefinition pointDefinition = new PointDefinition();

    @Override
    public Insert populate(Insert insert) {
        return insert.value("definition", pointDefinition.serialize());
    }
}
