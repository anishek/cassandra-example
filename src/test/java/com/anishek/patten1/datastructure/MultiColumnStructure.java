package com.anishek.patten1.datastructure;

import com.anishek.patten1.write.ColumnStructure;
import com.datastax.driver.core.querybuilder.Insert;

import java.util.Random;

public class MultiColumnStructure implements ColumnStructure {
    private Random random = new Random(System.nanoTime());
    private Coordinates coordinates = new Coordinates(random);

    @Override
    public Insert populate(Insert insert) {
        return insert.value("cat1", Categories.categoryValues())
                .value("cat2", Categories.categoryValues())
                .value("lat", coordinates.lat())
                .value("lon", coordinates.lon())
                .value("a", random.nextLong());
    }
}
