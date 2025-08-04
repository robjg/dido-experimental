package dido.table.internal;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.table.LiveRow;
import dido.table.LiveTable;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class LiveTableBasicTest {

    @Test
    void insertTwoRows() {

        DataSchema schema = DataSchema.builder()
                .addNamed("Id", int.class)
                .addNamed("Fruit", String.class)
                .build();

        LiveTable table = LiveTableBasic.with().schema(schema).create();

        DidoData.withSchema(schema).many()
                .of(5, "Apple")
                .of(8, "Pear")
                .of(3, "Banana")
                .toList().forEach(table::onData);

        LiveRow row = table.getRow(DidoData.of(5));

        assertThat(row.getValueNamed("Fruit").getString(), is("Apple"));
        table.close();
    }
}