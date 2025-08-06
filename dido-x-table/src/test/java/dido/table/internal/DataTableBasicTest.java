package dido.table.internal;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.table.DataTable;
import org.junit.jupiter.api.Test;

class DataTableBasicTest {

    @Test
    void insertUpdateDelete() {

        DataSchema schema = DataSchema.builder()
                .addNamed("Id", int.class)
                .addNamed("Fruit", String.class)
                .build();

        DataTable<Integer> test = DataTableBasic.<Integer>withSchema(schema)
                .create();

        DidoData.withSchema(schema).many()
                .of(1, "Apple")
                .of(5, "Orange")
                .of(3, "Banana").toStream()
                .forEach(test::onData);

    }
}