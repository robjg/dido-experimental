package dido.table.internal;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.flow.util.KeyExtractors;
import dido.table.CloseableTable;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

class ReKeyedTableTest {

    DataSchema fruitSchema = DataSchema.builder()
            .addNamed("Id", String.class)
            .addNamed("Fruit", String.class)
            .addNamed("Colour", String.class)
            .build();


    List<DidoData> fruit = DidoData.withSchema(fruitSchema).many()
            .of("F1", "Apple", "Red")
            .of("F2", "Banana", "Yellow")
            .of("F3", "Orange", "Orange")
            .of("F4", "Apple", "Green")
            .toList();

    DataTableBasic<String> fruitTable = DataTableBasic.<String>withSchema(fruitSchema)
            .create();

    @Test
    void existingTables() {

        fruit.forEach(fruitTable::onData);

        CloseableTable<String> fruitByFruit = ReKeyedTable
                .remapKey(fruitTable,
                        KeyExtractors.<String>fromNamed("Fruit").keyExtractorFor(fruitSchema));

        assertThat(fruitByFruit.getSchema(), is(fruitSchema));

        assertThat(fruitByFruit.keySet(), contains("Apple", "Banana", "Orange"));

        assertThat(fruitByFruit.get("Apple"), is(DidoData.of( "F4", "Apple", "Green")));
        assertThat(fruitByFruit.get("Banana"), is(DidoData.of( "F2", "Banana", "Yellow")));
        assertThat(fruitByFruit.get("Orange"), is(DidoData.of( "F3", "Orange", "Orange")));

        fruitTable.onDelete(DidoData.of("F4"));

        assertThat(fruitByFruit.get("Apple"), is(DidoData.of( "F1", "Apple", "Red")));

        fruitByFruit.close();
    }

}