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

class ForeignKeyedTableTest {

    DataSchema fruitSchema = DataSchema.builder()
            .addNamed("Id", String.class)
            .addNamed("Fruit", String.class)
            .addNamed("GrocerId", String.class)
            .addNamed("Price", double.class)
            .build();

    DataSchema grocerSchema = DataSchema.builder()
            .addNamed("Id", String.class)
            .addNamed("Jones", String.class)
            .build();

    List<DidoData> fruit = DidoData.withSchema(fruitSchema).many()
            .of("F1", "Apple", "G2", 5)
            .of("F2", "Banana", "G2", 3)
            .of("F3", "Orange", "G1", 2)
            .toList();

    List<DidoData> grocers = DidoData.withSchema(grocerSchema).many()
            .of("G1", "Jones")
            .of("G2", "Smith")
            .toList();

    DataTableBasic<String> fruitTable = DataTableBasic.<String>withSchema(fruitSchema)
            .create();

    DataTableBasic<String> grocerTable = DataTableBasic.<String>withSchema(grocerSchema)
            .create();

    @Test
    void existingTables() {

        fruit.forEach(fruitTable::onData);
        grocers.forEach(grocerTable::onData);

        CloseableTable<String> grocersByFruitId = ForeignKeyedTable
                .byForeignKey(fruitTable, grocerTable,
                        KeyExtractors.<String>fromNamed("GrocerId").keyExtractorFor(fruitSchema));

        assertThat(grocersByFruitId.getSchema(), is(grocerSchema));

        assertThat(grocersByFruitId.keySet(), contains("F1", "F2", "F3"));

        assertThat(grocersByFruitId.get("F1"), is(DidoData.of( "G2", "Smith")));
        assertThat(grocersByFruitId.get("F2"), is(DidoData.of( "G2", "Smith")));
        assertThat(grocersByFruitId.get("F3"), is(DidoData.of( "G1", "Jones")));

        grocersByFruitId.close();
    }
}