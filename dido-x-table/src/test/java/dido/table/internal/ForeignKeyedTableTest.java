package dido.table.internal;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.flow.DidoSubscriber;
import dido.flow.util.KeyUtil;
import dido.flow.util.SubscriberUtil;
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

    DataTableBasic<String> fruitTable = DataTableBasic.forSchema(fruitSchema);

    DataTableBasic<String> grocerTable = DataTableBasic.forSchema(grocerSchema);

    @Test
    void existingTables() {

        DidoSubscriber fruitSubscriber  = SubscriberUtil.didoSubscriberFrom(
                fruitTable, fruitTable.getSchema());
        DidoSubscriber grocerSubscriber  = SubscriberUtil.didoSubscriberFrom(
                grocerTable, grocerTable.getSchema());

        fruit.forEach(fruitSubscriber::onData);
        grocers.forEach(grocerSubscriber::onData);

        CloseableTable<String> grocersByFruitId = ForeignKeyedTable
                .byForeignKey(fruitTable, grocerTable,
                        KeyUtil.fromNamed(fruitTable.getSchema(), "GrocerId"));

        assertThat(grocersByFruitId.getSchema(), is(grocerSchema));

        assertThat(grocersByFruitId.keySet(), contains("F1", "F2", "F3"));

        assertThat(grocersByFruitId.get("F1"), is(DidoData.of( "G2", "Smith")));
        assertThat(grocersByFruitId.get("F2"), is(DidoData.of( "G2", "Smith")));
        assertThat(grocersByFruitId.get("F3"), is(DidoData.of( "G1", "Jones")));

        grocersByFruitId.close();
    }
}