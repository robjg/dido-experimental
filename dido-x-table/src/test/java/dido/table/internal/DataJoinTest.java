package dido.table.internal;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.table.DataTable;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

class DataJoinTest {

    DataSchema fruitSchema = DataSchema.builder()
            .addNamed("Fruit", String.class)
            .addNamed("GrocerId", String.class)
            .addNamed("Price", double.class)
            .build();

    DataSchema grocerSchema = DataSchema.builder()
            .addNamed("Id", String.class)
            .addNamed("Jones", String.class)
            .build();

    @Test
    void simpleLeftJoin() {

        List<DidoData> fruit = DidoData.withSchema(fruitSchema).many()
                .of("Apple", "G2", 5)
                .of("Banana", "G2", 3)
                .of("Orange", "G1", 2)
                .toList();

        List<DidoData> grocers = DidoData.withSchema(grocerSchema).many()
                .of("G1", "Jones")
                .of("G2", "Smith")
                .toList();

        List<DidoData> results = new ArrayList<>();

        DataTable<String> fruitTable = DataTableBasic.<String>withSchema(fruitSchema)
                .create();

        DataTable<String> grocerTable = DataTableBasic.<String>withSchema(grocerSchema)
                .create();

        DataTable<String> joined = DataJoin.from(fruitTable).primaryKeys()
                .leftJoin(grocerTable);
    }
}