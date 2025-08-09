package dido.table.internal;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.SchemaBuilder;
import dido.flow.util.KeyExtractors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

class DataJoinTest {

    DataSchema fruitSchema = DataSchema.builder()
            .addNamed("Id", String.class)
            .addNamed("Fruit", String.class)
            .addNamed("GrocerId", String.class)
            .addNamed("Price", double.class)
            .build();

    DataSchema colourSchema = DataSchema.builder()
            .addNamed("Id", String.class)
            .addNamed("Colour", String.class)
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

    List<DidoData> colours = DidoData.withSchema(colourSchema).many()
            .of("F1", "Green")
            .of("F2", "Yellow")
            .of("F4", "Red")
            .toList();

    List<DidoData> grocers = DidoData.withSchema(grocerSchema).many()
            .of("G1", "Jones")
            .of("G2", "Smith")
            .toList();

    DataTableBasic<String> fruitTable = DataTableBasic.<String>withSchema(fruitSchema)
            .create();

    DataTableBasic<String> colourTable = DataTableBasic.<String>withSchema(colourSchema)
            .create();

    DataTableBasic<String> grocerTable = DataTableBasic.<String>withSchema(grocerSchema)
            .create();

    @BeforeEach
            void setUp() {
        fruit.forEach(fruitTable::onData);
        colours.forEach(colourTable::onData);
        grocers.forEach(grocerTable::onData);
    }


    @Test
    void simpleInnerJoin() {

        List<DidoData> results = new ArrayList<>();

        fruit.forEach(fruitTable::onData);
        colours.forEach(colourTable::onData);

        DataSchema expectedSchema = SchemaBuilder
                .builderFrom(fruitSchema)
                .concat(colourSchema).build();

        DataJoin<String> joined = DataJoin.from(fruitTable).primaryKeys()
                .innerJoin(colourTable);

        assertThat(joined.getSchema(), is(expectedSchema));

        DidoData f1 = joined.get("F1");

        assertThat(f1.getSchema(), is(expectedSchema));
        assertThat(f1, is(DidoData.of("F1", "Apple", "G2", 5, "F1", "Green")));
        assertThat(joined.containsKey("F2"), is(true));
        assertThat(joined.containsKey("F3"), is(false));
        assertThat(joined.get("F3"), nullValue());

        joined.close();
    }

    @Test
    void innerJoinForeignKey() {

        DataJoin<String> joined = DataJoin.from(fruitTable).foreignKey(
                KeyExtractors.<String>fromNamed("GrocerId"))
                .innerJoin(grocerTable);

        DataSchema expectedSchema = SchemaBuilder
                .builderFrom(fruitSchema)
                .concat(grocerSchema).build();

        assertThat(joined.getSchema(), is(expectedSchema));

        fruit.forEach(fruitTable::onData);
        grocers.forEach(grocerTable::onData);

        DidoData f1 = joined.get("F1");

        assertThat(f1.getSchema(), is(expectedSchema));
        assertThat(f1, is(DidoData.of("F1", "Apple", "G2", 5, "G2", "Smith")));
        assertThat(joined.containsKey("F2"), is(true));
        assertThat(joined.containsKey("F3"), is(true));
        assertThat(joined.get("F3"), is(DidoData.of("F3", "Orange", "G1", 2, "G1", "Jones")));

        joined.close();
    }
}