package dido.data.partial;

import dido.data.DataSchema;
import dido.data.DidoData;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class ArrayPartialDataTest {

    DataSchema schema = DataSchema.builder()
            .addNamed("Id", String.class)
            .addNamed("Fruit", String.class)
            .addNamed("Colour", String.class)
            .addNamed("Qty", int.class)
            .addNamed("Price", double.class)
            .addNamed("MinQty", int.class)
            .build();

    DidoData fruit = DidoData.withSchema(schema)
            .of("F1", "Apple", "Red", 5, 27.3, 2);


    @Test
    void disparateFields() {

        PartialSchema partialSchema = ArrayPartialData.schemaFrom(schema)
                .withNames("Colour", "Price");

        PartialData partialData = ArrayPartialData.withSchema(partialSchema)
                .of("Green", 22.4);

        assertThat(partialData, is(DidoData.of("Green", 22.4)));
    }
}