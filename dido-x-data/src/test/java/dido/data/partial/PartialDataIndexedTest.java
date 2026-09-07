package dido.data.partial;

import dido.data.DataSchema;
import dido.data.DidoData;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class PartialDataIndexedTest {

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
    void simpleCreate() {

        PartialData partialData = PartialDataIndexed.of(fruit, 2, 5, 6);

        assertThat(partialData.getData(), is(fruit));
        assertThat(partialData.getIndices(), is(new int[] { 2, 5, 6}));

    }
}