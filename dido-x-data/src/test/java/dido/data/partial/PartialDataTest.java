package dido.data.partial;

import dido.data.DataSchema;
import dido.data.DidoData;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class PartialDataTest {

    @Test
    void make() {

        DataSchema schema = DataSchema.builder()
                .addNamed("Fruit", String.class)
                .addNamed("Qty", int.class)
                .addNamed("Price", double.class)
                .build();

        DidoData data = DidoData.withSchema(schema).of("Apple", 5, 23.2);

        PartialData partialData = PartialData.of(data, "Fruit", "Price");

        DataSchema expectedSchema = DataSchema.builder()
                .addNamedAt(1, "Fruit", String.class)
                .addNamedAt(3, "Price", double.class)
                .build();

        DidoData expected = DidoData.withSchema(expectedSchema).of("Apple", 23.2);

        assertThat(partialData, is(expected));

    }
}