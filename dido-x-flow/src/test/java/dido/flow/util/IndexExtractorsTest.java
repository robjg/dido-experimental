package dido.flow.util;

import dido.data.DataSchema;
import dido.data.DidoData;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class IndexExtractorsTest {

    DataSchema schema = DataSchema.builder()
            .addNamed("Fruit", String.class)
            .addNamed("Price", double.class)
            .build();

    @Test
    void fromFirstField() {

        IndexExtractor test = IndexExtractors.fromFirstField(schema);

        Comparable<?> key = test.indexOf(DidoData.withSchema(schema)
                .of("Apple", 23.2));

        assertThat(key, is("Apple"));
    }
}