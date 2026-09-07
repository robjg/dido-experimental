package dido.flow.util;

import dido.data.DataSchema;
import dido.data.DidoData;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class KeyUtilTest {

    DataSchema schema = DataSchema.builder()
            .addNamed("Fruit", String.class)
            .addNamed("Price", double.class)
            .build();

    @Test
    void fromFirstField() {

        Function<DidoData, String> test = KeyUtil.fromFirstField(schema);

        Comparable<?> key = test.apply(DidoData.withSchema(schema)
                .of("Apple", 23.2));

        assertThat(key, is("Apple"));
    }
}