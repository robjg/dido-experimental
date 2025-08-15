package dido.data.partial;

import dido.data.*;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class PartialDataDelegateFactoryTest {

    @Test
    void simpleCreate() {

        DataFactoryProvider dfp = DataFactoryProvider.newInstance();

        DataSchema schema = SchemaBuilder.builderFor(dfp.getSchemaFactory())
                .addNamed("Fruit", String.class)
                .addNamed("Qty", int.class)
                .addNamed("Price", double.class)
                .build();

        PartialDataDelegateFactory test = PartialDataDelegateFactory.of(
                dfp.factoryFor(schema), "Fruit", "Price");

        WritableData writableData = test.getWritableData();
        writableData.setNamed("Fruit", "Apple");
        writableData.setNamed("Price", 23.2);

        DidoData didoData = test.toData();

        DidoData expected = DidoData.of("Apple", 23.2);

        assertThat(didoData, is(expected));

    }
}