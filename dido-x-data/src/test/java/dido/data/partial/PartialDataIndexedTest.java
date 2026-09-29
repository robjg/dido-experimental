package dido.data.partial;

import dido.data.DataSchema;
import dido.data.DidoData;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

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
        assertThat(partialData.getIndices(), is(new int[] { 2, 5, 6 }));

        assertThat(partialData.firstIndex(), is(2));
        assertThat(partialData.nextIndex(2), is(5));
        assertThat(partialData.nextIndex(5), is(6));
        assertThat(partialData.nextIndex(6), is(0));
        assertThat(partialData.nextIndex(5), is(6));
        assertThat(partialData.lastIndex(), is(6));

        assertThat(partialData.toString(), is("{[2:Fruit]=Apple, [5:Price]=27.3, [6:MinQty]=2}"));
    }

    @Test
    void empty() {

        PartialData partialData = PartialDataIndexed.of(fruit);

        assertThat(partialData.getIndices(), is(new int[0]));

        assertThat(partialData.firstIndex(), is(0));
        assertThat(partialData.lastIndex(), is(0));

        assertThat(partialData.toString(), is("{}"));

    }

    @Test
    void single() {

        PartialData partialData = PartialDataIndexed.of(fruit, 5);

        assertThat(partialData.getIndices(), is(new int[] { 5 }));

        assertThat(partialData.firstIndex(), is(5));
        assertThat(partialData.nextIndex(5), is(0));
        assertThat(partialData.lastIndex(), is(5));

        assertThat(partialData.toString(), is("{[5:Price]=27.3}"));
    }

    @Test
    void randomNext() {

        PartialData partialData = PartialDataIndexed.of(fruit, 2, 5, 6);

        assertThat(partialData.getData(), is(fruit));
        assertThat(partialData.getIndices(), is(new int[] { 2, 5, 6 }));

        assertThat(partialData.nextIndex(6), is(0));
        assertThat(partialData.nextIndex(5), is(6));
        assertThat(partialData.nextIndex(2), is(5));
        assertThat(partialData.nextIndex(5), is(6));
        assertThat(partialData.nextIndex(5), is(6));
        assertThat(partialData.nextIndex(2), is(5));
    }

    @Test
    void equalsHashCode() {

        PartialData partialData1 = PartialDataIndexed.of(fruit, 2, 5, 6);
        PartialData partialData2 = PartialDataIndexed.of(fruit, 2, 5, 6);
        PartialData partialData3 = PartialDataIndexed.of(fruit, 2, 3, 5);
        PartialData partialData4 = PartialDataIndexed.of(fruit, 2);
        PartialData partialData5 = PartialDataIndexed.of(fruit, 2);
        PartialData partialData6 = PartialDataIndexed.of(fruit);
        PartialData partialData7 = PartialDataIndexed.of(fruit);

        assertThat(partialData1.hashCode(), is(partialData2.hashCode()));
        assertThat(partialData4.hashCode(), is(partialData5.hashCode()));
        assertThat(partialData6.hashCode(), is(partialData7.hashCode()));

        assertThat(partialData1, is(partialData2));
        assertThat(partialData1, not(is(partialData3)));
        assertThat(partialData2, not(is(partialData4)));
        assertThat(partialData5, is(partialData4));
        assertThat(partialData6, not(is(partialData4)));
        assertThat(partialData6, is(partialData7));
    }
}