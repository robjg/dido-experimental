package dido.table.util;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.mutable.MutableArrayData;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;

class MutableDataHelperTest {

    @Test
    void createUpdate() {

        DataSchema schema = DataSchema.builder()
                .addNamed("Fruit", String.class)
                .addNamed("Qty", int.class)
                .addNamed("Price", double.class)
                .build();

        MutableDataHelper test = MutableDataHelper.forSchema(schema);

        DidoData data1 = DidoData.withSchema(schema).of("Apple", 5, 22.4);

        MutableArrayData mutable = test.copy(data1);

        assertThat(mutable, is(DidoData.withSchema(schema).of("Apple", 5, 22.4)));

        int[] updates1 = test.update(data1, mutable);

        assertThat(updates1, is(new int[0]));
        assertThat(mutable, is(DidoData.withSchema(schema).of("Apple", 5, 22.4)));

        DidoData data2 = DidoData.withSchema(schema).of("Apple", 6, 22.4);

        int[] updates2 = test.update(data2, mutable);

        assertThat(updates2, is(new int[] {2}));
        assertThat(mutable, is(DidoData.withSchema(schema).of("Apple", 6, 22.4)));
    }
}