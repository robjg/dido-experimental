package dido.table.internal;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.table.LiveRow;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;

class MethodOperationBuilderTest {

    @Test
    void idea() {

        DataSchema schema = DataSchema.builder()
                .addNamed("Qty", int.class)
                .build();

        MethodOperationBuilder test = new MethodOperationBuilder(schema);

        Consumer<LiveRow> consumer = test.readingNamed("Qty")
                .settingNamed("Qty")
                .processor(new MultiplyBy2());

        ArrayRowImpl row = new ArrayRowImpl(schema);
        row.load(DidoData.withSchema(schema).of(2));

        consumer.accept(row);

        assertThat(row.getValueNamed("Qty").getInt(), Matchers.is(4));
    }

    public static class MultiplyBy2 {

        void multiply(int qty,
                      Consumer<Integer> qtyWrite) {

            qtyWrite.accept(qty * 2);
        }
    }

}