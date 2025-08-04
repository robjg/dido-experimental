package dido.table.internal;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.SchemaFactory;
import dido.data.partial.PartialData;
import dido.flow.Receiver;
import dido.table.LiveRow;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;

class MethodOperationBuilderTest {

    static class OurReceiver implements Receiver {

        List<DidoData> data = new ArrayList<>();

        List<PartialData> partial = new ArrayList<>();

        @Override
        public void onData(DidoData data) {
            this.data.add(data);
        }

        @Override
        public void onPartial(PartialData partial) {
            this.partial.add(partial);
        }

        @Override
        public void onDelete(PartialData partial) {
            throw new RuntimeException("Unexpected");
        }
    }

    @Test
    void idea() {

        DataSchema schema = DataSchema.builder()
                .addNamed("Qty", int.class)
                .build();

        SchemaFactory schemaFactory = SchemaFactory.newInstanceFrom(schema);

        MethodOperationBuilder test = new MethodOperationBuilder(schema, schemaFactory);

        Consumer<LiveRow> consumer = test.readingNamed("Qty")
                .writingNamed("Qty")
                .processor(new MultiplyBy2());

        OurReceiver receiver = new OurReceiver();

        ArrayRowImpl row = new ArrayRowImpl(schemaFactory.toSchema(), receiver);
        row.onData(DidoData.withSchema(schema).of(2), List.of(consumer));

        assertThat(row.getValueNamed("Qty").getInt(), Matchers.is(4));
    }

    public static class MultiplyBy2 {

        void multiply(int qty,
                      Consumer<Integer> qtyWrite) {

            qtyWrite.accept(qty * 2);
        }
    }

    @Test
    void addTwoColumns() {

        DataSchema schema = DataSchema.builder()
                .addNamed("a", int.class)
                .addNamed("b", int.class)
                .build();

        SchemaFactory schemaFactory = SchemaFactory.newInstanceFrom(schema);

        MethodOperationBuilder test = new MethodOperationBuilder(schema, schemaFactory);

        Consumer<LiveRow> consumer = test
                .readingNamed("a")
                .readingNamed("b")
                .writingNamed("c", int.class)
                .processor(new AddTwoColumns());

        OurReceiver receiver = new OurReceiver();

        ArrayRowImpl row = new ArrayRowImpl(schemaFactory.toSchema(), receiver);
        row.load(DidoData.withSchema(schema).of(2, 2), List.of(consumer));

        assertThat(row.getValueNamed("c").getInt(), Matchers.is(4));
    }

    public static class AddTwoColumns {

        void add(int a,
                      int b,
                      Consumer<Integer> c) {

            c.accept(a + b);
        }
    }

}