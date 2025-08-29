package dido.table.internal;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.partial.PartialData;
import dido.flow.DidoSubscriber;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;

class MethodOperationBuilderTest {

    static class OurDidoSubscriber implements DidoSubscriber {

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
        public void onDelete(DidoData keyData) {
            throw new RuntimeException("Unexpected");
        }
    }

    @Test
    void idea() {

        DataSchema schema = DataSchema.builder()
                .addNamed("Qty", int.class)
                .build();

        LiveOperation op = LiveOperationBuilder.forSchema(schema)
                .addOp(new MethodOperationBuilder()
                        .readingNamed("Qty")
                        .writingNamed("Qty")
                        .processor(new MultiplyBy2()))
                .build();

        OurDidoSubscriber receiver = new OurDidoSubscriber();

        ArrayRowImpl row = new ArrayRowImpl(op.getFullSchema(), receiver);
        row.onData(DidoData.withSchema(schema).of(2), op);

        assertThat(row.getValueNamed("Qty").get(), Matchers.is(4));
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

        LiveOperation op = LiveOperationBuilder.forSchema(schema)
                .addOp(new MethodOperationBuilder()
                        .readingNamed("a")
                        .readingNamed("b")
                        .writingNamed("c", int.class)
                        .processor(new AddTwoColumns()))
                .build();

        OurDidoSubscriber receiver = new OurDidoSubscriber();

        ArrayRowImpl row = new ArrayRowImpl(op.getFullSchema(), receiver);
        row.load(DidoData.withSchema(schema).of(2, 2), op);

        assertThat(row.getValueNamed("c").get(), Matchers.is(4));
    }

    public static class AddTwoColumns {

        void add(int a,
                 int b,
                 Consumer<Integer> c) {

            c.accept(a + b);
        }
    }

}