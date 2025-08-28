package dido.table;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.partial.PartialData;
import dido.flow.DidoSubscriber;
import dido.flow.DidoSubscription;
import dido.table.internal.DataTableBasic;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

class DataTableTest {

    static class Recorder implements DidoSubscriber {

        List<String> results = new ArrayList<>();

        @Override
        public void onData(DidoData data) {
            results.add("onData: " + data);
        }

        @Override
        public void onPartial(PartialData data) {
            results.add("onPartial: " + data);
        }

        @Override
        public void onDelete(DidoData data) {
            results.add("onDelete: " + data);
        }
    }

    @Test
    void asPublisher() {

        DataSchema schema = DataSchema.builder()
                .addNamed("Id", int.class)
                .addNamed("Fruit", String.class)
                .addNamed("Qty", int.class)
                .build();

        DataTableBasic<Integer> test = DataTableBasic.<Integer>withSchema(schema)
                .create();

        Recorder recorder = new Recorder();

        DidoSubscription subscription = test.didoSubscribe(recorder);

        assertThat(subscription.getSchema(), is(schema));

        test.onData(DidoData.withSchema(schema)
                .of(1, "Apple", 7));

        assertThat(recorder.results, contains("onData: {[1:Id]=1, [2:Fruit]=Apple, [3:Qty]=7}"));
        recorder.results.clear();

        test.onPartial(PartialData.fromSchema(schema).withNames("Id", "Qty").of(1, 5));

        assertThat(recorder.results, contains("onPartial: {[1:Id]=1, [3:Qty]=5}"));
        recorder.results.clear();

        test.onDelete(DidoData.of(1));

        // Todo: fix this
        assertThat(recorder.results, contains("onDelete: null"));
        recorder.results.clear();

        subscription.close();
    }
}