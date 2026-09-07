package dido.table;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.partial.PartialData;
import dido.data.schema.SubSchema;
import dido.flow.DidoSubscriber;
import dido.flow.DidoSubscription;
import dido.flow.KeyedDidoSubscriber;
import dido.flow.util.SubscriberUtil;
import dido.table.internal.DataTableBasic;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

class DataTableTest {

    static class Recorder implements KeyedDidoSubscriber<Integer> {

        List<String> results = new ArrayList<>();

        @Override
        public void onData(Integer key, DidoData data) {
            results.add("onData: " + data);
        }

        @Override
        public void onPartial(Integer key, PartialData partial) {
            results.add("onPartial: " + partial);
        }

        @Override
        public void onDelete(Integer key) {
            results.add("onDelete: " + key);
        }
    }

    @Test
    void asPublisher() {

        DataSchema schema = DataSchema.builder()
                .addNamed("Id", int.class)
                .addNamed("Fruit", String.class)
                .addNamed("Qty", int.class)
                .build();

        DataTableBasic<Integer> test = DataTableBasic.forSchema(schema);

        Recorder recorder = new Recorder();

        DidoSubscription subscription = test.subscribe(recorder);

        assertThat(subscription.getSchema(), is(schema));

         DidoSubscriber didoSubscriber  = SubscriberUtil.didoSubscriberFrom(
                 test, test.getSchema());

        didoSubscriber.onData(DidoData.withSchema(schema)
                .of(1, "Apple", 7));

        assertThat(recorder.results, contains("onData: {[1:Id]=1, [2:Fruit]=Apple, [3:Qty]=7}"));
        recorder.results.clear();

        DataSchema subSchema = SubSchema.from(schema).withNames("Id", "Qty");

        didoSubscriber.onPartial(PartialData.from(DidoData.withSchema(subSchema).of(1, 5))
                .withIndices(subSchema.getIndices()));

        assertThat(recorder.results, contains("onPartial: {[1:Id]=1, [3:Qty]=5}"));
        recorder.results.clear();

        didoSubscriber.onDelete(DidoData.withSchema(SubSchema.from(schema).withIndices(1))
                .of(1));

        assertThat(recorder.results, contains("onDelete: 1"));
        recorder.results.clear();

        subscription.close();
    }
}