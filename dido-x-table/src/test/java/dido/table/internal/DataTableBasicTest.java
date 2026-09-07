package dido.table.internal;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.partial.PartialData;
import dido.data.schema.SubSchema;
import dido.flow.DidoSubscriber;
import dido.flow.QuietlyCloseable;
import dido.flow.KeyedDidoSubscriber;
import dido.flow.util.SubscriberUtil;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class DataTableBasicTest {

    static class Recorder implements KeyedDidoSubscriber<Integer> {

        List<String> results = new ArrayList<>();

        @Override
        public void onData(Integer key, DidoData data) {
            results.add("onData: " + key + ", " + data);
        }

        @Override
        public void onPartial(Integer key, PartialData partial) {
            results.add("onPartial: " + key + ", " + partial);
        }

        @Override
        public void onDelete(Integer key) {
            results.add("onDelete: " + key);
        }
    }

    @Test
    void insertUpdateDelete() {

        DataSchema schema = DataSchema.builder()
                .addNamed("Id", int.class)
                .addNamed("Fruit", String.class)
                .addNamed("Qty", int.class)
                .build();

        DataTableBasic<Integer> test = DataTableBasic.forSchema(schema);

        DidoSubscriber didoSubscriber  = SubscriberUtil.didoSubscriberFrom(
                test, test.getSchema());

        DidoData.withSchema(schema).many()
                .of(1, "Apple", 7)
                .of(5, "Orange", 12)
                .of(3, "Banana", 5).toStream()
                .forEach(didoSubscriber::onData);

        assertThat(test.keySet(), contains(1, 3, 5));

        assertThat(test.get(5), is(DidoData.of(5, "Orange", 12)));

        didoSubscriber.onData(DidoData.withSchema(schema)
                .of(2, "Pear", 14));

        assertThat(test.keySet(), contains(1, 2, 3, 5));

        SubSchema subSchema = SubSchema.from(schema).withIndices(1, 2);

        didoSubscriber.onPartial(PartialData.from(DidoData.withSchema(subSchema).of(5, "Grape"))
                .withIndices(subSchema.getIndices()));

        assertThat(test.get(5), is(DidoData.of(5, "Grape", 12)));

        didoSubscriber.onDelete(DidoData.of(3));

        assertThat(test.keySet(), contains(1, 2, 5));
    }

    @Test
    void subscribe() {

        DataSchema schema = DataSchema.builder()
                .addNamed("Id", int.class)
                .addNamed("Fruit", String.class)
                .addNamed("Qty", int.class)
                .build();

        DataTableBasic<Integer> test = DataTableBasic.forSchema(schema);

        DidoSubscriber didoSubscriber  = SubscriberUtil.didoSubscriberFrom(
                test, test.getSchema());

        DidoData.withSchema(schema).many()
                .of(1, "Apple", 7)
                .of(5, "Orange", 12)
                .of(3, "Banana", 5).toStream()
                .forEach(didoSubscriber::onData);

        Recorder recorder = new Recorder();
        QuietlyCloseable close = test.subscribe(recorder);

        didoSubscriber.onData(DidoData.withSchema(schema).of(2, "Pear", 14));

        assertThat(recorder.results, contains("onData: 2, {[1:Id]=2, [2:Fruit]=Pear, [3:Qty]=14}"));
        recorder.results.clear();

        didoSubscriber.onPartial(PartialData.of(
                DidoData.withSchema(SubSchema.from(schema).withIndices(1, 2)).of(5, "Grape")));

        assertThat(recorder.results, contains("onPartial: 5, {[1:Id]=5, [2:Fruit]=Grape}"));
        recorder.results.clear();

        didoSubscriber.onDelete(DidoData.withSchema(SubSchema.from(schema).withIndices(1)).of(3));

        assertThat(recorder.results, contains("onDelete: 3"));
        recorder.results.clear();

        close.close();

        didoSubscriber.onDelete(DidoData.withSchema(SubSchema.from(schema).withIndices(1)).of(5));

        assertThat(recorder.results, empty());
    }
}