package dido.table.util;

import dido.data.DidoData;
import dido.data.partial.PartialUpdate;
import dido.flow.QuietlyCloseable;
import dido.table.KeyedSubscriber;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

class KeyedSubscribersTest {

    static class OurSubscriber implements KeyedSubscriber<Integer> {

        List<String> results = new ArrayList<>();

        @Override
        public void onData(Integer key, DidoData data) {
            results.add("onData: " + key + "=" + data);
        }

        @Override
        public void onPartial(Integer key, PartialUpdate partial) {
            results.add("onPartial: " + key + "=" + partial.getData());
        }

        @Override
        public void onDelete(Integer key, DidoData data) {
            results.add("onDelete: " + key + "=" + data);
        }
    }

    @Test
    void replaceAsExpected() {

        DidoData apple = DidoData.of("Apple");

        KeyedDataSubscribers<Integer> test = new KeyedDataSubscribers<>(apple.getSchema());

        test.onData(1, apple);
        test.onPartial(1, PartialUpdate.from(apple).withIndices(1));
        test.onDelete(1, apple);

        OurSubscriber s1 = new OurSubscriber();

        QuietlyCloseable close1 = test.addSubscriber(s1);

        DidoData orange = DidoData.of("Orange");

        test.onData(2, orange);
        test.onPartial(2, PartialUpdate.from(orange).withIndices(1));
        test.onDelete(2, orange);

        assertThat(s1.results, contains("onData: 2={[1:f_1]=Orange}", "onPartial: 2={[1:f_1]=Orange}", "onDelete: 2={[1:f_1]=Orange}"));

        s1.results.clear();

        OurSubscriber s2 = new OurSubscriber();

        QuietlyCloseable close2 = test.addSubscriber(s2);

        close1.close();

        close2.close();
    }
}