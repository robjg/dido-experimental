package dido.table.util;

import dido.data.DidoData;
import dido.data.partial.PartialData;
import dido.flow.QuietlyCloseable;
import dido.flow.KeyedDidoSubscriber;
import dido.flow.util.KeyedDidoDataSubscribers;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

class KeyedDidoSubscribersTest {

    static class OurDidoSubscriber implements KeyedDidoSubscriber<Integer> {

        List<String> results = new ArrayList<>();

        @Override
        public void onData(Integer key, DidoData data) {
            results.add("onData: " + key + "=" + data);
        }

        @Override
        public void onPartial(Integer key, PartialData partial) {
            results.add("onPartial: " + key + "=" + partial.getData());
        }

        @Override
        public void onDelete(Integer key) {
            results.add("onDelete: " + key);
        }
    }

    @Test
    void replaceAsExpected() {

        DidoData apple = DidoData.of("Apple");

        KeyedDidoDataSubscribers<Integer> test = new KeyedDidoDataSubscribers<>(apple.getSchema());

        test.onData(1, apple);
        test.onPartial(1, PartialData.from(apple).withIndices(1));
        test.onDelete(1);

        OurDidoSubscriber s1 = new OurDidoSubscriber();

        QuietlyCloseable close1 = test.addSubscriber(s1);

        DidoData orange = DidoData.of("Orange");

        test.onData(2, orange);
        test.onPartial(2, PartialData.from(orange).withIndices(1));
        test.onDelete(2);

        assertThat(s1.results, contains("onData: 2={[1:f_1]=Orange}", "onPartial: 2={[1:f_1]=Orange}", "onDelete: 2"));

        s1.results.clear();

        OurDidoSubscriber s2 = new OurDidoSubscriber();

        QuietlyCloseable close2 = test.addSubscriber(s2);

        close1.close();

        close2.close();
    }
}