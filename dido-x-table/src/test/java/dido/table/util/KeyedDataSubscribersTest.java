package dido.table.util;

import dido.data.DidoData;
import dido.data.partial.PartialData;
import dido.flow.QuietlyCloseable;
import dido.table.KeyedDataSubscriber;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

class KeyedDataSubscribersTest {

    static class OurSubscriber implements KeyedDataSubscriber<Integer> {

        List<String> results = new ArrayList<>();

        @Override
        public void onData(Integer key, DidoData data) {
            results.add("onData: " + key + "=" + data);
        }

        @Override
        public void onPartial(Integer key, PartialData data) {
            results.add("onPartial: " + key + "=" + data);
        }

        @Override
        public void onDelete(Integer key) {
            results.add("onDelete: " + key);
        }
    }

    @Test
    void replaceAsExpected() {

        KeyedDataSubscribers<Integer> test = new KeyedDataSubscribers<>();

        test.onData(1, DidoData.of("Apple"));
        test.onPartial(1, PartialData.of(DidoData.of("Apple"), 1));
        test.onDelete(1);

        OurSubscriber s1 = new OurSubscriber();

        QuietlyCloseable close1 = test.addSubscriber(s1);

        test.onData(2, DidoData.of("Orange"));
        test.onPartial(2, PartialData.of(DidoData.of("Orange"), 1));
        test.onDelete(2);

        assertThat(s1.results, contains("onData: 2={[1:f_1]=Orange}", "onPartial: 2={[1:f_1]=Orange}", "onDelete: 2"));

        s1.results.clear();

        OurSubscriber s2 = new OurSubscriber();

        QuietlyCloseable close2 = test.addSubscriber(s2);

        close1.close();

        close2.close();
    }
}