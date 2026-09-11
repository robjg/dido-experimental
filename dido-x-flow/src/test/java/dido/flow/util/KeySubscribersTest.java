package dido.flow.util;

import dido.flow.KeyConsumer;
import dido.flow.QuietlyCloseable;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

class KeySubscribersTest {

    static class Results implements KeyConsumer<Object> {

        List<String> results = new ArrayList<>();

        @Override
        public void onInsert(Object key) {
            results.add("Insert: " + key.toString());
        }

        @Override
        public void onDelete(Object key) {
            results.add("Delete: " + key.toString());
        }
    }

    @Test
    void addRemove() {

        KeySubscribers<String> keySubscribers = new KeySubscribers<>();
        assertThat(keySubscribers.hasSubscribers(), is(false));

        keySubscribers.onInsert("A");

        Results results1 = new Results();
        QuietlyCloseable close1 = keySubscribers.addKeySubscriber(results1);

        assertThat(keySubscribers.hasSubscribers(), is(true));

        keySubscribers.onInsert("B");

        Results results2 = new Results();
        QuietlyCloseable close2 = keySubscribers.addKeySubscriber(results2);

        keySubscribers.onInsert("C");

        Results results3 = new Results();
        QuietlyCloseable close3 = keySubscribers.addKeySubscriber(results3);

        keySubscribers.onInsert("D");

        close1.close();

        keySubscribers.onDelete("A");

        close3.close();

        keySubscribers.onDelete("B");

        close2.close();

        assertThat(keySubscribers.hasSubscribers(), is(false));

        keySubscribers.onDelete("C");

        assertThat(results1.results, contains("Insert: B", "Insert: C", "Insert: D"));

        assertThat(results2.results, contains("Insert: C", "Insert: D", "Delete: A", "Delete: B"));

        assertThat(results3.results, contains("Insert: D", "Delete: A"));

    }
}