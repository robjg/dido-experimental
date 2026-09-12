package dido.platform.util;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

class ConsumerListTest {

    @Test
    void replaceAsExpected() {

        List<String> results = new ArrayList<>();

        AtomicReference<Consumer<? super String>> consumer = new AtomicReference<>();

        Consumer<String> first = s -> results.add("First: " + s);

        ConsumerList.maybeReplace(consumer.get(), first,
                c -> consumer.set(c));

        assertThat(consumer.get(), is(first));

        consumer.get().accept("1");

        assertThat(results, contains("First: 1"));

        results.clear();

        Consumer<String> second = s -> results.add("Second: " + s);

        ConsumerList.maybeReplace(consumer.get(), second,
                c -> consumer.set(c));

        consumer.get().accept("2");

        assertThat(results, contains("First: 2", "Second: 2"));
    }
}