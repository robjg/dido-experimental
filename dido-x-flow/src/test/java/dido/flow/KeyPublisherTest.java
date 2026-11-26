package dido.flow;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

class KeyPublisherTest {

    @Test
    void ofList() {

        class Results implements KeyConsumer<Number> {
            boolean complete;
            List<Number> keys = new ArrayList<>();

            @Override
            public void onKey(Number key) {
                keys.add(key);
            }

            @Override
            public void snapshotComplete() {
                complete = true;
            }

            @Override
            public void onDelete(Number key) {
                throw new UnsupportedOperationException("Unexpected.");
            }
        };
        Results results = new Results();

        KeyPublisher.of(List.of(1,2,3))
                .subscribeKeys(results)
                .close();

        assertThat(results.complete, is(true));
        assertThat(results.keys, contains(1, 2, 3));
    }
}