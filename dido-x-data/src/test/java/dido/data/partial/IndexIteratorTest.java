package dido.data.partial;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class IndexIteratorTest {

    @Test
    void simpleNext() {

        IndexIterator it = IndexIterator.of(4, 5, 6);

        assertThat(it.next(), is(4));
        assertThat(it.next(), is(5));
        assertThat(it.next(), is(6));
        assertThat(it.next(), is(0));
    }

    @Test
    void empty() {

        IndexIterator it = IndexIterator.of();

        assertThat(it.next(), is(0));
    }

}